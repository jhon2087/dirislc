from flask import Flask, render_template, jsonify, send_file
from ftplib import FTP
import pandas as pd
import re
from datetime import datetime
import os
import logging
from io import BytesIO
import json
import pytz

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- CONFIGURACIÓN ---
# Es recomendable usar variables de entorno en producción
FTP_HOST = os.getenv('FTP_HOST', "pe01-st.hostinglabs.net")
FTP_USER = os.getenv('FTP_USER', "dirislc@pe02-st.hostinglabs.net")
FTP_PASS = os.getenv('FTP_PASS', "+2xaTGHZ7N$w+r*4")
BASE_PATH = "/archivos"
ARCHIVO_LISTA = "ver.xlsx"

def conectar_ftp():
    """Establece conexión con el servidor FTP"""
    try:
        ftp = FTP(FTP_HOST, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        logger.info(f"Conexión FTP exitosa a {FTP_HOST}")
        return ftp
    except Exception as e:
        logger.error(f"Error al conectar FTP: {str(e)}")
        raise

def obtener_archivos_ftp(mes=None):
    """Obtiene la lista de archivos del FTP para un mes específico"""
    try:
        if mes is None:
            mes = datetime.now().strftime("%m")
        
        ftp_path = f"{BASE_PATH}/{mes}"
        
        ftp = conectar_ftp()
        ftp.cwd(ftp_path)
        archivos = ftp.nlst()
        ftp.quit()
        
        logger.info(f"Se encontraron {len(archivos)} archivos en {ftp_path}")
        return archivos, mes
    except Exception as e:
        logger.error(f"Error al obtener archivos: {str(e)}")
        raise

def procesar_datos(archivos):
    """Procesa los datos del Excel y compara con archivos FTP"""
    try:
        # Leer Excel
        df = pd.read_excel(ARCHIVO_LISTA)
        df = df.iloc[:, :2]
        df.columns = ["RENIPRES", "E.S"]
        df = df.dropna(subset=["RENIPRES"])
        df["RENIPRES"] = df["RENIPRES"].astype(int)
        
        # Extraer RENIPRES de archivos FTP
        patron = re.compile(r'^RAtenDet-(\d+)-')
        renipres_en_archivos = [
            int(patron.search(a).group(1))
            for a in archivos if patron.search(a)
        ]
        
        # Comparar
        faltantes = df[~df["RENIPRES"].isin(renipres_en_archivos)]
        extras = [a for a in renipres_en_archivos if a not in df["RENIPRES"].tolist()]
        
        # Estadísticas
        total = len(df)
        encontrados = len(renipres_en_archivos)
        faltantes_count = len(faltantes)
        porcentaje = round((encontrados / total) * 100, 2) if total > 0 else 0
        
        return {
            'total': total,
            'encontrados': encontrados,
            'faltantes': faltantes.to_dict(orient="records"),
            'faltantes_count': faltantes_count,
            'extras': extras,
            'porcentaje': porcentaje,
            'df_completo': df
        }
    except Exception as e:
        logger.error(f"Error al procesar datos: {str(e)}")
        raise

def obtener_detalle_establecimientos():
    """
    Construye tabla:
    RIS, RENIPRES, ESTABLECIMIENTO, FECHA_CARGA (real FTP), ARCHIVOS_CARGADOS
    """
    df = pd.read_excel(ARCHIVO_LISTA)
    df = df.iloc[:, :3]
    df.columns = ["RENIPRES", "E.S", "RIS"]
    df = df.dropna()

    df["RENIPRES"] = df["RENIPRES"].astype(int)

    # Conexión FTP
    ftp = conectar_ftp()

    # Ir al mes actual
    mes_actual = datetime.now().strftime("%m")
    ftp.cwd(f"{BASE_PATH}/{mes_actual}")

    archivos = ftp.nlst()

    patron = re.compile(r'^RAtenDet-(\d+)-(.+)$')
    detalle = []

    for _, row in df.iterrows():
        ren = row["RENIPRES"]

        archivos_es = []
        fecha_carga = None

        for arch in archivos:
            m = patron.match(arch)
            if m and int(m.group(1)) == ren:
                archivos_es.append(arch)

                # Obtener fecha real desde FTP
                try:
                    info = ftp.sendcmd(f"MDTM {arch}")  # formato: 213 YYYYMMDDHHMMSS
                    fecha_str = info.replace("213 ", "")

                    # Convertir MDTM → datetime UTC
                    fecha_utc = datetime.strptime(fecha_str, "%Y%m%d%H%M%S")

                    # Convertir a zona horaria Lima
                    tz_utc = pytz.utc
                    tz_lima = pytz.timezone("America/Lima")
                    fecha_lima = tz_utc.localize(fecha_utc).astimezone(tz_lima)

                    fecha_carga = fecha_lima

                except Exception as e:
                    fecha_carga = None

        detalle.append({
            "RIS": row["RIS"],
            "RENIPRES": ren,
            "ESTABLECIMIENTO": row["E.S"],
            "FECHA_CARGA": fecha_carga.strftime("%d/%m/%Y %H:%M:%S") if fecha_carga else "—",
            "ARCHIVOS": ", ".join(archivos_es) if archivos_es else "—"
        })

    ftp.quit()
    return detalle


# 

@app.route("/")
def index():
    """Ruta principal - Dashboard"""
    try:
        archivos, mes = obtener_archivos_ftp()
        datos = procesar_datos(archivos)
        
        return render_template(
            "dashboard.html",
            **datos,
            mes_actual=mes,
            fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        )
    except Exception as e:
        logger.error(f"Error en la ruta principal: {str(e)}")
        return render_template("error.html", error=str(e)), 500
    
# 

@app.route("/mes/<mes>")
def verificar_mes(mes):
    """Verifica archivos de un mes específico"""
    try:
        if not mes.isdigit() or int(mes) < 1 or int(mes) > 12:
            return render_template("error.html", error="Mes inválido"), 400
        
        mes = mes.zfill(2)
        archivos, _ = obtener_archivos_ftp(mes)
        datos = procesar_datos(archivos)
        
        return render_template(
            "dashboard.html",
            **datos,
            mes_actual=mes,
            fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        )
    except Exception as e:
        logger.error(f"Error al verificar mes {mes}: {str(e)}")
        return render_template("error.html", error=str(e)), 500

@app.route("/api/datos")
def api_datos():
    """API JSON para obtener datos sin recargar la página"""
    try:
        archivos, mes = obtener_archivos_ftp()
        datos = procesar_datos(archivos)
        
        # Convertir a formato JSON serializable
        datos_json = {
            'total': datos['total'],
            'encontrados': datos['encontrados'],
            'faltantes': datos['faltantes'],
            'faltantes_count': datos['faltantes_count'],
            'extras': datos['extras'],
            'porcentaje': datos['porcentaje'],
            'mes': mes,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(datos_json)
    except Exception as e:
        logger.error(f"Error en API: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route("/exportar/excel")
def exportar_excel():
    """Exporta los resultados a Excel"""
    try:
        archivos, mes = obtener_archivos_ftp()
        datos = procesar_datos(archivos)
        
        # Crear archivo Excel en memoria
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Hoja 1: Faltantes
            df_faltantes = pd.DataFrame(datos['faltantes'])
            df_faltantes.to_excel(writer, sheet_name='Faltantes', index=False)
            
            # Hoja 2: Extras
            if datos['extras']:
                df_extras = pd.DataFrame({'RENIPRES_Extra': datos['extras']})
                df_extras.to_excel(writer, sheet_name='Extras', index=False)
            
            # Hoja 3: Resumen
            df_resumen = pd.DataFrame({
                'Métrica': ['Total Esperado', 'Encontrados', 'Faltantes', 'Extras', 'Porcentaje'],
                'Valor': [
                    datos['total'],
                    datos['encontrados'],
                    datos['faltantes_count'],
                    len(datos['extras']),
                    f"{datos['porcentaje']}%"
                ]
            })
            df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'reporte_ftp_{mes}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logger.error(f"Error al exportar: {str(e)}")
        return render_template("error.html", error=str(e)), 500


@app.route("/establecimientos")
def ver_establecimientos():
    try:
        detalle = obtener_detalle_establecimientos()
        return render_template(
            "establecimientos.html",
            detalle=detalle
        )
    except Exception as e:
        logger.error(f"Error mostrando tabla de establecimientos: {str(e)}")
        return render_template("error.html", error=str(e)), 500

@app.errorhandler(404)
def not_found(e):
    return render_template("error.html", error="Página no encontrada"), 404

@app.errorhandler(500)
def internal_error(e):
    return render_template("error.html", error="Error interno del servidor"), 500

if __name__ == "__main__":
    import threading
    import webbrowser
    
    url = "http://127.0.0.1:5000"
    
    # Solo abrir navegador en el proceso principal
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    logger.info(f"Iniciando servidor en {url}")
    app.run(debug=True, host='0.0.0.0', port=5000)
