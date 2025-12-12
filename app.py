from flask import Flask, render_template, jsonify, send_file
from ftplib import FTP
import pandas as pd
import re
from datetime import datetime
import os
import logging
from io import BytesIO
import pytz

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- CONFIG ---
FTP_HOST = os.getenv('FTP_HOST', "pe01-st.hostinglabs.net")
FTP_USER = os.getenv('FTP_USER', "dirislc@pe02-st.hostinglabs.net")
FTP_PASS = os.getenv('FTP_PASS', "+2xaTGHZ7N$w+r*4")
BASE_PATH = "/archivos"
ARCHIVO_LISTA = "ver.xlsx"


def conectar_ftp():
    try:
        ftp = FTP(FTP_HOST, timeout=20)
        ftp.login(FTP_USER, FTP_PASS)
        logger.info("FTP conectado")
        return ftp
    except Exception as e:
        logger.error(f"Error FTP: {e}")
        raise


# ===========================
#   OPTIMIZADO PARA VELOCIDAD
# ===========================

def obtener_archivos_ftp(mes=None):
    """
    Retorna SOLO archivos del día actual,
    de forma optimizada (rápida).
    """
    try:
        tz_lima = pytz.timezone("America/Lima")
        hoy = datetime.now(tz_lima).date()

        if mes is None:
            mes = datetime.now().strftime("%m")

        ftp = conectar_ftp()
        ftp.cwd(f"{BASE_PATH}/{mes}")

        # 🔥 OPTIMIZACIÓN 1:
        # Filtrar archivos ANTES de consultar MDTM
        archivos = [a for a in ftp.nlst() if a.startswith("RAtenDet-")]

        archivos_hoy = []
        mdtm_cache = {}

        for arch in archivos:

            # 🔥 OPTIMIZACIÓN 2: usar cache si ya se obtuvo MDTM
            if arch in mdtm_cache:
                fecha_lima = mdtm_cache[arch]
            else:
                try:
                    info = ftp.sendcmd(f"MDTM {arch}")
                    fecha_str = info.replace("213 ", "")
                    fecha_utc = datetime.strptime(fecha_str, "%Y%m%d%H%M%S")

                    fecha_lima = pytz.utc.localize(fecha_utc).astimezone(tz_lima)
                    mdtm_cache[arch] = fecha_lima

                except:
                    continue

            # Solo archivos de HOY
            if fecha_lima.date() == hoy:
                archivos_hoy.append(arch)

        ftp.quit()
        return archivos_hoy, mes

    except Exception as e:
        logger.error(f"Error obteniendo archivos optimizados: {e}")
        raise


def procesar_datos(archivos):
    """
    Compara RENIPRES vs archivos del día actual (optimizado)
    """
    try:
        df = pd.read_excel(ARCHIVO_LISTA)
        df = df.iloc[:, :2]
        df.columns = ["RENIPRES", "E.S"]
        df = df.dropna()
        df["RENIPRES"] = df["RENIPRES"].astype(int)

        patron = re.compile(r'^RAtenDet-(\d+).xlsx')

        encontrados = set()
        for a in archivos:
            m = patron.match(a)
            if m:
                encontrados.add(int(m.group(1)))

        faltantes = df[~df["RENIPRES"].isin(encontrados)]
        extras = [x for x in encontrados if x not in df["RENIPRES"].tolist()]

        total = len(df)
        porcentaje = round((len(encontrados) / total) * 100, 2)

        return {
            "total": total,
            "encontrados": len(encontrados),
            "faltantes": faltantes.to_dict(orient="records"),
            "faltantes_count": len(faltantes),
            "extras": extras,
            "porcentaje": porcentaje
        }

    except Exception as e:
        logger.error(f"Error en procesamiento: {e}")
        raise


def obtener_detalle_establecimientos():
    """
    Versión ULTRA optimizada:
    - nlst solo 1 vez
    - MDTM solo 1 vez por archivo
    - Diccionario RENIPRES → info
    - Cruce instantáneo con Excel
    """
    try:
        tz_lima = pytz.timezone("America/Lima")
        hoy = datetime.now(tz_lima).date()

        # Leer Excel
        df = pd.read_excel(ARCHIVO_LISTA)
        df = df.iloc[:, :3]
        df.columns = ["RENIPRES", "E.S", "RIS"]
        df = df.dropna()
        df["RENIPRES"] = df["RENIPRES"].astype(int)

        # FTP
        ftp = conectar_ftp()
        mes_actual = datetime.now().strftime("%m")
        ftp.cwd(f"{BASE_PATH}/{mes_actual}")

        # 🔥 Obtener solo archivos RAtenDet- y MDTM una sola vez
        archivos = ftp.nlst()
        archivos = [a for a in archivos if a.startswith("RAtenDet-")]

        patron = re.compile(r'^RAtenDet-(\d+).xlsx')
        mdtm_cache = {}

        # 🔥 Diccionario renipres → (fecha, archivo)
        dic_archivos = {}

        for arch in archivos:
            m = patron.match(arch)
            if not m:
                continue

            ren = int(m.group(1))

            # MDTM solo una vez
            if arch not in mdtm_cache:
                try:
                    info = ftp.sendcmd(f"MDTM {arch}")
                    fecha_str = info.replace("213 ", "")
                    fecha_utc = datetime.strptime(fecha_str, "%Y%m%d%H%M%S")
                    fecha_lima = pytz.utc.localize(fecha_utc).astimezone(tz_lima)
                    mdtm_cache[arch] = fecha_lima
                except:
                    continue

            fecha_arch = mdtm_cache[arch]

            # Solo archivos de HOY
            if fecha_arch.date() != hoy:
                continue

            # Guardar en diccionario
            dic_archivos[ren] = {
                "fecha": fecha_arch,
                "archivo": arch
            }

        ftp.quit()

        # 🔥 Armar tabla final MUY RÁPIDO
        detalle = []
        for _, row in df.iterrows():
            ren = row["RENIPRES"]

            if ren in dic_archivos:
                fecha = dic_archivos[ren]["fecha"]
                archivo = dic_archivos[ren]["archivo"]
                fecha_fmt = fecha.strftime("%d/%m/%Y %H:%M:%S")
            else:
                fecha_fmt = "—"
                archivo = "—"

            detalle.append({
                "RIS": row["RIS"],
                "RENIPRES": ren,
                "ESTABLECIMIENTO": row["E.S"],
                "FECHA_CARGA": fecha_fmt,
                "ARCHIVOS": archivo
            })

        return detalle

    except Exception as e:
        logger.error(f"Error optimizando establecimientos: {e}")
        raise



@app.route("/")
def index():
    try:
        archivos, mes = obtener_archivos_ftp()
        datos = procesar_datos(archivos)
    except:
        archivos = []
        datos = {"total": 0, "encontrados": 0, "faltantes": [], "extras": [], "porcentaje": 0}

    return render_template(
        "dashboard.html",
        **datos,
        mes_actual=mes if archivos else "--",
        fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    )



@app.route("/establecimientos")
def ver_establecimientos():
    detalle = obtener_detalle_establecimientos()
    return render_template("establecimientos.html", detalle=detalle)


if __name__ == "__main__":
    import threading
    import webbrowser
    
    url = "http://127.0.0.1:5000"
    
    # Solo abrir navegador en el proceso principal
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    logger.info(f"Iniciando servidor en {url}")
    app.run(debug=True, host='0.0.0.0', port=5000)


