# app.py
from flask import Flask, render_template, jsonify, send_file
from ftplib import FTP
import pandas as pd
import re
from datetime import datetime, timedelta
import os
import logging
from io import BytesIO
import pytz
import time
from functools import wraps

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

# Configuración de caché - AUMENTADO para mejor rendimiento
CACHE_TTL_SEGUNDOS = 300  # 5 minutos en lugar de 60 segundos
ULTIMA_ACTUALIZACION = None

# ============================================================
#  OPTIMIZACIÓN 1: Leer Excel UNA SOLA VEZ al arrancar la app
# ============================================================
def cargar_excel():
    """Carga el archivo Excel una sola vez al iniciar la aplicación"""
    try:
        df_base = pd.read_excel(ARCHIVO_LISTA)
        df2 = df_base.iloc[:, :2].copy()
        df2.columns = ["RENIPRES", "E.S"]
        df2 = df2.dropna()
        df2["RENIPRES"] = df2["RENIPRES"].astype(int)

        df3 = df_base.iloc[:, :3].copy()
        df3.columns = ["RENIPRES", "E.S", "RIS"]
        df3 = df3.dropna()
        df3["RENIPRES"] = df3["RENIPRES"].astype(int)
        
        logger.info(f"Excel cargado exitosamente: {len(df2)} establecimientos")
        return df2, df3
    except Exception as e:
        logger.error(f"Error al cargar Excel: {e}")
        # Retornar DataFrames vacíos como fallback
        return pd.DataFrame(), pd.DataFrame()

DF_2COL, DF_3COL = cargar_excel()

def conectar_ftp():
    """Establece conexión FTP con timeout mejorado"""
    try:
        ftp = FTP(FTP_HOST, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        logger.info("FTP conectado exitosamente")
        return ftp
    except Exception as e:
        logger.error(f"Error FTP: {e}")
        raise

# ============================================================
#  OPTIMIZACIÓN 2: Caché FTP mejorado con control de tiempo
# ============================================================
_cache_ftp = {
    "datos": None,
    "timestamp": None,
    "mes": None,
    "fecha_consulta": None,
}

def obtener_archivos_ftp_cached(force_refresh=False, mes=None):
    """
    Retorna dict {renipres: fecha_lima} de archivos.
    Usa caché para no re-consultar el FTP en cada request.
    
    Args:
        force_refresh: Si es True, fuerza una nueva consulta al FTP
        mes: Mes específico a consultar (opcional)
    """
    tz_lima = pytz.timezone("America/Lima")
    hoy = datetime.now(tz_lima).date()
    
    if mes is None:
        mes = datetime.now().strftime("%m")
    
    ahora = datetime.now()
    cache = _cache_ftp
    
    # Verificar si debemos usar el caché
    usar_cache = (
        not force_refresh and
        cache["datos"] is not None and 
        cache["timestamp"] is not None and
        cache["mes"] == mes and
        (ahora - cache["timestamp"]).total_seconds() < CACHE_TTL_SEGUNDOS
    )
    
    if usar_cache:
        logger.info(f"✅ Usando caché FTP (expira en {CACHE_TTL_SEGUNDOS - (ahora - cache['timestamp']).total_seconds():.0f} segundos)")
        return cache["datos"], mes
    
    # Si llegamos aquí, necesitamos consultar el FTP
    logger.info("🔄 Consultando FTP (caché expirado o forzado)")
    
    patron = re.compile(r'^RAtenDet-(\d+)\.xlsx$')
    dic_archivos = {}
    
    try:
        ftp = conectar_ftp()
        
        # Intentar navegar al directorio del mes
        try:
            ftp.cwd(f"{BASE_PATH}/{mes}")
            logger.info(f"Accediendo a /{BASE_PATH}/{mes}")
        except Exception as e:
            logger.warning(f"No se pudo acceder a carpeta {mes}: {e}")
            # Intentar con mes alternativo
            mes_alt = datetime.now().strftime("%m")
            if mes_alt != mes:
                try:
                    ftp.cwd(f"{BASE_PATH}/{mes_alt}")
                    mes = mes_alt
                    logger.info(f"Usando carpeta alternativa: {mes}")
                except:
                    ftp.quit()
                    return {}, mes
        
        # Usar MLSD si está disponible
        try:
            logger.info("Usando MLSD para listar archivos")
            for nombre, facts in ftp.mlsd(facts=["modify"]):
                m = patron.match(nombre)
                if not m:
                    continue
                
                modify = facts.get("modify", "")
                if not modify:
                    continue
                
                ren = int(m.group(1))
                fecha_utc = datetime.strptime(modify, "%Y%m%d%H%M%S")
                fecha_lima = pytz.utc.localize(fecha_utc).astimezone(tz_lima)
                
                if fecha_lima.date() == hoy:
                    dic_archivos[ren] = {
                        "fecha": fecha_lima,
                        "archivo": nombre,
                    }
            
            logger.info(f"MLSD exitoso: {len(dic_archivos)} archivos encontrados")
            
        except Exception as e:
            logger.warning(f"MLSD falló ({e}), usando MDTM como fallback")
            dic_archivos = _fallback_mdtm(ftp, mes, hoy, tz_lima, patron)
        
        ftp.quit()
        
    except Exception as e:
        logger.error(f"Error crítico en consulta FTP: {e}")
        # En caso de error, retornar datos anteriores si existen
        if cache["datos"] is not None:
            logger.warning("⚠️ Usando caché anterior debido a error FTP")
            return cache["datos"], cache["mes"]
        return {}, mes
    
    # Guardar en caché
    _cache_ftp["datos"] = dic_archivos
    _cache_ftp["timestamp"] = ahora
    _cache_ftp["mes"] = mes
    _cache_ftp["fecha_consulta"] = datetime.now(tz_lima)
    
    logger.info(f"✅ Caché actualizado: {len(dic_archivos)} archivos de hoy")
    return dic_archivos, mes

def _fallback_mdtm(ftp, mes, hoy, tz_lima, patron):
    """Fallback por si el servidor FTP no soporta MLSD."""
    try:
        archivos = ftp.nlst()
    except:
        return {}
    
    dic = {}
    archivos_filtrados = [a for a in archivos if a.startswith("RAtenDet-")]
    
    for arch in archivos_filtrados:
        m = patron.match(arch)
        if not m:
            continue
        try:
            info = ftp.sendcmd(f"MDTM {arch}")
            fecha_str = info.replace("213 ", "")
            fecha_utc = datetime.strptime(fecha_str, "%Y%m%d%H%M%S")
            fecha_lima = pytz.utc.localize(fecha_utc).astimezone(tz_lima)
            if fecha_lima.date() == hoy:
                ren = int(m.group(1))
                dic[ren] = {"fecha": fecha_lima, "archivo": arch}
        except Exception as e:
            logger.debug(f"Error MDTM para {arch}: {e}")
            continue
    
    logger.info(f"MDTM fallback: {len(dic)} archivos encontrados")
    return dic

# ============================================================
#  Lógica de negocio con caché
# ============================================================

def procesar_datos(dic_archivos):
    """Procesa los datos con los archivos encontrados"""
    if DF_2COL.empty:
        return {
            "total": 0,
            "encontrados": 0,
            "faltantes": [],
            "faltantes_count": 0,
            "extras": [],
            "porcentaje": 0,
        }
    
    df = DF_2COL
    encontrados = set(dic_archivos.keys())
    faltantes = df[~df["RENIPRES"].isin(encontrados)]
    renipres_set = set(df["RENIPRES"].tolist())
    extras = [x for x in encontrados if x not in renipres_set]
    total = len(df)
    porcentaje = round((len(encontrados) / total) * 100, 2) if total else 0
    
    return {
        "total": total,
        "encontrados": len(encontrados),
        "faltantes": faltantes.to_dict(orient="records"),
        "faltantes_count": len(faltantes),
        "extras": extras,
        "porcentaje": porcentaje,
    }

def obtener_detalle_establecimientos(dic_archivos):
    """Genera detalle de establecimientos con estado de carga"""
    if DF_3COL.empty:
        return []
    
    detalle = []
    for _, row in DF_3COL.iterrows():
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
            "ARCHIVOS": archivo,
        })
    return detalle

# ============================================================
#  Rutas Flask optimizadas
# ============================================================

@app.route("/")
def index():
    """Página principal del dashboard"""
    mes = datetime.now().strftime("%m")
    
    try:
        # Obtener datos del caché (sin forzar refresh)
        dic_archivos, mes = obtener_archivos_ftp_cached(force_refresh=False)
        datos = procesar_datos(dic_archivos)
        
        # Obtener fecha de última actualización
        ultima_act = _cache_ftp["fecha_consulta"]
        if ultima_act:
            ultima_act_str = ultima_act.strftime("%d/%m/%Y %H:%M:%S")
        else:
            ultima_act_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
    except Exception as e:
        logger.error(f"Error en /: {e}")
        dic_archivos = {}
        datos = {
            "total": len(DF_2COL) if not DF_2COL.empty else 0,
            "encontrados": 0, 
            "faltantes": [], 
            "faltantes_count": 0, 
            "extras": [], 
            "porcentaje": 0
        }
        ultima_act_str = "Error en la consulta"
    
    return render_template(
        "dashboard.html",
        **datos,
        mes_actual=mes,
        ultima_actualizacion=ultima_act_str,
        fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    )

@app.route("/establecimientos")
def ver_establecimientos():
    """Página de listado de establecimientos"""
    try:
        # Usar caché sin forzar refresh
        dic_archivos, _ = obtener_archivos_ftp_cached(force_refresh=False)
        detalle = obtener_detalle_establecimientos(dic_archivos)
    except Exception as e:
        logger.error(f"Error en /establecimientos: {e}")
        detalle = []
    
    return render_template("establecimientos.html", detalle=detalle)

@app.route("/api/refresh")
def api_refresh():
    """Endpoint para forzar actualización de datos desde FTP"""
    try:
        logger.info("🔄 Forzando actualización de datos FTP")
        inicio = time.time()
        
        dic_archivos, mes = obtener_archivos_ftp_cached(force_refresh=True)
        datos = procesar_datos(dic_archivos)
        
        tiempo = round(time.time() - inicio, 2)
        
        return jsonify({
            "success": True,
            "message": f"Datos actualizados en {tiempo} segundos",
            "data": datos,
            "tiempo": tiempo
        })
    except Exception as e:
        logger.error(f"Error en refresh: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/status")
def api_status():
    """Endpoint para verificar estado del caché"""
    cache = _cache_ftp
    ahora = datetime.now()
    
    if cache["timestamp"]:
        edad = (ahora - cache["timestamp"]).total_seconds()
        expira_en = max(0, CACHE_TTL_SEGUNDOS - edad)
    else:
        edad = None
        expira_en = None
    
    return jsonify({
        "cache_activo": cache["datos"] is not None,
        "edad_cache": f"{edad:.0f} segundos" if edad else "No disponible",
        "expira_en": f"{expira_en:.0f} segundos" if expira_en else "No disponible",
        "mes_actual": cache["mes"],
        "archivos_en_cache": len(cache["datos"]) if cache["datos"] else 0,
        "ttl_configurado": CACHE_TTL_SEGUNDOS,
        "excel_cargado": not DF_2COL.empty,
        "total_establecimientos": len(DF_2COL) if not DF_2COL.empty else 0
    })

@app.route("/cache/invalidar")
def invalidar_cache():
    """Endpoint para forzar re-consulta del FTP"""
    _cache_ftp["datos"] = None
    _cache_ftp["timestamp"] = None
    _cache_ftp["fecha_consulta"] = None
    logger.info("Caché invalidado manualmente")
    return jsonify({
        "ok": True, 
        "mensaje": "Caché invalidado. La próxima consulta refrescará los datos del FTP"
    })

@app.route("/health")
def health_check():
    """Endpoint para health check en Render.com"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(_cache_ftp["datos"]) if _cache_ftp["datos"] else 0
    })

# ============================================================
#  Inicio de la aplicación
# ============================================================

if __name__ == "__main__":
    import threading
    import webbrowser
    
    # Cargar caché inicial en segundo plano para no bloquear el inicio
    def cargar_cache_inicial():
        """Carga el caché inicial después de que la app inicie"""
        time.sleep(2)  # Esperar a que la app inicie
        try:
            logger.info("🔄 Cargando caché inicial en segundo plano...")
            obtener_archivos_ftp_cached(force_refresh=True)
            logger.info("✅ Caché inicial cargado correctamente")
        except Exception as e:
            logger.error(f"❌ Error cargando caché inicial: {e}")
    
    # Iniciar carga de caché en segundo plano si no estamos en modo debug
    if not os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Thread(target=cargar_cache_inicial, daemon=True).start()
    
    url = "http://127.0.0.1:5000"
    
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    logger.info(f"Iniciando servidor en {url}")
    logger.info(f"TTL de caché configurado: {CACHE_TTL_SEGUNDOS} segundos")
    
    # Puerto para Render.com
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
