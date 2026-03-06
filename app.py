from flask import Flask, render_template, jsonify, send_file
from ftplib import FTP
import pandas as pd
import re
from datetime import datetime, timedelta
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

# ============================================================
#  OPTIMIZACIÓN 1: Leer Excel UNA SOLA VEZ al arrancar la app
# ============================================================
def cargar_excel():
    df_base = pd.read_excel(ARCHIVO_LISTA)
    df2 = df_base.iloc[:, :2].copy()
    df2.columns = ["RENIPRES", "E.S"]
    df2 = df2.dropna()
    df2["RENIPRES"] = df2["RENIPRES"].astype(int)

    df3 = df_base.iloc[:, :3].copy()
    df3.columns = ["RENIPRES", "E.S", "RIS"]
    df3 = df3.dropna()
    df3["RENIPRES"] = df3["RENIPRES"].astype(int)

    return df2, df3

DF_2COL, DF_3COL = cargar_excel()
logger.info(f"Excel cargado: {len(DF_2COL)} establecimientos")


def conectar_ftp():
    try:
        ftp = FTP(FTP_HOST, timeout=20)
        ftp.login(FTP_USER, FTP_PASS)
        logger.info("FTP conectado")
        return ftp
    except Exception as e:
        logger.error(f"Error FTP: {e}")
        raise


# ============================================================
#  OPTIMIZACIÓN 2: Caché FTP con TTL de 60 segundos
#  Evita reconectar al FTP en cada request del navegador
# ============================================================
_cache_ftp = {
    "datos": None,
    "timestamp": None,
    "mes": None,
}
CACHE_TTL_SEGUNDOS = 60  # Ajustá según necesidad


def obtener_archivos_ftp_cached(mes=None):
    """
    Retorna dict {renipres: fecha_lima} de archivos de HOY.
    Usa caché para no re-consultar el FTP en cada request.
    """
    tz_lima = pytz.timezone("America/Lima")
    hoy = datetime.now(tz_lima).date()

    if mes is None:
        mes = datetime.now().strftime("%m")

    ahora = datetime.now()
    cache = _cache_ftp

    # ¿Hay datos válidos en caché?
    if (
        cache["datos"] is not None
        and cache["timestamp"] is not None
        and cache["mes"] == mes
        and (ahora - cache["timestamp"]).total_seconds() < CACHE_TTL_SEGUNDOS
    ):
        logger.info("Usando caché FTP")
        return cache["datos"], mes

    # ============================================================
    #  OPTIMIZACIÓN 3: mlsd() trae nombre + fecha en UNA sola
    #  llamada al FTP. Antes: 1 llamada MDTM × N archivos.
    #  Ahora: 1 sola llamada total. Hasta 100x más rápido.
    # ============================================================
    patron = re.compile(r'^RAtenDet-(\d+)\.xlsx$')
    ftp = conectar_ftp()
    ftp.cwd(f"{BASE_PATH}/{mes}")

    dic_archivos = {}
    try:
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

    except Exception as e:
        # Fallback a MDTM si el servidor no soporta MLSD
        logger.warning(f"mlsd falló ({e}), usando MDTM como fallback")
        dic_archivos = _fallback_mdtm(ftp, mes, hoy, tz_lima, patron)

    ftp.quit()

    # Guardar en caché
    _cache_ftp["datos"] = dic_archivos
    _cache_ftp["timestamp"] = ahora
    _cache_ftp["mes"] = mes

    logger.info(f"FTP consultado: {len(dic_archivos)} archivos de hoy")
    return dic_archivos, mes


def _fallback_mdtm(ftp, mes, hoy, tz_lima, patron):
    """Fallback por si el servidor FTP no soporta MLSD."""
    archivos = [a for a in ftp.nlst() if a.startswith("RAtenDet-")]
    dic = {}
    for arch in archivos:
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
        except:
            continue
    return dic


# ============================================================
#  Lógica de negocio — sin tocar FTP directamente
# ============================================================

def procesar_datos(dic_archivos):
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
    df = DF_3COL
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
            "ARCHIVOS": archivo,
        })
    return detalle


# ============================================================
#  Rutas Flask
# ============================================================

@app.route("/")
def index():
    mes = datetime.now().strftime("%m")
    try:
        dic_archivos, mes = obtener_archivos_ftp_cached()
        datos = procesar_datos(dic_archivos)
    except Exception as e:
        logger.error(f"Error en /: {e}")
        dic_archivos = {}
        datos = {"total": 0, "encontrados": 0, "faltantes": [], "faltantes_count": 0, "extras": [], "porcentaje": 0}

    return render_template(
        "dashboard.html",
        **datos,
        mes_actual=mes,
        fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    )


@app.route("/establecimientos")
def ver_establecimientos():
    try:
        dic_archivos, _ = obtener_archivos_ftp_cached()
        detalle = obtener_detalle_establecimientos(dic_archivos)
    except Exception as e:
        logger.error(f"Error en /establecimientos: {e}")
        detalle = []
    return render_template("establecimientos.html", detalle=detalle)


@app.route("/cache/invalidar")
def invalidar_cache():
    """Endpoint para forzar re-consulta del FTP (útil para debug)."""
    _cache_ftp["datos"] = None
    _cache_ftp["timestamp"] = None
    return jsonify({"ok": True, "mensaje": "Caché invalidado"})


if __name__ == "__main__":
    import threading
    import webbrowser

    url = "http://127.0.0.1:5000"

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    logger.info(f"Iniciando servidor en {url}")
    app.run(debug=True, host='0.0.0.0', port=5000)
