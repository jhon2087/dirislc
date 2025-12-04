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

        patron = re.compile(r'^RAtenDet-(\d+)-')

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
    Obtiene info de establecimientos SOLO con archivos del día actual (optimizado)
    """
    df = pd.read_excel(ARCHIVO_LISTA)
    df = df.iloc[:, :3]
    df.columns = ["RENIPRES", "E.S", "RIS"]
    df = df.dropna()
    df["RENIPRES"] = df["RENIPRES"].astype(int)

    ftp = conectar_ftp()
    mes_actual = datetime.now().strftime("%m")
    ftp.cwd(f"{BASE_PATH}/{mes_actual}")

    archivos = [a for a in ftp.nlst() if a.startswith("RAtenDet-")]

    patron = re.compile(r'^RAtenDet-(\d+)-')
    tz_lima = pytz.timezone("America/Lima")
    hoy = datetime.now(tz_lima).date()

    detalle = []
    mdtm_cache = {}

    for _, row in df.iterrows():
        ren = row["RENIPRES"]
        archivos_es = []
        fecha_carga = None

        for arch in archivos:
            m = patron.match(arch)
            if not m:
                continue

            if int(m.group(1)) != ren:
                continue

            # Cache MDTM (MUY RÁPIDO)
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

            if fecha_arch.date() != hoy:
                continue

            archivos_es.append(arch)
            fecha_carga = fecha_arch

        detalle.append({
            "RIS": row["RIS"],
            "RENIPRES": ren,
            "ESTABLECIMIENTO": row["E.S"],
            "FECHA_CARGA": fecha_carga.strftime("%d/%m/%Y %H:%M:%S") if fecha_carga else "—",
            "ARCHIVOS": ", ".join(archivos_es) if archivos_es else "—"
        })

    ftp.quit()
    return detalle


@app.route("/")
def index():
    archivos, mes = obtener_archivos_ftp()
    datos = procesar_datos(archivos)
    return render_template(
        "dashboard.html",
        **datos,
        mes_actual=mes,
        fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    )


@app.route("/establecimientos")
def ver_establecimientos():
    detalle = obtener_detalle_establecimientos()
    return render_template("establecimientos.html", detalle=detalle)


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
