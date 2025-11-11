from flask import Flask, render_template
from ftplib import FTP
import pandas as pd
import re
from datetime import datetime

app = Flask(__name__)

@app.route("/")
def verificar_ftp():
    # --- CONFIGURACIÓN ---
    FTP_HOST = "pe01-st.hostinglabs.net"   # 👉 Cambia por tu servidor
    FTP_USER = "dirislc@pe02-st.hostinglabs.net"
    FTP_PASS = "+2xaTGHZ7N$w+r*4"
    BASE_PATH = "/archivos"            # carpeta base en el FTP
    archivo_lista = "ver.xlsx" 

    # --- RUTA AUTOMÁTICA ---
    mes_actual = datetime.now().strftime("%m")
    FTP_PATH = f"{BASE_PATH}/{mes_actual}"

    # --- CONEXIÓN FTP ---
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    archivos = ftp.nlst()
    ftp.quit()

    # --- LEE EXCEL ---
    df = pd.read_excel(archivo_lista)
    df = df.iloc[:, :2]
    df.columns = ["RENIPRES", "E.S"]
    df = df.dropna(subset=["RENIPRES"])
    df["RENIPRES"] = df["RENIPRES"].astype(int)
    lista_excel = df["RENIPRES"].tolist()

    # --- BUSCA ARCHIVOS FTP ---
    patron = re.compile(r'^RAtenDet-(\d+)-')
    renipres_en_archivos = [
        int(patron.search(a).group(1))
        for a in archivos if patron.search(a)
    ]

    # --- COMPARA ---
    faltantes = df[~df["RENIPRES"].isin(renipres_en_archivos)]
    extras = [a for a in renipres_en_archivos if a not in lista_excel]

    total = len(lista_excel)
    encontrados = len(renipres_en_archivos)
    faltantes_count = len(faltantes)
    porcentaje = round((encontrados / total) * 100, 2)

    return render_template("dashboard.html",
                           total=total,
                           encontrados=encontrados,
                           faltantes=faltantes.to_dict(orient="records"),
                           faltantes_count=faltantes_count,
                           extras=extras,
                           porcentaje=porcentaje)

if __name__ == "__main__":
    import threading, webbrowser, os

    url = "http://127.0.0.1:5000"

    # Solo abrir navegador en el proceso principal (no en el reloader)
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    app.run(debug=True)
