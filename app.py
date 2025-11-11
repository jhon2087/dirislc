
from flask import Flask, render_template, jsonify
from ftplib import FTP
import pandas as pd
import re, os
from datetime import datetime

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/api")
def api():
    FTP_HOST="pe01-st.hostinglabs.net"
    FTP_USER="dirislc@pe02-st.hostinglabs.net"
    FTP_PASS="+2xaTGHZ7N$w+r*4"
    BASE="/archivos"

    month=datetime.now().strftime("%m")
    path=f"{BASE}/{month}"

    renipres=[]
    try:
        ftp=FTP(FTP_HOST)
        ftp.login(FTP_USER,FTP_PASS)
        ftp.cwd(path)
        files=ftp.nlst()
        ftp.quit()

        pattern=re.compile(r"^RAtenDet-(\d{8})-")
        for f in files:
            m=pattern.search(f)
            if m:
                n=int(m.group(1).lstrip("0") or "0")
                renipres.append(n)

    except:
        files=[]
        renipres=[]

    if not os.path.exists("ver.xlsx"):
        return jsonify({
            "total":0,"encontrados":len(renipres),"faltantes":[],
            "faltantes_count":0,"porcentaje":0,
            "ftp_folder":path,
            "timestamp":datetime.now().isoformat()
        })

    df=pd.read_excel("ver.xlsx")
    df=df.iloc[:,0:2]
    df.columns=["RENIPRES","ES"]
    df=df.dropna(subset=["RENIPRES"])
    df["RENIPRES"]=df["RENIPRES"].astype(int)
    excel=df["RENIPRES"].tolist()

    falt=df[~df["RENIPRES"].isin(renipres)]
    total=len(excel)
    encontrados=len(renipres)
    falt_count=len(falt)
    por=round((encontrados/total)*100,2) if total else 0

    return jsonify({
        "total":total,
        "encontrados":encontrados,
        "faltantes":falt.to_dict(orient="records"),
        "faltantes_count":falt_count,
        "porcentaje":por,
        "ftp_folder":path,
        "timestamp":datetime.now().isoformat()
    })

if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000)
