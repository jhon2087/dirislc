# app.py - Versión optimizada
from flask import Flask, render_template, jsonify, send_file
from ftplib import FTP
import pandas as pd
import re
from datetime import datetime, timedelta
import os
import logging
from io import BytesIO
import pytz
import threading
import time

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
# OPTIMIZACIÓN 1: Carga y preprocesamiento de datos
# ============================================================
def cargar_y_preprocesar_excel():
    """Carga el Excel y crea estructuras optimizadas para búsqueda"""
    df_base = pd.read_excel(ARCHIVO_LISTA)
    
    # DataFrame para dashboard (2 columnas)
    df2 = df_base.iloc[:, :2].copy()
    df2.columns = ["RENIPRES", "E.S"]
    df2 = df2.dropna()
    df2["RENIPRES"] = df2["RENIPRES"].astype(int)
    
    # DataFrame para detalle (3 columnas)
    df3 = df_base.iloc[:, :3].copy()
    df3.columns = ["RENIPRES", "E.S", "RIS"]
    df3 = df3.dropna()
    df3["RENIPRES"] = df3["RENIPRES"].astype(int)
    
    # Crear diccionario para búsqueda rápida O(1)
    establecimientos_dict = {}
    for _, row in df3.iterrows():
        establecimientos_dict[row["RENIPRES"]] = {
            "RIS": row["RIS"],
            "ESTABLECIMIENTO": row["E.S"]
        }
    
    # Set de RENIPRES para búsqueda rápida
    renipres_set = set(df2["RENIPRES"].tolist())
    
    return {
        "df2": df2,
        "df3": df3,
        "establecimientos_dict": establecimientos_dict,
        "renipres_set": renipres_set,
        "total": len(df2)
    }

# Cargar datos UNA SOLA VEZ
DATOS = cargar_y_preprocesar_excel()
logger.info(f"Excel cargado: {DATOS['total']} establecimientos")

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
# OPTIMIZACIÓN 2: Caché con actualización programada
# ============================================================
class FTPCache:
    """Cache con actualización en segundo plano"""
    
    def __init__(self, intervalo_minutos=30):
        self.intervalo = intervalo_minutos * 60
        self.datos = {
            "archivos": {},  # {renipres: info}
            "mes": None,
            "timestamp": None
        }
        self.lock = threading.Lock()
        self.actualizando = False
        self.iniciar_hilo_actualizacion()
    
    def iniciar_hilo_actualizacion(self):
        """Inicia hilo que actualiza la caché periódicamente"""
        def actualizar_periodicamente():
            while True:
                try:
                    time.sleep(self.intervalo)
                    self.actualizar_cache()
                except Exception as e:
                    logger.error(f"Error en actualización periódica: {e}")
        
        thread = threading.Thread(target=actualizar_periodicamente, daemon=True)
        thread.start()
        logger.info(f"Hilo de actualización iniciado (cada {self.intervalo//60} minutos)")
        
        # Actualización inicial inmediata
        self.actualizar_cache()
    
    def actualizar_cache(self):
        """Actualiza la caché con datos del FTP"""
        if self.actualizando:
            logger.info("Ya hay una actualización en curso, omitiendo...")
            return
        
        self.actualizando = True
        try:
            logger.info("Iniciando actualización de caché...")
            inicio = time.time()
            
            mes = datetime.now().strftime("%m")
            tz_lima = pytz.timezone("America/Lima")
            hoy = datetime.now(tz_lima).date()
            
            archivos = self._obtener_archivos_ftp(mes, hoy, tz_lima)
            
            with self.lock:
                self.datos["archivos"] = archivos
                self.datos["mes"] = mes
                self.datos["timestamp"] = time.time()
            
            duracion = time.time() - inicio
            logger.info(f"Caché actualizada: {len(archivos)} archivos en {duracion:.2f}s")
            
        except Exception as e:
            logger.error(f"Error actualizando caché: {e}")
        finally:
            self.actualizando = False
    
    def _obtener_archivos_ftp(self, mes, hoy, tz_lima):
        """Obtiene archivos del FTP usando MLSD optimizado"""
        patron = re.compile(r'^RAtenDet-(\d+)\.xlsx$')
        archivos = {}
        
        try:
            ftp = conectar_ftp()
            ftp.cwd(f"{BASE_PATH}/{mes}")
            
            # Usar MLSD para obtener todo en una sola llamada
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
                    archivos[ren] = {
                        "fecha": fecha_lima,
                        "archivo": nombre,
                        "fecha_str": fecha_lima.strftime("%d/%m/%Y %H:%M:%S")
                    }
            
            ftp.quit()
            
        except Exception as e:
            logger.warning(f"MLSD falló ({e}), usando fallback...")
            archivos = self._fallback_mdtm(mes, hoy, tz_lima, patron)
        
        return archivos
    
    def _fallback_mdtm(self, mes, hoy, tz_lima, patron):
        """Fallback usando MDTM"""
        ftp = conectar_ftp()
        ftp.cwd(f"{BASE_PATH}/{mes}")
        archivos = {}
        
        try:
            lista = ftp.nlst()
            archivos_ftp = [a for a in lista if a.startswith("RAtenDet-")]
            
            for arch in archivos_ftp:
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
                        archivos[ren] = {
                            "fecha": fecha_lima,
                            "archivo": arch,
                            "fecha_str": fecha_lima.strftime("%d/%m/%Y %H:%M:%S")
                        }
                except:
                    continue
        finally:
            ftp.quit()
        
        return archivos
    
    def obtener_datos(self):
        """Obtiene los datos de caché (acceso thread-safe)"""
        with self.lock:
            # Si no hay datos, forzar actualización
            if not self.datos["archivos"]:
                logger.info("Caché vacía, forzando actualización...")
                self.actualizar_cache()
            return self.datos["archivos"].copy()

# Inicializar caché con actualización cada 30 minutos
cache_ftp = FTPCache(intervalo_minutos=30)

# ============================================================
# OPTIMIZACIÓN 3: Preprocesamiento de resultados
# ============================================================

def procesar_datos_optimizado(archivos_ftp):
    """Procesa datos usando estructuras precalculadas"""
    encontrados_set = set(archivos_ftp.keys())
    
    # Usar sets para diferencia rápida O(1)
    faltantes_set = DATOS["renipres_set"] - encontrados_set
    extras_set = encontrados_set - DATOS["renipres_set"]
    
    # Generar lista de faltantes con datos del Excel
    faltantes = []
    for ren in faltantes_set:
        faltantes.append({
            "RENIPRES": ren,
            "E.S": DATOS["establecimientos_dict"][ren]["ESTABLECIMIENTO"]
        })
    
    # Ordenar por RENIPRES
    faltantes.sort(key=lambda x: x["RENIPRES"])
    
    return {
        "total": DATOS["total"],
        "encontrados": len(encontrados_set),
        "faltantes": faltantes,
        "faltantes_count": len(faltantes_set),
        "extras": list(extras_set),
        "porcentaje": round((len(encontrados_set) / DATOS["total"]) * 100, 2) if DATOS["total"] else 0,
    }

def obtener_detalle_optimizado(archivos_ftp):
    """Genera detalle usando estructuras precalculadas"""
    detalle = []
    
    # Iterar sobre diccionario precalculado (más rápido que DataFrame)
    for ren, info in DATOS["establecimientos_dict"].items():
        if ren in archivos_ftp:
            fecha_str = archivos_ftp[ren]["fecha_str"]
            archivo = archivos_ftp[ren]["archivo"]
        else:
            fecha_str = "—"
            archivo = "—"
        
        detalle.append({
            "RIS": info["RIS"],
            "RENIPRES": ren,
            "ESTABLECIMIENTO": info["ESTABLECIMIENTO"],
            "FECHA_CARGA": fecha_str,
            "ARCHIVOS": archivo,
        })
    
    # Ordenar por RIS para mejor visualización
    detalle.sort(key=lambda x: (x["RIS"], x["RENIPRES"]))
    
    return detalle

# ============================================================
# Rutas Flask - Rápidas porque usan caché precalculada
# ============================================================

@app.route("/")
def index():
    """Dashboard principal - respuesta casi instantánea"""
    try:
        archivos_ftp = cache_ftp.obtener_datos()
        datos = procesar_datos_optimizado(archivos_ftp)
    except Exception as e:
        logger.error(f"Error en /: {e}")
        datos = {
            "total": DATOS["total"],
            "encontrados": 0,
            "faltantes": [],
            "faltantes_count": 0,
            "extras": [],
            "porcentaje": 0
        }
    
    return render_template(
        "dashboard.html",
        **datos,
        mes_actual=datetime.now().strftime("%m"),
        fecha_consulta=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        ultima_actualizacion=datetime.now().strftime("%Y")
    )

@app.route("/establecimientos")
def ver_establecimientos():
    """Listado de establecimientos - respuesta casi instantánea"""
    try:
        archivos_ftp = cache_ftp.obtener_datos()
        detalle = obtener_detalle_optimizado(archivos_ftp)
    except Exception as e:
        logger.error(f"Error en /establecimientos: {e}")
        detalle = []
    
    return render_template("establecimientos.html", detalle=detalle)

@app.route("/api/datos")
def api_datos():
    """Endpoint JSON para actualización asíncrona (AJAX)"""
    try:
        archivos_ftp = cache_ftp.obtener_datos()
        datos = procesar_datos_optimizado(archivos_ftp)
        return jsonify(datos)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/detalle")
def api_detalle():
    """Endpoint JSON para detalle (AJAX)"""
    try:
        archivos_ftp = cache_ftp.obtener_datos()
        detalle = obtener_detalle_optimizado(archivos_ftp)
        return jsonify(detalle)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cache/forzar")
def forzar_actualizacion():
    """Endpoint para forzar actualización manual"""
    cache_ftp.actualizar_cache()
    return jsonify({
        "ok": True,
        "mensaje": "Actualización de caché iniciada",
        "archivos": len(cache_ftp.datos["archivos"])
    })

@app.route("/cache/estado")
def cache_estado():
    """Ver estado de la caché"""
    with cache_ftp.lock:
        ultima = cache_ftp.datos["timestamp"]
        if ultima:
            hace = time.time() - ultima
            mensaje = f"Última actualización hace {hace//60:.0f} minutos"
        else:
            mensaje = "No actualizada"
        
        return jsonify({
            "archivos_en_cache": len(cache_ftp.datos["archivos"]),
            "mes": cache_ftp.datos["mes"],
            "ultima_actualizacion": mensaje,
            "actualizando": cache_ftp.actualizando
        })

# ============================================================
# Inicio de la aplicación
# ============================================================

if __name__ == "__main__":
    import webbrowser
    
    url = "http://127.0.0.1:5000"
    
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    logger.info(f"Iniciando servidor en {url}")
    
    # Usar producción con múltiples hilos para mejor rendimiento
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
