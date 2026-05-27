import asyncio
import sys
import os
import subprocess
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ean_utils import cargar_config_supermercados

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(RAIZ, "logs")
PRICES_DIR = os.path.join(RAIZ, "data", "prices")
RUTA_LOG = os.path.join(LOG_DIR, "etl.log")
ARCHIVO_CONSOLIDADO = os.path.join(RAIZ, "data", "consolidado_precios.csv")
ARCHIVO_MAESTRA = os.path.join(RAIZ, "data", "tabla_maestra.csv")
MAX_REINTENTOS = 2
DELAY_REINTENTO = 30


def log(mensaje: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ts}] {mensaje}"
    print(linea)
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(RUTA_LOG, "a", encoding="utf-8") as f:
        f.write(linea + "\n")


def obtener_scrapers() -> list[dict]:
    config = cargar_config_supermercados()
    nombre_a_id = {v: k for k, v in config.items()}
    scrapers = []
    mapeo_archivos = {
        "Vea": ("scrapers", "vea", "vea_scraper.py"),
        "ChangoMas": ("scrapers", "chango_mas", "masOnline_scraper.py"),
        "La Banderita": ("scrapers", "labanderita", "laBanderita_scraper.py"),
        "Cooperativa Obrera": ("scrapers", "coope", "coope_scraper.py"),
    }
    for nombre, (rel_carpeta, subcarpeta, archivo) in mapeo_archivos.items():
        ruta = os.path.join(RAIZ, rel_carpeta, subcarpeta, archivo)
        cwd = os.path.join(RAIZ, rel_carpeta, subcarpeta)
        scrapers.append({
            "nombre": nombre,
            "id": nombre_a_id.get(nombre, "??"),
            "ruta": ruta,
            "cwd": cwd,
        })
    return scrapers


async def ejecutar_un_scraper(scraper: dict) -> bool:
    nombre = scraper["nombre"]
    for intento in range(1, MAX_REINTENTOS + 1):
        log(f"  [{nombre}] Intento {intento}/{MAX_REINTENTOS}...")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, scraper["ruta"],
            cwd=scraper["cwd"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode == 0:
            log(f"  [{nombre}] OK")
            return True
        log(f"  [{nombre}] Falló (código {proc.returncode})")
        if stderr:
            for linea in stderr.decode("utf-8", errors="replace").strip().split("\n"):
                log(f"    [{nombre}] stderr: {linea.strip()}")
        if intento < MAX_REINTENTOS:
            log(f"  [{nombre}] Reintentando en {DELAY_REINTENTO}s...")
            await asyncio.sleep(DELAY_REINTENTO)
    log(f"  [{nombre}] Se ignora por hoy")
    return False


async def ejecutar_scrapers() -> dict:
    log("Paso 1/3: Ejecutando scrapers en paralelo...")
    scrapers = obtener_scrapers()
    tareas = [ejecutar_un_scraper(s) for s in scrapers]
    resultados = await asyncio.gather(*tareas)
    exitosos = sum(1 for r in resultados if r)
    for s, ok in zip(scrapers, resultados):
        s["exitoso"] = ok
    log(f"Scrapers: {exitosos}/{len(scrapers)} exitosos")
    return scrapers


def consolidar():
    log("Paso 2/3: Consolidando precios...")
    from consolidar_precios import consolidar, CARPETA_PRECIOS, ARCHIVO_SALIDA
    stats = consolidar()
    log(f"Consolidación: {stats['archivos']} archivos, {stats['registros']} registros")
    return stats


def generar_maestra():
    log("Paso 3/3: Generando tabla maestra...")
    from tabla_maestra import procesar, ARCHIVO_CONSOLIDADO, ARCHIVO_CATALOGO
    if not os.path.exists(ARCHIVO_CONSOLIDADO):
        log("  Error: no existe consolidado_precios.csv, salteando")
        return None
    if not os.path.exists(ARCHIVO_CATALOGO):
        log("  Error: no existe catalogo_productos_normalizado.csv, salteando")
        return None
    stats = procesar()
    log(f"Tabla maestra: {stats['unicos_final']} productos únicos")
    return stats


def limpiar_intermedios():
    import glob
    patron = os.path.join(PRICES_DIR, "precios_*.csv")
    archivos = glob.glob(patron)
    for ruta in archivos:
        try:
            os.remove(ruta)
            log(f"  Eliminado: {os.path.basename(ruta)}")
        except Exception as e:
            log(f"  Error al eliminar {os.path.basename(ruta)}: {e}")
    log(f"Intermedios: {len(archivos)} archivos eliminados")


def mostrar_resumen(scrapers, stats_consolidacion, stats_maestra):
    log("=" * 50)
    log("RESUMEN ETL")
    log("=" * 50)
    for s in scrapers:
        estado = "OK" if s["exitoso"] else "FALLÓ"
        log(f"  {s['nombre']} ({s['id']}): {estado}")
    log("---")
    if stats_consolidacion:
        log(f"Consolidados: {stats_consolidacion['registros']} registros")
    if stats_maestra:
        log(f"Tabla maestra: {stats_maestra['unicos_final']} productos únicos")
        log(f"  Filtrados (fuera de catálogo): {stats_maestra['no_catalogo']}")
    log("=" * 50)
    log("ETL COMPLETADO")


async def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

    log("=" * 50)
    log("INICIO ETL")
    log("=" * 50)

    stats_consolidacion = None
    stats_maestra = None

    scrapers = await ejecutar_scrapers()

    stats_consolidacion = consolidar()

    limpiar_intermedios()

    stats_maestra = generar_maestra()

    mostrar_resumen(scrapers, stats_consolidacion, stats_maestra)


if __name__ == "__main__":
    asyncio.run(main())
