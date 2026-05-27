import asyncio
import sys
import os
import subprocess
from datetime import datetime, timezone

from etl.shared import cargar_config_supermercados
from etl.scrape.vea import VeaScraper
from etl.scrape.chango_mas import ChangoMasScraper
from etl.scrape.coope import CoopeScraper
from etl.scrape.labanderita import LaBanderitaScraper

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(RAIZ, "logs")
PRICES_DIR = os.path.join(RAIZ, "data", "prices")
RUTA_LOG = os.path.join(LOG_DIR, "etl.log")
ARCHIVO_CONSOLIDADO = os.path.join(RAIZ, "data", "consolidado_precios.csv")
ARCHIVO_MAESTRA = os.path.join(RAIZ, "data", "tabla_maestra.csv")
MAX_REINTENTOS = 2
DELAY_REINTENTO = 30

SCRAPER_CLASSES = [
    ("Vea", VeaScraper),
    ("ChangoMas", ChangoMasScraper),
    ("Cooperativa Obrera", CoopeScraper),
    ("La Banderita", LaBanderitaScraper),
]


def log(mensaje: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ts}] {mensaje}"
    print(linea)
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(RUTA_LOG, "a", encoding="utf-8") as f:
        f.write(linea + "\n")


async def ejecutar_un_scraper(nombre: str, cls) -> bool:
    for intento in range(1, MAX_REINTENTOS + 1):
        log(f"  [{nombre}] Intento {intento}/{MAX_REINTENTOS}...")
        try:
            scraper = cls()
            if asyncio.iscoroutinefunction(cls.run):
                await scraper.run()
            else:
                await asyncio.to_thread(scraper.run)
            log(f"  [{nombre}] OK")
            return True
        except Exception as e:
            log(f"  [{nombre}] Falló: {e}")
            if intento < MAX_REINTENTOS:
                log(f"  [{nombre}] Reintentando en {DELAY_REINTENTO}s...")
                await asyncio.sleep(DELAY_REINTENTO)
    log(f"  [{nombre}] Se ignora por hoy")
    return False


async def ejecutar_scrapers() -> dict:
    log("Paso 1/5: Ejecutando scrapers en paralelo...")
    tareas = [ejecutar_un_scraper(n, c) for n, c in SCRAPER_CLASSES]
    resultados = await asyncio.gather(*tareas)
    exitosos = sum(1 for r in resultados if r)
    log(f"Scrapers: {exitosos}/{len(SCRAPER_CLASSES)} exitosos")
    return {n: ok for (n, _), ok in zip(SCRAPER_CLASSES, resultados)}


def consolidar():
    log("Paso 2/5: Consolidando precios...")
    from etl.step_consolidate import consolidar as fn, CARPETA_PRECIOS, ARCHIVO_SALIDA
    stats = fn()
    log(f"Consolidación: {stats['archivos']} archivos, {stats['registros']} registros")
    return stats


def generar_maestra():
    log("Paso 3/5: Generando tabla maestra...")
    from etl.step_master import procesar, ARCHIVO_CONSOLIDADO, ARCHIVO_CATALOGO
    if not os.path.exists(ARCHIVO_CONSOLIDADO):
        log("  Error: no existe consolidado_precios.csv, salteando")
        return None
    if not os.path.exists(ARCHIVO_CATALOGO):
        log("  Error: no existe catalogo_productos.csv, salteando")
        return None
    stats = procesar()
    log(f"Tabla maestra: {stats['unicos_final']} productos únicos")
    return stats


def subir_a_db():
    log("Paso 4/5: Subiendo a base de datos...")
    if not os.path.exists(ARCHIVO_MAESTRA):
        log("  Error: no existe tabla_maestra.csv, salteando")
        return None
    from etl.step_upload import subir
    stats = subir(ARCHIVO_MAESTRA)
    log(f"Subida DB: {stats['insertados']} registros")
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


def mostrar_resumen(resultados_scrapers, stats_consolidacion, stats_maestra, stats_upload):
    log("=" * 50)
    log("RESUMEN ETL")
    log("=" * 50)
    for nombre, ok in resultados_scrapers.items():
        estado = "OK" if ok else "FALLÓ"
        log(f"  {nombre}: {estado}")
    log("---")
    if stats_consolidacion:
        log(f"Consolidados: {stats_consolidacion['registros']} registros")
    if stats_maestra:
        log(f"Tabla maestra: {stats_maestra['unicos_final']} productos únicos")
        log(f"  Filtrados (fuera de catálogo): {stats_maestra['no_catalogo']}")
    if stats_upload:
        log(f"Subida DB: {stats_upload['insertados']} registros")
    log("=" * 50)
    log("ETL COMPLETADO")


async def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

    log("=" * 50)
    log("INICIO ETL")
    log("=" * 50)

    resultados_scrapers = await ejecutar_scrapers()

    stats_consolidacion = consolidar()

    limpiar_intermedios()

    stats_maestra = generar_maestra()

    stats_upload = subir_a_db()

    mostrar_resumen(resultados_scrapers, stats_consolidacion, stats_maestra, stats_upload)


if __name__ == "__main__":
    asyncio.run(main())
