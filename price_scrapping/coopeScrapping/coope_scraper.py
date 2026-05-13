"""
Scraper - La Coope en Casa
URL base: https://www.lacoopeencasa.coop/listado/categoria/{categoria}/pagina--{n}

Extrae: EAN, precio
Salida:  resultados/precios_lacoope_YYYYMMDD_HHMMSS.csv

Requisitos:
    pip install playwright beautifulsoup4
    python -m playwright install chromium
"""

import os
import csv
import re
import time
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

# ── Configuraciones Generales ──────────────────────────────────────────────────

ARCHIVO_MAPEO = "mapeo_codigo_coope.csv" # Cambia esto por el nombre real de tu archivo de mapeo
CARPETA_SALIDA = "resultados"

# ── Tiempos configurables (en milisegundos salvo DELAY_BETWEEN) ────────────────

SCROLL_STEP_PX        = 600    # pixeles por paso de scroll
SCROLL_STEP_WAIT_MS   = 400    # espera tras cada paso de scroll
SCROLL_PASS_WAIT_MS   = 1000   # espera al terminar una pasada completa
PAGE_LOAD_WAIT_MS     = 2000   # espera extra tras networkidle
PAGE_TIMEOUT_MS       = 30000  # timeout maximo de carga de pagina
DELAY_BETWEEN_PAGES   = 1.0    # segundos entre requests (float)
MAX_RETRIES           = 3      # reintentos ante timeout

# ── Categorias ─────────────────────────────────────────────────────────────────

BASE_URL = "https://www.lacoopeencasa.coop/listado/categoria/{categoria}/pagina--{pagina}"

CATEGORIAS = {
    "almacen":       "almacen/2",
    "frescos":       "frescos/3",
    "bebidas":       "bebidas/4",
    "perfumeria":    "perfumeria/5",
    "limpieza":      "limpieza/6",
    "casa-y-jardin": "casa-y-jardin/7",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def cargar_mapeo_csv(ruta_archivo: str) -> dict:
    """
    Lee el CSV de mapeo y devuelve un diccionario {codigo_interno: ean}.
    """
    mapeo = {}
    try:
        with open(ruta_archivo, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for fila in reader:
                ean = fila['ean'].strip()
                codigo_int = fila['codigo_interno_coope'].strip()
                mapeo[codigo_int] = ean
        print(f"Mapeo cargado con {len(mapeo)} codigos.")
    except FileNotFoundError:
        print(f"[!] Error: No se encontro el archivo de mapeo '{ruta_archivo}'.")
        sys.exit(1)
    return mapeo

def scroll_to_bottom(page) -> None:
    """
    Scrollea paso a paso para que el lazy loading de Angular renderice
    cada producto a medida que entra al viewport.
    """
    prev_height = -1
    while True:
        total_height = page.evaluate("document.body.scrollHeight")
        position = 0
        while position < total_height:
            position += SCROLL_STEP_PX
            page.evaluate(f"window.scrollTo(0, {position})")
            page.wait_for_timeout(SCROLL_STEP_WAIT_MS)
            total_height = page.evaluate("document.body.scrollHeight")

        page.wait_for_timeout(SCROLL_PASS_WAIT_MS)
        new_height = page.evaluate("document.body.scrollHeight")

        if new_height == prev_height:
            break
        prev_height = new_height

def has_next_page(soup: BeautifulSoup) -> bool:
    """
    Devuelve True si hay una pagina siguiente.
    """
    ul = soup.find("ul", class_="pagination")
    if not ul:
        return False
    return ul.find("use", href=re.compile(r"derecha")) is not None

def parse_products(soup: BeautifulSoup, mapeo_codigos: dict) -> list[dict]:
    """
    Extrae productos del HTML renderizado.
    Filtra usando el diccionario de mapeo. Solo devuelve ean y precio.
    """
    productos = []
    for tarjeta in soup.find_all(id=re.compile(r"^tarjeta-articulo-\d+$")):
        m = re.search(r"tarjeta-articulo-(\d+)", tarjeta["id"])
        if not m:
            continue
        
        codigo_interno = m.group(1)
        
        # Filtro vital: Si el codigo no esta en nuestro mapeo, lo ignoramos y pasamos al siguiente
        ean = mapeo_codigos.get(codigo_interno)
        if not ean:
            continue

        entero_tag  = tarjeta.find(class_="precio-entero")
        decimal_tag = tarjeta.find(class_="precio-decimal")

        if entero_tag:
            entero  = entero_tag.get_text(strip=True).replace("$", "").strip()
            decimal = decimal_tag.get_text(strip=True) if decimal_tag else "00"
            precio  = f"{entero},{decimal}"
        else:
            precio = ""

        # Solo adjuntamos EAN y Precio
        productos.append({"ean": ean, "precio": precio})
        
    return productos

def fetch_fresh_page(browser, url: str) -> str | None:
    """
    Abre una pagina nueva (contexto fresco) para evitar que Angular acumule el DOM.
    """
    for intento in range(1, MAX_RETRIES + 1):
        page = browser.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
            page.wait_for_timeout(PAGE_LOAD_WAIT_MS)
            scroll_to_bottom(page)
            html = page.content()
            return html
        except PlaywrightTimeout:
            print(f"  [!] Timeout (intento {intento}/{MAX_RETRIES}): {url}")
            if intento == MAX_RETRIES:
                return None
            time.sleep(2)
        finally:
            page.close()
    return None

# ── Scraper principal ──────────────────────────────────────────────────────────

def scrape_all(mapeo_codigos: dict):
    timestamp = datetime.now()
    print(f"=== Inicio scraping: {timestamp.strftime('%Y-%m-%d %H:%M:%S')} ===\n")

    todos = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )

        for nombre_cat, slug in CATEGORIAS.items():
            print(f"-- Categoria: {nombre_cat}")

            url_p1 = BASE_URL.format(categoria=slug, pagina=1)
            html = fetch_fresh_page(context, url_p1)
            if not html:
                print(f"  [!] No se pudo cargar pagina 1. Saltando.\n")
                continue

            soup   = BeautifulSoup(html, "html.parser")
            # Pasamos el diccionario de mapeo a la funcion de parseo
            prods  = parse_products(soup, mapeo_codigos)
            print(f"  Pagina 1 -> {len(prods)} productos mapeados y extraidos")
            todos.extend(prods)

            n = 2
            while has_next_page(soup):
                time.sleep(DELAY_BETWEEN_PAGES)
                url_pn = BASE_URL.format(categoria=slug, pagina=n)
                html_n = fetch_fresh_page(context, url_pn)
                if not html_n:
                    print(f"  [!] No se pudo cargar pagina {n}. Omitiendo.")
                    break
                
                soup    = BeautifulSoup(html_n, "html.parser")
                prods_n = parse_products(soup, mapeo_codigos)
                print(f"  Pagina {n} -> {len(prods_n)} productos mapeados y extraidos")
                todos.extend(prods_n)
                n += 1

            print()

        context.close()
        browser.close()

    print(f"Total productos scrapeados con exito: {len(todos)}\n")
    return todos, timestamp

# ── Guardar CSV ────────────────────────────────────────────────────────────────

def guardar_csv(productos: list[dict], timestamp: datetime) -> str:
    # Asegurarse de que la subcarpeta exista
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    
    nombre_archivo = f"precios_lacoope_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    ruta_completa = os.path.join(CARPETA_SALIDA, nombre_archivo)
    
    with open(ruta_completa, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Encabezados limpios, sin timestamp dentro del archivo
        w.writerow(["EAN", "PRECIO"])
        for p in productos:
            w.writerow([p["ean"], p["precio"]])
            
    print(f"Archivo guardado exitosamente en: {ruta_completa}")
    return ruta_completa

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Cargamos el mapeo en una variable
    mapeo_cargado = cargar_mapeo_csv(ARCHIVO_MAPEO)
    
    # 2. Iniciamos el scrapeo
    resultado, ts = scrape_all(mapeo_cargado)
    
    # 3. Guardamos los resultados si existen
    if resultado:
        guardar_csv(resultado, ts)
    else:
        print("No se obtuvieron productos. Revisa la conexion, el sitio o el archivo de mapeo.")
        sys.exit(1)