"""
Scraper - La Coope en Casa
URL base: https://www.lacoopeencasa.coop/listado/categoria/{categoria}/pagina--{n}

Extrae: codigo_interno, nombre_producto, precio_producto, url_imagen
Salida: precios_lacoope_YYYYMMDD_HHMMSS.csv
"""

import csv
import re
import time
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

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
    Devuelve True si hay una pagina siguiente mediante el paginador.
    """
    ul = soup.find("ul", class_="pagination")
    if not ul:
        return False
    return ul.find("use", href=re.compile(r"derecha")) is not None


def parse_products(soup: BeautifulSoup) -> list[dict]:
    """
    Extrae productos del HTML renderizado incluyendo la URL de la imagen.
    """
    productos = []
    # Busca el contenedor principal de cada producto por su ID
    for tarjeta in soup.find_all(id=re.compile(r"^tarjeta-articulo-\d+$")):
        # Extraer codigo interno desde el ID
        m = re.search(r"tarjeta-articulo-(\d+)", tarjeta["id"])
        if not m:
            continue
        codigo = m.group(1)

        # Extraer nombre
        nombre_tag = tarjeta.find(class_="articulo-descripcion")
        nombre = nombre_tag.get_text(strip=True).rstrip("...").strip() if nombre_tag else ""

        # Extraer precio
        entero_tag  = tarjeta.find(class_="precio-entero")
        decimal_tag = tarjeta.find(class_="precio-decimal")
        if entero_tag:
            entero  = entero_tag.get_text(strip=True).replace("$", "").strip()
            decimal = decimal_tag.get_text(strip=True) if decimal_tag else "00"
            precio  = f"{entero},{decimal}"
        else:
            precio = ""

        # Extraer URL de la imagen (src)
        img_tag = tarjeta.find("img")
        url_imagen = img_tag.get("src") if img_tag and img_tag.has_attr("src") else ""

        productos.append({
            "codigo_interno": codigo, 
            "nombre_producto": nombre, 
            "precio_producto": precio,
            "url_imagen": url_imagen
        })
    return productos


def fetch_fresh_page(browser, url: str) -> str | None:
    """
    Abre una pagina nueva para cada URL para evitar acumulacion de DOM.
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

def scrape_all():
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
            prods  = parse_products(soup)
            print(f"  Pagina 1 -> {len(prods)} productos")
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
                prods_n = parse_products(soup)
                print(f"  Pagina {n} -> {len(prods_n)} productos")
                todos.extend(prods_n)
                n += 1

            print()

        context.close()
        browser.close()

    print(f"Total productos scrapeados: {len(todos)}\n")
    return todos, timestamp


# ── Guardar CSV ────────────────────────────────────────────────────────────────

def guardar_csv(productos: list[dict], timestamp: datetime) -> str:
    nombre = f"imagenes_laCoope.csv"
    with open(nombre, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Se inicia directamente con la cabecera sin timestamp interno
        w.writerow(["codigo_interno", "nombre_producto", "precio_producto", "url_imagen"])
        
        for p in productos:
            w.writerow([
                p["codigo_interno"], 
                p["nombre_producto"], 
                p["precio_producto"], 
                p["IMAGEN"]
            ])
            
    print(f"Archivo guardado: {nombre}")
    return nombre


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    resultado, ts = scrape_all()
    if resultado:
        guardar_csv(resultado, ts)
    else:
        print("No se obtuvieron productos. Revisa la conexion o el sitio.")
        sys.exit(1)