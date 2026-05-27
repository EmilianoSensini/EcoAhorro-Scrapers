import os
import csv
import re
import time
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

from etl.shared import normalizar_ean, precio_a_float, construir_id, cargar_config_supermercados

SCROLL_STEP_PX        = 600
SCROLL_STEP_WAIT_MS   = 400
SCROLL_PASS_WAIT_MS   = 1000
PAGE_LOAD_WAIT_MS     = 2000
PAGE_TIMEOUT_MS       = 30000
DELAY_BETWEEN_PAGES   = 1.0
MAX_RETRIES           = 3

BASE_URL = "https://www.lacoopeencasa.coop/listado/categoria/{categoria}/pagina--{pagina}"

CATEGORIAS = {
    "almacen":       "almacen/2",
    "frescos":       "frescos/3",
    "bebidas":       "bebidas/4",
    "perfumeria":    "perfumeria/5",
    "limpieza":      "limpieza/6",
    "casa-y-jardin": "casa-y-jardin/7",
}


class CoopeScraper:
    SUPERMERCADO = "Cooperativa Obrera"

    def __init__(self):
        config = cargar_config_supermercados()
        self.id_supermercado = next(k for k, v in config.items() if v == self.SUPERMERCADO)
        self.ahora = datetime.now(timezone.utc)
        ruta_script = os.path.dirname(os.path.abspath(__file__))
        self.archivo_mapeo = os.path.join(ruta_script, "mapeo_codigo_coope.csv")
        base_dir = os.path.abspath(os.path.join(ruta_script, "..", ".."))
        self.carpeta_salida = os.path.join(base_dir, "data", "prices")

    @staticmethod
    def _cargar_mapeo_csv(ruta_archivo: str) -> dict:
        mapeo = {}
        with open(ruta_archivo, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for fila in reader:
                ean = fila["ean"].strip()
                codigo_int = fila["codigo_interno_coope"].strip()
                mapeo[codigo_int] = ean
        print(f"Mapeo cargado con {len(mapeo)} codigos.")
        return mapeo

    @staticmethod
    def _scroll_to_bottom(page) -> None:
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

    @staticmethod
    def _has_next_page(soup: BeautifulSoup) -> bool:
        ul = soup.find("ul", class_="pagination")
        if not ul:
            return False
        return ul.find("use", href=re.compile(r"derecha")) is not None

    def _parse_products(self, soup: BeautifulSoup, mapeo_codigos: dict) -> list[list]:
        productos = []
        for tarjeta in soup.find_all(id=re.compile(r"^tarjeta-articulo-\d+$")):
            m = re.search(r"tarjeta-articulo-(\d+)", tarjeta["id"])
            if not m:
                continue
            codigo_interno = m.group(1)
            ean = mapeo_codigos.get(codigo_interno)
            if not ean:
                continue
            id_producto = normalizar_ean(ean)
            if id_producto is None:
                continue
            entero_tag  = tarjeta.find(class_="precio-entero")
            decimal_tag = tarjeta.find(class_="precio-decimal")
            if not entero_tag:
                continue
            entero  = entero_tag.get_text(strip=True).replace("$", "").strip()
            decimal = decimal_tag.get_text(strip=True) if decimal_tag else "00"
            precio_str = f"{entero},{decimal}"
            precio_valor = precio_a_float(precio_str)
            productos.append([
                construir_id(self.id_supermercado, id_producto),
                id_producto,
                self.id_supermercado,
                precio_valor,
                self.ahora.isoformat(),
            ])
        return productos

    @staticmethod
    def _fetch_fresh_page(context, url: str) -> str | None:
        for intento in range(1, MAX_RETRIES + 1):
            page = context.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
                page.wait_for_timeout(PAGE_LOAD_WAIT_MS)
                CoopeScraper._scroll_to_bottom(page)
                return page.content()
            except PlaywrightTimeout:
                print(f"  [!] Timeout (intento {intento}/{MAX_RETRIES}): {url}")
                if intento == MAX_RETRIES:
                    return None
                time.sleep(2)
            finally:
                page.close()
        return None

    def run(self):
        ts = self.ahora
        print(f"=== Inicio scraping: {ts.strftime('%Y-%m-%d %H:%M:%S')} ===\n")

        mapeo = self._cargar_mapeo_csv(self.archivo_mapeo)
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
                html = self._fetch_fresh_page(context, url_p1)
                if not html:
                    print(f"  [!] No se pudo cargar pagina 1. Saltando.\n")
                    continue
                soup = BeautifulSoup(html, "html.parser")
                prods = self._parse_products(soup, mapeo)
                print(f"  Pagina 1 -> {len(prods)} productos mapeados y extraidos")
                todos.extend(prods)

                n = 2
                while self._has_next_page(soup):
                    time.sleep(DELAY_BETWEEN_PAGES)
                    url_pn = BASE_URL.format(categoria=slug, pagina=n)
                    html_n = self._fetch_fresh_page(context, url_pn)
                    if not html_n:
                        print(f"  [!] No se pudo cargar pagina {n}. Omitiendo.")
                        break
                    soup = BeautifulSoup(html_n, "html.parser")
                    prods_n = self._parse_products(soup, mapeo)
                    print(f"  Pagina {n} -> {len(prods_n)} productos mapeados y extraidos")
                    todos.extend(prods_n)
                    n += 1
                print()

            context.close()
            browser.close()

        print(f"Total productos scrapeados con exito: {len(todos)}\n")

        if not todos:
            print("No se obtuvieron productos. Revisa la conexion, el sitio o el archivo de mapeo.")
            return False

        os.makedirs(self.carpeta_salida, exist_ok=True)
        nombre = f"precios_lacoope_{ts.strftime('%Y%m%d_%H%M%S')}.csv"
        ruta = os.path.join(self.carpeta_salida, nombre)
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, quoting=csv.QUOTE_ALL)
            w.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])
            for p in todos:
                w.writerow(p)
        print(f"Archivo guardado exitosamente en: {ruta}")
        return True


if __name__ == "__main__":
    CoopeScraper().run()
