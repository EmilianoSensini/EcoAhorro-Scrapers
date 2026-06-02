import re
import csv
import os
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from etl.shared import normalizar_ean, precio_a_float, construir_id, cargar_config_supermercados

TARGET_URL = "https://www.labanderita.ar/shop"


class LaBanderitaScraper:
    SUPERMERCADO = "La Banderita"

    def __init__(self):
        config = cargar_config_supermercados()
        self.id_supermercado = next(k for k, v in config.items() if v == self.SUPERMERCADO)
        self.ahora = datetime.now(timezone.utc)

    @staticmethod
    def _is_valid_ean(value: str) -> bool:
        return bool(re.match(r"^\d{8,}$", value.strip()))

    def _get_html(self) -> str:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1280,900")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        driver = webdriver.Chrome(options=options)
        try:
            print(f"Abriendo {TARGET_URL} ...")
            driver.get(TARGET_URL)
            time.sleep(4)

            last_height = driver.execute_script("return document.body.scrollHeight")
            round_num = 0
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2.5)
                new_height = driver.execute_script("return document.body.scrollHeight")
                round_num += 1
                print(f"  Scroll {round_num}: {new_height}px")
                if new_height == last_height:
                    time.sleep(2)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        print("  -> Fin de pagina.")
                        break
                last_height = new_height

            return driver.page_source
        finally:
            driver.quit()

    def _extract_products(self, html: str) -> list[list]:
        soup = BeautifulSoup(html, "html.parser")

        name_to_ean = {}
        for ean, name in re.findall(r"a: '([^']+)',b: '([^']+)'", html):
            if self._is_valid_ean(ean):
                name_to_ean[name.strip()] = ean.strip()

        results = []
        seen = set()

        for item in soup.find_all("li", class_="product"):
            title_el = item.find("h3", class_="kw-details-title")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)

            price = None
            price_el = item.find("span", class_="price")
            if price_el:
                for amt in reversed(price_el.find_all("span", class_="amount")):
                    txt = amt.get_text(strip=True)
                    if txt:
                        price = txt
                        break
            if not price:
                continue

            ean = name_to_ean.get(name, "ean_desconocido")
            if ean == "ean_desconocido" or not self._is_valid_ean(ean):
                continue

            key = (ean, name)
            if key in seen:
                continue
            seen.add(key)

            id_producto = normalizar_ean(ean)
            if id_producto is None:
                continue

            results.append([
                construir_id(self.id_supermercado, id_producto),
                id_producto,
                self.id_supermercado,
                precio_a_float(price),
                self.ahora.isoformat(),
            ])

        return results

    def run(self):
        ruta_script = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.abspath(os.path.join(ruta_script, "..", ".."))
        data_dir = os.path.join(base_dir, "data", "prices")
        os.makedirs(data_dir, exist_ok=True)

        ts = self.ahora.strftime("%Y%m%d_%H%M%S")
        output = os.path.join(data_dir, f"precios_labanderita_{ts}.csv")
        print(f"Inicio: {ts}\n")

        html = self._get_html()

        print("\nExtrayendo productos...")
        products = self._extract_products(html)

        if not products:
            print("No se encontraron productos con EAN valido.")
            return False

        print(f"  Total de productos validos procesados: {len(products)}")

        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])
            writer.writerows(products)

        print(f"\nGuardado: {output} ({len(products)} productos)")
        return True


if __name__ == "__main__":
    LaBanderitaScraper().run()
