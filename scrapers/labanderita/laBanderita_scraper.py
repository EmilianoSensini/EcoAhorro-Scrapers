import re
import csv
import sys
import time
import os
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

_RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _RAIZ)
from utilidades.ean_utils import normalizar_ean, precio_a_float, parse_precio_argentino, construir_id, cargar_config_supermercados

SUPERMERCADO = "La Banderita"
TARGET_URL = "https://www.labanderita.ar/shop"

def is_valid_ean(value: str) -> bool:
    return bool(re.match(r'^\d{8,}$', value.strip()))

def parse_precio_labanderita(price_str: str) -> float:
    s = price_str.replace("$", "").replace(" ", "").strip()
    m = re.search(r"[\d.,]+", s)
    if not m:
        return 0.0
    s = m.group()
    if re.search(r"\d+,\d{2}$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0

def get_html() -> str:
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

def extract_products(html: str, id_supermercado: str, ahora) -> list[list]:
    soup = BeautifulSoup(html, 'html.parser')

    name_to_ean = {}
    for ean, name in re.findall(r"a: '([^']+)',b: '([^']+)'", html):
        if is_valid_ean(ean):
            name_to_ean[name.strip()] = ean.strip()

    results = []
    seen = set()

    for item in soup.find_all('li', class_='product'):
        title_el = item.find('h3', class_='kw-details-title')
        if not title_el:
            continue
        name = title_el.get_text(strip=True)

        price = None
        price_el = item.find('span', class_='price')
        if price_el:
            for amt in reversed(price_el.find_all('span', class_='amount')):
                txt = amt.get_text(strip=True)
                if txt:
                    price = txt
                    break
        if not price:
            continue

        ean = name_to_ean.get(name, 'ean_desconocido')
        if ean == 'ean_desconocido' or not is_valid_ean(ean):
            continue

        key = (ean, name)
        if key in seen:
            continue
        seen.add(key)

        id_producto = normalizar_ean(ean)
        if id_producto is None:
            continue

        results.append([
            construir_id(id_supermercado, id_producto),
            id_producto,
            id_supermercado,
            parse_precio_labanderita(price),
            ahora.isoformat()
        ])

    return results

if __name__ == '__main__':
    config = cargar_config_supermercados()
    id_supermercado = next(k for k, v in config.items() if v == SUPERMERCADO)
    ahora = datetime.now(timezone.utc)

    base_dir = os.path.abspath(_RAIZ)
    data_dir = os.path.join(base_dir, "data", "prices")
    os.makedirs(data_dir, exist_ok=True)
    timestamp_file = ahora.strftime('%Y%m%d_%H%M%S')
    output_filename = os.path.join(data_dir, f"precios_labanderita_{timestamp_file}.csv")

    print(f"Inicio: {timestamp_file}\n")

    html = get_html()

    print("\nExtrayendo productos...")
    products = extract_products(html, id_supermercado, ahora)

    if not products:
        print("No se encontraron productos con EAN valido.")
        sys.exit(1)

    print(f"  Total de productos validos procesados: {len(products)}")

    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])
        writer.writerows(products)

    print(f"\nGuardado: {output_filename} ({len(products)} productos)")
