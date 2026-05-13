"""
Scraper labanderita.ar/shop
Extrae EAN y precio de los productos.
Solo guarda los productos que poseen un EAN numérico válido.

Requiere: pip install beautifulsoup4 selenium
Necesita ChromeDriver instalado y en el PATH.
"""

import re
import csv
import sys
import time
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

TARGET_URL = "https://www.labanderita.ar/shop"

def is_valid_ean(value: str) -> bool:
    return bool(re.match(r'^\d{8,}$', value.strip()))

def format_price(price_val):
    """Convierte un valor numérico o string a formato: 1.234,56"""
    try:
        if isinstance(price_val, str):
            price_val = price_val.replace('$', '').replace(' ', '').strip()
            if ',' in price_val:
                price_val = price_val.replace('.', '').replace(',', '.')
            elif '.' in price_val:
                if price_val.count('.') > 1:
                    price_val = price_val.replace('.', '')
                else:
                    partes = price_val.split('.')
                    if len(partes[1]) == 3 and len(partes[0]) <= 3:
                        price_val = price_val.replace('.', '')
        val = float(price_val)
        return "{:,.2f}".format(val).replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

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
                # Segundo chequeo para asegurarse que realmente llegó al final
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    print("  -> Fin de pagina.")
                    break
            last_height = new_height

        return driver.page_source
    finally:
        driver.quit()

def extract_products(html: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')

    # Mapa nombre -> EAN desde el JS embebido
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

        # Precio: último <span class="amount"> no vacío
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

        # Filtrar si el EAN es desconocido o no cumple el formato
        if ean == 'ean_desconocido' or not is_valid_ean(ean):
            continue

        key = (ean, name)
        if key in seen:
            continue
        seen.add(key)

        results.append({'EAN': ean, 'PRECIO': format_price(price)})

    return results

def save_csv(products: list[dict], filename: str):
    import os
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['EAN', 'PRECIO'])
        writer.writeheader()
        writer.writerows(products)
    print(f"\nGuardado: {filename} ({len(products)} productos)")

if __name__ == '__main__':
    # Usamos un formato seguro para nombres de archivo
    timestamp_file = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"resultados/precios_labanderita_{timestamp_file}.csv"
    
    print(f"Inicio: {timestamp_file}\n")

    html = get_html()

    print("\nExtrayendo productos...")
    products = extract_products(html)

    if not products:
        print("No se encontraron productos con EAN valido.")
        sys.exit(1)

    print(f"  Total de productos validos procesados: {len(products)}")

    save_csv(products, output_filename)