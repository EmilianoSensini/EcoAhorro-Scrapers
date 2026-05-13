"""
Scraper labanderita.ar/shop
Extrae EAN, NOMBRE, MARCA, PRESENTACION, ID_CATEGORIA, IMAGEN.
Solo guarda los productos que poseen un EAN numérico válido.
Ignora categoría (queda en blanco).
Extrae la presentación desde el nombre del producto mediante RegEx.

Requiere: pip install selenium
Necesita ChromeDriver instalado y en el PATH.
"""

import re
import csv
import sys
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

TARGET_URL = "https://www.labanderita.ar/shop"

def is_valid_ean(value: str) -> bool:
    # Retorna True solo si el EAN esta compuesto enteramente por 8 o mas digitos
    return bool(re.match(r'^\d{8,}$', value.strip()))

def extract_presentation(name: str) -> str:
    # Busca un numero (que puede tener comas, puntos o barras) seguido opcionalmente de un espacio y una unidad de medida
    patron = r'(\d+(?:[.,/]\d+)?\s*(?:ml|l|lt|cc|cm3|g|gr|grs|kg))\b'
    match = re.search(patron, name, re.IGNORECASE)
    return match.group(1).strip() if match else ""

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

def extract_products(html: str) -> list[dict]:
    results = []
    seen = set()

    # 1. Extraer imágenes: creamos un mapeo de Nombre del producto -> URL de la imagen
    imagenes_map = {}
    # Buscamos todas las etiquetas img que contengan la clase de los productos
    img_tags = re.findall(r'<img[^>]+class="[^"]*kw-prodimage-img[^"]*"[^>]+>', html)
    for tag in img_tags:
        src_match = re.search(r'data-src="([^"]+)"', tag)
        alt_match = re.search(r'alt="([^"]+)"', tag)
        
        if src_match and alt_match:
            nombre_img = alt_match.group(1).strip()
            imagenes_map[nombre_img] = src_match.group(1)

    # 2. Buscamos en el HTML los bloques de JavaScript que declaran los productos
    matches = re.findall(r"a:\s*'([^']+)',b:\s*'([^']+)',c:\s*'([^']+)'", html)

    for ean, nombre, marca in matches:
        ean = ean.strip()
        nombre = nombre.strip()
        marca = marca.strip()
        
        # Filtrar los codigos invalidos
        if not is_valid_ean(ean):
            continue

        # Evitar duplicados
        if ean in seen:
            continue
        seen.add(ean)

        # Extraer presentacion del nombre
        presentacion = extract_presentation(nombre)
        
        # Buscar la URL de la imagen en nuestro mapeo
        imagen_url = imagenes_map.get(nombre, "N/A")

        results.append({
            'EAN': ean,
            'NOMBRE': nombre,
            'MARCA': marca,
            'PRESENTACION': presentacion,
            'ID_CATEGORIA': '',
            'IMAGEN': imagen_url
        })

    return results

def save_csv(products: list[dict], filename: str):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        # Imprimimos el encabezado manualmente incluyendo IMAGEN
        f.write("EAN,NOMBRE,MARCA,PRESENTACION,ID_CATEGORIA,IMAGEN\n")
        
        # Guardamos forzando a que todos los campos lleven comillas dobles
        # NOTA: Se cambió 'ID/EAN' por 'EAN' para que coincida exactamente con las keys del diccionario y no cause error.
        writer = csv.DictWriter(
            f, 
            fieldnames=['EAN', 'NOMBRE', 'MARCA', 'PRESENTACION', 'ID_CATEGORIA', 'IMAGEN'], 
            quoting=csv.QUOTE_ALL
        )
        writer.writerows(products)
        
    print(f"\nGuardado: {filename} ({len(products)} productos validos)")

if __name__ == '__main__':
    timestamp_file = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_filename = f"imagenes_labanderita.csv"
    
    html = get_html()

    print("\nExtrayendo productos...")
    products = extract_products(html)

    if not products:
        print("No se encontraron productos con EAN valido.")
        sys.exit(1)

    print(f"  Total procesados listos para exportar: {len(products)}")

    save_csv(products, output_filename)