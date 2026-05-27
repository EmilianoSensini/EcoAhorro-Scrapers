import asyncio
import re
import logging
import csv
import aiohttp
import os
from datetime import datetime, timezone
from playwright.async_api import async_playwright

import sys
_RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _RAIZ)
from utilidades.ean_utils import normalizar_ean, precio_a_float, construir_id, cargar_config_supermercados

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

SUPERMERCADO = "Vea"

async def consultar_producto(session, ean, id_supermercado, ahora):
    url = f"https://www.vea.com.ar/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    product = data[0]
                    items = product.get('items', [])
                    raw_price = items[0]['sellers'][0]['commertialOffer']['Price'] if items and items[0].get('sellers') else 0
                    id_producto = normalizar_ean(ean)
                    if id_producto is None:
                        return None
                    return [
                        construir_id(id_supermercado, id_producto),
                        id_producto,
                        id_supermercado,
                        float(raw_price),
                        ahora.isoformat()
                    ]
    except Exception as e:
        logger.error(f"Error en API para EAN {ean}: {e}")
    return None

async def main():
    config = cargar_config_supermercados()
    id_supermercado = next(k for k, v in config.items() if v == SUPERMERCADO)
    ahora = datetime.now(timezone.utc)

    base_dir = os.path.abspath(_RAIZ)
    data_dir = os.path.join(base_dir, "data", "prices")
    os.makedirs(data_dir, exist_ok=True)
    timestamp_file = ahora.strftime('%Y%m%d_%H%M%S')
    archivo_salida = os.path.join(data_dir, f"precios_vea_{timestamp_file}.csv")

    with open(archivo_salida, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        writer.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])

    mapeo_categorias = {
        1: "bebidas", 2: "almacen", 3: "frutas-y-verduras", 4: "carnes",
        5: "lacteos", 6: "limpieza", 7: "electro", 8: "tiempo-libre",
        9: "perfumeria", 10: "mundo-bebe", 12: "congelados",
        13: "panaderia-y-pasteleria", 14: "pastas-frescas",
        15: "rotiseria", 16: "mascotas", 17: "hogar-y-textil"
    }

    base_url = "https://www.vea.com.ar/"
    all_scraped_eans = set()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        async with aiohttp.ClientSession() as session:
            for category_id, category_name in mapeo_categorias.items():
                url_categoria = f"{base_url}{category_name}"
                logger.info(f"\n--- EXPLORANDO: {category_name.upper()} ---")
                page_number = 1

                while True:
                    paginated_url = f"{url_categoria}?page={page_number}"
                    logger.info(f"Página {page_number} -> {paginated_url}")

                    try:
                        await page.goto(paginated_url, wait_until="domcontentloaded", timeout=60000)
                        await asyncio.sleep(4)

                        html_content = await page.content()
                        found_eans = set(re.findall(r'\b7\d{12}\b', html_content))

                        if not found_eans:
                            logger.info(f"Fin de {category_name}: No se detectaron EANs.")
                            break

                        new_eans = found_eans - all_scraped_eans

                        if new_eans:
                            api_tasks = [consultar_producto(session, ean, id_supermercado, ahora) for ean in new_eans]
                            api_results = await asyncio.gather(*api_tasks)

                            with open(archivo_salida, mode='a', newline='', encoding='utf-8') as csv_file:
                                writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                                for product_result in api_results:
                                    if product_result: writer.writerow(product_result)

                            all_scraped_eans.update(new_eans)
                            logger.info(f"✅ +{len(new_eans)} productos guardados.")
                        else:
                            logger.info("Página repetida o sin novedades. Saltando...")
                            break

                        page_number += 1

                    except Exception as e:
                        logger.error(f"Error en {category_name} pág {page_number}: {e}")
                        break

        await browser.close()

    logger.info(f"PROCESO TERMINADO. Total unívocos: {len(all_scraped_eans)}")

if __name__ == "__main__":
    asyncio.run(main())
