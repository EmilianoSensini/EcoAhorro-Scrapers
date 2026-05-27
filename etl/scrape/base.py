import asyncio
import re
import logging
import csv
import aiohttp
import os
from datetime import datetime, timezone
from playwright.async_api import async_playwright

from etl.shared import normalizar_ean, construir_id, cargar_config_supermercados

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()


class VtexScraper:
    SUPERMERCADO: str = ""
    BASE_URL: str = ""
    CATEGORIAS: dict = {}

    def __init__(self):
        config = cargar_config_supermercados()
        self.id_supermercado = next(k for k, v in config.items() if v == self.SUPERMERCADO)
        self.ahora = datetime.now(timezone.utc)
        self.all_scraped_eans: set = set()
        self.output_file: str | None = None

    async def consultar_producto(self, session: aiohttp.ClientSession, ean: str) -> list | None:
        url = f"{self.BASE_URL}api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data:
                    return None
                product = data[0]
                items = product.get("items", [])
                raw_price = (
                    items[0]["sellers"][0]["commertialOffer"]["Price"]
                    if items and items[0].get("sellers")
                    else 0
                )
                id_producto = normalizar_ean(ean)
                if id_producto is None:
                    return None
                return [
                    construir_id(self.id_supermercado, id_producto),
                    id_producto,
                    self.id_supermercado,
                    float(raw_price),
                    self.ahora.isoformat(),
                ]
        except Exception as e:
            logger.error(f"Error en API para EAN {ean}: {e}")
        return None

    def nombre_archivo(self) -> str:
        raise NotImplementedError

    def construir_url_categoria(self, category_index: int, category_value: str) -> str:
        return f"{self.BASE_URL}{category_value}"

    def construir_url_paginada(self, url_categoria: str, page_number: int) -> str:
        raise NotImplementedError

    def mensaje_categoria(self, category_index: int, category_value: str) -> str:
        return f"\n--- EXPLORANDO: {category_value.upper()} ---"

    async def run(self):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        data_dir = os.path.join(base_dir, "data", "prices")
        os.makedirs(data_dir, exist_ok=True)
        self.output_file = os.path.join(data_dir, self.nombre_archivo())

        with open(self.output_file, mode="w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, quoting=csv.QUOTE_ALL)
            w.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            async with aiohttp.ClientSession() as session:
                for cat_idx, cat_val in self.CATEGORIAS.items():
                    url_cat = self.construir_url_categoria(cat_idx, cat_val)
                    logger.info(self.mensaje_categoria(cat_idx, cat_val))
                    page_number = 1

                    while True:
                        paginated_url = self.construir_url_paginada(url_cat, page_number)
                        logger.info(f"Página {page_number} -> {paginated_url}")

                        try:
                            await page.goto(paginated_url, wait_until="domcontentloaded", timeout=60000)
                            await asyncio.sleep(4)

                            html = await page.content()
                            found = set(re.findall(r"\b7\d{12}\b", html))

                            if not found:
                                logger.info(f"Fin de categoría {cat_idx}: No se detectaron EANs.")
                                break

                            new_eans = found - self.all_scraped_eans

                            if new_eans:
                                tasks = [self.consultar_producto(session, e) for e in new_eans]
                                results = await asyncio.gather(*tasks)

                                with open(self.output_file, mode="a", newline="", encoding="utf-8") as f:
                                    w = csv.writer(f, quoting=csv.QUOTE_ALL)
                                    for r in results:
                                        if r:
                                            w.writerow(r)

                                self.all_scraped_eans.update(new_eans)
                                logger.info(f"✅ +{len(new_eans)} productos guardados.")
                            else:
                                logger.info("Página repetida o sin novedades. Saltando...")
                                break

                            page_number += 1

                        except Exception as e:
                            logger.error(f"Error en categoría {cat_idx} pág {page_number}: {e}")
                            break

            await browser.close()

        logger.info(f"PROCESO TERMINADO. Total unívocos: {len(self.all_scraped_eans)}")
