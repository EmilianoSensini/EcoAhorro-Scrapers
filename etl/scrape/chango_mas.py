import asyncio
from etl.scrape.base import VtexScraper


class ChangoMasScraper(VtexScraper):
    SUPERMERCADO = "ChangoMas"
    BASE_URL = "https://www.masonline.com.ar/"
    CATEGORIAS = {
        1: "3454", 2: "3431", 3: "3432", 4: "3433",
        5: "3455", 6: "3434", 7: "272", 8: "273",
        9: "3435", 10: "3436", 11: "262", 12: "275",
        13: "3437", 14: "3438", 15: "3688",
    }

    def construir_url_paginada(self, url_categoria: str, page_number: int) -> str:
        return f"{url_categoria}?map=productClusterIds&page={page_number}"

    def nombre_archivo(self) -> str:
        return f"precios_changomas_{self.ahora.strftime('%Y%m%d_%H%M%S')}.csv"

    def mensaje_categoria(self, category_index: int, category_value: str) -> str:
        return f"\n--- EXPLORANDO: Categoría {category_index} (ID: {category_value}) ---"


if __name__ == "__main__":
    asyncio.run(ChangoMasScraper().run())
