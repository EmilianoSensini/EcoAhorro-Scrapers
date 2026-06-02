import asyncio
from etl.scrape.base import VtexScraper


class VeaScraper(VtexScraper):
    SUPERMERCADO = "Vea"
    BASE_URL = "https://www.vea.com.ar/"
    CATEGORIAS = {
        1: "bebidas", 2: "almacen", 3: "frutas-y-verduras", 4: "carnes",
        5: "lacteos", 6: "limpieza", 7: "electro", 8: "tiempo-libre",
        9: "perfumeria", 10: "mundo-bebe", 12: "congelados",
        13: "panaderia-y-pasteleria", 14: "pastas-frescas",
        15: "rotiseria", 16: "mascotas", 17: "hogar-y-textil",
    }

    def construir_url_paginada(self, url_categoria: str, page_number: int) -> str:
        return f"{url_categoria}?page={page_number}"

    def nombre_archivo(self) -> str:
        return f"precios_vea_{self.ahora.strftime('%Y%m%d_%H%M%S')}.csv"


if __name__ == "__main__":
    asyncio.run(VeaScraper().run())
