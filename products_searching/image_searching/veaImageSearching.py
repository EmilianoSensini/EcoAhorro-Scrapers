import asyncio
import re
import logging
import csv
import aiohttp
from playwright.async_api import async_playwright

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

async def auto_scroll(page):
    """Hace scroll hacia abajo para asegurar que todos los productos se carguen."""
    await page.evaluate("""
        async () => {
            await new Promise((resolve) => {
                let totalHeight = 0;
                let distance = 100;
                let timer = setInterval(() => {
                    let scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if(totalHeight >= scrollHeight){
                        clearInterval(timer);
                        resolve();
                    }
                }, 100);
            });
        }
    """)

async def consultar_producto(session, ean, categoria):
    """Consulta la API de VTEX para obtener detalles del producto."""
    url = f"https://www.vea.com.ar/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    p = data[0]
                    items = p.get('items', [])
                    nombre = p.get('productName', 'N/A')
                    marca = p.get('brand', 'N/A').upper()
                    
                    item_name = items[0].get('name', '') if items else ''
                    patron_pres = re.compile(r'(\d+(?:[.,]\d+)?\s*(?:gr|g|kg|cc|ml|l|lts|lt|cm3))\b', re.IGNORECASE)
                    match = patron_pres.search(nombre)
                    
                    presentacion = match.group(1).lower() if match else (item_name if item_name else 'N/A')
                    
                    imagen_url = "N/A"
                    if items and items[0].get('images'):
                        imagen_url = items[0]['images'][0].get('imageUrl', 'N/A')
                        
                    return [str(ean), nombre, marca, presentacion, categoria, imagen_url]
    except Exception as e:
        logger.error(f"Error en API para EAN {ean}: {e}")
    return None

async def main():
    archivo_salida = "imagenes_vea.csv"
    
    with open(archivo_salida, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["ID/EAN", "NOMBRE", "MARCA", "PRESENTACION", "ID_CATEGORIA", "IMAGEN"])

    mapeo_categorias = {
        1: "bebidas", 2: "almacen", 3: "frutas-y-verduras", 4: "carnes",
        5: "lacteos", 6: "limpieza", 7: "electro", 8: "tiempo-libre",
        9: "perfumeria", 10: "mundo-bebe", 12: "congelados",
        13: "panaderia-y-pasteleria", 14: "pastas-frescas",
        15: "rotiseria", 16: "mascotas", 17: "hogar-y-textil"
    }
    
    url_base = "https://www.vea.com.ar/"
    todos_los_eans = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        async with aiohttp.ClientSession() as session:
            for id_cat, slug_cat in mapeo_categorias.items():
                url_categoria = f"{url_base}{slug_cat}"
                logger.info(f"\n--- EXPLORANDO: {slug_cat.upper()} ---")
                pagina = 1

                while True:
                    url_paginada = f"{url_categoria}?page={pagina}"
                    logger.info(f"Página {pagina} -> {url_paginada}")
                    
                    try:
                        await page.goto(url_paginada, wait_until="networkidle", timeout=60000)
                        
                        # PASO CRÍTICO: Scrolling para cargar productos remanentes
                        await auto_scroll(page)
                        await asyncio.sleep(2) # Espera final de renderizado
                        
                        html = await page.content()
                        eans_encontrados = set(re.findall(r'\b7\d{12}\b', html))
                        
                        if not eans_encontrados:
                            logger.info(f"Fin de {slug_cat}: No se detectaron EANs.")
                            break
                        
                        nuevos = eans_encontrados - todos_los_eans
                        
                        if nuevos:
                            tareas = [consultar_producto(session, ean, slug_cat) for ean in nuevos]
                            resultados = await asyncio.gather(*tareas)
                            
                            with open(archivo_salida, mode='a', newline='', encoding='utf-8') as f:
                                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                                for r in resultados:
                                    if r: writer.writerow(r)
                            
                            todos_los_eans.update(nuevos)
                            logger.info(f"+{len(nuevos)} productos nuevos (Total en página: {len(eans_encontrados)})")
                        else:
                            logger.info("Sin productos nuevos en esta página. Finalizando categoría.")
                            break
                        
                        pagina += 1
                        
                    except Exception as e:
                        logger.error(f"Error en {slug_cat} pág {pagina}: {e}")
                        break

        await browser.close()
    
    logger.info(f"PROCESO TERMINADO. Total unívocos: {len(todos_los_eans)}")

if __name__ == "__main__":
    asyncio.run(main())