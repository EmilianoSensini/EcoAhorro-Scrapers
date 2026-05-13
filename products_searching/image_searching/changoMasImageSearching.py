import asyncio
import re
import logging
import csv
import aiohttp
from playwright.async_api import async_playwright

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

def extraer_presentacion(texto_item, texto_producto):
    """
    Busca medidas de presentación utilizando una expresión regular.
    Soporta formatos como: 330 Gr, 1 kg, 500 ml, 1.5 lt, 500cc.
    """
    regex = r'(\d+(?:[.,]\d+)?\s*(?:gr|g|kg|ml|l|lt|cc|cm3|oz))\b'
    
    # 1. Buscar en el nombre específico del item
    match = re.search(regex, texto_item, re.IGNORECASE)
    if match:
        return match.group(1).lower()
        
    # 2. Si no lo encuentra, buscar en el nombre general del producto
    match = re.search(regex, texto_producto, re.IGNORECASE)
    if match:
        return match.group(1).lower()
        
    # 3. Fallback: usar el nombre del item si es distinto al del producto
    if texto_item and texto_item != "N/A" and texto_item != texto_producto:
        return texto_item
        
    return "N/A"

async def consultar_producto(session, ean, nombre_categoria_fijo):
    """Consulta la API de VTEX, asigna el nombre de categoría del mapeo y extrae la imagen."""
    url = f"https://www.masonline.com.ar/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    p = data[0]
                    items = p.get('items', [])
                    
                    nombre = p.get('productName', 'N/A')
                    marca = p.get('brand', 'N/A')
                    nombre_item = items[0].get('name', 'N/A') if items else 'N/A'
                    
                    presentacion = extraer_presentacion(nombre_item, nombre)
                    
                    # Extracción de la URL de la imagen principal
                    imagen_url = "N/A"
                    if items and items[0].get('images'):
                        imagen_url = items[0]['images'][0].get('imageUrl', 'N/A')

                    # Retornamos la fila con el nombre descriptivo de la categoría y la imagen
                    return [
                        ean, 
                        nombre, 
                        marca, 
                        presentacion,
                        nombre_categoria_fijo,
                        imagen_url
                    ]
    except Exception as e:
        logger.error(f"Error en API para EAN {ean}: {e}")
    return None

async def main():
    archivo_salida = "imagenes_chango_mas.csv"
    
    # 1. Preparación del CSV con encabezados actualizados
    with open(archivo_salida, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["ID/EAN", "NOMBRE", "MARCA", "PRESENTACION", "ID_CATEGORIA", "IMAGEN"])

    # 2. Mapeo de Categorías (ID de cluster : Nombre para el CSV)
    mapeo_categorias = {
        "3454": "almacen",
        "3431": "carniceria-pescaderia-y-verduleria",
        "3432": "frescos-y-congelados",
        "3433": "bebidas",
        "3455": "perfumeria",
        "3434": "belleza",
        "272": "limpieza",
        "273": "bebes-y-ninos",
        "3435": "mascotas",
        "3436": "hogar",
        "262": "electrodomesticos",
        "275": "tecnologia",
        "3437": "deporte-ocio-y-aire-libre",
        "3438": "automotor",
        "3688": "indumentaria-calzado-y-accesorios"
    }
    
    url_base = "https://www.masonline.com.ar/"
    todos_los_eans = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        async with aiohttp.ClientSession() as session:
            for cat_id, cat_nombre in mapeo_categorias.items():
                url_categoria = f"{url_base}{cat_id}"
                logger.info(f"\n--- PROCESANDO: {cat_nombre.upper()} ---")
                pagina = 1

                while True:
                    url_paginada = f"{url_categoria}?map=productClusterIds&page={pagina}"
                    logger.info(f"Página {pagina} -> {url_paginada}")
                    
                    try:
                        await page.goto(url_paginada, wait_until="domcontentloaded", timeout=60000)
                        await asyncio.sleep(4) # Espera para carga de contenido dinámico
                        
                        html = await page.content()
                        # Extraer EANs (formato estándar de 13 dígitos que empiezan con 7)
                        eans_encontrados = set(re.findall(r'\b7\d{12}\b', html))
                        
                        if not eans_encontrados:
                            logger.info(f"Fin de {cat_nombre}: No hay más productos.")
                            break
                        
                        # Filtrar solo los que no procesamos antes
                        nuevos = eans_encontrados - todos_los_eans
                        
                        if nuevos:
                            # Consultas asíncronas a la API pasando el nombre de la categoría
                            tareas = [consultar_producto(session, ean, cat_nombre) for ean in nuevos]
                            resultados = await asyncio.gather(*tareas)
                            
                            # Guardado incremental en el CSV
                            with open(archivo_salida, mode='a', newline='', encoding='utf-8') as f:
                                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                                for r in resultados:
                                    if r:
                                        writer.writerow(r)
                            
                            todos_los_eans.update(nuevos)
                            logger.info(f"+{len(nuevos)} productos nuevos en {cat_nombre}.")
                        else:
                            logger.info("Sin novedades en esta página. Finalizando categoría.")
                            break
                        
                        pagina += 1
                        
                    except Exception as e:
                        logger.error(f"Error en {cat_nombre} (Pág {pagina}): {e}")
                        break

        await browser.close()
    
    logger.info(f"PROCESO FINALIZADO. Total de productos únicos: {len(todos_los_eans)}")

if __name__ == "__main__":
    asyncio.run(main())