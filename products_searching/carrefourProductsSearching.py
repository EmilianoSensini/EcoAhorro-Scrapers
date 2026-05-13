import asyncio
import re
import json
import logging
import csv
from playwright.async_api import async_playwright
import nest_asyncio

# Parche para evitar bloqueos en entornos interactivos (Jupyter, VS Code, Spyder)
nest_asyncio.apply()

# Configuracion de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

def extraer_presentacion(nombre):
    """Extrae la presentacion del nombre usando expresiones regulares."""
    patron = re.compile(r'(\d+(?:[.,]\d+)?\s*(?:gr|g|kg|cc|ml|l|lts|lt|cm3))\b', re.IGNORECASE)
    match = patron.search(nombre)
    if match:
        return match.group(1).lower()
    return "N/A"

async def main():
    archivo_salida = "productos_carrefour.csv"
    
    with open(archivo_salida, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["ID/EAN", "NOMBRE", "MARCA", "PRESENTACION", "ID_CATEGORIA"])

    # Categorias con formato de URL correcto (sin tildes y en minusculas)
    categorias = [
    "electro-y-tecnologia/tv-y-soportes", 
    "electro-y-tecnologia/lavado", 
    "electro-y-tecnologia/cuidado-personal-y-salud", 
    "electro-y-tecnologia/celulares", 
    "electro-y-tecnologia/termotanques-y-calefones", 
    "electro-y-tecnologia/informatica-y-gaming", 
    "electro-y-tecnologia/climatizacion", 
    "electro-y-tecnologia/cocinas-y-hornos", 
    "electro-y-tecnologia/heladeras-y-freezers", 
    "electro-y-tecnologia/pequenos-electrodomesticos", 
    "electro-y-tecnologia/audio", 
    "electro-y-tecnologia/instalaciones", 
    "electro-y-tecnologia/electro-limpieza", 

    "hogar/cocina-y-comedor", 
    "hogar/muebles", 
    "hogar/dormitorio", 
    "hogar/bano", 
    "hogar/deco-y-organizacion", 
    "hogar/ferreteria-y-construccion",

    "almacen/aceites-y-vinagres",
    "almacen/enlatados-y-conservas", 
    "almacen/snacks", 
    "almacen/pastas-secas", 
    "almacen/sal-aderezos-y-saborizantes",
    "almacen/arroz-y-legumbres", 
    "almacen/caldos-sopas-y-pure", 
    "almacen/harinas", 
    "almacen/reposteria-y-postres",
    "almacen/comidas-instantaneas",

    "desayuno-y-merienda/galletitas-bizcochitos-y-tostadas", 
    "desayuno-y-merienda/infusiones", 
    "desayuno-y-merienda/golosinas-y-chocolates", 
    "desayuno-y-merienda/budines-magdalenas", 
    "desayuno-y-merienda/azucar-y-endulzantes", 
    "desayuno-y-merienda/yerba", 
    "desayuno-y-merienda/mermelada-y-otros-dulces", 
    "desayuno-y-merienda/cafe", 
    "desayuno-y-merienda/cereales-y-barritas", 

    "bebidas/cervezas", 
    "bebidas/gaseosas", 
    "bebidas/bebidas-energizantes", 
    "bebidas/vinos", 
    "bebidas/aguas", 
    "bebidas/espumantes-y-sidras", 
    "bebidas/fernet-y-aperitivos", 
    "bebidas/jugos",
    "bebidas/bebidas-blancas", 
    "bebidas/bebidas-isotonicas", 

    "lacteos-y-productos-frescos/leches",
    "lacteos-y-productos-frescos/cremas-de-leche", 
    "lacteos-y-productos-frescos/dulce-de-membrillo-y-otros-dulces", 
    "lacteos-y-productos-frescos/yogures", 
    "lacteos-y-productos-frescos/postres", 
    "lacteos-y-productos-frescos/salchichas", 
    "lacteos-y-productos-frescos/mantecas-margarinas-y-levaduras", 
    "lacteos-y-productos-frescos/huevos", 
    "lacteos-y-productos-frescos/quesos", 
    "lacteos-y-productos-frescos/dulce-de-leche", 
    "lacteos-y-productos-frescos/tapas-y-pastas-frescas", 
    "lacteos-y-productos-frescos/fiambres",

    "carnes-y-pescados/carne-vacuna", 
    "carnes-y-pescados/achuras-y-embutidos", 
    "carnes-y-pescados/carbon-y-encendido", 
    "carnes-y-pescados/pollo-y-granja", 
    "carnes-y-pescados/pescados-y-mariscos", 
    "carnes-y-pescados/carne-de-cerdo", 
    "carnes-y-pescados/rebozados", 

    "frutas-y-verduras/frutas",
    "frutas-y-verduras/verduras-cortadas-y-ensaladas-preparadas", 
    "frutas-y-verduras/verduras", 
    "frutas-y-verduras/frutos-secos", 

    "panaderia/elaboracion-carrefour",
    "panaderia/pan-rallado-y-rebozadores", 
    "panaderia/sandwiches-empanadas-y-tartas", 
    "panaderia/panificados", 
    "panaderia/pizzas-y-prepizzas", 
    "panaderia/bizcochuelos-y-piononos",

    "congelados/hamburguesas", 
    "congelados/comidas-y-panificados", 
    "congelados/nuggets-y-rebozados", 
    "congelados/helados-y-postres", 
    "congelados/papas", 
    "congelados/verduras-y-frutas", 
    "congelados/pescados-y-mariscos", 
    "congelados/pollo", 

    "limpieza/limpieza-de-la-ropa", 
    "limpieza/lavandinas", 
    "limpieza/desodorantes-de-ambiente", 
    "limpieza/limpieza-del-hogar", 
    "limpieza/rollos-de-cocina-y-servilletas", 
    "limpieza/insecticidas", 
    "limpieza/papeles-higienicos", 
    "limpieza/articulos-de-limpieza", 
    "limpieza/limpieza-de-cocina", 
    "limpieza/limpieza-de-bano", 

    "perfumeria-y-farmacia/cuidado-del-cabello",
    "perfumeria-y-farmacia/cuidado-de-la-piel", 
    "perfumeria-y-farmacia/algodones-e-hisopos", 
    "perfumeria-y-farmacia/cuidado-dental", 
    "perfumeria-y-farmacia/antitranspirantes-y-desodorantes", 
    "perfumeria-y-farmacia/farmacia-y-ortopedia", 
    "perfumeria-y-farmacia/jabones", 
    "perfumeria-y-farmacia/cuidado-corporal", 
    "perfumeria-y-farmacia/proteccion-femenina", 
    "perfumeria-y-farmacia/repelentes", 
    "perfumeria-y-farmacia/perfumes-y-maquillaje",

    "mundo-bebe/panales", 
    "mundo-bebe/alimentos-para-bebe", 
    "mundo-bebe/pelelas-y-baneras", 
    "mundo-bebe/toallitas-humedas", 
    "mundo-bebe/cuidado-maternal", 
    "mundo-bebe/cochecitos-y-butacas", 
    "mundo-bebe/chupetes-mamaderas-y-accesorios", 
    "mundo-bebe/higiene-para-bebes",
    "mundo-bebe/cunas-y-muebles",

    "mascotas/alimentos-para-perros",
    "mascotas/alimentos-para-gatos", 
    "mascotas/higiene-para-mascotas", 
    "mascotas/accesorios-para-mascotas", 

    "indumentaria/adultos",
    "indumentaria/bebes", 
    "indumentaria/ninos-y-ninas", 

    "jugueteria-y-libreria/libreria",
    "jugueteria-y-libreria/libros", 
    "jugueteria-y-libreria/jugueteria", 

    "automotor/todo-para-el-auto",
    "automotor/todo-para-la-moto", 
    "automotor/herramientas-vehiculares", 

    "aire-libre-y-ocio/deportes-y-tiempo-libre", 
    "aire-libre-y-ocio/aire-libre", 
    "aire-libre-y-ocio/piletas-y-accesorios", 
    "aire-libre-y-ocio/viajes", 
    "aire-libre-y-ocio/parrillas-y-accesorios"
    ]
    
    url_base = "https://www.carrefour.com.ar/"
    todos_los_eans = set()

    async with async_playwright() as pw:
        # headless=False para ver el navegador (cambiar a True para ejecutar en servidores sin pantalla)
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        for categoria in categorias:
            logger.info(f"\n--- EXPLORANDO: Categoria {categoria} ---")
            pagina = 1

            while True:
                url_paginada = f"{url_base}{categoria}?page={pagina}"
                logger.info(f"Pagina {pagina} -> {url_paginada}")
                
                try:
                    # Cambiamos a domcontentloaded pero agregamos una espera explicita despues
                    await page.goto(url_paginada, wait_until="domcontentloaded", timeout=60000)
                    
                    # 1. Esperamos a que la pagina decida si hay productos o si esta vacia
                    try:
                        # Aumentamos el timeout a 30 segundos (30000 ms) para sitios lentos
                        await page.wait_for_selector(
                            '.valtech-carrefourar-search-result-3-x-gallery, .valtech-carrefourar-search-result-3-x-notFoundChildren', 
                            timeout=30000
                        )
                    except Exception:
                        logger.error(f"Timeout en la pagina {pagina}. La pagina no cargo correctamente.")
                        break

                    # Pausa estabilizadora para permitir que los scripts JSON-LD se inyecten
                    await page.wait_for_timeout(2000)

                    # 2. Validacion de pagina vacia
                    es_pagina_vacia = await page.locator('.valtech-carrefourar-search-result-3-x-notFoundChildren').is_visible()
                    if es_pagina_vacia:
                        logger.info(f"Fin de categoria {categoria}: La pagina {pagina} no contiene productos.")
                        break

                    # 3. Scroll progresivo para forzar la carga dinamica (lazy loading)
                    for _ in range(5):
                        await page.evaluate("window.scrollBy(0, 1000)")
                        await page.wait_for_timeout(800)
                    
                    # 4. Extraer todos los scripts JSON-LD de la pagina
                    script_tags = await page.locator('script[type="application/ld+json"]').all_inner_texts()
                    
                    nuevos_en_esta_pagina = 0
                    resultados_csv = []

                    for text in script_tags:
                        try:
                            data = json.loads(text)
                            # Buscamos especificamente el bloque que contiene la lista de productos
                            if data.get('@type') == 'ItemList' and 'itemListElement' in data:
                                for elemento in data['itemListElement']:
                                    item = elemento.get('item', {})
                                    
                                    ean = item.get('mpn', '').strip()
                                    nombre = item.get('name', 'N/A').strip()
                                    marca = item.get('brand', {}).get('name', 'N/A').strip()
                                    
                                    # Si no hay EAN o ya lo guardamos, lo saltamos
                                    if not ean or ean in todos_los_eans:
                                        continue
                                    
                                    presentacion = extraer_presentacion(nombre)
                                    
                                    # Fila: ID/EAN, NOMBRE, MARCA, PRESENTACION, ID_CATEGORIA
                                    resultados_csv.append([ean, nombre, marca, presentacion, categoria])
                                    todos_los_eans.add(ean)
                                    nuevos_en_esta_pagina += 1
                        except json.JSONDecodeError:
                            continue
                    
                    # 5. Guardado en CSV y validacion de continuidad
                    if nuevos_en_esta_pagina > 0:
                        with open(archivo_salida, mode='a', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                            writer.writerows(resultados_csv)
                        logger.info(f"Pagina {pagina}: +{nuevos_en_esta_pagina} nuevos (Total guardados: {len(todos_los_eans)})")
                    else:
                        logger.info(f"Pagina {pagina}: No se detectaron EANs nuevos. Finalizando categoria por seguridad.")
                        break
                    
                    pagina += 1
                    
                except Exception as e:
                    logger.error(f"Error procesando {categoria} pag {pagina}: {e}")
                    break

        await browser.close()
    
    logger.info(f"PROCESO TERMINADO. Total productos unicos guardados: {len(todos_los_eans)}")

if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        logger.info("Iniciando como tarea en loop existente...")
        loop.create_task(main())
    else:
        logger.info("Iniciando con asyncio.run()...")
        asyncio.run(main())