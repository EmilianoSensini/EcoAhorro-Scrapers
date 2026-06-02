import asyncio
import re
from playwright.async_api import async_playwright

async def obtenerCodigoCoope(ean):
    async with async_playwright() as p:
        # Lanzamos el navegador en modo headless
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        url = f"https://www.lacoopeencasa.coop/listado/busqueda-avanzada/{ean}"
        
        try:
            # Navegación con espera a la carga del DOM
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            
            # Esperar a que el selector de la tarjeta de producto esté presente
            await page.wait_for_selector('[id*="tarjeta-articulo-"]', timeout=10000)
            
            # Extracción del código mediante el contenido del HTML
            html = await page.content()
            match = re.search(r'id="tarjeta-articulo-(\d+)"', html)
            
            return match.group(1) if match else None
            
        except Exception:
            return None
        finally:
            await browser.close()