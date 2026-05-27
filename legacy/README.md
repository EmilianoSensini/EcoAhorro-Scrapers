# Legacy — Scripts de construcción del catálogo

Esta carpeta contiene scripts que fueron utilizados durante la fase inicial del proyecto para construir el catálogo base de productos y realizar ingeniería inversa sobre la API de Cooperativa Obrera. Ya no forman parte del pipeline ETL activo, pero se conservan como referencia.

## Contenido

### `utilities/`
Herramientas para unificar, filtrar y enriquecer el catálogo de productos.
- `unificadorCatalogos.py` — Unifica CSVs de distintos supermercados en un solo catálogo.
- `filtrarSoloEansValidos.py` — Filtra productos con EAN inválido (no numérico o largo incorrecto).
- `filtrarEncontrados.py` — Filtra productos encontrados vs no encontrados en búsquedas.
- `buscarProductosQueEstanEnLaCoope.py` — Cruza el catálogo contra los productos disponibles en la Cooperativa Obrera.
- `reemplazarCodigoCoopePorEan.py` — Reemplaza códigos internos de la Coope por EANs reales.
- `agregarImagenesACatalogo.py` — Agrega columnas de imágenes al catálogo.

### `ean_mapping/`
Scripts para ingeniería inversa del sitio de Cooperativa Obrera: mapeo entre EAN (código de barras universal) y el código interno que usa el sitio.
- `eanScrappingCoope.py` — Scraper que, dado un EAN, obtiene el código interno de la Coope.
- `mapeoCoopeParalelo.py` — Versión paralela/concurrente del mapeo masivo EAN → código Coope.
- `eanGeneralMapping.py` — Mapeo general del catálogo completo.

### `product_searching/`
Scripts para búsqueda de productos en cada supermercado y descarga de imágenes. Usados para enriquecer el catálogo con datos faltantes.
- `carrefourProductsSearching.py`, `changoMasProductsSearching.py`, `laBanderitaProductsSearching.py`, `veaProductsSearching.py` — Búsqueda de productos por nombre en cada supermercado.
- `image_searching/` — Scripts y CSVs con URLs de imágenes descargadas.
- `preciosClarosProductsSearching.js` — Script JS para búsqueda en Precios Claros.

### `run_all_scrapers.py`
Primer intento de orquestador de scrapers. Fue reemplazado por `utilidades/ejecutar_etl.py` que corre los scrapers como subprocesos asíncronos, con reintentos y logging centralizado.
