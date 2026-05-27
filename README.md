# EcoAhorro-Scrapers

Pipeline ETL que extrae precios de supermercados argentinos (Cooperativa Obrera, Vea, ChangoMás, La Banderita), los normaliza y los carga en una base de datos PostgreSQL. Se ejecuta diariamente vía GitHub Actions.

## Arquitectura

```
Scrapers (Playwright / Selenium)
  Vea ──┐
  Chango ─┤
  Coope ─┤──► CSVs en data/prices/
  Banderita ┘
                  │
                  ▼
      step_consolidate.py ────► data/consolidado_precios.csv
                  │
                  ▼
        step_master.py ───────► data/tabla_maestra.csv
                  │
                  ▼
        step_upload.py ───────► PostgreSQL (PreciosUnificados)
```

## Estructura del proyecto

```
├── etl/
│   ├── run.py                    # Orquestador del pipeline completo
│   ├── shared.py                 # Utilidades: normalizar_ean, precio_a_float, construir_id
│   ├── step_catalog.py           # Normaliza catálogo de productos (one-time)
│   ├── step_consolidate.py       # Une CSVs individuales en consolidado_precios.csv
│   ├── step_master.py            # Filtra contra catálogo y deduplica → tabla_maestra.csv
│   ├── step_upload.py            # Upsert de tabla_maestra.csv a PostgreSQL
│   └── scrape/
│       ├── base.py               # VtexScraper — clase base para VTEX (Playwright + aiohttp)
│       ├── vea.py                # VeaScraper — async, 17 categorías
│       ├── chango_mas.py         # ChangoMasScraper — async, 15 categorías
│       ├── coope.py              # CoopeScraper — sync, Playwright + BS4
│       ├── labanderita.py        # LaBanderitaScraper — sync, Selenium + BS4
│       └── mapeo_codigo_coope.csv
├── config/
│   └── supermercados_config.json # Mapping idSupermercado → nombre comercial
├── data/
│   └── catalogo_productos.csv    # Catálogo normalizado (EANs 13 dígitos)
├── legacy/                       # Scripts descartados, conservados como referencia
├── tests/
│   ├── test_shared.py            # 27 tests
│   ├── test_step_catalog.py      # 12 tests
│   ├── test_step_consolidate.py  # 3 tests
│   └── test_step_master.py       # 4 tests
├── .github/workflows/etl.yml
└── pyproject.toml
```

## Setup

### Requisitos

- Python >= 3.12
- Playwright Chromium (para scrapers con Playwright)
- ChromeDriver (para La Banderita — Selenium lo maneja automáticamente)

### Instalación

```bash
pip install -e .
playwright install chromium
```

### Variable de entorno (opcional)

```
DATABASE_URL=postgresql://usuario:password@host:5432/ecoahorro
```

Si no está definida, `step_upload.py` saltea la subida a DB sin error.

## Uso

### Pipeline completo

```bash
python -m etl.run
```

Ejecuta: scrapeo → consolidación → tabla maestra → subida DB + limpieza de intermedios.

### Pasos individuales

```bash
python -m etl.step_consolidate
python -m etl.step_master
python -m etl.step_catalog data/catalogo_crudo.csv
```

## Scrapers

| Supermercado    | Clase              | Tecnología                   | Estrategia                          |
|-----------------|--------------------|------------------------------|-------------------------------------|
| Vea             | `VeaScraper`       | Playwright + VTEX API        | Navega categorías, extrae EANs del HTML, consulta precio por API |
| ChangoMás       | `ChangoMasScraper` | Playwright + VTEX API        | Igual que Vea (ambos usan VTEX)     |
| Cooperativa Obrera | `CoopeScraper`  | Playwright + BeautifulSoup   | Navega paginación, mapea código interno → EAN via CSV |
| La Banderita    | `LaBanderitaScraper` | Selenium + BeautifulSoup  | Scroll infinito, extrae EANs del JS embebido |

Todos se ejecutan en modo headless.

## Formato de datos

Todos los CSVs comparten el mismo esquema:

| Columna          | Tipo   | Ejemplo                       | Descripción                              |
|------------------|--------|-------------------------------|------------------------------------------|
| `id`             | string | `779123456789001`             | EAN (13 dígitos) + idSupermercado (2 dígitos) |
| `idProducto`     | string | `7791234567890`               | EAN normalizado a 13 dígitos             |
| `idSupermercado` | string | `01`                          | Código del supermercado                  |
| `precio`         | float  | `1234.56`                     | Precio en ARS, punto como separador decimal |
| `actualizacion`  | string | `2026-05-27T06:00:00+00:00`   | Timestamp ISO 8601 UTC                   |

### Procesamiento de precios

Los precios se scrapean en formato argentino (coma decimal, punto opcional como separador de miles, símbolo `$` opcional). Ejemplos de strings parseables:

| Entrada     | Resultado |
|-------------|-----------|
| `$1.234,56` | `1234.56` |
| `5890`      | `5890.0`  |
| `5,89`      | `5.89`    |
| `$ 1.234`   | `1234.0`  |

## Pipeline detallado

### 1. Scrapeo (`etl/scrape/`)

Cada scraper produce un CSV en `data/prices/precios_<supermercado>_<timestamp>.csv`.

- **VTEX** (`base.py`): abre el supermercado con Playwright, navega cada categoría página por página, extrae todos los EANs (patrón `7\d{12}`) del HTML, consulta la API de VTEX para obtener el precio actual, y guarda. Detecta fin de categoría cuando no aparecen EANs nuevos.
- **Coope**: abre cada categoría con Playwright, hace scroll para cargar productos dinámicos, parsea el HTML con BeautifulSoup, mapea el código interno del producto al EAN usando `mapeo_codigo_coope.csv`.
- **La Banderita**: abre la tienda con Selenium, hace scroll infinito hasta el final, extrae EANs del JavaScript embebido (`a: 'EAN', b: 'nombre'`), cruza contra los elementos del DOM.

### 2. Consolidación (`step_consolidate.py`)

Lee todos los `precios_*.csv` de `data/prices/`, valida que tengan los encabezados correctos, y los fusiona en `data/consolidado_precios.csv`.

### 3. Tabla maestra (`step_master.py`)

Filtra el consolidado contra `data/catalogo_productos.csv`: solo conserva productos cuyo EAN existe en el catálogo. Luego deduplica: si un mismo producto (mismo `id`) aparece varias veces, conserva el más reciente según `actualizacion`. Produce `data/tabla_maestra.csv`.

### 4. Subida a DB (`step_upload.py`)

Lee `tabla_maestra.csv` y hace upsert en la tabla `PreciosUnificados` de PostgreSQL usando `ON CONFLICT ("id") DO UPDATE`. Columna `id` es la clave primaria.

## CI/CD

El pipeline se ejecuta automáticamente en GitHub Actions:

- **Schedule**: una vez al día a las 05:00 UTC (02:00 Argentina)
- **Trigger manual**: vía `workflow_dispatch`

### Workflow (`etl.yml`)

```yaml
steps:
  - checkout
  - setup Python 3.12
  - pip install -e .
  - playwright install chromium
  - python -m etl.run
  - upload artifacts (CSVs)
```

Para activar la subida a DB, agregar el secreto `DATABASE_URL` en la configuración del repositorio (Settings → Secrets and variables → Actions).

## Configuración

### supermercados_config.json

```json
{
    "01": "Cooperativa Obrera",
    "02": "Vea",
    "03": "ChangoMas",
    "04": "La Banderita"
}
```

El `idSupermercado` se usa como prefijo/sufijo en el `id` compuesto y como identificador en la tabla de precios.

### Catálogo de productos

`data/catalogo_productos.csv` contiene los productos que el pipeline considera válidos. Cualquier producto scrapeado que no esté en este catálogo se descarta en `step_master.py`.

### Mapeo Coope

`etl/scrape/mapeo_codigo_coope.csv` traduce los códigos internos que usa el sitio de Cooperativa Obrera a EANs reales. Fue generado durante la fase de construcción del catálogo.

## Testing

```bash
pip install -e .[test]
pytest tests/
```

46 tests que cubren:
- Normalización de EANs (padding a 13 dígitos, validación)
- Parseo de precios en formato argentino
- Construcción de IDs compuestos
- Procesamiento del catálogo (invalidos, duplicados)
- Consolidación de múltiples archivos
- Filtrado y deduplicación de la tabla maestra

## Legacy

La carpeta `legacy/` contiene scripts utilizados durante la construcción inicial del catálogo de productos y la ingeniería inversa de la API de Cooperativa Obrera. No forman parte del pipeline activo. Ver `legacy/README.md` para más detalles.
