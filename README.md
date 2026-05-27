# EcoAhorro-Scrapers

Extrae precios de supermercados argentinos (Cooperativa Obrera, Vea, ChangoMás, La Banderita), los normaliza y los carga en PostgreSQL. Corre automáticamente todos los días.

## Quick start

```bash
pip install -e .
playwright install chromium
python -m etl.run
```

La variable de entorno `DATABASE_URL` es opcional. Si existe, el pipeline sube los precios a PostgreSQL.

## Cómo funciona

```
Scrapers (Vea, ChangoMas, Coope, La Banderita)
  └── CSVs en data/prices/
         │
         ▼
  step_consolidate.py → data/consolidado_precios.csv
         │
         ▼
  step_master.py      → data/tabla_maestra.csv
         │
         ▼
  step_upload.py      → PostgreSQL (PreciosUnificados)
```

Todos los scrapers corren en paralelo. Cada uno guarda un CSV con los precios de su supermercado. El pipeline los une, filtra contra el catálogo, quita duplicados y sube a la base de datos.

## Supermercados

| Nombre | Tecnología | ID |
|---|---|---|
| Vea | Playwright + VTEX API | 02 |
| ChangoMás | Playwright + VTEX API | 03 |
| Cooperativa Obrera | Playwright + BeautifulSoup | 01 |
| La Banderita | Selenium + BeautifulSoup | 04 |

## Formato de salida

Todos los CSVs usan el mismo esquema:

| Columna | Tipo | Ejemplo |
|---|---|---|
| `id` | string | `779123456789001` |
| `idProducto` | string | EAN de 13 dígitos |
| `idSupermercado` | string | `01` |
| `precio` | float | `1234.56` |
| `actualizacion` | string | `2026-05-27T13:00:00+00:00` |

## CI/CD

El pipeline corre automáticamente en GitHub Actions:

- **Schedule**: todos los días a las 16:00 UTC (13:00 hora Argentina)
- **Manual**: botón "Run workflow" desde la pestaña Actions

Para subir a la base de datos en producción, agregar el secret `DATABASE_URL` en:
**Settings → Secrets and variables → Actions**

## Desarrollo

```bash
pip install -e .[test]
pytest tests/
```

46 tests cubren normalización de EANs, parseo de precios, consolidación y generación de tabla maestra.

## Legacy

La carpeta `legacy/` contiene scripts usados para construir el catálogo inicial. Ya no forman parte del pipeline activo.
