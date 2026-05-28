# Documentación de pipeline de datos

Pipeline ETL (Extract, Transform, Load) que extrae precios de supermercados argentinos (Cooperativa Obrera, Vea, ChangoMás, La Banderita), los normaliza y los sube a PostgreSQL.

- **Extract**: extrae precios de 4 supermercados mediante scrapers.
- **Transform**: consolida, filtra contra el catálogo, deduplica y normaliza precios.
- **Load**: sube los datos finales a PostgreSQL.

Corre todos los días de manera automática a la madrugada, permitiendo también disparo manual.

## Quick start

```bash
pip install -e .
playwright install chromium
python -m etl.run
```

## Cómo funciona

```
Scrapers (Vea, ChangoMas, Coope, La Banderita)
  └── CSVs en data/prices/
         │
         ▼
  Paso 1 — Extracción
         │
         ▼
  Paso 2 — Consolidación → data/consolidado_precios.csv
         │
         ▼
  Paso 3 — Limpieza → elimina CSVs temporales
         │
         ▼
  Paso 4 — Tabla maestra → data/tabla_maestra.csv
         │
         ▼
  Paso 5 — Upload → PostgreSQL (solo si DATABASE_URL existe)
```

### Paso 1 — Extracción

Los 4 scrapers se ejecutan en paralelo usando `asyncio.gather()`. Cada uno se conecta al sitio del supermercado, recorre las páginas de productos, extrae los códigos de barras (EAN) y sus precios, y guarda el resultado en un CSV temporal dentro de `data/prices/`. El nombre del archivo incluye un timestamp para evitar colisiones entre ejecuciones.

| Supermercado | ID | Tecnología | Estrategia |
|---|---|---|---|
| Vea | 02 | Playwright + VTEX API | Playwright recorre categorías y extrae EANs del HTML. Luego se consulta la API de VTEX (la plataforma de e-commerce del supermercado) para obtener el precio de cada producto de forma asíncrona. |
| ChangoMás | 03 | Playwright + VTEX API | Similar a Vea pero usa clusters de productos (IDs agrupados) en vez de categorías directas. |
| Cooperativa Obrera | 01 | Playwright + BeautifulSoup | Playwright carga las páginas, BeautifulSoup parsea el HTML. Los códigos internos se traducen a EANs mediante un mapeo predefinido. |
| La Banderita | 04 | Selenium + BeautifulSoup | Selenium carga la página completa, BeautifulSoup extrae los datos. Los EANs se obtienen mediante ingeniería inversa sobre scripts JavaScript embebidos. |

**Salida**: 4 archivos en `data/prices/` (uno por supermercado).

### Paso 2 — Consolidación

Une todos los CSVs individuales generados en el paso 1 en un solo archivo. Valida que cada CSV tenga el esquema correcto (5 columnas). Si un archivo tiene un formato diferente, se omite sin detener el proceso.

**Salida**: `data/consolidado_precios.csv` con todos los registros de todos los supermercados.

### Paso 3 — Limpieza

Elimina los CSVs temporales de `data/prices/` que se generaron en el paso 1. Esto mantiene el directorio limpio entre ejecuciones.

### Paso 4 — Tabla maestra

Esta es la etapa de transformación más importante. Aplica medidas de precaución sobre los datos consolidados para garantizar la integridad y robustez del pipeline:

1. **Filtrado por catálogo**: verifica que cada producto exista en el catálogo de ~13.600 productos. Los productos no catalogados se descartan. Esto evita que entren datos no deseados o incompletos al sistema.

2. **Deduplicación**: si un mismo producto aparece varias veces en la misma ejecución (por un error del scraper, un reintento, o cualquier otra causa), se queda con el registro más reciente. Esto evita duplicados en la base de datos.

3. **Normalización de precios**: convierte los precios de formato argentino (`1.234,56`) a float estándar (`1234.56`). Cada supermercado formatea sus precios de manera distinta en la web; esta normalización asegura que todos los precios queden en un formato uniforme y utilizable, sin depender de cómo cada sitio los presente.

Estas tres acciones convierten datos crudos y potencialmente inconsistentes en una tabla limpia, unificada y confiable.

**Salida**: `data/tabla_maestra.csv`

### Paso 5 — Subida a PostgreSQL

Sube la tabla maestra a la base de datos mediante **UPSERT**: si el producto ya existe (mismo `id`), se actualiza su precio y fecha; si es nuevo, se inserta. Esto significa que la tabla `PreciosUnificados` siempre refleja el último precio conocido de cada producto.

Este paso es **condicional**: solo se ejecuta si la variable de entorno `DATABASE_URL` está configurada. Si no existe, el pipeline igual completa los pasos 1 a 4 y genera los CSVs, pero no toca la base de datos.

## Ingeniería inversa en Cooperativa Obrera

Cooperativa Obrera no expone códigos de barras (EANs) en su sitio. Cada producto tiene un código interno propio (ej: `707432`). Para resolver esto:

1. Se obtuvo una lista de productos de otros supermercados que sí tienen EANs.
2. Se buscaron esos productos en el sitio de Cooperativa y se registró qué código interno le corresponde a cada EAN.
3. El resultado es un CSV de mapeo con ~3.500 asociaciones que se usa durante el scrapeo para identificar cada producto.

## Catálogo de productos

El catálogo es una lista estática de ~13.600 productos con EAN, nombre, marca, presentación e imagen. Se optó por un catálogo estático como simplificación: sin él, cada scraper tendría que obtener información adicional de cada producto, lo que complejizaría el sistema y lo haría más frágil. El catálogo funciona como **whitelist**: solo los precios de productos presentes en él pasan a la tabla maestra.

## Formato de salida

Todos los CSVs usan el mismo esquema:

| Columna | Tipo | Ejemplo |
|---|---|---|
| `id` | string | `027792798007387` |
| `idProducto` | string | EAN de 13 dígitos |
| `idSupermercado` | string | `01` |
| `precio` | float | `1234.56` |
| `actualizacion` | string | `2026-05-27T13:00:00+00:00` |

El campo `id` es un compuesto del código del supermercado (2 dígitos) + EAN (13 dígitos). Esto garantiza unicidad por producto y supermercado.

## Resiliencia del pipeline

- Cada scraper tiene **2 intentos** para ejecutarse. Si el primer intento falla, espera 30 segundos y reintenta.
- Si ambos intentos fallan, el scraper se **ignora silenciosamente** y el pipeline continúa con los demás.
- Esto garantiza que un error en un supermercado no detenga todo el pipeline.
- Los scrapers individuales también manejan errores internos: timeouts de página, respuestas vacías, etc.

## Ejecución autónoma mediante GitHub Actions

[GitHub Actions](https://docs.github.com/en/actions) es un sistema de automatización de GitHub que permite ejecutar tareas programadas o manuales en la nube, sin necesidad de un servidor propio.

En este proyecto se usa para:

- Ejecutar el pipeline todos los días automáticamente.
- Generar los CSVs y subirlos como artefactos descargables.
- Subir precios a PostgreSQL si se configura el secret `DATABASE_URL`.

| Configuración | Valor |
|---|---|
| Horario | 05:00 UTC (02:00 hora Argentina) |
| Manual | Botón "Run workflow" desde la pestaña Actions |
| Artefactos | `consolidado_precios.csv`, `tabla_maestra.csv`, `logs/etl.log` |
| Secret | `DATABASE_URL` en Settings → Secrets and variables → Actions |

## Legacy

La carpeta `legacy/` contiene scripts usados para construir el catálogo inicial. Ya no forman parte del pipeline activo.
