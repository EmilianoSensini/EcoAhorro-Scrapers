import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

AR_TZ = timezone(timedelta(hours=-3))

DATABASE_URL = os.environ.get("DATABASE_URL")

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "aggregate.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def parse_database_url(database_url: str) -> dict:
    parsed = urlparse(database_url)
    params = parse_qs(parsed.query)
    ssl_mode = params.get("sslmode", ["prefer"])[0]
    return {
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "sslmode": ssl_mode,
    }


def calcular_precios_promedio(cur) -> list:
    logger.info("Calculando precios promedio por producto...")
    cur.execute(
        """
        SELECT "idProducto", AVG("precio") as precioPromedio
        FROM "PreciosUnificados"
        GROUP BY "idProducto"
        """
    )
    resultados = cur.fetchall()
    logger.info(f"Se obtuvieron {len(resultados)} precios promedio")
    return resultados


def guardar_historial(precios_promedio: list) -> int:
    import psycopg2

    fecha_argentina = datetime.now(AR_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    fecha_utc = fecha_argentina.astimezone(timezone.utc)

    logger.info(f"Guardando historial para fecha: {fecha_utc.isoformat()}")

    db_params = parse_database_url(DATABASE_URL)
    conn = psycopg2.connect(**db_params)
    guardados = 0
    try:
        with conn.cursor() as cur:
            for ean, precio in precios_promedio:
                cur.execute(
                    """
                    INSERT INTO "HistorialPrecios" ("id", "fechaGuardado", "precioPromedio")
                    VALUES (%s, %s, %s)
                    ON CONFLICT ("id", "fechaGuardado") DO UPDATE SET
                        "precioPromedio" = EXCLUDED."precioPromedio"
                    """,
                    (ean, fecha_utc, precio),
                )
                guardados += 1
        conn.commit()
        logger.info(f"{guardados} registros guardados en HistorialPrecios")
    except Exception as e:
        logger.error(f"Error al guardar en la base de datos: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    return guardados


def main():
    logger.info("=" * 50)
    logger.info("Inicio de price-history-aggregator")

    if not DATABASE_URL:
        logger.error("DATABASE_URL no definida")
        sys.exit(1)

    try:
        import psycopg2

        db_params = parse_database_url(DATABASE_URL)
        conn = psycopg2.connect(**db_params)
        try:
            with conn.cursor() as cur:
                precios_promedio = calcular_precios_promedio(cur)
        finally:
            conn.close()

        guardados = guardar_historial(precios_promedio)

        logger.info(f"Proceso completado exitosamente: {guardados} registros guardados")
    except Exception as e:
        logger.error(f"Error en el proceso: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
