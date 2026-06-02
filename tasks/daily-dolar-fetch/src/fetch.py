import os
import sys
import logging
import requests
from datetime import datetime, timezone, timedelta

AR_TZ = timezone(timedelta(hours=-3))

DATABASE_URL = os.environ.get("DATABASE_URL")
API_URL = "https://dolarapi.com/v1/dolares/blue"

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "dolar.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def fetch_dolar_blue() -> float:
    logger.info(f"Consultando {API_URL}")
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    precio_venta = data.get("venta")
    if precio_venta is None:
        raise ValueError(f"Campo 'venta' no encontrado en la respuesta: {data}")
    logger.info(f"Precio dolar blue (venta): ${precio_venta}")
    return float(precio_venta)


def save_dolar_price(precio: float) -> None:
    import psycopg2

    fecha_argentina = datetime.now(AR_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    fecha_utc = fecha_argentina.astimezone(timezone.utc)

    logger.info(f"Guardando precio para fecha: {fecha_utc.isoformat()}")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "precioDolar" ("fechaGuardado", "precioPromedio")
                VALUES (%s, %s)
                ON CONFLICT ("fechaGuardado") DO UPDATE SET
                    "precioPromedio" = EXCLUDED."precioPromedio"
                """,
                (fecha_utc, precio),
            )
        conn.commit()
        logger.info(f"Precio guardado exitosamente: ${precio} para {fecha_utc.date()}")
    except Exception as e:
        logger.error(f"Error al guardar en la base de datos: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    logger.info("=" * 50)
    logger.info("Inicio de daily-dolar-fetch")

    if not DATABASE_URL:
        logger.error("DATABASE_URL no definida")
        sys.exit(1)

    try:
        precio = fetch_dolar_blue()
        save_dolar_price(precio)
        logger.info("Proceso completado exitosamente")
    except Exception as e:
        logger.error(f"Error en el proceso: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
