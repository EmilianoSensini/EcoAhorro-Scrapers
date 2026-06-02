import os
import csv
import sys
import argparse

DATABASE_URL = os.environ.get("DATABASE_URL")


def subir(ruta_csv: str) -> dict:
    if not DATABASE_URL:
        print("  DATABASE_URL no definida, salteando subida a DB")
        return {"insertados": 0}

    import psycopg2

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur, open(ruta_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            filas = list(reader)
            for fila in filas:
                cur.execute(
                    """
                    INSERT INTO "PreciosUnificados"
                        ("id", "idProducto", "idSupermercado", "precio", "actualizacion")
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ("id") DO UPDATE SET
                        "precio" = EXCLUDED."precio",
                        "actualizacion" = EXCLUDED."actualizacion"
                    """,
                    (
                        fila["id"],
                        fila["idProducto"],
                        fila["idSupermercado"],
                        float(fila["precio"]),
                        fila["actualizacion"],
                    ),
                )
        conn.commit()
        n = len(filas)
        print(f"  {n} registros insertados/actualizados en PreciosUnificados")
        return {"insertados": n}
    except Exception as e:
        print(f"  Error en subida DB: {e}")
        conn.rollback()
        return {"insertados": 0}
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Sube tabla maestra a PostgreSQL")
    parser.add_argument("csv", nargs="?", default=None,
                        help="Ruta al CSV a subir. Si no se especifica, usa data/tabla_maestra.csv")
    args = parser.parse_args()

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ruta = args.csv or os.path.join(base, "data", "tabla_maestra.csv")

    if not os.path.exists(ruta):
        print(f"Error: no se encuentra {ruta}")
        sys.exit(1)

    print(f"Subiendo {ruta} a la base de datos...")
    stats = subir(ruta)
    print(f"Resultado: {stats['insertados']} registros")


if __name__ == "__main__":
    main()
