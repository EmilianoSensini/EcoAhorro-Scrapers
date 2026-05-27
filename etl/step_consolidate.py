import os
import csv
import glob
from datetime import datetime

CARPETA_PRECIOS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "prices"
)
ARCHIVO_SALIDA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "consolidado_precios.csv"
)
COLUMNAS = ["id", "idProducto", "idSupermercado", "precio", "actualizacion"]


def consolidar() -> dict:
    patron = os.path.join(CARPETA_PRECIOS, "precios_*.csv")
    archivos = sorted(glob.glob(patron))
    total_leidos = 0
    archivos_procesados = 0

    with open(ARCHIVO_SALIDA, "w", newline="", encoding="utf-8") as f_salida:
        writer = csv.writer(f_salida, quoting=csv.QUOTE_ALL)
        writer.writerow(COLUMNAS)

        for ruta in archivos:
            nombre = os.path.basename(ruta)
            try:
                with open(ruta, newline="", encoding="utf-8") as f_entrada:
                    reader = csv.DictReader(f_entrada)
                    if reader.fieldnames != COLUMNAS:
                        print(f"  Saltando {nombre}: encabezados no coinciden")
                        continue
                    filas = 0
                    for fila in reader:
                        writer.writerow([
                            fila["id"],
                            fila["idProducto"],
                            fila["idSupermercado"],
                            fila["precio"],
                            fila["actualizacion"]
                        ])
                        filas += 1
                    total_leidos += filas
                    archivos_procesados += 1
                    print(f"  {nombre}: {filas} registros")
            except Exception as e:
                print(f"  Error en {nombre}: {e}")

    return {"archivos": archivos_procesados, "registros": total_leidos}


def main():
    print("Consolidando precios...")
    print(f"Origen: {CARPETA_PRECIOS}")
    print()

    stats = consolidar()

    print()
    print(f"Archivos procesados: {stats['archivos']}")
    print(f"Registros totales:   {stats['registros']}")
    print(f"Salida:              {ARCHIVO_SALIDA}")


if __name__ == "__main__":
    main()
