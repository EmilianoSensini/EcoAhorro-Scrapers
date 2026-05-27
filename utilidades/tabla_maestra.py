import os
import csv
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ean_utils import normalizar_ean

CARPETA_DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
ARCHIVO_CONSOLIDADO = os.path.join(CARPETA_DATA, "consolidado_precios.csv")
ARCHIVO_CATALOGO = os.path.join(CARPETA_DATA, "catalogo_productos_normalizado.csv")
ARCHIVO_SALIDA = os.path.join(CARPETA_DATA, "tabla_maestra.csv")


def cargar_catalogo(ruta: str) -> set:
    catalogo = set()
    with open(ruta, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columna_id = "ID/EAN" if "ID/EAN" in reader.fieldnames else "idProducto"
        for fila in reader:
            id_raw = fila.get(columna_id, "")
            id_normalizado = normalizar_ean(id_raw)
            if id_normalizado:
                catalogo.add(id_normalizado)
    return catalogo


def procesar() -> dict:
    print("Cargando catálogo...")
    catalogo = cargar_catalogo(ARCHIVO_CATALOGO)
    print(f"  Productos en catálogo: {len(catalogo)}")
    print()

    print("Filtrando y deduplicando...")
    mejores: dict[str, list] = {}
    total_leidos = 0
    catalogo_filtrados = 0
    no_catalogo = 0

    with open(ARCHIVO_CONSOLIDADO, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for fila in reader:
            total_leidos += 1
            id_producto = fila["idProducto"]

            if id_producto not in catalogo:
                no_catalogo += 1
                continue

            catalogo_filtrados += 1

            id_compuesto = fila["id"]
            actualizacion = fila["actualizacion"]

            if id_compuesto in mejores:
                if actualizacion > mejores[id_compuesto][4]:
                    mejores[id_compuesto] = [
                        id_compuesto,
                        id_producto,
                        fila["idSupermercado"],
                        fila["precio"],
                        actualizacion
                    ]
            else:
                mejores[id_compuesto] = [
                    id_compuesto,
                    id_producto,
                    fila["idSupermercado"],
                    fila["precio"],
                    actualizacion
                ]

    with open(ARCHIVO_SALIDA, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])
        writer.writerows(mejores.values())

    return {
        "leidos": total_leidos,
        "no_catalogo": no_catalogo,
        "catalogo_validos": catalogo_filtrados,
        "unicos_final": len(mejores)
    }


def main():
    if not os.path.exists(ARCHIVO_CONSOLIDADO):
        print(f"Error: no se encuentra {ARCHIVO_CONSOLIDADO}")
        print("Ejecuta primero consolidar_precios.py")
        sys.exit(1)

    if not os.path.exists(ARCHIVO_CATALOGO):
        print(f"Error: no se encuentra {ARCHIVO_CATALOGO}")
        print("Ejecuta primero normalizar_catalogo.py")
        sys.exit(1)

    stats = procesar()

    print()
    print("Resumen:")
    print(f"  Leídos:            {stats['leidos']}")
    print(f"  Fuera de catálogo: {stats['no_catalogo']}")
    print(f"  Válidos catálogo:  {stats['catalogo_validos']}")
    print(f"  Únicos (último):   {stats['unicos_final']}")
    print(f"  Salida:            {ARCHIVO_SALIDA}")


if __name__ == "__main__":
    main()
