import csv
import sys
import os

COLUMNA_ID = "ID/EAN"
LONGITUD_EAN = 13


def normalizar_id(id_raw: str) -> str | None:
    id_str = id_raw.strip().strip('"').strip("'")
    id_str = id_str.zfill(LONGITUD_EAN)
    if len(id_str) == LONGITUD_EAN and id_str.isdigit():
        return id_str
    return None


def procesar(archivo_entrada: str, archivo_salida: str) -> dict:
    vistos: set[str] = set()
    stats = {"leidos": 0, "invalidos": 0, "duplicados": 0, "escritos": 0}

    with (
        open(archivo_entrada, mode="r", encoding="utf-8") as f_in,
        open(archivo_salida, mode="w", encoding="utf-8", newline="") as f_out,
    ):
        lector = csv.DictReader(f_in)
        if COLUMNA_ID not in lector.fieldnames:
            print(f"Error: columna '{COLUMNA_ID}' no encontrada en {archivo_entrada}")
            sys.exit(1)

        escritor = csv.DictWriter(f_out, fieldnames=lector.fieldnames)
        escritor.writeheader()

        for fila in lector:
            stats["leidos"] += 1
            id_normalizado = normalizar_id(fila.get(COLUMNA_ID, ""))

            if id_normalizado is None:
                stats["invalidos"] += 1
                continue

            if id_normalizado in vistos:
                stats["duplicados"] += 1
                continue

            vistos.add(id_normalizado)
            fila[COLUMNA_ID] = id_normalizado
            escritor.writerow(fila)
            stats["escritos"] += 1

    return stats


def main():
    if len(sys.argv) >= 3:
        archivo_entrada = sys.argv[1]
        archivo_salida = sys.argv[2]
    elif len(sys.argv) == 2:
        archivo_entrada = sys.argv[1]
        base, ext = os.path.splitext(archivo_entrada)
        archivo_salida = f"{base}_limpio{ext}"
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        archivo_entrada = os.path.join(
            script_dir, "..", "data", "catalogo_productos.csv"
        )
        archivo_salida = os.path.join(
            script_dir, "..", "data", "catalogo_productos_normalizado.csv"
        )

    archivo_entrada = os.path.abspath(archivo_entrada)
    archivo_salida = os.path.abspath(archivo_salida)

    if not os.path.exists(archivo_entrada):
        print(f"Error: no se encuentra el archivo '{archivo_entrada}'")
        sys.exit(1)

    print(f"Entrada: {archivo_entrada}")
    print(f"Salida:  {archivo_salida}")
    print()

    stats = procesar(archivo_entrada, archivo_salida)

    print("Resumen:")
    print(f"  Leídos:     {stats['leidos']}")
    print(f"  Inválidos:  {stats['invalidos']}")
    print(f"  Duplicados: {stats['duplicados']}")
    print(f"  Escritos:   {stats['escritos']}")


if __name__ == "__main__":
    main()
