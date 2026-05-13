import csv
import os

# ==========================================
# VARIABLES DE ARCHIVOS
# ==========================================
ARCHIVO_BASE = 'productos_coope-vea-mas.csv'
ARCHIVO_NUEVOS = 'productos_labanderita.csv'
ARCHIVO_SALIDA = 'productos_coope-vea-mas-labanderita.csv'
# ==========================================

def unificar_catalogos():
    # Verificamos que los archivos de entrada existan
    if not os.path.exists(ARCHIVO_BASE):
        print(f"Error: No se encontro el archivo {ARCHIVO_BASE}")
        return
    if not os.path.exists(ARCHIVO_NUEVOS):
        print(f"Error: No se encontro el archivo {ARCHIVO_NUEVOS}")
        return

    eans_existentes = set()
    filas_finales = []
    campos = []

    # 1. Leer el primer archivo (base)
    with open(ARCHIVO_BASE, mode='r', encoding='utf-8') as f1:
        reader1 = csv.DictReader(f1)
        campos = reader1.fieldnames
        
        for fila in reader1:
            eans_existentes.add(fila['ID/EAN'])
            filas_finales.append(fila)
            
    cantidad_original = len(filas_finales)

    # 2. Leer el segundo archivo y buscar los faltantes
    productos_nuevos = 0
    with open(ARCHIVO_NUEVOS, mode='r', encoding='utf-8') as f2:
        reader2 = csv.DictReader(f2)
        
        for fila in reader2:
            ean = fila['ID/EAN']
            # Si el EAN no esta en el archivo base, lo agregamos
            if ean not in eans_existentes:
                filas_finales.append(fila)
                # Lo agregamos al set para evitar duplicados si el archivo 2 tiene EANs repetidos
                eans_existentes.add(ean)
                productos_nuevos += 1

    # 3. Escribir el nuevo archivo combinado
    with open(ARCHIVO_SALIDA, mode='w', newline='', encoding='utf-8') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(filas_finales)

    # Mostrar resumen
    print("Proceso completado con exito.")
    print("-" * 30)
    print(f"Productos en el archivo base: {cantidad_original}")
    print(f"Productos nuevos agregados:   {productos_nuevos}")
    print(f"Total en el nuevo archivo:    {len(filas_finales)}")
    print(f"Guardado como:                {ARCHIVO_SALIDA}")


if __name__ == '__main__':
    unificar_catalogos()