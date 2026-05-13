import csv

# ==========================================
# VARIABLES DE CONFIGURACIÓN DE ARCHIVOS
# ==========================================
archivo_original = "imagenes_laCoope.csv"
archivo_mapeo = "mapeo_codigo_coope.csv"
archivo_salida = "imagenes_coopeEan.csv"
# ==========================================

def procesar_archivos():
    # 1. Leer el archivo de mapeo y crear un diccionario {codigo_interno_coope: ean}
    mapeo_ean = {}
    with open(archivo_mapeo, mode="r", encoding="utf-8") as f_mapeo:
        lector_mapeo = csv.DictReader(f_mapeo)
        for fila in lector_mapeo:
            codigo = fila["codigo_interno_coope"].strip()
            ean = fila["ean"].strip()
            mapeo_ean[codigo] = ean

    # 2. Leer el archivo original, filtrar, reemplazar y escribir en la salida
    with open(archivo_original, mode="r", encoding="utf-8") as f_orig, \
         open(archivo_salida, mode="w", encoding="utf-8", newline="") as f_salida:
        
        lector_orig = csv.DictReader(f_orig)
        
        # Definir las columnas de salida (reemplazamos codigo_interno por ean)
        campos_salida = ["ean", "nombre_producto", "precio_producto", "url_imagen"]
        
        escritor = csv.DictWriter(f_salida, fieldnames=campos_salida)
        escritor.writeheader()

        filas_escritas = 0
        for fila in lector_orig:
            codigo_actual = fila["codigo_interno"].strip()
            
            # Si el código existe en el mapeo, lo incluimos con el nuevo EAN
            if codigo_actual in mapeo_ean:
                nueva_fila = {
                    "ean": mapeo_ean[codigo_actual],
                    "nombre_producto": fila["nombre_producto"],
                    "precio_producto": fila["precio_producto"],
                    "url_imagen": fila["url_imagen"]
                }
                escritor.writerow(nueva_fila)
                filas_escritas += 1

    print(f"Proceso completado. Se guardaron {filas_escritas} productos en '{archivo_salida}'.")

if __name__ == "__main__":
    procesar_archivos()