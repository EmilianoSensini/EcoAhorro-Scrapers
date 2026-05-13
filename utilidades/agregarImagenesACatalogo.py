import csv

# --- VARIABLES GLOBALES DE CONFIGURACIÓN ---
ARCHIVO_CATALOGO = "catalogo_actualizado.csv"
ARCHIVO_SALIDA = "catalogo_actualizado.csv"
ARCHIVO_IMAGENES = "imagenes_laBanderita.csv"

# Diccionario global para mantener los datos en memoria
diccionario_imagenes = {}

def cargar_mapeos_imagenes():
    """
    Lee el archivo CSV definido en la variable global ARCHIVO_IMAGENES
    y guarda los datos en el diccionario global. No utiliza parámetros.
    """
    global diccionario_imagenes
    diccionario_imagenes.clear()
    
    try:
        with open(ARCHIVO_IMAGENES, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ean = row.get('ID/EAN', row.get('EAN', row.get('ean', ''))).strip()
                imagen = row.get('IMAGEN', row.get('url', row.get('url_imagen', ''))).strip()
                
                if ean and imagen and imagen.upper() != "N/A":
                    if ean not in diccionario_imagenes:
                        diccionario_imagenes[ean] = imagen
                        
        print(f"Archivo leído correctamente: {ARCHIVO_IMAGENES}")
        print(f"Se cargaron {len(diccionario_imagenes)} imágenes únicas en memoria.\n")
        
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de imágenes '{ARCHIVO_IMAGENES}'")
    except Exception as e:
        print(f"Error procesando '{ARCHIVO_IMAGENES}': {e}")

def actualizar_catalogo():
    """
    Lee el catálogo maestro, cruza los datos con el diccionario global
    y genera el archivo de salida. No utiliza parámetros.
    """
    if not diccionario_imagenes:
        print("El diccionario de imágenes está vacío. No hay datos para cruzar.")
        return

    filas_actualizadas = []
    contador_actualizados = 0
    
    try:
        with open(ARCHIVO_CATALOGO, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            encabezados = list(reader.fieldnames)
            
            # Aseguramos que la columna IMAGEN exista en los encabezados
            if 'IMAGEN' not in encabezados:
                encabezados.append('IMAGEN')
            
            for row in reader:
                ean = row.get('ID/EAN', '').strip()
                imagen_actual = row.get('IMAGEN', '').strip()
                
                # Si no tiene imagen asignada, buscamos en el diccionario
                if not imagen_actual or imagen_actual.upper() == "N/A":
                    if ean in diccionario_imagenes:
                        row['IMAGEN'] = diccionario_imagenes[ean]
                        contador_actualizados += 1
                    else:
                        row['IMAGEN'] = row.get('IMAGEN', 'N/A')
                
                filas_actualizadas.append(row)
                
    except FileNotFoundError:
        print(f"Error crítico: No se encontró el catálogo principal '{ARCHIVO_CATALOGO}'")
        return

    # Guardar los resultados en el archivo de salida
    with open(ARCHIVO_SALIDA, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=encabezados, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(filas_actualizadas)
        
    print("Proceso finalizado con éxito.")
    print(f"Se añadieron imágenes nuevas a {contador_actualizados} productos.")
    print(f"Archivo generado: {ARCHIVO_SALIDA}")

if __name__ == "__main__":
    print("Iniciando cruce de datos...\n")
    
    # Se llama a las funciones sin pasarles ningún argumento
    cargar_mapeos_imagenes()
    actualizar_catalogo()