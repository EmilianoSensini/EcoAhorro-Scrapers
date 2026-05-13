import csv

def es_ean_valido(valor):
    """
    Verifica si el string es un EAN/UPC válido.
    Debe contener solo dígitos y tener una longitud de 8, 12, 13 o 14.
    """
    valor = str(valor).strip()
    return valor.isdigit() and len(valor) in [8, 12, 13, 14]

def filtrar_csv():
    archivo_entrada = "productos_carrefour.csv"
    archivo_salida = "productos_carrefour_filtrados.csv"

    try:
        with open(archivo_entrada, mode='r', encoding='utf-8') as f_in, \
             open(archivo_salida, mode='w', newline='', encoding='utf-8') as f_out:
            
            # Usamos DictReader y DictWriter para manejar fácilmente las columnas
            lector = csv.DictReader(f_in)
            columnas = lector.fieldnames
            
            # Configuramos el escritor para mantener las comillas en todos los campos
            escritor = csv.DictWriter(f_out, fieldnames=columnas, quoting=csv.QUOTE_ALL)
            escritor.writeheader()
            
            total_procesados = 0
            total_guardados = 0
            
            for fila in lector:
                total_procesados += 1
                ean_actual = fila.get("ID/EAN", "")
                
                if es_ean_valido(ean_actual):
                    escritor.writerow(fila)
                    total_guardados += 1
                    
        print(f"Proceso finalizado con éxito.")
        print(f"Productos evaluados: {total_procesados}")
        print(f"Productos guardados con EAN válido: {total_guardados}")
        print(f"Descartados: {total_procesados - total_guardados}")
        print(f"Archivo generado: {archivo_salida}")

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo '{archivo_entrada}'. Asegúrate de que esté en la misma carpeta.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    filtrar_csv()