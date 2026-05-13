import pandas as pd

# 1. Define aquí los nombres de tus archivos
archivo_datos_completos = 'catalogo_unificado.csv'  # El que tiene todas las columnas (NOMBRE, MARCA, etc.)
archivo_solo_codigos = 'mapeo_coope_filtrado.csv'     # El que tiene las columnas ean, codigo_interno_coope
archivo_nuevo = 'resultado.csv'            # El nombre que quieres para el archivo creado

try:
    # Leer el primer archivo (datos completos)
    # Usamos dtype=str para evitar que los números largos (códigos de barras) se dañen
    df_principal = pd.read_csv(archivo_datos_completos, dtype={'ID/EAN': str})
    
    # Leer el segundo archivo (el filtro)
    df_filtro = pd.read_csv(archivo_solo_codigos, dtype={'ean': str})
    
    # Extraer solo los códigos del segundo archivo
    codigos_a_filtrar = df_filtro['ean'].unique()
    
    # Filtrar el archivo principal para dejar solo los que coinciden
    df_final = df_principal[df_principal['ID/EAN'].isin(codigos_a_filtrar)]
    
    # Guardar el archivo nuevo
    df_final.to_csv(archivo_nuevo, index=False)
    
    print("Archivo creado con éxito.")
    print("Total de productos guardados:", len(df_final))

except FileNotFoundError as e:
    print("Error: No se pudo encontrar uno de los archivos. Verifica los nombres. Detalle:", e)
except KeyError as e:
    print("Error: Una de las columnas esperadas no existe en el archivo. Detalle:", e)
except Exception as e:
    print("Ocurrió un error inesperado:", e)