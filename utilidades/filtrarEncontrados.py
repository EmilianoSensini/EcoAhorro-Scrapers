import pandas as pd

# 1. Cargar el archivo original (asegúrate de poner el nombre correcto de tu archivo)
df = pd.read_csv('mapeo_codigo_coope.csv')

# 2. Mantener solo las filas donde el código sea diferente a "No encontrado"
df_limpio = df[df['codigo_interno_coope'] != 'No encontrado']

# 3. Guardar el resultado en un nuevo archivo CSV
df_limpio.to_csv('mapeo_coope_filtrado.csv', index=False)

print("Las filas han sido eliminadas correctamente.")