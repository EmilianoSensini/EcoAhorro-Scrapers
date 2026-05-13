import asyncio
import csv
from eanScrappingCoope import obtenerCodigoCoope

# Configuración de rutas (strings directos)
ARCHIVO_ENTRADA = "catalogo_unificado.csv"
ARCHIVO_SALIDA = "mapeo_codigo_coope2.csv"

# Configuración de concurrencia
CONCURRENCIA_MAXIMA = 10  # Ajusta este número según la estabilidad del sitio
semaphore = asyncio.Semaphore(CONCURRENCIA_MAXIMA)
archivo_lock = asyncio.Lock()

async def procesar_producto(ean, escritor_csv, f_salida):
    """Procesa un solo EAN con control de concurrencia"""
    async with semaphore:
        try:
            codigo_interno = await obtenerCodigoCoope(ean)
            
            # Escritura segura en el archivo
            async with archivo_lock:
                escritor_csv.writerow({
                    "ean": ean,
                    "codigo_interno_coope": codigo_interno if codigo_interno else "No encontrado"
                })
                f_salida.flush()
            
            print(f"Procesado: {ean} -> {codigo_interno}")
        except Exception as e:
            print(f"Error en EAN {ean}: {e}")

async def ejecutar_consulta():
    try:
        # Abrir archivos
        with open(ARCHIVO_ENTRADA, mode='r', encoding='utf-8') as f_entrada, \
             open(ARCHIVO_SALIDA, mode='w', encoding='utf-8', newline='') as f_salida:
            
            # Cargar datos
            datos = list(csv.DictReader(f_entrada))
            campos_salida = ["ean", "codigo_interno_coope"]
            escritor_csv = csv.DictWriter(f_salida, fieldnames=campos_salida)
            escritor_csv.writeheader()
            
            print(f"Iniciando mapeo de {len(datos)} productos...")

            # Crear lista de tareas
            tareas = []
            for fila in datos:
                ean = fila.get("ID/EAN")
                if ean:
                    tareas.append(procesar_producto(ean, escritor_csv, f_salida))

            # Ejecutar todo en paralelo
            await asyncio.gather(*tareas)

        print(f"\nProceso finalizado. Resultados guardados en: {ARCHIVO_SALIDA}")

    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo en la ruta {ARCHIVO_ENTRADA}")
    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    asyncio.run(ejecutar_consulta())