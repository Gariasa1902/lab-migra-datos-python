import pandas as pd
from pathlib import Path
import logging
from typing import Iterator

# Configuración básica de Logging
# Esto reemplaza al clásico "print()" y es una práctica profesional para rastrear errores.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def leer_csv_en_fragmentos(ruta_archivo: Path, tamaño_fragmento: int = 10000) -> Iterator[pd.DataFrame]:
    """
    Lee un archivo CSV en partes (chunks) usando pandas, devolviendo un generador.
    
    Parámetros:
        ruta_archivo (Path): La ubicación del archivo usando pathlib.
        tamaño_fragmento (int): Cantidad de filas a leer por cada iteración.
        
    Retorna:
        Un iterador que produce DataFrames de pandas.
    """
    logging.info(f"Iniciando la lectura del archivo: {ruta_archivo.name} en bloques de {tamaño_fragmento} filas.")
    
    try:
        # pd.read_csv con el parámetro 'chunksize' no carga todo a la memoria,
        # sino que crea un iterador.
        iterador_csv = pd.read_csv(ruta_archivo, chunksize=tamaño_fragmento)
        
        for chunk in iterador_csv:
            # La palabra reservada 'yield' convierte esta función en un Generador.
            # Pausa la ejecución aquí, entrega el fragmento de datos, y espera a ser llamada de nuevo.
            yield chunk
            
    except FileNotFoundError:
        logging.error(f"Error: No se encontró el archivo en la ruta {ruta_archivo}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al leer el archivo: {e}")
        raise

def procesar_datos_masivos(ruta_archivo: Path) -> None:
    """
    Función principal que consume el generador para procesar los datos.
    Aquí demostramos la modularidad llamando a otra función.
    """
    # Usamos pathlib para asegurar compatibilidad de rutas en cualquier sistema operativo
    if not ruta_archivo.exists():
        logging.error("El archivo no existe. Abortando proceso.")
        return

    # Llamamos a nuestro generador
    generador_datos = leer_csv_en_fragmentos(ruta_archivo, tamaño_fragmento=5000)
    
    filas_procesadas = 0
    
    # Iteramos sobre cada fragmento (DataFrame) que nos entrega el generador
    for fragmento in generador_datos:
        # Aquí iría la lógica de transformación (limpiar nulos, cambiar formatos, etc.)
        # Por ahora, solo contaremos las filas para demostrar que funciona sin desbordar memoria.
        cantidad_filas = len(fragmento)
        filas_procesadas += cantidad_filas
        
        logging.info(f"Procesado un bloque de {cantidad_filas} filas. Total acumulado: {filas_procesadas}")

    logging.info("¡Procesamiento masivo completado con éxito!")