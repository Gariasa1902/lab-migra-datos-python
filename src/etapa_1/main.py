import pandas as pd
from pathlib import Path
import logging

# Importamos la función que creamos en nuestro otro módulo
from procesamiento import procesar_datos_masivos

# Configuramos el logging para este archivo también
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def generar_csv_prueba(ruta: Path, cantidad_filas: int = 25000) -> None:
    """
    Genera un archivo CSV de prueba con datos simulados si no existe previamente.
    """
    if ruta.exists():
        logging.info(f"El archivo {ruta.name} ya existe. Omitiendo la creación.")
        return

    logging.info(f"Generando archivo CSV de prueba '{ruta.name}' con {cantidad_filas} registros...")
    
    # Creamos un diccionario con datos simulados utilizando comprensión de listas
    datos = {
        "id_candidato": range(1, cantidad_filas + 1),
        "nombre": [f"Candidato_{i}" for i in range(1, cantidad_filas + 1)],
        "estado_postulacion": ["Activa" if i % 2 == 0 else "Cerrada" for i in range(1, cantidad_filas + 1)]
    }
    
    # Convertimos el diccionario en un DataFrame de pandas y lo guardamos como CSV
    df = pd.DataFrame(datos)
    df.to_csv(ruta, index=False)
    
    logging.info("Archivo de prueba generado exitosamente.")

# Este bloque es fundamental en Python
if __name__ == '__main__':
    # 1. Definimos la ruta donde queremos el archivo usando pathlib
    ruta_archivo_csv = Path("candidatos_ape_prueba.csv")
    
    # 2. Generamos el archivo de prueba
    generar_csv_prueba(ruta_archivo_csv)
    
    # 3. Ejecutamos nuestra función de procesamiento modular
    logging.info("--- Iniciando proceso de lectura por fragmentos ---")
    procesar_datos_masivos(ruta_archivo_csv)