"""
import logging
from sqlalchemy import text
from conexiones import obtener_conexion_postgres
from extractor import extraer_datos_oracle
from transformador import transformar_datos
from cargador import cargar_datos_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preparar_entorno_destino():
    ===Limpia la tabla destino para evitar duplicados en cada prueba.===
    motor = obtener_conexion_postgres()
    with motor.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS candidatos_etl"))
        logging.info("Tabla 'candidatos_etl' reiniciada en PostgreSQL.")

def ejecutar_pipeline():
    logging.info("=== Iniciando Pipeline ETL Modular ===")
    preparar_entorno_destino()
    
    query_origen = "SELECT id_candidato, nombre, profesion, experiencia_anios FROM candidatos_ape"
    
    try:
        # 1. Extracción (Obtenemos el iterador)
        iterador_lotes = extraer_datos_oracle(query_origen, tamano_lote=2)
        
        lotes_procesados = 0
        for lote in iterador_lotes:
            lotes_procesados += 1
            logging.info(f"--- Procesando Lote #{lotes_procesados} ---")
            
            # 2. Transformación
            lote_transformado = transformar_datos(lote)
            
            # 3. Carga
            cargar_datos_postgres(lote_transformado, 'candidatos_etl')
            
        logging.info("=== Pipeline ETL finalizado con éxito ===")
        
    except Exception as e:
        logging.error(f"Error crítico en el pipeline ETL: {e}")

if __name__ == "__main__":
    ejecutar_pipeline()
"""    
# Actualizador del pipeline para incluir validación después de cada lote procesado
import logging
from sqlalchemy import text
from conexiones import obtener_conexion_postgres
from extractor import extraer_datos_oracle
from transformador import transformar_datos
from cargador import cargar_datos_postgres

# Importamos las nuevas funciones de validación
from validacion import validar_esquema, generar_checksum, validar_conteos_cruzados

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preparar_entorno_destino():
    motor = obtener_conexion_postgres()
    with motor.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS candidatos_etl"))
        logging.info("Tabla 'candidatos_etl' reiniciada en PostgreSQL.")

def ejecutar_pipeline():
    logging.info("=== Iniciando Pipeline ETL Modular con Validación ===")
    preparar_entorno_destino()
    
    query_origen = "SELECT id_candidato, nombre, profesion, experiencia_anios FROM candidatos_ape"
    
    try:
        iterador_lotes = extraer_datos_oracle(query_origen, tamano_lote=2)
        lotes_procesados = 0
        
        for lote in iterador_lotes:
            lotes_procesados += 1
            logging.info(f"\n--- Procesando Lote #{lotes_procesados} ---")
            
            # Generamos el hash en el origen
            hash_origen = generar_checksum(lote)
            logging.info(f"Checksum del lote original: {hash_origen}")
            
            # Transformación
            lote_transformado = transformar_datos(lote)
            
            # Validación de Integridad (Pandera)
            if not validar_esquema(lote_transformado):
                logging.error("El lote no pasó la validación. Saltando inserción de este lote.")
                continue
            
            # Carga
            cargar_datos_postgres(lote_transformado, 'candidatos_etl')
            
        logging.info("\n=== Pipeline ETL finalizado. Iniciando auditoría final ===")
        
        # Ejecutamos el conteo cruzado al terminar todo el proceso
        validar_conteos_cruzados()
        
    except Exception as e:
        logging.error(f"Error crítico en el pipeline ETL: {e}")

if __name__ == "__main__":
    ejecutar_pipeline()