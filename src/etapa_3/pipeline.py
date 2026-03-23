import logging
from sqlalchemy import text
from conexiones import obtener_conexion_postgres
from extractor import extraer_datos_oracle
from transformador import transformar_datos
from cargador import cargar_datos_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preparar_entorno_destino():
    """Limpia la tabla destino para evitar duplicados en cada prueba."""
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