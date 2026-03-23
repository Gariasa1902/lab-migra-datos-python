import logging
from sqlalchemy import text
from conexiones import obtener_conexion_oracle, obtener_conexion_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ejecutar_migracion():
    """
    Extrae datos de Oracle y los inserta en PostgreSQL manejando transacciones.
    """
    motor_oracle = obtener_conexion_oracle()
    motor_pg = obtener_conexion_postgres()

    if not motor_oracle or not motor_pg:
        logging.error("Conexiones fallidas. Abortando migración.")
        return

    logging.info("--- Iniciando proceso de migración ---")

    # --- PASO 1: EXTRACCIÓN (Desde Oracle) ---
    datos_extraidos = []
    with motor_oracle.connect() as conn_ora:
        resultado = conn_ora.execute(text("SELECT id_candidato, nombre, profesion, experiencia_anios FROM candidatos_ape"))
        
        # Transformamos las filas obtenidas en una lista de diccionarios
        datos_extraidos = [
            {
                "id_candidato": fila[0], 
                "nombre": fila[1], 
                "profesion": fila[2], 
                "experiencia_anios": fila[3]
            } 
            for fila in resultado.fetchall()
        ]
    
    logging.info(f"Se extrajeron {len(datos_extraidos)} registros de Oracle.")

    if not datos_extraidos:
        logging.info("No hay datos para migrar.")
        return

    # --- PASO 2: CARGA (Hacia PostgreSQL con Transacciones) ---
    with motor_pg.connect() as conn_pg:
        # Iniciamos la transacción manualmente para tener control total
        transaccion = conn_pg.begin() 
        try:
            # Crear tabla destino si no existe
            conn_pg.execute(text("""
                CREATE TABLE IF NOT EXISTS candidatos_migrados (
                    id_candidato INTEGER PRIMARY KEY,
                    nombre VARCHAR(100),
                    profesion VARCHAR(100),
                    experiencia_anios INTEGER
                )
            """))

            # Limpiamos la tabla destino para la prueba
            conn_pg.execute(text("TRUNCATE TABLE candidatos_migrados"))

            # Query parametrizada para inserción masiva
            query_insert = text("""
                INSERT INTO candidatos_migrados (id_candidato, nombre, profesion, experiencia_anios)
                VALUES (:id_candidato, :nombre, :profesion, :experiencia_anios)
            """)
            
            conn_pg.execute(query_insert, datos_extraidos)
            
            # Si todo salió bien hasta aquí, guardamos los cambios definitivamente
            transaccion.commit() 
            logging.info("Migración exitosa: Transacción confirmada (COMMIT) en PostgreSQL.")
            
        except Exception as e:
            # Si algo falla (ej. un tipo de dato incorrecto), revertimos absolutamente todo
            transaccion.rollback() 
            logging.error(f"Error en la migración. Se aplicó ROLLBACK para proteger la base de datos. Detalles: {e}")