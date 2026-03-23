import pandas as pd
from conexiones import obtener_conexion_postgres
import logging

def cargar_datos_postgres(df: pd.DataFrame, nombre_tabla: str) -> None:
    """
    Inserta un DataFrame directamente en PostgreSQL.
    """
    motor = obtener_conexion_postgres()
    if not motor:
        raise ConnectionError("No se pudo establecer conexión con PostgreSQL.")
        
    logging.info(f"Cargando lote de {len(df)} registros en PostgreSQL (Tabla: {nombre_tabla})...")
    
    # to_sql convierte el DataFrame en sentencias INSERT automáticamente
    # if_exists='append' asegura que agreguemos los datos sin borrar los anteriores
    df.to_sql(nombre_tabla, con=motor, if_exists='append', index=False)