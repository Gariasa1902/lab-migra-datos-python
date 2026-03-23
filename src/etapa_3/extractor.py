import pandas as pd
from typing import Iterator
from conexiones import obtener_conexion_oracle
import logging

def extraer_datos_oracle(query: str, tamano_lote: int = 2) -> Iterator[pd.DataFrame]:
    """
    Ejecuta una consulta en Oracle y devuelve un iterador de DataFrames.
    """
    motor = obtener_conexion_oracle()
    if not motor:
        raise ConnectionError("No se pudo establecer conexión con Oracle.")
    
    logging.info(f"Iniciando extracción desde Oracle en lotes de {tamano_lote} registros...")
    
    # pd.read_sql con chunksize no carga todo a la vez, entrega un generador
    return pd.read_sql(query, con=motor.connect(), chunksize=tamano_lote)