import pandas as pd
import pandera as pa
import hashlib
import logging
from sqlalchemy import text
from conexiones import obtener_conexion_oracle, obtener_conexion_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 1. Validaciones de Integridad (usando Pandera)
# Definimos un "contrato" estricto que los datos deben cumplir antes de cargarse
esquema_candidatos = pa.DataFrameSchema({
    "id_candidato": pa.Column(int, checks=pa.Check.gt(0)), # Debe ser mayor a 0
    "nombre": pa.Column(str, nullable=False), # No puede ser nulo
    "profesion": pa.Column(str, nullable=False),
    "experiencia_anios": pa.Column(int, checks=pa.Check.ge(0)), # Mayor o igual a 0
    "nivel_seniority": pa.Column(str, checks=pa.Check.isin(["Junior", "Semi-Senior", "Senior"]))
})

def validar_esquema(df: pd.DataFrame) -> bool:
    """Valida que el lote de datos cumpla con las reglas de negocio."""
    try:
        esquema_candidatos.validate(df)
        logging.info("Validación de integridad exitosa: El lote cumple con el esquema.")
        return True
    except pa.errors.SchemaError as e:
        logging.error(f"Error de integridad en los datos: {e}")
        return False

# 2. Hash y Checksum
def generar_checksum(df: pd.DataFrame) -> str:
    """
    Genera un hash MD5 a partir de los datos del DataFrame.
    Sirve para comparar si el contenido exacto se mantuvo inalterado.
    """
    # Convertimos los datos a una cadena y calculamos su hash
    datos_string = df.to_json().encode()
    hash_md5 = hashlib.md5(datos_string).hexdigest()
    return hash_md5

# 3. Conteos Cruzados
def validar_conteos_cruzados():
    """Compara la cantidad de registros entre Oracle (Origen) y PostgreSQL (Destino)."""
    motor_oracle = obtener_conexion_oracle()
    motor_pg = obtener_conexion_postgres()

    if not motor_oracle or not motor_pg:
        logging.error("No se pudieron conectar las bases de datos para el conteo cruzado.")
        return

    with motor_oracle.connect() as conn_ora:
        total_oracle = conn_ora.execute(text("SELECT COUNT(*) FROM candidatos_ape")).scalar()

    with motor_pg.connect() as conn_pg:
        total_pg = conn_pg.execute(text("SELECT COUNT(*) FROM candidatos_etl")).scalar()

    logging.info("--- Resumen de Conteo Cruzado ---")
    logging.info(f"Registros en Oracle (Origen): {total_oracle}")
    logging.info(f"Registros en PostgreSQL (Destino): {total_pg}")

    if total_oracle == total_pg:
        logging.info("¡Validación exitosa! Los conteos coinciden perfectamente.")
    else:
        logging.warning("¡Alerta! Hay discrepancia en la cantidad de registros migrados.")