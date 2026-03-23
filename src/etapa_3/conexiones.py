import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Cadenas de conexión (URIs)
# Formato general: dialecto+driver://usuario:password@host:puerto/base_de_datos
URI_POSTGRES = "postgresql+psycopg2://usuario_ape:password123@localhost:5433/destino_ape"

# Para Oracle XE, el formato usa el service_name
URI_ORACLE = "oracle+oracledb://SYSTEM:password123@localhost:1521/?service_name=XE"

def obtener_conexion_postgres() -> Engine | None:
    """
    Crea y retorna el motor de conexión para PostgreSQL.
    """
    try:
        # echo=False evita que se imprima cada query SQL en la terminal (útil en producción)
        motor_postgres = create_engine(URI_POSTGRES, echo=False)
        
        # Probamos la conexión rápidamente
        with motor_postgres.connect() as conexion:
            logging.info("¡Conexión exitosa a PostgreSQL establecida!")
            
        return motor_postgres
    
    except SQLAlchemyError as e:
        logging.error(f"Error al conectar con PostgreSQL: {e}")
        return None

def obtener_conexion_oracle() -> Engine | None:
    """
    Crea y retorna el motor de conexión para Oracle.
    """
    try:
        motor_oracle = create_engine(URI_ORACLE, echo=False)
        
        with motor_oracle.connect() as conexion:
            logging.info("¡Conexión exitosa a Oracle establecida!")
            
        return motor_oracle
        
    except SQLAlchemyError as e:
        logging.error(f"Error al conectar con Oracle: {e}")
        return None