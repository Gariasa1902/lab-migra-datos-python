import logging

# Importamos las funciones de nuestro módulo de conexiones
from conexiones import obtener_conexion_postgres, obtener_conexion_oracle

# Configuración básica de Logging para visualizar los resultados
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def probar_conexiones():
    """
    Función principal para probar la conectividad con las bases de datos.
    """
    logging.info("--- Iniciando prueba de conexiones a Bases de Datos ---")
    
    # Verificamos la conexión a la base de datos destino (PostgreSQL)
    logging.info("Probando conexión a PostgreSQL...")
    motor_pg = obtener_conexion_postgres()
    if motor_pg:
        logging.info("Motor de PostgreSQL listo para recibir operaciones.\n")
    else:
        logging.error("Falló la inicialización del motor de PostgreSQL.\n")

    # Verificamos la conexión a la base de datos origen (Oracle)
    logging.info("Probando conexión a Oracle XE...")
    motor_oracle = obtener_conexion_oracle()
    if motor_oracle:
        logging.info("Motor de Oracle listo para recibir operaciones.\n")
    else:
        logging.error("Falló la inicialización del motor de Oracle.\n")

    logging.info("--- Prueba de conexiones finalizada ---")

# Punto de entrada de la aplicación
if __name__ == '__main__':
    probar_conexiones()