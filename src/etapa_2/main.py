"""
import logging

# Importamos las funciones de nuestro módulo de conexiones
from conexiones import obtener_conexion_postgres, obtener_conexion_oracle

# Configuración básica de Logging para visualizar los resultados
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def probar_conexiones():
    === > remplaza doble comilla de comentaio
    Función principal para probar la conectividad con las bases de datos.
    ===
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
"""    

# Actualizo el código main para los dos nuevos modulos:
import logging
from setup_oracle import inicializar_datos_oracle
from migracion import ejecutar_migracion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    logging.info("--- Iniciando App Etapa 2 ---")
    
    # 1. Preparamos el entorno origen
    inicializar_datos_oracle()
    
    # 2. Ejecutamos la migración
    ejecutar_migracion()
    
    logging.info("--- Ejecución finalizada ---")