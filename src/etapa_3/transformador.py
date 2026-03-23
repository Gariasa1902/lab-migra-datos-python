import pandas as pd
import logging

def transformar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica reglas de negocio (limpieza y transformación) a un lote de datos.
    """
    logging.info(f"Transformando lote de {len(df)} registros...")
    
    # Es buena práctica trabajar sobre una copia del DataFrame
    df_limpio = df.copy()
    
    # Regla 1: Estandarizar nombres a mayúsculas
    df_limpio['nombre'] = df_limpio['nombre'].str.upper()
    
    # Regla 2: Crear una nueva columna basada en los años de experiencia
    # Aplicamos una función lambda rápida para categorizar
    df_limpio['nivel_seniority'] = df_limpio['experiencia_anios'].apply(
        lambda x: 'Senior' if x >= 5 else ('Semi-Senior' if x >= 3 else 'Junior')
    )
    
    return df_limpio