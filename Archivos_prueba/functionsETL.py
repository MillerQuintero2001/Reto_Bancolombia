"""
Miller Alexis Quintero García

'functionsETL.py': Este archivo contiene las funciones necesarias para realizar
las acciones principales ETL para el dataset dado, teniendo su implementación
en el archivo 'mainETL.py'

Importante: Muchos de los pasos realizados, están apoyados en el EDA inicial en el
Jupyter Notbook: https://drive.google.com/file/d/1-2XFMenhNg1lKEYPe4VQzsbzPf8XaYYJ/view?usp=sharing
"""

# Importar librerías necesarias
import pandas as pd
import numpy as np
import datetime


# Función para parsear la columna 'finaltrxdate' del DataFrame
def parse_finaltrxhour(df):
    """
    Función para analizar la columna 'finaltrxhour' del DataFrame.
    Se asume que la columna contiene horas en formato HHMMSScc (HH:MM:SS:cc).
    Se extraen los componentes de hora, minuto, segundo y centésimas de segundo.
    Se verifican los rangos lógicos de cada componente.
    
    Parámetros:
    df : DataFrame
        DataFrame que contiene la columna 'finaltrxhour'.
    
    Retorna:
    is_HHMMSS : bool
        True si la columna 'finaltrxhour' tiene el formato HHMMSScc, False en caso contrario.
    """
    
    # Copia del DataFrame original
    df_temp = df.copy()

    # Convertir a string para manipular los dígitos
    df_temp['hour_str'] = df_temp['finaltrxhour'].astype(str).str.zfill(8) # Asegura 8 dígitos, rellenando con ceros si es necesario

    # Extraer componentes (hipótesis: HHMMSScc)
    df_temp['parsed_hour'] = pd.to_numeric(df_temp['hour_str'].str[0:2], errors='coerce')
    df_temp['parsed_minute'] = pd.to_numeric(df_temp['hour_str'].str[2:4], errors='coerce')
    df_temp['parsed_second'] = pd.to_numeric(df_temp['hour_str'].str[4:6], errors='coerce')
    df_temp['parsed_decimalseconds'] = pd.to_numeric(df_temp['hour_str'].str[6:8], errors='coerce') # O milisegundos

    # Verificar rangos lógicos
    hora = df_temp['parsed_hour'].max() == 23 and df_temp['parsed_hour'].min() == 0
    minuto = df_temp['parsed_minute'].max() == 59 and df_temp['parsed_minute'].min() == 0
    segundo = df_temp['parsed_second'].max() == 59 and df_temp['parsed_second'].min() == 0
    decimalsegundo = df_temp['parsed_decimalseconds'].max() == 99 and df_temp['parsed_decimalseconds'].min() == 0
    is_HHMMSS = hora and minuto and segundo and decimalsegundo

    return is_HHMMSS


# Función para parsear el formato numérico HHMMSScc a un objeto datetime.time
def parse_trx_hour(num_hour):
    """
    Parsea el formato numérico HHMMSScc a un objeto datetime.time.
    Asume que el número de 7 u 8 dígitos es HHMMSSc c.
    """
    if pd.isna(num_hour):
        return None # Mantener nulos si los hay

    s_hour = str(int(num_hour)).zfill(8) # Asegurar 8 dígitos

    try:
        # Descartamos las centésimas de segundos, y nos quedamo con HHMMSS
        hour = int(s_hour[0:2])
        minute = int(s_hour[2:4])
        second = int(s_hour[4:6])

        # Aquí los descartamos para un formato de hora estándar HH:MM:SS
        return datetime.time(hour, minute, second)
    except ValueError:
        return None # En caso de que haya un número que no se pueda parsear
    

# Función para obtener únicamente el valor de HH
def get_hour(num_hour):
    """
    Obtiene únicamente el valor de HH de un número en formato HHMMSScc.
    """
    if pd.isna(num_hour):
        return None # Mantener nulos si los hay

    s_hour = str(int(num_hour)).zfill(8) # Asegurar 8 dígitos

    try:
        hour = int(s_hour[0:2])
        return hour
    except ValueError:
        return None # En caso de que haya un número que no se pueda parsear
    

# Funcion para encontrar valores que representan cero en una lista
def encontrar_ceros(lista_valores_unicos):
    """
    Encuentra valores que representan cero en una lista,
    manejando enteros, flotantes y strings.

    Args:
        lista_valores_unicos (list): Una lista de valores únicos de una columna.

    Returns:
        list: Una lista de los valores que representan cero.
    """
    ceros_encontrados = []
    for valor in lista_valores_unicos:
        # 1. Manejo de enteros y flotantes
        if isinstance(valor, (int, float)):
            if valor == 0:
                ceros_encontrados.append(valor)
        # 2. Manejo de strings
        elif isinstance(valor, str):
            try:
                # Intentar convertir el string a un número y verificar si es cero
                numero_desde_string = float(valor)
                if numero_desde_string == 0:
                    ceros_encontrados.append(valor)
            except ValueError:
                # Si no se puede convertir a número, no es un cero numérico en string
                pass
    return ceros_encontrados


def parse_responsecode(df):

    # Creamos una copia de la serie por buena práctica y seguridad
    columna_transformada = df['responsecode'].copy()

    # Obtenemos la lista de valores ceros de la columna 'responsecode'
    ceros = encontrar_ceros(columna_transformada.unique())

    # Identificamos los valores que no son nulos
    no_nulos = columna_transformada.notna()

    # Máscara para los valores que deben ser 0 (están en la lista y no son nulos)
    debe_ser_cero = no_nulos & columna_transformada.isin(ceros)

    # Máscara para los valores que deben ser 1 (no están en la lista y no son nulos)
    debe_ser_menos_uno = no_nulos & ~columna_transformada.isin(ceros)


    # Aplicamos las transformaciones en el orden correcto

    # Primero, asignar 1 indicando éxito a los que originalmente eran 0
    columna_transformada.loc[debe_ser_cero] = 1

    # Luego, asignar 0 a los que eran originalmente diferentes de 0, para indicador de no exitosa
    columna_transformada.loc[debe_ser_menos_uno] = 0

    # Asigamos -1 a los valores nulos
    columna_transformada.loc[columna_transformada.isna()] = -1

    return columna_transformada