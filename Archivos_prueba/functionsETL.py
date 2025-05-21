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
    Analiza la columna 'finaltrxhour' del DataFrame para verificar si sigue
    el formato HHMMSScc (HoraMinutoSegundoCentésimas).

    Extrae los componentes de hora, minuto, segundo y centésimas de segundo
    y verifica si sus rangos son lógicos (0-23 para hora, 0-59 para minuto/segundo,
    0-99 para centésimas).

    Parámetros:
    df : pd.DataFrame
        DataFrame que contiene la columna 'finaltrxhour'.

    Retorna:
    bool
        True si la columna 'finaltrxhour' parece tener el formato HHMMSScc
        con rangos de tiempo válidos, False en caso contrario o si ocurre un error.
    """
    if not isinstance(df, pd.DataFrame):
        print("Error: El parámetro 'df' debe ser un DataFrame de Pandas.")
        return False
    
    if 'finaltrxhour' not in df.columns:
        print("Error: La columna 'finaltrxhour' no se encuentra en el DataFrame.")
        return False

    try:
        # Copia del DataFrame original para evitar SettingWithCopyWarning
        df_temp = df.copy()

        # Convertir a string para manipular los dígitos.
        # Maneja nulos convirtiéndolos a NaN y luego a string 'nan', que zfill no afectará.
        # Los valores no numéricos se convertirán a NaN por to_numeric.
        df_temp['hour_str'] = df_temp['finaltrxhour'].astype(str).str.zfill(8)

        # Extraer componentes (hipótesis: HHMMSScc)
        # errors='coerce' convierte valores no válidos a NaN, lo que es útil aquí.
        df_temp['parsed_hour'] = pd.to_numeric(df_temp['hour_str'].str[0:2], errors='coerce')
        df_temp['parsed_minute'] = pd.to_numeric(df_temp['hour_str'].str[2:4], errors='coerce')
        df_temp['parsed_second'] = pd.to_numeric(df_temp['hour_str'].str[4:6], errors='coerce')
        df_temp['parsed_decimalseconds'] = pd.to_numeric(df_temp['hour_str'].str[6:8], errors='coerce')

        # Eliminar filas con valores NaN en los componentes parseados para la verificación de rangos
        df_temp.dropna(subset=['parsed_hour', 'parsed_minute', 'parsed_second', 'parsed_decimalseconds'], inplace=True)

        if df_temp.empty:
            print("Advertencia: No hay datos válidos para analizar en 'finaltrxhour' después del parseo.")
            return False

        # Verificar rangos lógicos
        # Se verifica que el valor máximo no exceda el límite superior y el mínimo no sea menor al límite inferior.
        # No se asume que el rango completo (0-23, 0-59) deba estar presente, solo que los valores existentes sean válidos.
        hora_valida = (df_temp['parsed_hour'].max() <= 23) and (df_temp['parsed_hour'].min() >= 0)
        minuto_valido = (df_temp['parsed_minute'].max() <= 59) and (df_temp['parsed_minute'].min() >= 0)
        segundo_valido = (df_temp['parsed_second'].max() <= 59) and (df_temp['parsed_second'].min() >= 0)
        decimalsegundo_valido = (df_temp['parsed_decimalseconds'].max() <= 99) and (df_temp['parsed_decimalseconds'].min() >= 0)
        
        is_HHMMSS = hora_valida and minuto_valido and segundo_valido and decimalsegundo_valido

        return is_HHMMSS
    except Exception as e:
        print(f"Error inesperado al analizar 'finaltrxhour': {e}")
        return False


# Función para parsear el formato numérico HHMMSScc a un objeto datetime.time
def parse_trx_hour(num_hour):
    """
    Parsea un número que representa la hora en formato HHMMSScc a un objeto datetime.time.
    Los últimos dos dígitos (centésimas de segundo) se descartan para obtener un formato
    estándar de hora (HH:MM:SS).

    Parámetros:
    num_hour : int or float
        El número que representa la hora (ej. 3075400 para 03:07:54).
        Puede ser NaN (np.nan o pd.NA) si el valor original es nulo.

    Retorna:
    datetime.time or None
        Un objeto datetime.time si el parseo es exitoso, o None si el valor de entrada
        es nulo o no se puede parsear a un formato de hora válido.
    """
    if pd.isna(num_hour):
        return None # Mantener nulos si los hay

    try:
        # Convertir a string y asegurar 8 dígitos, rellenando con ceros a la izquierda si es necesario.
        # Esto es crucial para el slicing posterior.
        s_hour = str(int(num_hour)).zfill(8)

        # Extraer componentes de hora, minuto y segundo
        hour = int(s_hour[0:2])
        minute = int(s_hour[2:4])
        second = int(s_hour[4:6])

        # Validar rangos de los componentes extraídos
        if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
            print(f"Advertencia: Valores de hora/minuto/segundo fuera de rango para {num_hour}. Retornando None.")
            return None
            
        return datetime.time(hour, minute, second)
    except (ValueError, TypeError) as e:
        # Captura errores si num_hour no es convertible a int, o si el slicing falla
        print(f"Error al parsear el número de hora '{num_hour}': {e}. Retornando None.")
        return None
    except Exception as e:
        print(f"Error inesperado en parse_trx_hour para '{num_hour}': {e}. Retornando None.")
        return None
    

# Función para obtener únicamente el valor de HH
def get_hour(num_hour):
    """
    Extrae únicamente el componente de hora (HH) de un número en formato HHMMSScc.

    Parámetros:
    num_hour : int or float
        El número que representa la hora (ej. 3075400 para 03:07:54).
        Puede ser NaN (np.nan o pd.NA) si el valor original es nulo.

    Retorna:
    int or None
        El componente de hora como un entero (0-23) si el parseo es exitoso,
        o None si el valor de entrada es nulo o no se puede parsear.
    """
    if pd.isna(num_hour):
        return None # Mantener nulos si los hay

    try:
        # Convertir a string y asegurar 8 dígitos
        s_hour = str(int(num_hour)).zfill(8)
        
        hour = int(s_hour[0:2])
        
        # Validar que la hora esté en un rango válido
        if not (0 <= hour <= 23):
            print(f"Advertencia: Valor de hora fuera de rango para {num_hour}. Retornando None.")
            return None
            
        return hour
    except (ValueError, TypeError) as e:
        # Captura errores si num_hour no es convertible a int, o si el slicing falla
        print(f"Error al obtener la hora de '{num_hour}': {e}. Retornando None.")
        return None
    except Exception as e:
        print(f"Error inesperado en get_hour para '{num_hour}': {e}. Retornando None.")
        return None
    

# Funcion para encontrar valores que representan cero en una lista
def encontrar_ceros(lista_valores_unicos):
    """
    Identifica y retorna los valores que representan cero dentro de una lista,
    considerando enteros, flotantes y strings que puedan ser convertidos a cero.

    Parámetros:
    lista_valores_unicos : list
        Una lista de valores únicos extraídos de una columna de un DataFrame.

    Retorna:
    list
        Una lista de los valores que, al ser evaluados, representan el número cero.
    """
    if not isinstance(lista_valores_unicos, list):
        print("Error: El parámetro 'lista_valores_unicos' debe ser una lista.")
        return []

    ceros_encontrados = []
    for valor in lista_valores_unicos:
        # Manejo de enteros y flotantes
        if isinstance(valor, (int, float)):
            if valor == 0:
                ceros_encontrados.append(valor)
        # Manejo de strings
        elif isinstance(valor, str):
            try:
                # Intentar convertir el string a un número y verificar si es cero
                numero_desde_string = float(valor)
                if numero_desde_string == 0:
                    ceros_encontrados.append(valor)
            except ValueError:
                # Si no se puede convertir a número, no es un cero numérico en string
                pass
        # Ignorar otros tipos de datos (ej. booleanos, objetos) que no representen cero numéricamente
    return ceros_encontrados


# Función para parsear la columna 'responsecode' del DataFrame
def parse_responsecode(df):
    """
    Transforma la columna 'responsecode' del DataFrame en un indicador binario
    de éxito (1) o no éxito (0) para transacciones, manteniendo los nulos como -1.

    Los valores que representan "cero" (éxito) en la columna original se convierten a 1.
    Otros valores no nulos se convierten a 0 (no éxito).
    Los valores nulos originales se imputan con -1.

    Parámetros:
    df : pd.DataFrame
        El DataFrame que contiene la columna 'responsecode'.

    Retorna:
    pd.Series or None
        Una nueva Serie de Pandas con la columna 'responsecode' transformada,
        o None si la columna no existe o si ocurre un error.
    """
    if not isinstance(df, pd.DataFrame):
        print("Error: El parámetro 'df' debe ser un DataFrame de Pandas.")
        return None
        
    if 'responsecode' not in df.columns:
        print("Error: La columna 'responsecode' no se encuentra en el DataFrame.")
        return None

    try:
        # Creamos una copia de la serie por buena práctica y seguridad
        columna_transformada = df['responsecode'].copy()

        # Obtenemos la lista de valores que representan cero en la columna 'responsecode'
        # Esto se usa para identificar los códigos de respuesta exitosos.
        ceros = encontrar_ceros(columna_transformada.dropna().unique().tolist()) # Asegurarse de pasar una lista de valores únicos no nulos

        # Identificamos los valores que no son nulos
        no_nulos = columna_transformada.notna()

        # Máscara para los valores que deben ser 1 (están en la lista 'ceros' y no son nulos)
        debe_ser_uno = no_nulos & columna_transformada.isin(ceros)

        # Máscara para los valores que deben ser 0 (no están en la lista 'ceros' y no son nulos)
        debe_ser_cero = no_nulos & ~columna_transformada.isin(ceros)

        # Aplicamos las transformaciones en el orden correcto
        # Primero, asignar 1 indicando éxito a los que originalmente eran considerados 0
        columna_transformada.loc[debe_ser_uno] = 1

        # Luego, asignar 0 a los que eran originalmente diferentes de 0, para indicador de no exitosa
        columna_transformada.loc[debe_ser_cero] = 0

        # Asignamos -1 a los valores nulos que quedaron
        columna_transformada.loc[columna_transformada.isna()] = -1
        
        # Convertir la columna a tipo entero para asegurar consistencia
        columna_transformada = columna_transformada.astype(int)

        return columna_transformada
    except Exception as e:
        print(f"Error inesperado al parsear 'responsecode': {e}")
        return None


# Función para redefinir las columnas del DataFrame
def redefine_columns(df):
    """
    Reordena las columnas de un DataFrame, moviendo las dos últimas columnas
    a las posiciones 1 y 2 (índices 1 y 2) y renombra un conjunto específico de columnas.

    Parámetros:
    df : pd.DataFrame
        El DataFrame cuyas columnas se van a reordenar y renombrar.

    Retorna:
    pd.DataFrame or None
        Un nuevo DataFrame con las columnas reordenadas y renombradas,
        o None si el DataFrame de entrada es inválido o no tiene suficientes columnas.
    """
    if not isinstance(df, pd.DataFrame):
        print("Error: El parámetro 'df' debe ser un DataFrame de Pandas.")
        return None

    if len(df.columns) < 3: # Necesitamos al menos 3 columnas para mover las 2 últimas a pos 1 y 2
        print("Error: El DataFrame debe tener al menos 3 columnas para reordenar de esta manera.")
        return None

    try:
        # Obtener la lista actual de todas las columnas
        columns = df.columns.tolist()

        # Las dos últimas columnas del original
        last_two_cols = columns[-2:] # Por ejemplo, si son 'col_X', 'col_Y'

        # Las columnas restantes, excluyendo la primera y las dos últimas
        # Esto asegura que la primera columna (índice 0) se mantiene en su lugar
        # y el resto de las columnas se mueven después de las que se insertan.
        remaining_cols = columns[1:-2]

        # Construir el nuevo orden de columnas:
        # 1. La primera columna original (índice 0)
        # 2. Las dos últimas columnas del original (ahora en índices 1 y 2)
        # 3. El resto de las columnas
        new_column_order = [columns[0]] + last_two_cols + remaining_cols

        # Reindexar el DataFrame con el nuevo orden de columnas
        df_reordered = df[new_column_order].copy() # Usar .copy() para asegurar un nuevo DataFrame

        # Definimos los nuevos nombres de las columnas para mejor entendimiento
        nuevos_nombres = {
            'finaltrxday': 'Día',
            'hour_trx': 'Hora Transacción', # Asumiendo que esta es una de las columnas movidas o existentes
            'hour_only': 'Hora', # Asumiendo que esta es una de las columnas movidas o existentes
            'transactioncode': 'Código Transacción',
            'transactioncodedesc': 'Descripción Transacción',
            'responsecode': 'Transacción Exitosa',
            'responsecodedesc': 'Descripción Respuesta',
            'transactiontype': 'Monetaria'
        }

        # Renombramos las columnas.
        # inplace=True modifica el DataFrame directamente, pero es mejor retornar una copia modificada.
        # errors='ignore' evita errores si un nombre en nuevos_nombres no existe en el DataFrame.
        df_reordered.rename(columns=nuevos_nombres, inplace=True, errors='ignore')
        
        return df_reordered
    except Exception as e:
        print(f"Error inesperado al redefinir las columnas: {e}")
        return None

# Fin del archivo functionsETL.py