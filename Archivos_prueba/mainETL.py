"""
Miller Alexis Quintero García

'mainETL.py': Este archivo realiza las acciones principales para el dataset dado,
iniciando desde la extracción de datos cargando el archivo CSV, pasando por la 
transformación de los datos emplendo funciones y entendiendo la naturaleza y 
contexto de la información, para finalmente cargar los datos procesados en un 
archivo CSV para continuar su ciclo a Power BI.

Importante: Muchos de los pasos realizados, están apoyados en el EDA inicial en el
Jupyter Notbook: https://drive.google.com/file/d/1-2XFMenhNg1lKEYPe4VQzsbzPf8XaYYJ/view?usp=sharing
"""

# Importar librerías necesarias
import pandas as pd
import numpy as np
import datetime
import functionsETL

# Abrimos el archivo CSV, omitiendo primera columna ya que es de índices
# además, con "low_memory = False" evitamos el warning de que las columnas
# tienen diferentes tipos de datos, al hacer el parsing por defecto de pandas
df = pd.read_csv('data.csv', index_col = 0, low_memory = False)


# Dropeamos las columnas 'channel' y 'devicenameid' ya que estas
# contienen solo un valor único, que resume esas 2 columnas en que
# los datos corresponde a un solo canal:
# 'channel' = 'NEG' (Negocio posiblemente)
# 'devicenameid' = 'APP' (Aplicación posiblemente)
df.drop(columns=['channel', 'devicenameid'], inplace=True)


# Eliminamos 'finaltrxmonth' y 'finaltrxyear' ya que:
# 1. 'finaltrxyear' tenía un único valor de '2026' mientras el resto eran '2024',
#    asumiendo que es un error de tipeo, se elimna toda la columna entendiendo
#    que no todo el dataset es de 2024.
# 2. 'finaltrxmonth' tenía un solo valor de '13' mientras el resto eran '12',
#    asumiendo nuevamenteque es un error de tipeo, se elimna toda la columna,
#    entendiendo que todo el dataset es de 12 meses.
# Así, el dataset completo es el mes 12 de 2024 (Diciembre 2024)
df.drop(columns=['finaltrxmonth', 'finaltrxyear'], inplace=True)


if functionsETL.parse_finaltrxhour(df):
    # Parsear la columna 'finaltrxhour' a un objeto datetime.time con una nueva columna
    df['hour_trx'] = df['finaltrxhour'].apply(functionsETL.parse_trx_hour)
    # Extraer solo la hora de la columna 'hour_trx' para crear una nueva columna 'hour_only'
    df['hour_only'] = df['finaltrxhour'].apply(functionsETL.get_hour)
    # Dropeamos la columna 'finaltrxhour' pues ya la información está en las nuevas columnas
    df.drop(columns=['finaltrxhour'], inplace=True)

else:
    # Si no se puede parsear, se asigna un valor nulo
    df['hour_trx'] = pd.NaT
    df['hour_only'] = pd.NaT
    print("No se pudo parsear la columna 'finaltrxhour' a un objeto datetime.time")


# Procedemos ahora con la columna 'responsecode'
df['responsecode'] = functionsETL.parse_responsecode(df)


# Luego, cambiamos los valores de la columna 'transactiontype' según:
# 'Administrativa' = -1 que indica que las transaccion es monetaria
# Cualquier otro valor se hace = 0, que indica que la transacción no es monetaria
df['transactiontype'] = np.where(df['transactiontype'] == 'Administrativa', -1, 0)

# Quitamos la columna 'transactionvouchernumber' ya que contiene solo valores nulos
df.drop(columns=['transactionvouchernumber'], inplace=True)

# Redefinimos las columnas del DataFrame con un orden y nomenclatura más entendible
df = functionsETL.redefine_columns(df)

# Guardamos el DataFrame limpio en un nuevo archivo CSV
df.to_csv('transacciones_12-2024.csv', index=False)
print("El DataFrame limpio se ha guardado como 'transacciones_12-2024.csv'")