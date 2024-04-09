####################################################################
# DESCRIPCION: funciones generales para el uso de la solucion      #
# AUTOR: Fabricio Sánchez Garcés                                   #
####################################################################
import emoji
import zipfile
import os
from typing import List, Tuple
from datetime import datetime  


from pyspark.sql import SparkSession 
from pyspark.sql.functions import col,to_date,count, desc, asc, row_number, udf, explode
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType


file_path_zip = "tweets.json.zip"



# Funcion que permite leer un archivo .json, cargar la data a un datafram spark y retornar el mismo
def lee_archivo(file_path: str):
    json_file_path = "/tmp/" + file_path
    
    # Verifica si el archivo ZIP existe
    if not os.path.exists(file_path_zip):
        print("Error en lee_archivo: El archivo ZIP no existe.")
        return None
    
    try:    
        spark = SparkSession.builder \
            .appName("Lectura de JSON y creación de DataFrame") \
            .getOrCreate() 
        # Abre el archivo ZIP
        with zipfile.ZipFile(file_path_zip, 'r') as zip_ref:
            # Verifica si el archivo JSON existe dentro del ZIP
            if file_path not in zip_ref.namelist():
                print("Error en lee_archivo: El archivo JSON no existe dentro del archivo ZIP.")
                return None            
            
            # Verifica si el archivo existe antes de intentar borrarlo
            if os.path.exists(json_file_path):
                os.remove(json_file_path)# Borra el archivo

            # Extrae el archivo JSON en un directorio temporal 
            with zip_ref.open(file_path) as src, open(json_file_path, "wb") as dest:
                dest.write(src.read())

            df = spark.read.json(json_file_path)    

        return df      
    except Exception as e:
        # Verifica si el archivo existe antes de intentar borrarlo
        if os.path.exists(json_file_path):
            os.remove(json_file_path)# Borra el archivo
        # Si ocurre una excepción, imprime un mensaje de error y devuelve None
        print("Error en lee_archivo: ", str(e))
        return None    



# Funcion que permite resolver lo siguiente:
# Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días.
# retorna una lista de tuplas
def q1(file_path: str) -> List[Tuple[datetime.date, str]]:
    try: 
        # se llama a la funcion que lee el archivo json con la data y se lo recupera en un dataframe de spark
        df = lee_archivo(file_path).select(to_date(col("date")).alias("fecha") ,col("user.username").alias("usuario") ,col("user.id").alias("id_usuario"))
        # se verifica si se retorno informacion o no
        if df is not None:            
            # Primero, agrupa los datos por fecha y cuenta cuántos usuarios hay en cada fecha
            df_count_users = df.groupBy("fecha","usuario").agg(count("*").alias("cantidad_tw_usuario"))
            df_count_users = df_count_users.orderBy(asc("fecha"), desc("cantidad_tw_usuario"))
            # Definir la ventana de partición por fecha
            windowSpec = Window.partitionBy("fecha").orderBy(df_count_users["cantidad_tw_usuario"].desc())
            # Agregar el número de fila por partición (ordenado por cantidad_tw_usuario)
            df_count_users_rn = df_count_users.withColumn("row_number", row_number().over(windowSpec))
            # Filtrar el DataFrame para obtener solo los registros con el número de fila igual a 1 (el usuario con la mayor cantidad_tw_usuario para cada fecha)
            df_user_mas_pub = df_count_users_rn.filter(df_count_users_rn["row_number"] == 1).select(col("fecha") ,col("usuario") )
            #  Transformar los registros en tuplas bajo el formato requerido
            rows = df_user_mas_pub.collect()
            lista_tuplas = [(row["fecha"], row["usuario"]) for row in rows]

            return lista_tuplas 
        else:
            print("Error en q1: No se pudo cargar dataframe")
            return None    
    except Exception as e:
        # Si ocurre una excepción, imprime un mensaje de error y devuelve None
        print("Error en q1: ", str(e))
        return None    
  





# Definir una función para encontrar emojis en el texto del tweet
def find_emojis(text):
    return ''.join(c for c in text if c in emoji.EMOJI_DATA)
# Convertir la función de Python a una UDF de Spark
find_emojis_udf = udf(find_emojis, StringType())

# Define una función para dividir los emojis en una lista
def split_emojis(text):
    return [c for c in text]
# Convierte la función de Python a una UDF de Spark
split_emojis_udf = udf(split_emojis, ArrayType(StringType()))

# Funcion que permite resolver lo siguiente:
# Los top 10 emojis más usados con su respectivo conteo.
# retorna una lista de tuplas
def q2(file_path: str) -> List[Tuple[str, int]]:
    try: 
        # se llama a la funcion que lee el archivo json con la data y se lo recupera en un dataframe de spark
        df = lee_archivo(file_path).select(col("content").alias("tweet"))
        # se verifica si se retorno informacion o no
        if df is not None: 
            # Aplicar la UDF a la columna 'texto' para encontrar emojis y crear una nueva columna 'emojis' en el DataFrame
            df_con_emojis = df.withColumn('emojis', find_emojis_udf('tweet'))
            df_con_emojis = df_con_emojis.select('emojis').filter(df_con_emojis['emojis'] != '')

            # Divide los emojis en una lista y crea una nueva columna 'emoji_list'
            df_with_emoji_list = df_con_emojis.withColumn('emoji_list', split_emojis_udf(col('emojis')))

            # Usa explode() para convertir la lista de emojis en filas separadas
            df_exploded = df_with_emoji_list.select(explode('emoji_list').alias('emoji'))

            # Agrupa por la columna 'emoji' y cuenta cuántos hay en cada grupo
            df_grouped = df_exploded.groupBy('emoji').count()
            df_grouped= df_grouped.orderBy( desc("count")).limit(10)

            #  Transformar los registros en tuplas bajo el formato requerido
            rows = df_grouped.collect()
            lista_tuplas = [(row["emoji"], row["count"]) for row in rows]

            return lista_tuplas 
        else:
            print("Error en q2: No se pudo cargar dataframe")
            return None    
    except Exception as e:
        # Si ocurre una excepción, imprime un mensaje de error y devuelve None
        print("Error en q2: ", str(e))
        return None  
    

# Funcion que permite resolver lo siguiente:
# El top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos.
# retorna una lista de tuplas
def q3(file_path: str) -> List[Tuple[str, int]]:
    try: 
        # se llama a la funcion que lee el archivo json con la data y se lo recupera en un dataframe de spark
        df = lee_archivo(file_path).select(col("mentionedUsers.username").alias("usuarios_mencionados"))
        # se verifica si se retorno informacion o no
        if df is not None: 
            df = df.dropna(subset=['usuarios_mencionados'])    
            # Usa explode() para convertir la lista de menciones en filas separadas
            df_exploded = df.select(explode('usuarios_mencionados').alias('menciones'))
            # Agrupa por la columna 'menciones' y cuenta cuántos hay en cada grupo
            df_grouped = df_exploded.groupBy('menciones').count()
            df_grouped= df_grouped.orderBy( desc("count")).limit(10)
            #  Transformar los registros en tuplas bajo el formato requerido
            rows = df_grouped.collect()
            lista_tuplas = [(row["menciones"], row["count"]) for row in rows]

            return lista_tuplas 
        else:
            print("Error en q3: No se pudo cargar dataframe")
            return None    
    except Exception as e:
        # Si ocurre una excepción, imprime un mensaje de error y devuelve None
        print("Error en q3: ", str(e))
        return None  

    
