from typing import List, Tuple
from datetime import datetime
from pyspark.sqlx import SparkSession  

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Paso 1: Inicializar una sesión de Spark
    spark = SparkSession.builder \
        .appName("Lectura de JSON y creación de DataFrame") \
        .getOrCreate()
    
    df = spark.read.json(file_path)
    df.show(5)
    #pass


def prueba_m(file_path: str):
    # Paso 1: Inicializar una sesión de Spark
    spark = SparkSession.builder \
        .appName("Lectura de JSON y creación de DataFrame") \
        .getOrCreate()
    
    df = spark.read.json(file_path)
    df.show(5)
    