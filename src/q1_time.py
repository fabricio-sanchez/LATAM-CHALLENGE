####################################################################
# DESCRIPCION: resolucion de ejercicio 1 con analisis de tiempo    #
# AUTOR: Fabricio Sánchez Garcés                                   #
####################################################################

from typing import List, Tuple
from datetime import datetime  
from funciones_generales import q1

# Funcion que permite resolver lo siguiente:
# Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días.
# retorna una lista de tuplas
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    try: 
        #llamado a la funcion q resuelve el ejercicio 1
        lista_tuplas = q1(file_path)
        #se verifica si se retorno la informacion o no
        if lista_tuplas is not None:            
            return lista_tuplas 
        else:
            print("Error en q1_time: No se pudo cargar la informacion")
            return None    
    except Exception as e:
        # Si ocurre una excepción, imprime un mensaje de error y devuelve None
        print("Error en q1_time: ", str(e))
        return None 