####################################################################
# DESCRIPCION: resolucion de ejercicio 3 con analisis de memoria   #
# AUTOR: Fabricio Sánchez Garcés                                   #
####################################################################
from typing import List, Tuple
from funciones_generales import q3
from memory_profiler import profile

# Funcion que permite resolver lo siguiente:
# El top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos.
# retorna una lista de tuplas
@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    try: 
        #llamado a la funcion q resuelve el ejercicio 1
        lista_tuplas = q3(file_path)
        #se verifica si se retorno la informacion o no
        if lista_tuplas is not None:            
            return lista_tuplas 
        else:
            print("Error en q3_memory: No se pudo cargar la informacion")
            return None    
    except Exception as e:
        # Si ocurre una excepción, imprime un mensaje de error y devuelve None
        print("Error en q3_memory: ", str(e))
        return None     