from Utils.tools import Tools, CustomException
from Utils.querys import Querys

class PuntoEquilibrio:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.querys = Querys(self.db)

    # Función para guardar los valores del punto de equilibrio.
    def guardar_punto_equilibrio(self, data: dict):
        try:
            
            # Validamos que los datos no se repitan al guardar.
            self.querys.check_si_existe_periodicidad(data)

            # Guardamos la periodicidad.
            self.querys.guardar_periodicidad(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Proceso guardado con éxito.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para consultar la información periódica.
    def consultar_informacion_periodica(self, data: dict):
        try:
            # Consultamos la información periódica.
            response = self.querys.consultar_informacion_periodica(data)

            # Retornamos la información.
            return self.tools.output(200, "Consulta realizada con éxito.", response)

        except CustomException as e:
            print(f"Error al consultar información periódica: {e}")
            raise e
