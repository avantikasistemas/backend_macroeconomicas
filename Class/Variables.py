from Utils.tools import Tools, CustomException
from Utils.querys import Querys

class Variables:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.querys = Querys(self.db)

    # Función para obtener los parametros iniciales
    def guardar_variables(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            anio = int(data['anio'])
            pib = round(float(data['pib']), 2)
            ipc = round(float(data['ipc']), 2)
            devaluacion = round(float(data['devaluacion']), 2)
            salario = round(float(data['salario']), 2)
            crecimiento_avantika = round(float(data['crecimiento_avantika']), 2)
            fuente = data['fuente']
            
            # Validaciones
            if anio <= 0 or anio == '' or anio > 2100:
                msg = f'El año no debe estar vacío y o deber en un rango entre 2000 y 2100.'
                raise CustomException(msg)
            if ipc <= 0 or ipc > 100:
                msg ='El IPC debe ser mayor que 0 y menor o igual a 100.'
                raise CustomException(msg)
            if pib <= 0 or devaluacion <= 0:
                msg ='Los valores deben ser mayores a 0.'
                raise CustomException(msg)
            if salario <= 0:
                msg ='El salario debe ser mayores a 0.'
                raise CustomException(msg)
            if crecimiento_avantika <= 0 or crecimiento_avantika > 100:
                msg ='El creccimiento debe ser mayor que 0 y menor o igual a 100.'
                raise CustomException(msg)
            if not fuente:
                msg ='La fuente es obligatoria.'
                raise CustomException(msg)
            
            validar_anio = self.querys.validar_anio(anio)
            if validar_anio:
                self.querys.guardar_variables(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Proceso guardado con éxito.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los parametros iniciales
    def mostrar_variables(self):
        """ Api que realiza la consulta de los estados. """
        try:
            
            registros = self.querys.mostrar_variables()
            
            return self.tools.output(200, "Datos encontrados.", registros)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los parametros iniciales
    def actualizar_variables(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            pib = round(float(data['pib_proyectado']), 2)
            ipc = round(float(data['ipc_proyectado']), 2)
            devaluacion = round(float(data['devaluacion_proyectada']), 2)
            salario = round(float(data['aumento_salario_minimo']), 2)
            fuente = data['fuente']
            
            # Validaciones
            if ipc <= 0 or ipc > 100:
                msg ='El IPC debe ser mayor que 0 y menor o igual a 100.'
                raise CustomException(msg)
            if pib <= 0 or devaluacion <= 0:
                msg ='Los valores deben ser mayores a 0.'
                raise CustomException(msg)
            if salario <= 0:
                msg ='El salario debe ser mayores a 0.'
                raise CustomException(msg)
            if not fuente:
                msg ='La fuente es obligatoria.'
                raise CustomException(msg)
            
            self.querys.actualizar_variables(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Proceso actualizado con éxito.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e
