from Utils.tools import Tools, CustomException
from Utils.querys import Querys

class Sectores:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.querys = Querys(self.db)

    # Función para guardar los valores de los sectores.
    def guardar_valores(self, data: dict):
        try:
            anio = int(data['anio'])
            sectores = data['sectores']
                        
            validacion = self.querys.validar_valores(anio)
            if validacion:
                for key in sectores:
                    # Guardamos los valores en la base de datos.
                    self.querys.guardar_valores(anio, key)
                    
                    # Llamamos a la función para insertar los subsectores.
                    self.insertar_subsectores(anio, key)
                    
                # Query para traer todos los terceros
                terceros = self.querys.obtener_todos_terceros()
                if terceros:
                    for ter in terceros:
                        # Guardamos los valores de terceros en la base de datos.
                        self.querys.guardar_terceros_crecimiento(anio, ter, sectores)
            
            # Retornamos la información.
            return self.tools.output(200, "Proceso guardado con éxito.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los sectores registrados en la base de datos.
    def obtener_sectores(self):
        """ Api que realiza la consulta de los estados. """
        try:
            # Obtenemos el último año registrado en la base de datos.
            sectores = self.querys.obtener_sectores()
            
            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", sectores)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Query para obtener todos los registros de porcentajes de sectores.
    def obtener_registros(self):
        try:
            # Obtenemos el último año registrado en la base de datos.
            registros = self.querys.obtener_registros()
            
            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", registros)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Query para actualizar los datos de los porcentajes de sectores.
    def actualizar_valores(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            sector_porcentaje = round(float(data['sector_porcentaje']), 2)
            
            # Validaciones
            if sector_porcentaje > 100:
                msg ='El sector porcentaje debe ser menor o igual a 100.'
                raise CustomException(msg)

            # Actualizamos los valores en la base de datos.
            self.querys.actualizar_valores(data)
            
            # Actualizamos los subsectores en la base de datos.
            self.querys.actualizar_subsectores_general(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Valores actualizados.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los años de proyecciones.
    def obtener_anios_proyecciones(self):

        try:
            # Obtenemos los años de proyecciones registrados en la base de datos.
            anios_proyecciones = self.querys.obtener_anios_proyecciones()
            
            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", anios_proyecciones)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los años para los sectores.
    def obtener_anios_para_sectores(self):
        try:
            # Obtenemos los años registrados en la base de datos.
            anios = self.querys.obtener_anios_para_sectores()
            
            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", anios)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para insertar los subsectores en la base de datos.
    def insertar_subsectores(self, anio, key):

        try:
            # Capturamos el primer carácter del parámetro 'sector'.
            # primer_caracter_sector = str(key['concepto'])[0]

            # Obtenemos los subsectores registrados en la base de datos.
            subsectores = self.querys.obtener_subsectores(str(key['concepto']))
            
            # Insertamos los subsectores en la base de datos.
            for subsector in subsectores:
                self.querys.insertar_subsector(anio, key, subsector)
            
        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los subsectores de un sector.
    def obtener_subsectores_insertados(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            
            # Obtenemos los subsectores registrados en la base de datos.
            subsectores = self.querys.obtener_subsectores_insertados(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", subsectores)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para actualizar los subsectores en la base de datos.
    def actualizar_subsectores(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            # Actualizamos los subsectores en la base de datos.
            self.querys.actualizar_subsectores(data)
            
            # Actualizamos el porcentaje de subsector y cliente en la base de datos.
            self.querys.actualizar_porcentaje_subsector_y_cliente(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Datos actualizados.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para actualizar los subsectores en la base de datos.
    def actualizar_cliente(self, data: dict):
        """ Api que realiza la consulta de los estados. """
        try:
            # Actualizamos los subsectores en la base de datos.
            self.querys.actualizar_cliente(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Datos actualizados.")

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los clientes registrados en la base de datos.
    def obtener_clientes(self, data):
        try:
            
            if data["position"] <= 0:
                message = "El campo posición no es válido"
                raise CustomException(message)

            clientes = self.querys.obtener_clientes(data)

            registros = clientes["registros"]
            cant_registros = clientes["cant_registros"]

            if not registros:
                message = "No hay listado de que mostrar."
                return self.tools.output(200, message, data={
                "total_registros": 0,
                "total_pag": 0,
                "posicion_pag": 0,
                "registros": []
            })

            if cant_registros%data["limit"] == 0:
                total_pag = cant_registros//data["limit"]
            else:
                total_pag = cant_registros//data["limit"] + 1

            if total_pag < int(data["position"]):
                message = "La posición excede el número total de registros."
                return self.tools.output(200, message, data={
                "total_registros": 0,
                "total_pag": 0,
                "posicion_pag": 0,
                "registros": []
            })

            registros_dict = {
                "total_registros": cant_registros,
                "total_pag": total_pag,
                "posicion_pag": data["position"],
                "registros": registros
            }

            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", registros_dict)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e

    # Función para obtener los subsectores de un sector.
    def get_subsector_by_sector(self, data):
        try:
            # Obtenemos los subsectores registrados en la base de datos.
            subsectores = self.querys.get_subsector_by_sector(data)
            
            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", subsectores)

        except CustomException as e:
            print(f"Error al guardar solicitud: {e}")
            raise e
