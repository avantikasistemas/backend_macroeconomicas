from Utils.tools import Tools, CustomException
from sqlalchemy import text, or_, case
from sqlalchemy.sql import select
from collections import defaultdict
from datetime import datetime

class Querys:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.query_params = dict()

    # Query para validar el año de la proyección si existe.
    def validar_anio(self, anio):

        try:
            sql = """
                SELECT * FROM dbo.proyecta_variables_macroeconomicas WHERE ano_proyeccion = :anio AND estado = 1;
            """

            query = self.db.execute(text(sql), {"anio": anio}).fetchone()
            if query:
                raise CustomException(f"El año {anio} ya fue registrado anteriormente.")

            return True
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar datos de la solicitud.
    def guardar_variables(self, data: dict):
        try:
            sql = """
                INSERT INTO dbo.proyecta_variables_macroeconomicas (ano_proyeccion, pib_proyectado, ipc_proyectado, devaluacion_proyectada, aumento_salario_minimo, crecimiento_avantika, fuente, created_at)
                VALUES (:anio, :pib, :ipc, :devaluacion, :salario, :crecimiento_avantika, :fuente, :fecha_ingreso);
            """
            self.db.execute(
                text(sql), 
                {
                    "anio": int(data['anio']),
                    "pib": round(float(data['pib']), 2),
                    "ipc": round(float(data['ipc']), 2),
                    "devaluacion": round(float(data['devaluacion']), 2),
                    "salario": round(float(data['salario']), 2),
                    "crecimiento_avantika": round(float(data['crecimiento_avantika']), 2),
                    "fuente": data['fuente'],
                    "fecha_ingreso": datetime.now()
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para mostrar la información de los registros.
    def mostrar_variables(self):
        try:
            sql = """
                SELECT * FROM dbo.proyecta_variables_macroeconomicas WHERE estado = 1 ORDER BY ano_proyeccion DESC;
            """
            query = self.db.execute(text(sql)).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "id": key.id, 
                    "ano_proyeccion": key.ano_proyeccion,
                    "ipc_proyectado": key.ipc_proyectado,
                    "pib_proyectado": key.pib_proyectado,
                    "devaluacion_proyectada": key.devaluacion_proyectada,
                    "aumento_salario_minimo": key.aumento_salario_minimo,
                    "crecimiento_avantika": key.crecimiento_avantika,
                    "fuente": key.fuente,
                    "created_at": self.tools.format_date(str(key.created_at), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S") if str(key.created_at) else '',
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para actualizar datos del registro.
    def actualizar_variables(self, data: dict):
        try:
            sql = """
                UPDATE dbo.proyecta_variables_macroeconomicas
                SET pib_proyectado = :pib, ipc_proyectado = :ipc, devaluacion_proyectada = :devaluacion, aumento_salario_minimo = :salario, crecimiento_avantika = :crecimiento_avantika, fuente = :fuente
                WHERE ano_proyeccion = :anio AND id = :id;
            """
            self.db.execute(
                text(sql), 
                {
                    "id": int(data['id']),
                    "anio": int(data['ano_proyeccion']),
                    "pib": round(float(data['pib_proyectado']), 2),
                    "ipc": round(float(data['ipc_proyectado']), 2),
                    "devaluacion": round(float(data['devaluacion_proyectada']), 2),
                    "salario": round(float(data['aumento_salario_minimo']), 2),
                    "crecimiento_avantika": round(float(data['crecimiento_avantika']), 2),
                    "fuente": data['fuente']
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para validar el año registrado del sector.
    def validar_anio_sector(self, anio):

        try:
            sql = """
                SELECT * FROM dbo.proyecta_sectores_porcentajes WHERE anio = :anio AND estado = 1;
            """

            query = self.db.execute(text(sql), {"anio": anio}).fetchone()
            if query:
                raise CustomException(f"El año {anio} ya fue registrado anteriormente.")

            return True
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar datos del registro de sector.
    def guardar_valores(self, anio: int, data: dict):
        try:
            sql = """
                INSERT INTO dbo.proyecta_sectores_porcentajes (anio, sector, sector_porcentaje, hitrate, created_at)
                VALUES (:anio, :sector, :sector_porcentaje, :hitrate, :fecha_ingreso);
            """
            self.db.execute(
                text(sql), 
                {
                    "anio": anio,
                    "sector": str(data['concepto']),
                    "sector_porcentaje": round(float(data['porcentaje']), 2),
                    "hitrate": round(float(data['hitrate']), 2),
                    "fecha_ingreso": datetime.now()
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para mostrar todos los sectores.
    def obtener_sectores(self):
        try:
            # sql = """
            #     SELECT * FROM terceros_12 WHERE activo  = 'S' AND concepto_12 NOT IN ('ZZZZ', '50');
            # """
            sql = """
                SELECT id_sector, sector FROM dbo.proyecta_sectores group by id_sector, sector;
            """
            query = self.db.execute(text(sql)).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "concepto_12": key.id_sector, 
                    "descripcion": key.sector
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para mostrar todos los registros de porcentajes de sectores.
    def obtener_registros(self):
        try:
            sql = """
                SELECT psp.*, td.descripcion AS sector_nombre 
                FROM dbo.proyecta_sectores_porcentajes psp
                INNER JOIN terceros_12 td ON td.concepto_12 = psp.sector
                WHERE psp.estado = 1 AND td.activo = 'S'
                ORDER BY psp.id DESC;
            """
            query = self.db.execute(text(sql)).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "id": key.id, 
                    "anio": key.anio,
                    "sector": key.sector,
                    "sector_nombre": key.sector_nombre,
                    "sector_porcentaje": key.sector_porcentaje,
                    "hitrate": key.hitrate,
                    "created_at": self.tools.format_date(str(key.created_at), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S") if str(key.created_at) else '',
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para validar el año y sector registrado.
    def validar_valores(self, anio):
        try:
            sql = """
                SELECT * FROM dbo.proyecta_sectores_porcentajes WHERE anio = :anio AND estado = 1;
            """
            query = self.db.execute(text(sql), {"anio": anio}).fetchone()
            if query:
                raise CustomException(f"El año {anio} ya tiene registros guardados.")

            return True
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para actualizar datos del registro de sector.
    def actualizar_valores(self, data: dict):
        try:
            sql = """
                UPDATE dbo.proyecta_sectores_porcentajes
                SET sector_porcentaje = :sector_porcentaje, hitrate = :hitrate
                WHERE id = :id AND anio = :anio AND sector = :sector AND estado = 1;
            """
            self.db.execute(
                text(sql), 
                {
                    "id": int(data['id']),
                    "anio": int(data['anio']),
                    "sector": data['sector'],
                    "sector_porcentaje": round(float(data['sector_porcentaje']), 2),
                    "hitrate": round(float(data['hitrate']), 2)
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para obtener los años de proyecciones.
    def obtener_anios_proyecciones(self):
        try:
            sql = """
                SELECT DISTINCT anio FROM dbo.proyecta_sectores_porcentajes WHERE estado = 1 ORDER BY anio DESC;
            """
            query = self.db.execute(text(sql)).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "anio": key.anio, 
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los subsectores.
    def obtener_subsectores(self, sector: str):
        try:
            # sql = """
            #     SELECT * FROM terceros_14 WHERE concepto_14 LIKE :sector
            # """
            sql = """
                select * from dbo.proyecta_sectores where id_sector = :sector
            """
            query = self.db.execute(text(sql), {"sector": f"{sector}"}).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "concepto_14": key.id_subsector, 
                    "descripcion": key.subsector.upper()
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los años de proyecciones.
    def obtener_anios_para_sectores(self):
        try:
            sql = """
                SELECT ano_proyeccion, crecimiento_avantika FROM dbo.proyecta_variables_macroeconomicas WHERE estado = 1 ORDER BY ano_proyeccion DESC;
            """
            query = self.db.execute(text(sql)).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "anio": key.ano_proyeccion,
                    "crecimiento_avantika": key.crecimiento_avantika,
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar los subsectores en la base de datos.
    def insertar_subsector(self, anio, key, subsector):
        try:
            # Insertamos los subsectores en la base de datos.
           
            sql = """
                INSERT INTO dbo.proyecta_subsectores_porcentajes (anio, sector, sector_porcentaje, subsector, subsector_porcentaje, created_at)
                VALUES (:anio, :sector, :sector_porcentaje, :subsector, :subsector_porcentaje, :fecha_ingreso);
            """
            self.db.execute(
                text(sql), 
                {
                    "anio": anio,
                    "sector": str(key['concepto']),
                    "sector_porcentaje": round(float(key['porcentaje']), 2),
                    "subsector": str(subsector['concepto_14']),
                    "subsector_porcentaje": round(float(key['porcentaje']), 2),
                    "fecha_ingreso": datetime.now()
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para obtener los subsectores insertados.
    def obtener_subsectores_insertados(self, data: dict):
        try:
            sql = """
                SELECT psp.*, tc.descripcion as subsector_nombre
                FROM dbo.proyecta_subsectores_porcentajes psp
                INNER JOIN dbo.terceros_14 tc ON tc.concepto_14 = psp.subsector
                WHERE psp.anio = :anio AND psp.sector = :sector AND psp.estado = 1 AND tc.concepto_14 <> 'ZZZZ';
            """
            query = self.db.execute(text(sql), {"anio": data['anio'], "sector": data['sector']}).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "id": key.id, 
                    "anio": key.anio,
                    "sector": key.sector,
                    "subsector": key.subsector,
                    "subsector_nombre": key.subsector_nombre.upper(),
                    "subsector_porcentaje": key.subsector_porcentaje,
                    "created_at": self.tools.format_date(str(key.created_at), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S") if str(key.created_at) else '',
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para actualizar los subsectores.
    def actualizar_subsectores(self, data: dict):
        try:
            sql = """
                UPDATE dbo.proyecta_subsectores_porcentajes
                SET subsector_porcentaje = :subsector_porcentaje
                WHERE anio = :anio AND id = :id AND estado = 1;
            """
            self.db.execute(
                text(sql), 
                {
                    "id": int(data['id']),
                    "anio": int(data['anio']),
                    "subsector_porcentaje": round(float(data['subsector_porcentaje']), 2)
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para actualizar los subsectores.
    def actualizar_cliente(self, data: dict):
        try:
            sql = """
                UPDATE dbo.proyecta_terceros_crecimiento
                SET porcentaje_cliente = :porcentaje_cliente
                WHERE anio = :anio AND id = :id AND estado = 1;
            """
            self.db.execute(
                text(sql), 
                {
                    "id": int(data['id']),
                    "anio": int(data['anio']),
                    "porcentaje_cliente": round(float(data['porcentaje_cliente']), 2)
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()

    # Query para actualizar los subsectores generales.
    def actualizar_subsectores_general(self, data: dict):
        try:
            sql = """
                UPDATE dbo.proyecta_subsectores_porcentajes
                SET sector_porcentaje = :sector_porcentaje, subsector_porcentaje = :sector_porcentaje
                WHERE anio = :anio AND sector = :sector AND estado = 1;
            """
            self.db.execute(
                text(sql), 
                {
                    "anio": int(data['anio']),
                    "sector": data['sector'],
                    "sector_porcentaje": round(float(data['sector_porcentaje']), 2)
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()
            
    # Query para obtener todos los terceros.
    def obtener_todos_terceros(self):
        try:
            sql = """
                SELECT * FROM dbo.v_terceros_sectores;
            """
            query = self.db.execute(text(sql)).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "nit_real": key.nit_real, 
                    "nit": key.nit,
                    "nombres": key.nombres,
                    "id_sector": key.id_sector,
                    "sector": key.sector,
                    "id_subsector": key.id_subsector,
                    "subsector": key.subsector
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para guardar los terceros en la base de datos.
    def guardar_terceros_crecimiento(self, anio, ter, sectores):
        try:
                        
            sector_porcentaje = next((item['porcentaje'] for item in sectores if item['concepto'] == int(ter['id_sector'])), None)
            
            # Insertamos los terceros en la base de datos.
            sql = """
                INSERT INTO dbo.proyecta_terceros_crecimiento (anio, nit_real, nit, nombre, sector, porcentaje_sector, subsector, porcentaje_subsector, porcentaje_cliente, created_at)
                VALUES (:anio, :nit_real, :nit, :nombre, :sector, :porcentaje_sector, :subsector, :porcentaje_subsector, :porcentaje_cliente, :created_at);
            """
            self.db.execute(
                text(sql), 
                {
                    "anio": anio,
                    "nit_real": ter['nit_real'],
                    "nit": ter['nit'],
                    "nombre": ter['nombres'],
                    "sector": ter['id_sector'] if ter['id_sector'] else None,
                    "porcentaje_sector": round(float(sector_porcentaje), 2) if sector_porcentaje else 0,
                    "subsector": ter['id_subsector'] if ter['id_subsector'] else None,
                    "porcentaje_subsector": round(float(sector_porcentaje), 2) if sector_porcentaje else 0,
                    "porcentaje_cliente": round(float(sector_porcentaje), 2) if sector_porcentaje else 0,
                    "created_at": datetime.now()
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al guardar:", ex)
            self.db.rollback()
            raise CustomException("Error al guardar.")
        finally:
            self.db.close()
    
    # Query para obtener los terceros insertados.        
    def obtener_clientes(self, data):
        try:
            
            cant_registros = 0
            limit = data["limit"]
            position = data["position"]
            result = {"registros": [], "cant_registros": 0}
            response = list()
            anio = data["anio"]
            sector = data["sector"]
            subsector = data["subsector"]
            nombre_tercero = data["nombre_tercero"]
            self.query_params = {"anio": anio, "sector": sector}

            sql = """
                SELECT *, COUNT(*) OVER() AS total_registros
                FROM dbo.proyecta_terceros_crecimiento 
                WHERE anio = :anio AND sector = :sector AND estado = 1
            """
            
            if subsector:
                sql += " AND subsector = :subsector"
                self.query_params.update({"subsector": f"{subsector}"})
            
            if nombre_tercero:
                sql += " AND nombre LIKE :nombre_tercero"
                self.query_params.update({"nombre_tercero": f"%{nombre_tercero}%"})
                
            new_offset = self.obtener_limit(limit, position)
            self.query_params.update({"offset": new_offset, "limit": limit})
            sql = sql + " ORDER BY nombre ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY;"
            
            print(f"este es el sql", sql)

            query = self.db.execute(text(sql), self.query_params).fetchall()
            if query:
                cant_registros = query[0].total_registros

                # Retornar directamente una lista de diccionarios
                response = [
                    {
                        "id": key.id, 
                        "anio": key.anio,
                        "nit_real": key.nit_real,
                        "nit": key.nit,
                        "nombre": key.nombre,
                        # "sector": key.sector,
                        # "porcentaje_sector": key.porcentaje_sector,
                        # "subsector": key.subsector,
                        # "porcentaje_subsector": key.porcentaje_subsector,
                        "porcentaje_cliente": key.porcentaje_cliente,
                        "created_at": self.tools.format_date(str(key.created_at), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S") if str(key.created_at) else '',
                    } for key in query] if query else []
                
                result = {"registros": response, "cant_registros": cant_registros}
                
            return result
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Función para obtener el limite de para paginar
    def obtener_limit(self, limit: int, position: int):
        offset = (position - 1) * limit
        return offset

    # Query para obtener los subsectores por sector.
    def get_subsector_by_sector(self, data):
        try:
            sql = """
                SELECT * FROM dbo.proyecta_sectores WHERE id_sector = :sector AND estado = 1;
            """
            query = self.db.execute(text(sql), {"sector": f"{data['sector']}"}).fetchall()

            # Retornar directamente una lista de diccionarios
            return [
                {
                    "id_subsector": key.id_subsector, 
                    "subsector": key.subsector.upper()
                } for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para actualizar el porcentaje del subsector y cliente.
    def actualizar_porcentaje_subsector_y_cliente(self, data: dict):
        try:
            sql = """
                UPDATE dbo.proyecta_terceros_crecimiento
                SET porcentaje_subsector = :porcentaje_subsector, porcentaje_cliente = :porcentaje_cliente
                WHERE anio = :anio AND sector = :sector AND subsector = :subsector AND estado = 1;
            """
            self.db.execute(
                text(sql), 
                {
                    "anio": int(data['anio']),
                    "sector": str(data['sector']),
                    "subsector": str(data['subsector']),
                    "porcentaje_subsector": round(float(data['subsector_porcentaje']), 2),
                    "porcentaje_cliente": round(float(data['subsector_porcentaje']), 2)
                }
            )
            self.db.commit()               
                
        except Exception as ex:
            print("Error al actualizar:", ex)
            self.db.rollback()
            raise CustomException("Error al actualizar.")
        finally:
            self.db.close()
