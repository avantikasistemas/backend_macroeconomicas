from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Class.Sectores import Sectores
from Utils.decorator import http_decorator
from Config.db import get_db

sectores_router = APIRouter()

@sectores_router.post('/guardar_valores', tags=["Sectores"], response_model=dict)
@http_decorator
def guardar_valores(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).guardar_valores(data)
    return response

@sectores_router.post('/obtener_ultimo_anio', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_ultimo_anio(request: Request, db: Session = Depends(get_db)):
    response = Sectores(db).obtener_ultimo_anio()
    return response

@sectores_router.post('/obtener_sectores', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_sectores(request: Request, db: Session = Depends(get_db)):
    response = Sectores(db).obtener_sectores()
    return response

@sectores_router.post('/obtener_registros', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_registros(request: Request, db: Session = Depends(get_db)):
    response = Sectores(db).obtener_registros()
    return response

@sectores_router.post('/actualizar_valores', tags=["Sectores"], response_model=dict)
@http_decorator
def actualizar_valores(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).actualizar_valores(data)
    return response

@sectores_router.post('/obtener_anios_proyecciones', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_anios_proyecciones(request: Request, db: Session = Depends(get_db)):
    response = Sectores(db).obtener_anios_proyecciones()
    return response

@sectores_router.post('/obtener_subsectores', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_subsectores(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).obtener_subsectores(data)
    return response

@sectores_router.post('/obtener_anios_para_sectores', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_anios_para_sectores(request: Request, db: Session = Depends(get_db)):
    response = Sectores(db).obtener_anios_para_sectores()
    return response

@sectores_router.post('/obtener_subsectores_insertados', tags=["Sectores"], response_model=dict)
@http_decorator
def obtener_subsectores_insertados(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).obtener_subsectores_insertados(data)
    return response

@sectores_router.post('/actualizar_subsectores', tags=["Sectores"], response_model=dict)
@http_decorator
def actualizar_subsectores(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).actualizar_subsectores(data)
    return response

@sectores_router.post('/actualizar_cliente', tags=["Sectores"], response_model=dict)
@http_decorator
def actualizar_cliente(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).actualizar_cliente(data)
    return response

@sectores_router.post('/obtener_clientes', tags=["Terceros"], response_model=dict)
@http_decorator
def obtener_clientes(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).obtener_clientes(data)
    return response

@sectores_router.post('/get_subsector_by_sector', tags=["Terceros"], response_model=dict)
@http_decorator
def get_subsector_by_sector(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Sectores(db).get_subsector_by_sector(data)
    return response
