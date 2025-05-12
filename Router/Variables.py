from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Class.Variables import Variables
from Utils.decorator import http_decorator
from Config.db import get_db

variables_router = APIRouter()

@variables_router.post('/guardar_variables', tags=["Variables_Macroeconómicas"], response_model=dict)
@http_decorator
def guardar_variables(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Variables(db).guardar_variables(data)
    return response

@variables_router.post('/mostrar_variables', tags=["Variables_Macroeconómicas"], response_model=dict)
@http_decorator
def mostrar_variables(request: Request, db: Session = Depends(get_db)):
    response = Variables(db).mostrar_variables()
    return response

@variables_router.post('/actualizar_variables', tags=["Variables_Macroeconómicas"], response_model=dict)
@http_decorator
def actualizar_variables(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Variables(db).actualizar_variables(data)
    return response
