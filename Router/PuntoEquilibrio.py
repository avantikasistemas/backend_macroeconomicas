from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Class.PuntoEquilibrio import PuntoEquilibrio
from Utils.decorator import http_decorator
from Config.db import get_db

punto_equilibrio_router = APIRouter()

@punto_equilibrio_router.post('/guardar_punto_equilibrio', tags=["Punto Equilibrio"], response_model=dict)
@http_decorator
def guardar_punto_equilibrio(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = PuntoEquilibrio(db).guardar_punto_equilibrio(data)
    return response
    
@punto_equilibrio_router.post('/consultar_informacion_periodica', tags=["Punto Equilibrio"], response_model=dict)
@http_decorator
def consultar_informacion_periodica(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = PuntoEquilibrio(db).consultar_informacion_periodica(data)
    return response
    