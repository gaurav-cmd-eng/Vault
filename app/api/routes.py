from fastapi import APIRouter
from app.api.health import router as health_router
from app.api.nodes import router as nodes_router
from app.api.config import router as config_router
from app.api.objects import router as objects_router
from app.api.dashboard import router as dashboard_router

api_router = APIRouter()

api_router.include_router(health_router)
api_router.include_router(nodes_router)
api_router.include_router(config_router)
api_router.include_router(objects_router)
api_router.include_router(dashboard_router)
