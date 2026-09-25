from fastapi import APIRouter
from app.models.schemas import ConfigResponse
from app.services.system_service import system_service

router = APIRouter(tags=["Config"])

@router.get("/config", response_model=ConfigResponse)
async def get_config():
    """Returns cluster configuration, replication factors, and storage paths."""
    return system_service.get_config()
