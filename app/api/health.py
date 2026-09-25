from fastapi import APIRouter
from app.models.schemas import HealthResponse
from app.services.system_service import system_service

router = APIRouter(tags=["Health"])

@router.get("/health", response_model=HealthResponse)
async def get_health():
    """Returns cluster health and database connectivity status."""
    return await system_service.get_health()
