from typing import List
from fastapi import APIRouter, Query
from app.models.schemas import ClusterStatsResponse, EventResponse
from app.services.system_service import system_service

router = APIRouter(tags=["Dashboard"])

@router.get(
    "/stats",
    response_model=ClusterStatsResponse,
    summary="Get aggregated cluster statistics for dashboard",
)
async def get_cluster_stats():
    """
    Returns real-time aggregated metrics on nodes, objects, replicas, storage, and cluster status.
    """
    return await system_service.get_cluster_stats()

@router.get(
    "/events",
    response_model=List[EventResponse],
    summary="Get recent audit and system events",
)
async def get_recent_events(
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of recent events to return (newest first)",
    ),
):
    """
    Returns the most recent system, chaos, and repair audit events from SQLite.
    """
    return await system_service.get_events(limit=limit)
