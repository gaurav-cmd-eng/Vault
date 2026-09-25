from typing import List
from fastapi import APIRouter, HTTPException, status
from app.models.schemas import NodeInfo, NodeStateTransitionResponse
from app.services.system_service import system_service
from app.storage.node_manager import node_manager

router = APIRouter(tags=["Nodes"])

@router.get("/nodes", response_model=List[NodeInfo])
async def list_nodes():
    """Returns telemetry, status, and storage metrics for all storage nodes."""
    return system_service.get_nodes_info()

@router.get("/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str):
    """Returns telemetry for a specific storage node."""
    node = node_manager.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return NodeInfo(**node.get_stats())

@router.post(
    "/nodes/{node_id}/offline",
    response_model=NodeStateTransitionResponse,
    status_code=status.HTTP_200_OK,
    summary="Simulate node failure by taking node offline",
)
async def set_node_offline(node_id: str):
    """Marks a storage node as OFFLINE for chaos testing."""
    result = await node_manager.set_node_offline(node_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return NodeStateTransitionResponse(**result)

@router.post(
    "/nodes/{node_id}/online",
    response_model=NodeStateTransitionResponse,
    status_code=status.HTTP_200_OK,
    summary="Bring storage node back online",
)
async def set_node_online(node_id: str):
    """Marks a storage node as ONLINE."""
    result = await node_manager.set_node_online(node_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return NodeStateTransitionResponse(**result)
