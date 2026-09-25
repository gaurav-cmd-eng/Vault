from typing import List
from fastapi import APIRouter, HTTPException
from app.models.schemas import NodeInfo
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
