from datetime import datetime, timezone
from typing import Dict, Any, List
from app.config import settings
from app.database.db import db
from app.storage.node_manager import node_manager
from app.models.schemas import HealthResponse, NodeInfo, ConfigResponse

class SystemService:
    """Provides high-level system metrics and health status."""

    async def get_health(self) -> HealthResponse:
        # Check database health
        db_status = "connected"
        try:
            row = await db.fetch_one("SELECT 1")
            if not row or row[0] != 1:
                db_status = "unresponsive"
        except Exception:
            db_status = "error"

        nodes = node_manager.list_nodes()
        active_nodes = len([n for n in nodes if n.is_online])

        return HealthResponse(
            status="ok" if db_status == "connected" and active_nodes > 0 else "degraded",
            timestamp=datetime.now(timezone.utc).isoformat(),
            database_status=db_status,
            active_nodes=active_nodes,
            total_nodes=len(nodes),
            version=settings.version,
        )

    def get_nodes_info(self) -> List[NodeInfo]:
        nodes = node_manager.list_nodes()
        return [NodeInfo(**node.get_stats()) for node in nodes]

    def get_config(self) -> ConfigResponse:
        return ConfigResponse(
            num_nodes=settings.num_nodes,
            default_replication_factor=settings.default_replication_factor,
            write_quorum=settings.write_quorum,
            read_quorum=settings.read_quorum,
            storage_root=str(settings.storage_root),
            max_upload_size_bytes=settings.max_upload_size,
            health_check_interval_seconds=settings.health_check_interval,
        )

system_service = SystemService()
