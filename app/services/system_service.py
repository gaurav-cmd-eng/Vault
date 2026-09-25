import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from app.config import settings
from app.database.db import db
from app.storage.node_manager import node_manager
from app.models.schemas import (
    HealthResponse,
    NodeInfo,
    ConfigResponse,
    ClusterStatsResponse,
    EventResponse,
)

class SystemService:
    """Provides high-level system metrics, health status, and cluster statistics."""

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

    async def get_cluster_stats(self) -> ClusterStatsResponse:
        """Aggregates real-time cluster metrics across nodes, objects, and replicas."""
        nodes = node_manager.list_nodes()
        total_nodes = len(nodes)
        active_nodes = len([n for n in nodes if n.is_online])
        offline_nodes = total_nodes - active_nodes

        # Total physical storage used across all simulated node disks
        total_storage_used = sum(n.get_used_space() for n in nodes)

        # Total objects in database catalog
        obj_row = await db.fetch_one("SELECT COUNT(*) as count FROM objects")
        total_objects = obj_row["count"] if obj_row else 0

        # Replica counts by status
        healthy_count = 0
        corrupt_count = 0
        missing_count = 0

        replica_rows = await db.fetch_all(
            "SELECT status, COUNT(*) as count FROM replicas GROUP BY status"
        )
        for r in replica_rows:
            st = r["status"].upper()
            if st == "HEALTHY":
                healthy_count = r["count"]
            elif st == "CORRUPTED":
                corrupt_count = r["count"]
            elif st == "MISSING":
                missing_count = r["count"]

        # If any node is offline, account for degraded state
        is_degraded = (
            offline_nodes > 0
            or corrupt_count > 0
            or missing_count > 0
        )
        cluster_status = "DEGRADED" if is_degraded else "OK"

        return ClusterStatsResponse(
            total_nodes=total_nodes,
            active_nodes=active_nodes,
            offline_nodes=offline_nodes,
            total_objects=total_objects,
            total_storage_used_bytes=total_storage_used,
            healthy_replicas=healthy_count,
            corrupt_replicas=corrupt_count,
            missing_replicas=missing_count,
            cluster_status=cluster_status,
        )

    async def get_events(self, limit: int = 50) -> List[EventResponse]:
        """Fetches the latest audit events from SQLite, newest first."""
        query = """
            SELECT id, event_type, target, message, details, timestamp
            FROM events
            ORDER BY id DESC
            LIMIT ?
        """
        rows = await db.fetch_all(query, (limit,))
        events = []
        for r in rows:
            details_dict = None
            if r["details"]:
                try:
                    details_dict = json.loads(r["details"])
                except Exception:
                    details_dict = {"raw": r["details"]}

            events.append(
                EventResponse(
                    id=r["id"],
                    event_type=r["event_type"],
                    target=r["target"],
                    message=r["message"],
                    details=details_dict,
                    timestamp=str(r["timestamp"]),
                )
            )
        return events

system_service = SystemService()
