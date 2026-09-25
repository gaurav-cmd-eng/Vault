from datetime import datetime
from typing import Dict, List, Optional, Any
from app.config import settings
from app.storage.node import StorageNode
from app.database.db import db
from app.events.event_bus import event_bus
from app.events.types import EventType
from app.utils.logger import logger

class NodeManager:
    """Manages the cluster of simulated storage nodes."""

    def __init__(self):
        self._nodes: Dict[str, StorageNode] = {}
        self.initialize()

    def _ensure_initialized(self) -> None:
        """Ensures nodes are populated if uninitialized."""
        if not self._nodes:
            self.initialize()

    def initialize(self) -> None:
        """Initializes simulated storage nodes from settings and creates directories."""
        settings.ensure_directories()
        self._nodes.clear()

        for i in range(1, settings.num_nodes + 1):
            node_id = f"{settings.node_prefix}{i}"
            node_name = f"Storage Node {i}"
            node_dir = settings.nodes_dir / node_id

            node = StorageNode(
                node_id=node_id,
                name=node_name,
                directory_path=node_dir,
                capacity_bytes=settings.node_capacity_bytes,
            )
            node.ensure_directory()
            self._nodes[node_id] = node

        logger.info(f"Initialized {len(self._nodes)} storage nodes under {settings.nodes_dir}")

    async def sync_database(self) -> None:
        """Synchronizes initialized nodes with the SQLite nodes registry table."""
        for node in self._nodes.values():
            query = """
                INSERT INTO nodes (id, name, path, status, capacity_bytes, last_heartbeat)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    path=excluded.path,
                    status=excluded.status,
                    capacity_bytes=excluded.capacity_bytes,
                    last_heartbeat=CURRENT_TIMESTAMP
            """
            await db.execute(
                query,
                (
                    node.node_id,
                    node.name,
                    str(node.directory_path),
                    node.status,
                    node.capacity_bytes,
                ),
            )
        logger.info("Synchronized storage nodes with database registry")

    def get_node(self, node_id: str) -> Optional[StorageNode]:
        self._ensure_initialized()
        return self._nodes.get(node_id)

    def list_nodes(self) -> List[StorageNode]:
        self._ensure_initialized()
        return list(self._nodes.values())

    def list_online_nodes(self) -> List[StorageNode]:
        self._ensure_initialized()
        return [node for node in self._nodes.values() if node.is_online]

    async def set_node_status(self, node_id: str, status: str) -> bool:
        """Updates the status of a node and emits an event."""
        node = self.get_node(node_id)
        if not node:
            return False

        old_status = node.status
        new_status = status.upper()
        if old_status == new_status:
            return True

        node.set_status(new_status)

        # Update in database
        query = "UPDATE nodes SET status = ?, last_heartbeat = CURRENT_TIMESTAMP WHERE id = ?"
        await db.execute(query, (new_status, node_id))

        # Emit corresponding event
        if new_status == "ONLINE":
            await event_bus.emit(
                EventType.NODE_ONLINE,
                target=node_id,
                message=f"Node {node_id} is now ONLINE",
                details={"node_id": node_id, "path": str(node.directory_path)},
            )
        elif new_status == "OFFLINE":
            await event_bus.emit(
                EventType.NODE_OFFLINE,
                target=node_id,
                message=f"Node {node_id} went OFFLINE",
                details={"node_id": node_id},
            )
        elif new_status == "PARTITIONED":
            await event_bus.emit(
                EventType.NODE_PARTITIONED,
                target=node_id,
                message=f"Node {node_id} is PARTITIONED",
                details={"node_id": node_id},
            )

        return True

    async def set_node_offline(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Takes a node offline for chaos simulation."""
        node = self.get_node(node_id)
        if not node:
            return None
        previous_state = node.status
        await self.set_node_status(node_id, "OFFLINE")
        logger.info(f"[CHAOS] {node_id} marked OFFLINE")

        await event_bus.emit(
            EventType.NODE_FAILURE_SIMULATED,
            target=node_id,
            message=f"Simulated failure: node {node_id} taken OFFLINE",
            details={"node_id": node_id, "previous_state": previous_state, "new_state": "OFFLINE"},
        )
        return {
            "node_id": node_id,
            "previous_state": previous_state,
            "new_state": "OFFLINE",
            "timestamp": datetime.now().isoformat(),
        }

    async def set_node_online(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Brings a node online."""
        node = self.get_node(node_id)
        if not node:
            return None
        previous_state = node.status
        await self.set_node_status(node_id, "ONLINE")
        logger.info(f"[CHAOS] {node_id} marked ONLINE")

        return {
            "node_id": node_id,
            "previous_state": previous_state,
            "new_state": "ONLINE",
            "timestamp": datetime.now().isoformat(),
        }

# Global node manager singleton
node_manager = NodeManager()
