import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from app.config import settings
from app.database.db import db
from app.core.hashing import compute_sha256, verify_sha256
from app.core.placement import get_target_nodes
from app.core.locks import key_lock_manager
from app.storage.node_manager import node_manager
from app.events.event_bus import event_bus
from app.events.types import EventType
from app.utils.logger import logger

class QuorumError(Exception):
    """Raised when quorum cannot be satisfied."""
    pass

class ObjectNotFoundError(Exception):
    """Raised when the requested object key does not exist."""
    pass

class ObjectCorruptedError(Exception):
    """Raised when all available replicas fail integrity verification."""
    pass

class CoordinatorService:
    """Orchestrates quorum-based distributed reads and writes across storage nodes."""

    async def write_object(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        """
        Executes a quorum write across N replicas.
        Succeeds if at least W nodes confirm write.
        """
        async with key_lock_manager.acquire(key):
            checksum = compute_sha256(data)
            size = len(data)
            
            # 1. Determine target nodes using Rendezvous Hashing
            all_nodes = node_manager.list_nodes()
            online_node_ids = [n.node_id for n in all_nodes if n.is_online]
            
            target_node_ids = get_target_nodes(
                key=key,
                available_node_ids=online_node_ids,
                replication_factor=settings.default_replication_factor,
            )
            
            if len(target_node_ids) < settings.write_quorum:
                msg = (
                    f"Insufficient online nodes ({len(target_node_ids)}) to satisfy "
                    f"write quorum W={settings.write_quorum}"
                )
                logger.error(msg)
                raise QuorumError(msg)

            # 2. Parallel writes to target nodes
            async def _write_node(nid: str) -> Tuple[str, bool]:
                node = node_manager.get_node(nid)
                if not node:
                    return nid, False
                try:
                    await asyncio.to_thread(node.write_object, key, data)
                    return nid, True
                except Exception as e:
                    logger.warning(f"Write failed on node {nid} for object '{key}': {e}")
                    return nid, False

            tasks = [_write_node(nid) for nid in target_node_ids]
            results = await asyncio.gather(*tasks)
            
            successful_nodes = [nid for nid, success in results if success]
            failed_nodes = [nid for nid, success in results if not success]

            # 3. Check Write Quorum (W)
            if len(successful_nodes) < settings.write_quorum:
                # Quorum failed: roll back writes on successful nodes
                logger.error(
                    f"Write quorum failed for '{key}'. Successes: {len(successful_nodes)}/{settings.write_quorum}"
                )
                for nid in successful_nodes:
                    node = node_manager.get_node(nid)
                    if node:
                        await asyncio.to_thread(node.delete_object, key)
                raise QuorumError(
                    f"Write quorum not satisfied. Required: {settings.write_quorum}, achieved: {len(successful_nodes)}"
                )

            # 4. Commit metadata transaction to SQLite
            # Check if object exists to determine version
            existing = await db.fetch_one("SELECT version FROM objects WHERE key = ?", (key,))
            new_version = (existing["version"] + 1) if existing else 1

            insert_obj_query = """
                INSERT INTO objects (key, size, checksum, replication_factor, content_type, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                    size=excluded.size,
                    checksum=excluded.checksum,
                    replication_factor=excluded.replication_factor,
                    content_type=excluded.content_type,
                    version=excluded.version,
                    updated_at=CURRENT_TIMESTAMP
            """
            await db.execute(
                insert_obj_query,
                (key, size, checksum, settings.default_replication_factor, content_type, new_version),
            )

            # Insert/update replica records for all target nodes
            for nid in successful_nodes:
                replica_query = """
                    INSERT INTO replicas (object_key, node_id, version, status, last_verified_at)
                    VALUES (?, ?, ?, 'HEALTHY', CURRENT_TIMESTAMP)
                    ON CONFLICT(object_key, node_id) DO UPDATE SET
                        version=excluded.version,
                        status='HEALTHY',
                        last_verified_at=CURRENT_TIMESTAMP
                """
                await db.execute(replica_query, (key, nid, new_version))

            for nid in failed_nodes:
                replica_query = """
                    INSERT INTO replicas (object_key, node_id, version, status, last_verified_at)
                    VALUES (?, ?, ?, 'MISSING', CURRENT_TIMESTAMP)
                    ON CONFLICT(object_key, node_id) DO UPDATE SET
                        version=excluded.version,
                        status='MISSING',
                        last_verified_at=CURRENT_TIMESTAMP
                """
                await db.execute(replica_query, (key, nid, new_version))

            # 5. Emit event
            await event_bus.emit(
                EventType.OBJECT_CREATED,
                target=key,
                message=f"Object '{key}' stored with {len(successful_nodes)} replicas (version {new_version})",
                details={
                    "key": key,
                    "size": size,
                    "checksum": checksum,
                    "replicas": successful_nodes,
                    "version": new_version,
                },
            )

            return {
                "key": key,
                "size": size,
                "checksum": checksum,
                "version": new_version,
                "content_type": content_type,
                "replicas": successful_nodes,
                "created_at": datetime.now().isoformat(),
            }

    async def read_object(self, key: str) -> Tuple[bytes, str, Dict[str, Any]]:
        """
        Executes a quorum read across R replicas, verifying SHA-256 checksum.
        Returns (content_bytes, content_type, metadata_dict).
        """
        # 1. Fetch object metadata
        obj_row = await db.fetch_one("SELECT * FROM objects WHERE key = ?", (key,))
        if not obj_row:
            raise ObjectNotFoundError(f"Object '{key}' not found")

        expected_checksum = obj_row["checksum"]
        content_type = obj_row["content_type"]

        # 2. Get registered replica nodes
        replica_rows = await db.fetch_all(
            "SELECT node_id, status FROM replicas WHERE object_key = ?", (key,)
        )
        candidate_node_ids = [r["node_id"] for r in replica_rows]

        # If no replicas registered yet, fallback to Rendezvous target nodes
        if not candidate_node_ids:
            all_node_ids = [n.node_id for n in node_manager.list_nodes()]
            candidate_node_ids = get_target_nodes(
                key, all_node_ids, settings.default_replication_factor
            )

        # 3. Read and verify integrity
        valid_data: Optional[bytes] = None
        verified_node_id: Optional[str] = None
        corrupted_nodes: List[str] = []

        for nid in candidate_node_ids:
            node = node_manager.get_node(nid)
            if not node or not node.is_online:
                continue

            try:
                data = await asyncio.to_thread(node.read_object, key)
                if data is None:
                    continue

                if verify_sha256(data, expected_checksum):
                    valid_data = data
                    verified_node_id = nid
                    # Update replica verified timestamp
                    await db.execute(
                        "UPDATE replicas SET status='HEALTHY', last_verified_at=CURRENT_TIMESTAMP WHERE object_key=? AND node_id=?",
                        (key, nid),
                    )
                    break
                else:
                    # Corruption detected!
                    logger.warning(f"Data corruption detected on node {nid} for object '{key}'")
                    corrupted_nodes.append(nid)
                    await db.execute(
                        "UPDATE replicas SET status='CORRUPTED', last_verified_at=CURRENT_TIMESTAMP WHERE object_key=? AND node_id=?",
                        (key, nid),
                    )
                    await event_bus.emit(
                        EventType.CORRUPTION_DETECTED,
                        target=key,
                        message=f"Bit-rot/corruption detected on node {nid} for object '{key}'",
                        details={"key": key, "node_id": nid},
                    )
            except Exception as e:
                logger.warning(f"Error reading from node {nid} for '{key}': {e}")
                continue

        if valid_data is None:
            raise ObjectCorruptedError(
                f"No healthy replicas available for object '{key}'. Integrity verification failed."
            )

        # Emit read event
        await event_bus.emit(
            EventType.OBJECT_READ,
            target=key,
            message=f"Object '{key}' read from node {verified_node_id} and verified",
            details={"key": key, "verified_on": verified_node_id},
        )

        metadata = {
            "key": key,
            "size": obj_row["size"],
            "checksum": expected_checksum,
            "version": obj_row["version"],
            "content_type": content_type,
            "created_at": obj_row["created_at"],
            "updated_at": obj_row["updated_at"],
        }

        return valid_data, content_type, metadata

    async def delete_object(self, key: str) -> bool:
        """Deletes object and all its replicas across storage nodes and database."""
        async with key_lock_manager.acquire(key):
            obj_row = await db.fetch_one("SELECT * FROM objects WHERE key = ?", (key,))
            if not obj_row:
                return False

            # Delete physical replicas on all nodes
            all_nodes = node_manager.list_nodes()
            for node in all_nodes:
                try:
                    await asyncio.to_thread(node.delete_object, key)
                except Exception:
                    pass

            # Delete metadata
            await db.execute("DELETE FROM replicas WHERE object_key = ?", (key,))
            await db.execute("DELETE FROM objects WHERE key = ?", (key,))

            await event_bus.emit(
                EventType.OBJECT_DELETED,
                target=key,
                message=f"Object '{key}' deleted from all replicas and metadata catalog",
                details={"key": key},
            )
            return True

    async def get_object_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieves metadata and replica placement status for an object."""
        obj_row = await db.fetch_one("SELECT * FROM objects WHERE key = ?", (key,))
        if not obj_row:
            return None

        replica_rows = await db.fetch_all(
            "SELECT node_id, status, version, last_verified_at FROM replicas WHERE object_key = ?",
            (key,),
        )
        replicas = [
            {
                "node_id": r["node_id"],
                "status": r["status"],
                "version": r["version"],
                "last_verified_at": r["last_verified_at"],
            }
            for r in replica_rows
        ]

        return {
            "key": obj_row["key"],
            "size": obj_row["size"],
            "checksum": obj_row["checksum"],
            "replication_factor": obj_row["replication_factor"],
            "content_type": obj_row["content_type"],
            "version": obj_row["version"],
            "created_at": obj_row["created_at"],
            "updated_at": obj_row["updated_at"],
            "replicas": replicas,
        }

    async def list_objects(self) -> List[Dict[str, Any]]:
        """Lists all stored objects with replica details."""
        rows = await db.fetch_all("SELECT * FROM objects ORDER BY created_at DESC")
        objects_list = []
        for r in rows:
            meta = await self.get_object_metadata(r["key"])
            if meta:
                objects_list.append(meta)
        return objects_list

coordinator_service = CoordinatorService()
