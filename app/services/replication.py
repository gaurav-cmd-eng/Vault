import asyncio
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from app.config import settings
from app.database.db import db
from app.utils.hashing import compute_sha256, verify_sha256
from app.core.placement import get_target_nodes
from app.core.locks import key_lock_manager
from app.storage.node_manager import node_manager
from app.events.event_bus import event_bus
from app.events.types import EventType
from app.utils.logger import logger
from app.models.schemas import (
    ObjectMetadata,
    ObjectVerificationReport,
    ReplicaVerificationDetail,
    ReplicaCorruptionResponse,
    ObjectRepairResponse,
)

class ReplicationError(Exception):
    """Raised when replication requirements cannot be satisfied."""
    pass

class ObjectNotFoundError(Exception):
    """Raised when requested object does not exist."""
    pass

class ObjectCorruptedError(Exception):
    """Raised when all available replicas fail integrity verification."""
    pass

class ReplicationService:
    """Manages object storage, replication, retrieval, chaos injection, and self-healing."""

    async def store_object(
        self,
        filename: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        replication_factor: Optional[int] = None,
        custom_object_id: Optional[str] = None,
    ) -> ObjectMetadata:
        """Stores an uploaded object across multiple storage nodes according to replication factor."""
        if replication_factor is None:
            replication_factor = settings.default_replication_factor

        # 1. Validation
        if replication_factor < 1:
            raise ReplicationError(f"Replication factor must be at least 1 (got {replication_factor})")

        online_nodes = node_manager.list_online_nodes()
        total_online = len(online_nodes)
        if replication_factor > total_online:
            raise ReplicationError(
                f"Requested replication factor ({replication_factor}) exceeds available online storage nodes ({total_online})"
            )

        if len(data) > settings.max_upload_size:
            raise ReplicationError(
                f"Upload size ({len(data)} bytes) exceeds configured limit of {settings.max_upload_size} bytes"
            )

        object_id = custom_object_id or uuid.uuid4().hex

        # Emit upload started
        await event_bus.emit(
            EventType.OBJECT_UPLOAD_STARTED,
            target=object_id,
            message=f"Starting upload for '{filename}' ({len(data)} bytes)",
            details={"filename": filename, "size": len(data), "replication_factor": replication_factor},
        )
        logger.info(f"[UPLOAD] object {object_id} ({filename}) received, size={len(data)} bytes")

        # 2. Compute SHA-256
        checksum = compute_sha256(data)
        logger.info(f"[HASH] SHA-256 calculated: {checksum}")

        # 3. Deterministic replica placement
        online_node_ids = [n.node_id for n in online_nodes]
        target_node_ids = get_target_nodes(object_id, online_node_ids, replication_factor)
        logger.info(f"[REPLICATION] factor={replication_factor}, assigned nodes={target_node_ids}")

        async with key_lock_manager.acquire(object_id):
            written_nodes: List[str] = []
            try:
                # 4. Write to selected nodes
                for nid in target_node_ids:
                    node = node_manager.get_node(nid)
                    if not node or not node.is_online:
                        raise ReplicationError(f"Node '{nid}' became unavailable during replication")
                    
                    await asyncio.to_thread(node.write_object, object_id, data)
                    written_nodes.append(nid)
                    logger.info(f"[REPLICA] {object_id} -> {nid}")
                    
                    await event_bus.emit(
                        EventType.REPLICA_CREATED,
                        target=object_id,
                        message=f"Replica written to node {nid}",
                        details={"object_id": object_id, "node_id": nid},
                    )

                # 5. Persist metadata in SQLite
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                await db.execute(
                    """
                    INSERT INTO objects (object_id, filename, size, checksum, content_type, replication_factor, version, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(object_id) DO UPDATE SET
                        filename=excluded.filename,
                        size=excluded.size,
                        checksum=excluded.checksum,
                        content_type=excluded.content_type,
                        replication_factor=excluded.replication_factor,
                        version=version + 1,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (object_id, filename, len(data), checksum, content_type, replication_factor),
                )

                for nid in written_nodes:
                    await db.execute(
                        """
                        INSERT INTO replicas (object_id, node_id, version, status, last_verified_at)
                        VALUES (?, ?, 1, 'HEALTHY', CURRENT_TIMESTAMP)
                        ON CONFLICT(object_id, node_id) DO UPDATE SET
                            version=excluded.version,
                            status='HEALTHY',
                            last_verified_at=CURRENT_TIMESTAMP
                        """,
                        (object_id, nid),
                    )

                logger.info(f"[VERIFY] {len(written_nodes)}/{replication_factor} replicas successfully written")

                # Emit object created event
                await event_bus.emit(
                    EventType.OBJECT_CREATED,
                    target=object_id,
                    message=f"Object '{filename}' stored successfully across {len(written_nodes)} nodes",
                    details={
                        "object_id": object_id,
                        "filename": filename,
                        "checksum": checksum,
                        "replica_nodes": written_nodes,
                    },
                )

                return ObjectMetadata(
                    object_id=object_id,
                    filename=filename,
                    size=len(data),
                    checksum=checksum,
                    content_type=content_type,
                    replication_factor=replication_factor,
                    version=1,
                    created_at=now_str,
                    updated_at=now_str,
                    replica_nodes=written_nodes,
                )

            except Exception as e:
                # Clean up any partial writes if error occurs
                logger.error(f"[UPLOAD] Failed to complete replication for {object_id}: {e}")
                for nid in written_nodes:
                    node = node_manager.get_node(nid)
                    if node:
                        try:
                            await asyncio.to_thread(node.delete_object, object_id)
                        except Exception:
                            pass
                await event_bus.emit(
                    EventType.OBJECT_UPLOAD_FAILED,
                    target=object_id,
                    message=f"Failed to upload object: {e}",
                    details={"filename": filename, "error": str(e)},
                )
                raise e

    async def get_object_metadata(self, object_id: str) -> Optional[ObjectMetadata]:
        """Retrieves metadata for an object from database."""
        row = await db.fetch_one("SELECT * FROM objects WHERE object_id = ?", (object_id,))
        if not row:
            return None

        replica_rows = await db.fetch_all(
            "SELECT node_id FROM replicas WHERE object_id = ?", (object_id,)
        )
        replica_nodes = [r["node_id"] for r in replica_rows]

        return ObjectMetadata(
            object_id=row["object_id"],
            filename=row["filename"],
            size=row["size"],
            checksum=row["checksum"],
            content_type=row["content_type"],
            replication_factor=row["replication_factor"],
            version=row["version"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            replica_nodes=replica_nodes,
        )

    async def list_objects(self) -> List[ObjectMetadata]:
        """Lists all stored objects with their replica locations."""
        rows = await db.fetch_all("SELECT * FROM objects ORDER BY created_at DESC")
        result = []
        for row in rows:
            meta = await self.get_object_metadata(row["object_id"])
            if meta:
                result.append(meta)
        return result

    async def retrieve_object(self, object_id: str) -> Tuple[bytes, str, str]:
        """
        Retrieves object data from an available healthy replica with checksum verification.
        Returns (content_bytes, filename, content_type).
        """
        meta = await self.get_object_metadata(object_id)
        if not meta:
            raise ObjectNotFoundError(f"Object '{object_id}' not found")

        expected_checksum = meta.checksum

        # Try replicas in order
        for node_id in meta.replica_nodes:
            node = node_manager.get_node(node_id)
            if not node or not node.is_online:
                continue

            try:
                data = await asyncio.to_thread(node.read_object, object_id)
                if data is None:
                    continue

                # Verify SHA-256
                if verify_sha256(data, expected_checksum):
                    logger.info(f"[READ] Object {object_id} retrieved and verified from {node_id}")
                    await event_bus.emit(
                        EventType.OBJECT_DOWNLOADED,
                        target=object_id,
                        message=f"Object '{meta.filename}' downloaded and verified from {node_id}",
                        details={"object_id": object_id, "node_id": node_id, "checksum": expected_checksum},
                    )
                    return data, meta.filename, meta.content_type
                else:
                    logger.warning(f"[CORRUPT] Checksum mismatch on node {node_id} for object {object_id}")
                    await db.execute(
                        "UPDATE replicas SET status='CORRUPTED' WHERE object_id=? AND node_id=?",
                        (object_id, node_id),
                    )
            except Exception as e:
                logger.warning(f"Error reading from node {node_id}: {e}")
                continue

        raise ObjectCorruptedError(f"No valid replica found for object '{object_id}'. All replicas failed or missing.")

    async def corrupt_replica(self, object_id: str, node_id: str) -> ReplicaCorruptionResponse:
        """
        Injects deterministic corruption into exactly one physical replica on disk.
        Does NOT touch object metadata or expected SHA-256.
        """
        meta = await self.get_object_metadata(object_id)
        if not meta:
            raise ObjectNotFoundError(f"Object '{object_id}' not found")

        if node_id not in meta.replica_nodes:
            raise ReplicationError(f"Node '{node_id}' is not an assigned replica for object '{object_id}'")

        node = node_manager.get_node(node_id)
        if not node or not node.is_online:
            raise ReplicationError(f"Node '{node_id}' is not online or unavailable")

        path = node.get_object_path(object_id)
        if not path.exists():
            raise ObjectNotFoundError(f"Physical replica file does not exist on node '{node_id}'")

        data = path.read_bytes()

        # Deterministic, obvious corruption: alter bytes
        if len(data) >= 8:
            corrupted_data = b"CORRUPT!" + data[8:]
        else:
            corrupted_data = b"CORRUPTED_" + data

        # Write directly to disk
        path.write_bytes(corrupted_data)
        corrupted_hash = compute_sha256(corrupted_data)

        # Update replica status in database
        await db.execute(
            "UPDATE replicas SET status='CORRUPTED' WHERE object_id=? AND node_id=?",
            (object_id, node_id),
        )

        logger.info(f"[CHAOS] Corruption injected into {object_id} on {node_id}")

        await event_bus.emit(
            EventType.CORRUPTION_INJECTED,
            target=object_id,
            message=f"Corruption injected into replica on node {node_id}",
            details={
                "object_id": object_id,
                "node_id": node_id,
                "corrupted_checksum": corrupted_hash,
                "original_checksum": meta.checksum,
            },
        )

        return ReplicaCorruptionResponse(
            object_id=object_id,
            node_id=node_id,
            status="CORRUPTED",
            corrupted_checksum=corrupted_hash,
            original_checksum=meta.checksum,
            message=f"Physical replica on node {node_id} successfully corrupted",
        )

    async def verify_object(self, object_id: str) -> ObjectVerificationReport:
        """
        Checks all known replicas of an object on disk and verifies their SHA-256 hashes.
        Reports healthy, corrupt, missing, and unavailable status.
        """
        meta = await self.get_object_metadata(object_id)
        if not meta:
            raise ObjectNotFoundError(f"Object '{object_id}' not found")

        expected_checksum = meta.checksum
        replica_details: List[ReplicaVerificationDetail] = []
        healthy_count = 0
        corrupt_count = 0
        missing_count = 0
        unavailable_count = 0

        for node_id in meta.replica_nodes:
            node = node_manager.get_node(node_id)
            if not node:
                missing_count += 1
                replica_details.append(
                    ReplicaVerificationDetail(
                        node_id=node_id,
                        node_state="OFFLINE",
                        exists=False,
                        healthy=False,
                        status="MISSING",
                    )
                )
                continue

            # Check if node is offline or partitioned
            if not node.is_online:
                unavailable_count += 1
                replica_details.append(
                    ReplicaVerificationDetail(
                        node_id=node_id,
                        node_state=node.status,
                        exists=False,
                        healthy=False,
                        status="UNAVAILABLE",
                    )
                )
                continue

            try:
                data = await asyncio.to_thread(node.read_object, object_id)
                if data is None:
                    missing_count += 1
                    replica_details.append(
                        ReplicaVerificationDetail(
                            node_id=node_id,
                            node_state="ONLINE",
                            exists=False,
                            healthy=False,
                            status="MISSING",
                        )
                    )
                else:
                    actual_checksum = compute_sha256(data)
                    is_healthy = (actual_checksum.lower() == expected_checksum.lower())
                    if is_healthy:
                        healthy_count += 1
                        rep_status = "HEALTHY"
                    else:
                        corrupt_count += 1
                        rep_status = "CORRUPTED"

                    replica_details.append(
                        ReplicaVerificationDetail(
                            node_id=node_id,
                            node_state="ONLINE",
                            exists=True,
                            healthy=is_healthy,
                            status=rep_status,
                            checksum=actual_checksum,
                        )
                    )
            except Exception:
                missing_count += 1
                replica_details.append(
                    ReplicaVerificationDetail(
                        node_id=node_id,
                        node_state="ONLINE",
                        exists=False,
                        healthy=False,
                        status="MISSING",
                    )
                )

        logger.info(
            f"[VERIFY] object {object_id}: {healthy_count} healthy, {corrupt_count} corrupt, {missing_count} missing, {unavailable_count} unavailable"
        )
        
        await event_bus.emit(
            EventType.OBJECT_VERIFIED,
            target=object_id,
            message=f"Verified replicas: {healthy_count} healthy, {corrupt_count} corrupt, {missing_count + unavailable_count} missing/unavailable",
            details={
                "object_id": object_id,
                "healthy_replicas": healthy_count,
                "corrupt_replicas": corrupt_count,
                "missing_replicas": missing_count,
                "unavailable_replicas": unavailable_count,
            },
        )

        return ObjectVerificationReport(
            object_id=object_id,
            expected_checksum=expected_checksum,
            replicas=replica_details,
            healthy_replicas=healthy_count,
            corrupt_replicas=corrupt_count,
            missing_replicas=missing_count + unavailable_count,
            unavailable_replicas=unavailable_count,
        )

    async def repair_object(self, object_id: str) -> ObjectRepairResponse:
        """
        Self-healing repair service:
        Finds a verified healthy source replica, copies it atomically to any corrupt or missing nodes,
        and re-verifies SHA-256 integrity.
        """
        async with key_lock_manager.acquire(object_id):
            meta = await self.get_object_metadata(object_id)
            if not meta:
                raise ObjectNotFoundError(f"Object '{object_id}' not found")

            # 1. Locate a verified healthy source replica
            source_node_id: Optional[str] = None
            source_data: Optional[bytes] = None

            for nid in meta.replica_nodes:
                node = node_manager.get_node(nid)
                if not node or not node.is_online:
                    continue

                try:
                    data = await asyncio.to_thread(node.read_object, object_id)
                    if data and verify_sha256(data, meta.checksum):
                        source_node_id = nid
                        source_data = data
                        logger.info(f"[REPAIR] Source replica: {nid}")
                        break
                except Exception:
                    continue

            if source_data is None:
                logger.warning(f"[REPAIR] No healthy source replica found for {object_id}")
                raise ReplicationError(
                    f"Cannot repair object '{object_id}': No healthy source replica exists in cluster"
                )

            repaired_nodes: List[str] = []
            failed_repairs: List[Dict[str, Any]] = []

            # 2. Iterate through replicas needing repair
            for nid in meta.replica_nodes:
                if nid == source_node_id:
                    continue

                node = node_manager.get_node(nid)
                if not node or not node.is_online:
                    failed_repairs.append({
                        "node_id": nid,
                        "reason": f"Node '{nid}' is OFFLINE or unavailable",
                    })
                    continue

                # Check if this node actually needs repair
                needs_repair = False
                try:
                    existing_data = await asyncio.to_thread(node.read_object, object_id)
                    if existing_data is None or not verify_sha256(existing_data, meta.checksum):
                        needs_repair = True
                except Exception:
                    needs_repair = True

                if needs_repair:
                    logger.info(f"[REPAIR] Target replica: {nid}")
                    try:
                        # Write source content atomically
                        await asyncio.to_thread(node.write_object, object_id, source_data)
                        
                        # Verify SHA-256 on target
                        verified_data = await asyncio.to_thread(node.read_object, object_id)
                        if verified_data and verify_sha256(verified_data, meta.checksum):
                            logger.info("[REPAIR] SHA-256 verified")
                            logger.info(f"[REPAIR] Replica restored successfully on {nid}")
                            
                            # Update database
                            await db.execute(
                                "UPDATE replicas SET status='HEALTHY', last_verified_at=CURRENT_TIMESTAMP WHERE object_id=? AND node_id=?",
                                (object_id, nid),
                            )
                            repaired_nodes.append(nid)
                            
                            await event_bus.emit(
                                EventType.REPLICA_REPAIRED,
                                target=object_id,
                                message=f"Replica on node {nid} successfully repaired from {source_node_id}",
                                details={
                                    "object_id": object_id,
                                    "target_node": nid,
                                    "source_node": source_node_id,
                                },
                            )
                        else:
                            failed_repairs.append({
                                "node_id": nid,
                                "reason": "Integrity check failed after write",
                            })
                    except Exception as e:
                        logger.error(f"[REPAIR] Error repairing {nid}: {e}")
                        failed_repairs.append({
                            "node_id": nid,
                            "reason": str(e),
                        })

            # 3. Final verification report
            final_report = await self.verify_object(object_id)

            msg = (
                f"Successfully repaired {len(repaired_nodes)} replica(s)"
                if repaired_nodes
                else "No corrupted or missing replicas required repair on active nodes"
            )

            return ObjectRepairResponse(
                object_id=object_id,
                repaired_replicas=repaired_nodes,
                source_replica=source_node_id,
                failed_repairs=failed_repairs,
                healthy_replicas=final_report.healthy_replicas,
                corrupt_replicas=final_report.corrupt_replicas,
                missing_replicas=final_report.missing_replicas,
                unavailable_replicas=final_report.unavailable_replicas,
                message=msg,
            )

    async def delete_object(self, object_id: str) -> bool:
        """Deletes object metadata, replica metadata, and physical files from all replica nodes."""
        async with key_lock_manager.acquire(object_id):
            meta = await self.get_object_metadata(object_id)
            if not meta:
                return False

            # Delete physical files from all nodes
            for node_id in meta.replica_nodes:
                node = node_manager.get_node(node_id)
                if node:
                    try:
                        await asyncio.to_thread(node.delete_object, object_id)
                    except Exception:
                        pass

            # Also delete from any other node just in case
            for node in node_manager.list_nodes():
                try:
                    await asyncio.to_thread(node.delete_object, object_id)
                except Exception:
                    pass

            # Delete database records
            await db.execute("DELETE FROM replicas WHERE object_id = ?", (object_id,))
            await db.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))

            logger.info(f"[DELETE] Object {object_id} ({meta.filename}) deleted from all nodes and metadata")
            
            await event_bus.emit(
                EventType.OBJECT_DELETED,
                target=object_id,
                message=f"Object '{meta.filename}' ({object_id}) deleted",
                details={"object_id": object_id, "filename": meta.filename},
            )

            return True

replication_service = ReplicationService()
