import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from app.storage.exceptions import NodeOfflineError, NodePartitionedError

class StorageNode:
    """Represents an independent local simulated storage node with isolated object directory."""

    def __init__(
        self,
        node_id: str,
        name: str,
        directory_path: Path,
        capacity_bytes: int = 10 * 1024 * 1024 * 1024,
    ):
        self.node_id = node_id
        self.name = name
        self.directory_path = Path(directory_path)
        self.objects_dir = self.directory_path / "objects"
        self.capacity_bytes = capacity_bytes
        self.status = "ONLINE"  # "ONLINE", "OFFLINE", "PARTITIONED"
        self.last_heartbeat: Optional[datetime] = datetime.now()
        self.error_count: int = 0

    @property
    def is_online(self) -> bool:
        return self.status == "ONLINE"

    def ensure_directory(self) -> None:
        """Creates the physical directory and objects/ subfolder for this node."""
        self.directory_path.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    def _check_availability(self) -> None:
        """Validates that node is reachable."""
        if self.status == "OFFLINE":
            raise NodeOfflineError(f"Node '{self.node_id}' is OFFLINE")
        if self.status == "PARTITIONED":
            raise NodePartitionedError(f"Node '{self.node_id}' is PARTITIONED")

    def get_object_path(self, object_id: str) -> Path:
        """Returns deterministic physical file path for an object ID."""
        # Sanitize object_id to prevent any directory traversal
        safe_id = object_id.replace("/", "_").replace("\\", "_")
        return self.objects_dir / safe_id

    def write_object(self, object_id: str, data: bytes) -> bool:
        """Writes object payload atomically via a temporary file in the objects directory."""
        self._check_availability()
        self.ensure_directory()
        
        target_path = self.get_object_path(object_id)
        tmp_path = self.objects_dir / f".tmp_{uuid.uuid4().hex}"
        
        try:
            with open(tmp_path, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, target_path)
            self.heartbeat()
            return True
        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            self.error_count += 1
            raise e

    def read_object(self, object_id: str) -> Optional[bytes]:
        """Reads object payload from disk."""
        self._check_availability()
        target_path = self.get_object_path(object_id)
        if not target_path.exists():
            return None
        
        with open(target_path, "rb") as f:
            data = f.read()
        self.heartbeat()
        return data

    def delete_object(self, object_id: str) -> bool:
        """Deletes object from disk if it exists."""
        self._check_availability()
        target_path = self.get_object_path(object_id)
        if target_path.exists():
            target_path.unlink()
            self.heartbeat()
            return True
        return False

    def has_object(self, object_id: str) -> bool:
        """Checks if object file exists on this node."""
        self._check_availability()
        return self.get_object_path(object_id).exists()

    def count_objects(self) -> int:
        """Counts stored object files (excluding temp files)."""
        if not self.objects_dir.exists():
            return 0
        return sum(1 for p in self.objects_dir.iterdir() if p.is_file() and not p.name.startswith(".tmp_"))

    def get_used_space(self) -> int:
        """Computes total bytes occupied by stored object files."""
        if not self.objects_dir.exists():
            return 0
        total_size = 0
        for p in self.objects_dir.iterdir():
            if p.is_file() and not p.name.startswith(".tmp_"):
                try:
                    total_size += p.stat().st_size
                except OSError:
                    pass
        return total_size

    def heartbeat(self) -> None:
        """Updates node heartbeat timestamp."""
        if self.is_online:
            self.last_heartbeat = datetime.now()

    def set_status(self, status: str) -> None:
        """Sets the node status ('ONLINE', 'OFFLINE', 'PARTITIONED')."""
        self.status = status.upper()
        if self.status == "ONLINE":
            self.heartbeat()

    def get_stats(self) -> Dict[str, Any]:
        """Returns node telemetry and health metrics."""
        return {
            "node_id": self.node_id,
            "name": self.name,
            "path": str(self.directory_path),
            "status": self.status,
            "is_online": self.is_online,
            "stored_object_count": self.count_objects(),
            "storage_used_bytes": self.get_used_space(),
            "capacity_bytes": self.capacity_bytes,
            "last_heartbeat": (
                self.last_heartbeat.strftime("%Y-%m-%d %H:%M:%S")
                if self.last_heartbeat
                else None
            ),
        }
