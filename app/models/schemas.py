from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

# --- Phase 1 Models ---

class HealthResponse(BaseModel):
    """System health check response."""
    status: str = Field(..., example="ok")
    timestamp: str = Field(..., example="2026-09-26T03:00:00Z")
    database_status: str = Field(..., example="connected")
    active_nodes: int = Field(..., example=5)
    total_nodes: int = Field(..., example=5)
    version: str = Field(..., example="0.1.0")

class NodeInfo(BaseModel):
    """Storage node status and metrics."""
    node_id: str = Field(..., example="node_1")
    name: str = Field(..., example="Storage Node 1")
    path: str = Field(..., example="data/nodes/node_1")
    status: str = Field(..., example="ONLINE")  # ONLINE, OFFLINE, PARTITIONED
    is_online: bool = Field(..., example=True)
    stored_object_count: int = Field(..., example=0)
    storage_used_bytes: int = Field(..., example=0)
    capacity_bytes: int = Field(..., example=10737418240)
    last_heartbeat: Optional[str] = Field(None, example="2026-09-26 03:00:00")

class ConfigResponse(BaseModel):
    """Vault cluster configuration response."""
    num_nodes: int = Field(..., example=5)
    default_replication_factor: int = Field(..., example=3)
    write_quorum: int = Field(..., example=2)
    read_quorum: int = Field(..., example=2)
    storage_root: str = Field(..., example="C:/Users/.../data")
    max_upload_size_bytes: int = Field(..., example=52428800)
    health_check_interval_seconds: int = Field(..., example=10)

class EventResponse(BaseModel):
    """Audit / system event response."""
    id: int
    event_type: str
    target: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: str


# --- Phase 2 Object Storage & Replication Models ---

class ObjectMetadata(BaseModel):
    """Model representing a stored object and its replica placement."""
    object_id: str
    filename: str
    size: int
    checksum: str
    content_type: str
    replication_factor: int
    version: int
    created_at: str
    updated_at: str
    replica_nodes: List[str]

class ReplicaVerificationDetail(BaseModel):
    """Integrity check detail for a single replica node."""
    node_id: str
    node_state: str = "ONLINE"
    exists: bool
    healthy: bool
    status: str = "HEALTHY"  # HEALTHY, CORRUPTED, MISSING, UNAVAILABLE
    checksum: Optional[str] = None

class ObjectVerificationReport(BaseModel):
    """Cluster-wide integrity verification report for an object."""
    object_id: str
    expected_checksum: str
    replicas: List[ReplicaVerificationDetail]
    healthy_replicas: int
    corrupt_replicas: int
    missing_replicas: int
    unavailable_replicas: int = 0


# --- Phase 3 Chaos Engineering & Repair Models ---

class NodeStateTransitionResponse(BaseModel):
    """Result of taking a node offline, online, or partitioned."""
    node_id: str
    previous_state: str
    new_state: str
    timestamp: str

class ReplicaCorruptionResponse(BaseModel):
    """Result of injecting byte-level corruption into a physical replica."""
    object_id: str
    node_id: str
    status: str = "CORRUPTED"
    corrupted_checksum: str
    original_checksum: str
    message: str

class ObjectRepairResponse(BaseModel):
    """Result of a self-healing replica repair operation."""
    object_id: str
    repaired_replicas: List[str]
    source_replica: Optional[str] = None
    failed_repairs: List[Dict[str, Any]] = []
    healthy_replicas: int
    corrupt_replicas: int
    missing_replicas: int
    unavailable_replicas: int = 0
    message: str


# --- Phase 4 Dashboard & Observability Models ---

class ClusterStatsResponse(BaseModel):
    """Aggregated real-time cluster metrics for dashboard."""
    total_nodes: int
    active_nodes: int
    offline_nodes: int
    total_objects: int
    total_storage_used_bytes: int
    healthy_replicas: int
    corrupt_replicas: int
    missing_replicas: int
    cluster_status: str  # "OK" or "DEGRADED"
