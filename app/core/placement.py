import hashlib
from typing import List

def rendezvous_hash(key: str, node_id: str) -> int:
    """Computes a pseudo-random 64-bit weight for a given (key, node_id) pair."""
    # Salted MD5 / SHA-256 generates uniform distribution
    digest = hashlib.sha256(f"{key}:{node_id}".encode("utf-8")).digest()
    # Take first 8 bytes as an unsigned 64-bit integer
    return int.from_bytes(digest[:8], byteorder="big")

def get_target_nodes(key: str, available_node_ids: List[str], replication_factor: int) -> List[str]:
    """
    Selects the top-N nodes for a key using Highest Random Weight (Rendezvous Hashing).
    
    Guarantees:
    - Deterministic placement for any given key.
    - Minimal data churn when nodes are added or removed (only 1/M keys relocated).
    - Uniform distribution across all active nodes.
    """
    if not available_node_ids:
        return []
    
    # Sort node IDs by hash weight descending, with node_id as tie-breaker
    scored_nodes = sorted(
        available_node_ids,
        key=lambda nid: (rendezvous_hash(key, nid), nid),
        reverse=True,
    )
    
    return scored_nodes[:replication_factor]
