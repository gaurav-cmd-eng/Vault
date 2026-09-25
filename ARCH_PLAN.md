# Vault: Fault-Tolerant Distributed Object Storage System
## Architecture & Technical Specification Plan (4-Hour Hackathon MVP)

---

### 1. Requirements & Problem Analysis

Vault is designed to provide S3-like object storage semantics running locally on simulated unreliable nodes, with full demonstrable resilience against real faults:
- **Node Failures**: Entire storage nodes dropping offline and coming back online.
- **Network Partitions**: Simulated dropped requests or isolated nodes.
- **Bit-Rot & Data Corruption**: Physical byte tampering on disk detected via cryptographic hashes.
- **Replica Inconsistency & Healing**: Divergent replicas detected on read (Read Repair) or via background scanning (Active Scrubber).
- **Tunable Durability & Availability**: Configurable Quorum ($N$ replicas, $W$ write quorum, $R$ read quorum).
- **Live Demo Dashboard**: Interactive visual panel for uploading files, inspecting replica distributions, injecting chaos, and watching real-time self-healing.

---

### 2. High-Level Architecture

Vault employs a **Coordinator-Node Model** with a Dynamo-style quorum consistency protocol and centralized metadata tracking:

```
                  +----------------------------------------------+
                  |         Client / Web Dashboard UI            |
                  +----------------------------------------------+
                                         |
                                HTTP / REST API
                                         v
                  +----------------------------------------------+
                  |           Vault Coordinator (FastAPI)        |
                  |  - Key-level concurrency locks               |
                  |  - Rendezvous / Hash Node Placement          |
                  |  - Quorum Coordinator (W, R)                 |
                  |  - Background Repair & Scrubbing Worker      |
                  +----------------------------------------------+
                         /               |               \
            (Metadata)  /                | (I/O & Chaos)  \
                       v                 v                 v
            +-------------------+   +------------------------------------+
            | SQLite Metadata   |   | Simulated Storage Node Pool (1..M) |
            | - Objects         |   | - Node 1: ./data/nodes/node_1/     |
            | - Replicas        |   | - Node 2: ./data/nodes/node_2/     |
            | - Node Registry   |   | - Node 3: ./data/nodes/node_3/     |
            | - Audit Logs      |   | - Node 4: ./data/nodes/node_4/     |
            +-------------------+   | - Node 5: ./data/nodes/node_5/     |
                                    +------------------------------------+
```

#### Core Components:
1. **API Gateway / Coordinator**: FastAPI server routing client requests, computing object hashes, coordinating reads/writes across node replicas, evaluating quorums, and enforcing metadata transactions.
2. **Metadata Catalog (SQLite WAL)**: Maintains object metadata (key, version, size, SHA-256 checksum, content-type) and replica placement states (node_id, status: healthy/corrupt/missing, last_verified).
3. **Simulated Storage Nodes**: Virtual nodes mapped to isolated disk folders (`data/nodes/node_X/`). Each node has simulated network/hardware state flags (Online, Offline, Partitioned, Latency) intercepted at the I/O layer.
4. **Chaos Injection Engine**: Programmatic and UI controls to kill nodes, revive nodes, sever network links (partitions), and physically corrupt target byte ranges inside stored files.
5. **Self-Healing Engine (Dual-Layer)**:
   - **Reactive (Read Repair)**: On read, if any replica fails hash verification or is unreachable, the coordinator repairs it using verified data from healthy quorum replicas.
   - **Proactive (Background Scrubber)**: Periodic or triggered daemon that walks all objects, verifies SHA-256 hashes against disk files, and automatically re-replicates degraded objects to healthy nodes.
6. **Web Dashboard**: Single-page reactive dashboard with real-time SSE/polling event streaming, node topology cards, live chunk inspector, upload/download controls, and chaos trigger switches.

---

### 3. Project Directory Structure

```
Vault/
│
├── ARCH_PLAN.md                 # This technical specification
├── README.md                    # Setup, run, and demo instructions
├── requirements.txt             # Python dependencies (fastapi, uvicorn, aiosqlite, etc.)
│
├── data/                        # Local runtime storage (gitignored)
│   ├── metadata.db              # SQLite database (WAL mode)
│   └── nodes/                   # Simulated physical storage disks
│       ├── node_1/
│       ├── node_2/
│       ├── node_3/
│       ├── node_4/
│       └── node_5/
│
├── app/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app initialization & lifespan startup
│   ├── config.py                # System settings (default N=3, W=2, R=2, paths)
│   │
│   ├── core/
│   │   ├── __init__.py
│   │   ├── hashing.py           # SHA-256 computation & verification helpers
│   │   ├── placement.py         # Rendezvous (Highest Random Weight) hashing algorithm
│   │   └── locks.py             # Key-level asynchronous lock manager
│   │
│   ├── metadata/
│   │   ├── __init__.py
│   │   ├── db.py                # SQLite async connection & WAL setup
│   │   ├── schema.sql           # Tables: nodes, objects, replicas, audit_events
│   │   └── repository.py        # CRUD queries for metadata and replica tracking
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── node.py              # StorageNode abstraction (disk I/O, state checks)
│   │   ├── node_manager.py      # Pool of nodes (1..5), failure & chaos interceptor
│   │   └── chaos.py             # Byte corruption, drop simulation, partition logic
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── coordinator.py       # Write Quorum, Read Quorum, Read Repair execution
│   │   ├── scrubber.py          # Background integrity verifier & active self-healing
│   │   └── rebalancer.py        # Node addition/removal migration logic
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── objects.py           # PUT/GET/DELETE/HEAD object operations
│   │   ├── nodes.py             # Node status, list nodes, capacity metrics
│   │   ├── chaos.py             # Chaos endpoints (kill, revive, corrupt, partition)
│   │   └── maintenance.py       # Trigger scrub, trigger rebalance, system stats
│   │
│   └── static/                  # Web dashboard UI
│       ├── index.html           # Single-page UI with Tailwind + modern dashboard
│       ├── app.js               # Reactive state, charts/visuals, live logs
│       └── style.css            # Custom styles & status pulse indicators
│
└── run.py                       # Launch entry point (python run.py)
```

---

### 4. Data Model (SQLite Schema)

SQLite with Write-Ahead Logging (`WAL`) provides fast concurrent reads and transactional updates.

```sql
-- Storage Nodes Registry
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,               -- e.g., "node_1"
    name TEXT NOT NULL,                -- e.g., "Storage Node 1"
    path TEXT NOT NULL,                -- e.g., "data/nodes/node_1"
    status TEXT NOT NULL DEFAULT 'ONLINE', -- 'ONLINE', 'OFFLINE', 'PARTITIONED'
    is_active INTEGER NOT NULL DEFAULT 1,  -- For rebalance membership
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Objects Catalog
CREATE TABLE IF NOT EXISTS objects (
    key TEXT PRIMARY KEY,              -- Object key / name
    size INTEGER NOT NULL,             -- Bytes
    sha256 TEXT NOT NULL,              -- Cryptographic hash of content
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    version INTEGER NOT NULL DEFAULT 1,
    replication_factor INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Replica Tracking
CREATE TABLE IF NOT EXISTS replicas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_key TEXT NOT NULL,
    node_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'HEALTHY', -- 'HEALTHY', 'CORRUPTED', 'MISSING'
    last_verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(object_key) REFERENCES objects(key) ON DELETE CASCADE,
    FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE,
    UNIQUE(object_key, node_id)
);

-- Real-Time Audit & Chaos Event Log
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,         -- 'WRITE', 'READ', 'CORRUPTION', 'REPAIR', 'NODE_DOWN', etc.
    target TEXT NOT NULL,             -- Object key or Node ID
    message TEXT NOT NULL,
    details TEXT,                     -- JSON payload
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

### 5. Replication Strategy & Placement

1. **Placement Algorithm: Rendezvous Hashing (HRW - Highest Random Weight)**:
   - For an object key $K$ and active nodes $N_1, N_2, \dots, N_m$:
   - Score $S(K, N_i) = \text{hash}(K + N_i)$.
   - Sort nodes by descending score; choose top $N$ nodes as the replica set for $K$.
   - **Why Rendezvous Hashing?**
     - Extremely clean to implement (~25 lines of Python).
     - Uniform distribution across nodes.
     - Minimal rebalancing churn: adding or removing a node only redistributes $1/M$ keys rather than reshuffling the entire ring.
2. **Tunable Quorum ($N, W, R$)**:
   - Default: $N=3, W=2, R=2$.
   - **Write Protocol ($W$)**:
     1. Coordinator acquires key-write lock.
     2. Computes payload SHA-256.
     3. Selects top $N$ healthy nodes via Rendezvous hashing.
     4. Dispatches concurrent parallel writes to all $N$ nodes.
     5. If $\ge W$ nodes successfully write without error, the write commits in SQLite metadata and returns HTTP 201.
     6. If $< W$ succeed, attempts rollback/cleanup and returns HTTP 503 (Quorum Unreachable).
   - **Read Protocol ($R$)**:
     1. Coordinator selects top $N$ nodes for key.
     2. Dispatches reads to $R$ nodes concurrently.
     3. Computes SHA-256 for each fetched payload; compares against expected metadata hash.
     4. If valid hash matches, returns data immediately.
     5. If a replica is corrupt or missing: coordinator reads from the next available replica, serves client, and fires an asynchronous **Read-Repair** task to overwrite the corrupted replica with the verified payload.

---

### 6. Failure, Corruption & Repair Simulation

To meet the requirement that failures must actually be simulated on disk:

1. **Simulated Node Outages**:
   - Each node in `node_manager` has status `ONLINE`, `OFFLINE`, or `PARTITIONED`.
   - When a node is set to `OFFLINE` or `PARTITIONED`, any read/write directed to it immediately raises an `IOError` or `NetworkPartitionError`.
   - Node status changes are instantly broadcast to the dashboard.
2. **Real Bit-Rot & Data Corruption Injection**:
   - `POST /api/chaos/corrupt`:
     - Selects the actual file path on disk: `data/nodes/{node_id}/objects/{object_key}`.
     - Reads file bytes, intentionally flips 1-16 bits or injects garbage bytes, and writes it back to disk.
     - Updates metadata replica state to flag discrepancy during next read or scrub.
3. **Automatic Replica Repair**:
   - **On Read (Reactive)**: When reading an object, if any node returns a file whose SHA-256 does not match the metadata SHA-256, it is marked `CORRUPTED`. The coordinator uses another replica to repair that node's file on disk.
   - **On Scrub (Proactive)**: The scrubber scans all registered replicas, reads disk files, checks SHA-256. If a file is corrupted, missing, or a node was offline during initial write, it copies the valid chunk from a healthy replica to restore full $N$ replication.

---

### 7. Concurrency Handling

1. **In-Memory Key Locking**:
   - An asynchronous lock manager maintains a dictionary of `asyncio.Lock` keyed by `object_key`.
   - Prevents race conditions during concurrent writes or concurrent write/repair cycles on the same object.
2. **SQLite WAL & Concurrency**:
   - SQLite opened with `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=5000;`.
   - Dedicated connection handling via `aiosqlite` ensuring non-blocking coordinator event loops.
3. **Idempotent Atomic Disk Writes**:
   - Files are written to a temporary filename (`.tmp_{uuid}`) inside the node directory and atomically renamed to final key destination once flushed and synced (`os.replace`).

---

### 8. Integrity Verification & Scrubber

1. **Cryptographic Fingerprint**:
   - Standard `SHA-256` computed during initial upload before dispatching to nodes.
2. **Scrubber Loop**:
   - Runs in the background (every $X$ seconds or triggered via UI button).
   - Iterates through all objects in catalog:
     - Checks if all $N$ expected replicas exist on their designated nodes.
     - Reads contents and checks `hash == metadata.sha256`.
     - Logs results to `audit_events`.
     - Automatically invokes `repair_replica(key, corrupt_node, healthy_node)`.
3. **Dashboard Real-Time Feedback**:
   - Visual health badges: Green (Healthy, $N/N$), Yellow (Degraded, $W \le k < N$), Red (Corrupted/Offline, $< W$).

---

### 9. Rebalancing Strategy

1. **Node Addition / Removal**:
   - When a new node is added (e.g., `Node 6`), Rendezvous placement changes for approximately $1/M$ of objects.
2. **Rebalance Task**:
   - `POST /api/maintenance/rebalance`:
     - Evaluates target placement for all keys.
     - Detects objects currently stored on nodes that are no longer in their top-$N$ set.
     - Replicates data to the new designated node, verifies hash, and safely purges the old replica.
     - Minimizes recovery time and eliminates storage bloat.

---

### 10. Complete REST API Specification

| Category | Method | Endpoint | Description |
|---|---|---|---|
| **Objects** | `PUT` | `/api/objects/{key}` | Upload object (multipart or raw stream), enforce quorum $W$ |
| | `GET` | `/api/objects/{key}` | Download object with quorum $R$ and automatic read-repair |
| | `DELETE` | `/api/objects/{key}` | Delete object across all replicas and metadata |
| | `HEAD` | `/api/objects/{key}` | Retrieve metadata (size, SHA-256, replication status) |
| | `GET` | `/api/objects` | List all stored objects with replica breakdown |
| **Nodes** | `GET` | `/api/nodes` | List all nodes, status (online/offline), disk usage |
| | `POST` | `/api/nodes` | Add a new node to the cluster |
| **Chaos** | `POST` | `/api/chaos/node/{id}/kill` | Simulate node crash (offline) |
| | `POST` | `/api/chaos/node/{id}/revive` | Bring node back online |
| | `POST` | `/api/chaos/node/{id}/partition` | Toggle simulated network partition |
| | `POST` | `/api/chaos/corrupt` | Corrupt specific object replica on disk (bit-flip) |
| | `POST` | `/api/chaos/reset` | Heal all nodes and repair all corruption |
| **Maintenance** | `POST` | `/api/maintenance/scrub` | Run active integrity scrub and repair |
| | `POST` | `/api/maintenance/rebalance`| Trigger data rebalance across active nodes |
| | `GET` | `/api/maintenance/events` | Fetch latest audit/event stream |
| | `GET` | `/api/maintenance/stats` | Global cluster metrics (durability %, storage, etc.) |

---

### 11. Minimum Viable 4-Hour Feature Set (In-Scope)

1. **5 Simulated Storage Nodes** on local disk directory paths.
2. **Rendezvous Hashing** for deterministic, balanced replica placement.
3. **Quorum Write & Read** ($N=3, W=2, R=2$) with SHA-256 checksums.
4. **SQLite Metadata Store** in WAL mode for ACID metadata.
5. **Real Chaos Injection**:
   - Actual byte corruption inside disk files.
   - Node offline/kill & revive toggles.
   - Network partition simulation.
6. **Reactive Read-Repair**: Automatic healing when reading degraded/corrupt files.
7. **Proactive Background Scrubber**: Automated sweep detecting and fixing corrupt chunks.
8. **Interactive Web Dashboard**:
   - Visual nodes rack with online/offline/corrupted status indicators.
   - Live Object browser with download/delete/integrity audit.
   - Interactive Chaos Panel (Kill Node, Bit-Rot Corrupt, Partition, Trigger Scrub).
   - Real-time event log terminal showing quorum decisions and repairs.

---

### 12. Explicit Non-Goals (Out of Scope for 4-Hour Hackathon)

1. **Distributed Consensus Protocols from Scratch (Raft/Paxos)**: Unnecessary complexity; centralized coordinator with SQLite metadata and Dynamo-style quorum provides 100% of the demo value with zero distributed deadlock risk.
2. **Erasure Coding (Reed-Solomon)**: Implementing RS chunking and Galois field arithmetic is error-prone and time-consuming; $N$-way replication is clear, robust, and intuitive.
3. **User Authentication & ACLs**: Multi-user permissions add boilerplate without adding value to the distributed storage core.
4. **S3 Multipart Upload for Multi-GB Files**: Single-stream uploads (up to 100MB) are sufficient to test concurrency, integrity, and performance.
5. **Multi-Datacenter / Cross-Region Replication**: Local multi-node directory simulation on Windows laptop already meets the exact requirements.
