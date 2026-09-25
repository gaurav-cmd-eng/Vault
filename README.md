# Vault: Fault-Tolerant Distributed Object Storage

Vault is a fault-tolerant distributed object storage system designed to simulate independent storage nodes, real hardware faults, bit-rot, and self-healing replicas locally on a single machine.

---

## 1. Installation

Vault requires **Python 3.11+**.

Install the backend dependencies:
```bash
pip install -r requirements.txt
```

---

## 2. Running the Backend

Launch the Vault coordinator and simulated storage cluster:
```bash
python run.py
```
Or with Uvicorn directly:
```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Once running, interactive Swagger API docs are available at:
- **Interactive API Docs (Swagger UI)**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **Alternative Docs (ReDoc)**: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

## 3. How Simulated Nodes Work

- When Vault starts up, it automatically creates physical directories for each simulated storage node under:
  ```
  data/nodes/
  ├── node_1/
  ├── node_2/
  ├── node_3/
  ├── node_4/
  └── node_5/
  ```
- Each simulated node behaves as an isolated disk:
  - Has its own local file directory.
  - Maintains individual capacity and health metrics.
  - Can be marked `ONLINE`, `OFFLINE`, or `PARTITIONED` by the chaos engine.
- Node metadata and cluster topology are persisted in SQLite at `data/metadata.db` using Write-Ahead Logging (`WAL`) mode for high concurrency.

---

## 4. API Endpoints (Phase 1)

### Cluster Health
```bash
curl http://127.0.0.1:8000/api/health
```
Example response:
```json
{
  "status": "ok",
  "timestamp": "2026-09-26T03:00:00Z",
  "database_status": "connected",
  "active_nodes": 5,
  "total_nodes": 5,
  "version": "0.1.0"
}
```

### List Nodes & Storage Telemetry
```bash
curl http://127.0.0.1:8000/api/nodes
```
Example response:
```json
[
  {
    "node_id": "node_1",
    "name": "Storage Node 1",
    "path": "data/nodes/node_1",
    "status": "ONLINE",
    "is_online": true,
    "stored_object_count": 0,
    "storage_used_bytes": 0,
    "capacity_bytes": 10737418240,
    "last_heartbeat": "2026-09-26 03:00:00"
  },
  ...
]
```

### Cluster Configuration
```bash
curl http://127.0.0.1:8000/api/config
```
Example response:
```json
{
  "num_nodes": 5,
  "default_replication_factor": 3,
  "write_quorum": 2,
  "read_quorum": 2,
  "storage_root": "C:/Users/.../Vault/data",
  "max_upload_size_bytes": 52428800,
  "health_check_interval_seconds": 10
}
```

---

## 5. Running Tests

Execute the automated test suite with `pytest`:
```bash
pytest
```
