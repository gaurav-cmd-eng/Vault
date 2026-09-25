-- Vault Database Schema

-- Storage Node Registry
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ONLINE', -- 'ONLINE', 'OFFLINE', 'PARTITIONED'
    capacity_bytes INTEGER NOT NULL DEFAULT 10737418240, -- 10 GiB default
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Objects Metadata
CREATE TABLE IF NOT EXISTS objects (
    object_id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    size INTEGER NOT NULL,
    checksum TEXT NOT NULL,                -- SHA-256
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    replication_factor INTEGER NOT NULL DEFAULT 3,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Replicas Location & Health
CREATE TABLE IF NOT EXISTS replicas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'HEALTHY', -- 'HEALTHY', 'CORRUPTED', 'MISSING'
    last_verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
    UNIQUE(object_id, node_id)
);

-- System & Audit Events
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    target TEXT NOT NULL,
    message TEXT NOT NULL,
    details TEXT,                          -- JSON string
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id);
CREATE INDEX IF NOT EXISTS idx_replicas_object ON replicas(object_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
