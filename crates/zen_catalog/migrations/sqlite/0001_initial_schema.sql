-- Initial ZenithDB catalog schema. Tracks tenants, partitions, commit-id
-- allocation state, WAL objects, segments, and compaction leases.

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS partitions (
    tenant_id INTEGER NOT NULL,
    partition_id INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, partition_id)
);

CREATE TABLE IF NOT EXISTS commit_seq_state (
    tenant_id INTEGER NOT NULL,
    partition_id INTEGER NOT NULL,
    next_commit_id INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, partition_id)
);

CREATE TABLE IF NOT EXISTS wal_objects (
    wal_id BLOB PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    partition_id INTEGER NOT NULL,
    object_key TEXT NOT NULL,
    commit_id_min INTEGER NOT NULL,
    commit_id_max INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    schema_fingerprint BLOB NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS wal_objects_unconsumed
    ON wal_objects (tenant_id, partition_id, commit_id_min)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS segments (
    segment_id BLOB PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    partition_id INTEGER NOT NULL,
    object_key TEXT NOT NULL,
    level INTEGER NOT NULL DEFAULT 0,
    byte_count INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    time_min INTEGER NOT NULL,
    time_max INTEGER NOT NULL,
    trace_id_min BLOB NOT NULL,
    trace_id_max BLOB NOT NULL,
    commit_id_min INTEGER NOT NULL,
    commit_id_max INTEGER NOT NULL,
    schema_fingerprint BLOB NOT NULL,
    rowgroup_index BLOB NOT NULL,
    superseded_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS segments_active
    ON segments (tenant_id, partition_id, time_min, time_max)
    WHERE superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS compaction_leases (
    tenant_id INTEGER NOT NULL,
    partition_id INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, partition_id)
);
