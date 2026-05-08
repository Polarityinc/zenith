-- Initial ZenithDB catalog schema (PostgreSQL).
-- Tracks tenants, partitions, commit-id allocation state, WAL objects,
-- segments, and compaction leases.

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partitions (
    tenant_id     BIGINT NOT NULL,
    partition_id  BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, partition_id)
);

CREATE TABLE IF NOT EXISTS commit_seq_state (
    tenant_id        BIGINT NOT NULL,
    partition_id     BIGINT NOT NULL,
    next_commit_id   BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, partition_id)
);

CREATE TABLE IF NOT EXISTS wal_objects (
    wal_id              BYTEA PRIMARY KEY,
    tenant_id           BIGINT NOT NULL,
    partition_id        BIGINT NOT NULL,
    object_key          TEXT NOT NULL,
    commit_id_min       BIGINT NOT NULL,
    commit_id_max       BIGINT NOT NULL,
    byte_count          BIGINT NOT NULL,
    row_count           BIGINT NOT NULL,
    schema_fingerprint  BYTEA NOT NULL,
    consumed_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS wal_objects_unconsumed
    ON wal_objects (tenant_id, partition_id, commit_id_min)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS segments (
    segment_id          BYTEA PRIMARY KEY,
    tenant_id           BIGINT NOT NULL,
    partition_id        BIGINT NOT NULL,
    object_key          TEXT NOT NULL,
    level               SMALLINT NOT NULL DEFAULT 0,
    byte_count          BIGINT NOT NULL,
    row_count           BIGINT NOT NULL,
    time_min            BIGINT NOT NULL,
    time_max            BIGINT NOT NULL,
    trace_id_min        BYTEA NOT NULL,
    trace_id_max        BYTEA NOT NULL,
    commit_id_min       BIGINT NOT NULL,
    commit_id_max       BIGINT NOT NULL,
    schema_fingerprint  BYTEA NOT NULL,
    rowgroup_index      BYTEA NOT NULL,
    superseded_at       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS segments_active
    ON segments (tenant_id, partition_id, time_min, time_max)
    WHERE superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS compaction_leases (
    tenant_id     BIGINT NOT NULL,
    partition_id  BIGINT NOT NULL,
    worker_id     TEXT NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, partition_id)
);
