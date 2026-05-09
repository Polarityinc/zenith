-- Add coarse WAL-level pruning metadata so fresh, un-compacted objects can be
-- skipped before object-storage reads for trace_id and time-window queries.

ALTER TABLE wal_objects
    ADD COLUMN IF NOT EXISTS time_min BIGINT NOT NULL DEFAULT -9223372036854775808,
    ADD COLUMN IF NOT EXISTS time_max BIGINT NOT NULL DEFAULT 9223372036854775807,
    ADD COLUMN IF NOT EXISTS trace_id_min BYTEA NOT NULL DEFAULT decode('00000000000000000000000000000000', 'hex'),
    ADD COLUMN IF NOT EXISTS trace_id_max BYTEA NOT NULL DEFAULT decode('ffffffffffffffffffffffffffffffff', 'hex');

CREATE INDEX IF NOT EXISTS wal_objects_unconsumed_time
    ON wal_objects (tenant_id, partition_id, time_min, time_max, commit_id_min)
    WHERE consumed_at IS NULL;
