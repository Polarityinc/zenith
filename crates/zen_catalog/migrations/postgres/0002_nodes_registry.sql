-- Cluster node registry. Each node heartbeats its row every ~500 ms via
-- `zen_cluster::NodeRegistry::tick`. The shard map filters on
-- `last_heartbeat_ms` to skip stale peers.

CREATE TABLE IF NOT EXISTS nodes (
    node_id           BYTEA PRIMARY KEY,
    endpoint          TEXT NOT NULL,
    role              TEXT NOT NULL,
    shards            TEXT NOT NULL DEFAULT '*',
    last_heartbeat_ms BIGINT NOT NULL
);
