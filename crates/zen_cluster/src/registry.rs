//! Node registry: heartbeats this node into the catalog and reads back the
//! latest cluster view. The catalog (sqlite or postgres) is the source of
//! truth; we just keep a cached `ShardMap` refreshed on a background tick.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use zen_catalog::{Catalog, NodeRow};
use zen_common::ZenResult;

use crate::node::{NodeId, NodeInfo, NodeRole};
use crate::shard::ShardMap;

#[derive(Clone)]
pub struct NodeRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    pub local_id: NodeId,
    pub local_endpoint: String,
    pub local_role: NodeRole,
    pub local_shards: String,
    pub catalog: Arc<dyn Catalog>,
    pub map: RwLock<ShardMap>,
    pub heartbeat_period: Duration,
    pub heartbeat_ttl_ms: i64,
    pub replication_factor: usize,
}

impl NodeRegistry {
    pub fn new(
        local_id: NodeId,
        local_endpoint: String,
        local_role: NodeRole,
        local_shards: String,
        catalog: Arc<dyn Catalog>,
        replication_factor: usize,
        heartbeat_ttl_ms: i64,
    ) -> Self {
        let map = ShardMap::new(Vec::new(), replication_factor, heartbeat_ttl_ms);
        Self {
            inner: Arc::new(Inner {
                local_id,
                local_endpoint,
                local_role,
                local_shards,
                catalog,
                map: RwLock::new(map),
                heartbeat_period: Duration::from_millis(500),
                heartbeat_ttl_ms,
                replication_factor,
            }),
        }
    }

    pub fn local_id(&self) -> NodeId {
        self.inner.local_id
    }

    pub fn local_role(&self) -> NodeRole {
        self.inner.local_role
    }

    pub fn shard_map(&self) -> ShardMap {
        self.inner.map.read().clone()
    }

    /// One heartbeat + map-refresh cycle. Public so tests can drive it
    /// without spawning a background task.
    pub async fn tick(&self) -> ZenResult<()> {
        let now = now_ms();
        // Heartbeat ourselves.
        self.inner
            .catalog
            .upsert_node(NodeRow {
                node_id: uuid::Uuid::from_bytes(self.inner.local_id.as_bytes()),
                endpoint: self.inner.local_endpoint.clone(),
                role: self.inner.local_role.as_str().to_string(),
                shards: self.inner.local_shards.clone(),
                last_heartbeat_ms: now,
            })
            .await?;

        // Refresh cluster view.
        let rows = self.inner.catalog.list_nodes().await?;
        let nodes: Vec<NodeInfo> = rows
            .into_iter()
            .filter_map(|r| {
                let role = NodeRole::parse(&r.role)?;
                Some(NodeInfo {
                    node_id: NodeId::from_bytes(*r.node_id.as_bytes()),
                    endpoint: r.endpoint,
                    role,
                    shards: r.shards,
                    last_heartbeat_ms: r.last_heartbeat_ms,
                })
            })
            .collect();
        let map = ShardMap::new(
            nodes,
            self.inner.replication_factor,
            self.inner.heartbeat_ttl_ms,
        );
        *self.inner.map.write() = map;
        Ok(())
    }

    /// Current epoch ms helper exposed for routers/tests.
    pub fn now_ms(&self) -> i64 {
        now_ms()
    }

    /// Spawn the background heartbeat loop. Returns immediately; the task
    /// runs until the runtime shuts down.
    pub fn spawn_loop(self: &NodeRegistry) {
        let this = self.clone();
        let period = this.inner.heartbeat_period;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            // Skip the first immediate tick — we'll fire below.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(e) = this.tick().await {
                    tracing::warn!(error=%e, "registry tick failed");
                }
            }
        });
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
