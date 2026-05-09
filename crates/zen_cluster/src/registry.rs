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

#[cfg(test)]
mod tests {
    use super::*;

    use zen_catalog::MockCatalog;

    fn registry_for(
        catalog: Arc<dyn Catalog>,
        endpoint: &str,
        role: NodeRole,
        ttl_ms: i64,
    ) -> NodeRegistry {
        let id = NodeId::new();
        NodeRegistry::new(
            id,
            endpoint.to_string(),
            role,
            "*".into(),
            catalog,
            1,
            ttl_ms,
        )
    }

    #[tokio::test]
    async fn tick_upserts_node_into_catalog() {
        let cat: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
        let r = registry_for(cat.clone(), "http://a:8080", NodeRole::All, 5_000);
        r.tick().await.unwrap();
        let rows = cat.list_nodes().await.unwrap();
        assert_eq!(rows.len(), 1, "tick should upsert exactly one node row");
        assert_eq!(rows[0].endpoint, "http://a:8080");
        assert_eq!(rows[0].role, "all");
        assert_eq!(rows[0].shards, "*");
        // node_id round-trip: catalog stores it as uuid::Uuid; the registry
        // packs from NodeId.as_bytes(). They must match.
        assert_eq!(rows[0].node_id.as_bytes(), &r.local_id().as_bytes());
    }

    #[tokio::test]
    async fn shard_map_reflects_all_registered_nodes_after_tick() {
        let cat: Arc<dyn Catalog> = Arc::new(MockCatalog::new());

        let r1 = registry_for(cat.clone(), "http://a:8080", NodeRole::All, 5_000);
        let r2 = registry_for(cat.clone(), "http://b:8080", NodeRole::Worker, 5_000);
        let r3 = registry_for(cat.clone(), "http://c:8080", NodeRole::Coordinator, 5_000);
        r1.tick().await.unwrap();
        r2.tick().await.unwrap();
        r3.tick().await.unwrap();

        // After r3's tick its local view sees all three rows.
        let map = r3.shard_map();
        assert_eq!(
            map.nodes().len(),
            3,
            "shard map should contain all three registered nodes"
        );
    }

    #[tokio::test]
    async fn stale_node_excluded_from_all_alive_workers() {
        let cat: Arc<dyn Catalog> = Arc::new(MockCatalog::new());

        // ttl is 1 ms — wide enough that the just-ticked nodes are alive
        // when we observe them, narrow enough that we can backdate one row
        // by far more than the ttl and have it counted as stale.
        let ttl_ms = 1_000;
        let r1 = registry_for(cat.clone(), "http://alive:8080", NodeRole::All, ttl_ms);
        let r2 = registry_for(cat.clone(), "http://stale:8080", NodeRole::All, ttl_ms);
        r1.tick().await.unwrap();
        r2.tick().await.unwrap();
        // r1's local view now has just itself (its own tick happened
        // before r2 wrote its row). One more tick syncs r1 with the
        // catalog so it sees both rows.
        r1.tick().await.unwrap();

        // Both registries should see two alive workers right after tick.
        let map = r1.shard_map();
        let now = r1.now_ms();
        assert_eq!(map.all_alive_workers(now).len(), 2);

        // Now manually backdate r2's row in the catalog by a long time so
        // it exceeds the ttl when observed.
        let stale_ts = now - 10 * ttl_ms;
        cat.upsert_node(NodeRow {
            node_id: uuid::Uuid::from_bytes(r2.local_id().as_bytes()),
            endpoint: "http://stale:8080".into(),
            role: "all".into(),
            shards: "*".into(),
            last_heartbeat_ms: stale_ts,
        })
        .await
        .unwrap();
        // Refresh r1's local view by ticking again.
        r1.tick().await.unwrap();
        let map = r1.shard_map();
        let now = r1.now_ms();
        let alive = map.all_alive_workers(now);
        assert_eq!(alive.len(), 1, "stale node must be excluded");
        assert_eq!(alive[0].endpoint, "http://alive:8080");
    }

    #[tokio::test]
    async fn local_id_is_stable_and_round_trips_through_catalog() {
        let cat: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
        let r = registry_for(cat.clone(), "http://a:8080", NodeRole::All, 5_000);
        let want = r.local_id();
        // Multiple calls return the same id.
        assert_eq!(r.local_id(), want);
        // After a tick, the catalog row carries the same id bytes.
        r.tick().await.unwrap();
        let rows = cat.list_nodes().await.unwrap();
        assert_eq!(rows[0].node_id.as_bytes(), &want.as_bytes());
    }

    #[tokio::test]
    async fn two_registries_converge_on_same_shard_map() {
        let cat: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
        let r1 = registry_for(cat.clone(), "http://a:8080", NodeRole::All, 5_000);
        let r2 = registry_for(cat.clone(), "http://b:8080", NodeRole::All, 5_000);

        // Both heartbeat once.
        r1.tick().await.unwrap();
        r2.tick().await.unwrap();

        // Now they each refresh their shard view.
        r1.tick().await.unwrap();
        r2.tick().await.unwrap();

        let m1 = r1.shard_map();
        let m2 = r2.shard_map();
        assert_eq!(m1.nodes().len(), 2);
        assert_eq!(m2.nodes().len(), 2);

        let mut e1: Vec<_> = m1.nodes().iter().map(|n| n.endpoint.clone()).collect();
        let mut e2: Vec<_> = m2.nodes().iter().map(|n| n.endpoint.clone()).collect();
        e1.sort();
        e2.sort();
        assert_eq!(
            e1, e2,
            "after both nodes have ticked twice their shard maps must agree"
        );
    }
}
