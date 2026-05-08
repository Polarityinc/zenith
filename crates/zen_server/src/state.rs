//! Shared server state. All HTTP / gRPC handlers receive a clone of this.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use zen_catalog::Catalog;
use zen_cluster::{NodeRegistry, RemoteClient};
use zen_common::{Config, PartitionId, TenantId};
use zen_memtable::MemTable;
use zen_query::{LogicalPlan, SegmentCache, SegmentListCache};
use zen_storage::BlobStore;

use crate::middleware::auth::AuthState;

#[derive(Clone)]
pub struct ServerState {
    pub config: Config,
    pub catalog: Arc<dyn Catalog>,
    pub store: Arc<dyn BlobStore>,
    pub memtables: Arc<RwLock<std::collections::HashMap<(TenantId, PartitionId), MemTable>>>,
    pub seg_cache: SegmentCache,
    pub list_cache: SegmentListCache,
    /// Cache of parsed `LogicalPlan` by query-string hash. Saves ~100 µs of
    /// sqlparser cost per HTTP query when the same query repeats (typical
    /// dashboard refresh pattern).
    pub plan_cache: Arc<RwLock<std::collections::HashMap<u64, Arc<LogicalPlan>>>>,
    /// Cluster handle. `None` = single-node mode (no routing). `Some` =
    /// multi-node — heartbeats this node + drives `QueryRouter`.
    pub cluster: Option<NodeRegistry>,
    /// Inter-node HTTP client. Always present so handlers don't have to
    /// branch on Option; cheap when unused (lazy connections).
    pub remote: RemoteClient,
    /// Auth verifiers. Cloneable; verify methods are async + lock-free
    /// so the hot path adds ~50 ns on cache hit.
    pub auth: AuthState,
}

impl ServerState {
    pub fn new(config: Config, catalog: Arc<dyn Catalog>, store: Arc<dyn BlobStore>) -> Self {
        let auth = AuthState::from_config(&config);
        // If HMAC is configured, the inter-node `RemoteClient` must sign
        // outbound `/v1/internal/*` requests with the same secret the
        // receiver's `hmac_layer` will verify.
        let mut remote = RemoteClient::new(Duration::from_secs(30));
        if let Some(signer) = auth.hmac.clone() {
            remote = remote.with_signer(signer);
        }
        Self {
            config,
            catalog,
            store,
            memtables: Arc::new(RwLock::new(std::collections::HashMap::new())),
            seg_cache: SegmentCache::new(1024),
            list_cache: SegmentListCache::default(),
            plan_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
            cluster: None,
            remote,
            auth,
        }
    }

    /// Builder: enable multi-node mode by attaching a `NodeRegistry`. When
    /// set, the query handler routes via the registry's `ShardMap`.
    pub fn with_cluster(mut self, reg: NodeRegistry) -> Self {
        self.cluster = Some(reg);
        self
    }

    /// Parse + cache a query string. Re-uses an existing `Arc<LogicalPlan>` for
    /// repeats; rebuilds a fresh one with the right `tenant_id` so cached plans
    /// survive across tenants.
    pub fn parse_query(&self, q: &str, tenant_id: u64) -> zen_common::ZenResult<Arc<LogicalPlan>> {
        let key = xxhash_rust::xxh3::xxh3_64(q.as_bytes()) ^ tenant_id;
        if let Some(p) = self.plan_cache.read().get(&key) {
            return Ok(p.clone());
        }
        let plan = zen_ql::parse(q, tenant_id)?;
        let plan = Arc::new(plan);
        // Cap cache at 1024 distinct queries.
        let mut g = self.plan_cache.write();
        if g.len() >= 1024 {
            if let Some(k) = g.keys().next().copied() {
                g.remove(&k);
            }
        }
        g.insert(key, plan.clone());
        Ok(plan)
    }

    pub fn memtable_for(&self, tenant: TenantId, partition: PartitionId) -> MemTable {
        let g = self.memtables.read();
        if let Some(m) = g.get(&(tenant, partition)) {
            return m.clone();
        }
        drop(g);
        let mut g = self.memtables.write();
        g.entry((tenant, partition))
            .or_insert_with(|| MemTable::new(tenant, partition, self.config.ingest.flush_max_bytes))
            .clone()
    }
}

use parking_lot;
