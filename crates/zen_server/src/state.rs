//! Shared server state. All HTTP / gRPC handlers receive a clone of this.

use std::sync::Arc;

use parking_lot::RwLock;

use zen_catalog::Catalog;
use zen_common::{Config, PartitionId, TenantId};
use zen_memtable::MemTable;
use zen_query::{SegmentCache, SegmentListCache};
use zen_storage::BlobStore;

#[derive(Clone)]
pub struct ServerState {
    pub config: Config,
    pub catalog: Arc<dyn Catalog>,
    pub store: Arc<dyn BlobStore>,
    pub memtables: Arc<RwLock<std::collections::HashMap<(TenantId, PartitionId), MemTable>>>,
    pub seg_cache: SegmentCache,
    pub list_cache: SegmentListCache,
}

impl ServerState {
    pub fn new(config: Config, catalog: Arc<dyn Catalog>, store: Arc<dyn BlobStore>) -> Self {
        Self {
            config,
            catalog,
            store,
            memtables: Arc::new(RwLock::new(std::collections::HashMap::new())),
            seg_cache: SegmentCache::new(1024),
            list_cache: SegmentListCache::default(),
        }
    }

    pub fn memtable_for(&self, tenant: TenantId, partition: PartitionId) -> MemTable {
        let g = self.memtables.read();
        if let Some(m) = g.get(&(tenant, partition)) {
            return m.clone();
        }
        drop(g);
        let mut g = self.memtables.write();
        g.entry((tenant, partition))
            .or_insert_with(|| {
                MemTable::new(tenant, partition, self.config.ingest.flush_max_bytes)
            })
            .clone()
    }
}

use parking_lot;
