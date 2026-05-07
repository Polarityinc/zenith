//! Short-TTL cache for `Catalog::list_segments_in_range`.
//!
//! At high QPS, every query hits the catalog with the same (tenant, partition)
//! arguments. The catalog returns the same answer for ~seconds at a time
//! (segments don't change between writes/compactions). Caching the list for
//! 1 second cuts the per-query catalog roundtrip out of the hot path. The
//! tradeoff: newly-published segments may be invisible to queries for up to
//! 1 second.

use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;

use zen_catalog::{Catalog, SegmentRow};
use zen_common::{PartitionId, TenantId, ZenResult};

#[derive(Clone, Hash, Eq, PartialEq)]
struct Key {
    tenant: u64,
    partition: u32,
    time_min: i64,
    time_max: i64,
}

#[derive(Clone)]
pub struct SegmentListCache {
    inner: Cache<Key, Arc<Vec<SegmentRow>>>,
}

impl SegmentListCache {
    pub fn new(ttl: Duration, max_entries: u64) -> Self {
        Self {
            inner: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(max_entries)
                .build(),
        }
    }

    pub async fn list(
        &self,
        catalog: &Arc<dyn Catalog>,
        tenant: TenantId,
        partition: PartitionId,
        time_min: i64,
        time_max: i64,
    ) -> ZenResult<Arc<Vec<SegmentRow>>> {
        let k = Key {
            tenant: tenant.0,
            partition: partition.0,
            time_min,
            time_max,
        };
        if let Some(v) = self.inner.get(&k).await {
            return Ok(v);
        }
        let segs = catalog
            .list_segments_in_range(tenant, partition, time_min, time_max)
            .await?;
        let arc = Arc::new(segs);
        self.inner.insert(k, arc.clone()).await;
        Ok(arc)
    }
}

impl Default for SegmentListCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), 1024)
    }
}
