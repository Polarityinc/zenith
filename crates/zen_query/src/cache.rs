//! Result cache: memoize query results by `(tenant, query_hash, time_window)`.

use moka::future::Cache;
use xxhash_rust::xxh3::xxh3_64;

use crate::row::ResultSet;

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct CacheKey(pub u64);

#[derive(Clone)]
pub struct ResultCache {
    inner: Cache<CacheKey, ResultSet>,
}

impl ResultCache {
    pub fn new(max_bytes: u64) -> Self {
        let inner = Cache::builder()
            // approximate weigher: serialize length is a good enough estimate.
            .weigher(|_k: &CacheKey, v: &ResultSet| {
                serde_json::to_vec(v)
                    .map(|b| b.len().min(u32::MAX as usize) as u32)
                    .unwrap_or(1024)
            })
            .max_capacity(max_bytes)
            .time_to_live(std::time::Duration::from_secs(1))
            .build();
        Self { inner }
    }

    pub async fn get(&self, k: CacheKey) -> Option<ResultSet> {
        self.inner.get(&k).await
    }

    pub async fn put(&self, k: CacheKey, v: ResultSet) {
        self.inner.insert(k, v).await;
    }

    pub fn key_for(query_hash: u64, tenant_id: u64, time_bucket_seconds: u64) -> CacheKey {
        let mut h = [0u8; 24];
        h[0..8].copy_from_slice(&query_hash.to_le_bytes());
        h[8..16].copy_from_slice(&tenant_id.to_le_bytes());
        h[16..24].copy_from_slice(&time_bucket_seconds.to_le_bytes());
        CacheKey(xxh3_64(&h))
    }
}
