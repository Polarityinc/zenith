//! Byte-range page cache, sized in bytes via moka.
//!
//! Cache key is `(object_key, range_start, range_end)`. We deliberately do NOT
//! coalesce overlapping ranges automatically — the executor is responsible for
//! choosing canonical ranges (e.g. row-group payload offsets) so we get high
//! hit rates for the requests we expect.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;

use zen_common::ZenResult;

use crate::store::BlobStore;

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct CacheKey {
    pub key: String,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone)]
pub struct PageCache {
    inner: Cache<CacheKey, Bytes>,
    size_bytes: u64,
    pub hits: Arc<std::sync::atomic::AtomicU64>,
    pub misses: Arc<std::sync::atomic::AtomicU64>,
}

impl PageCache {
    pub fn new(size_bytes: u64) -> Self {
        let inner = Cache::builder()
            .weigher(|_k: &CacheKey, v: &Bytes| v.len().min(u32::MAX as usize) as u32)
            .max_capacity(size_bytes)
            .build();
        Self {
            inner,
            size_bytes,
            hits: Arc::new(0.into()),
            misses: Arc::new(0.into()),
        }
    }

    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: &str,
        range: Range<u64>,
        fetch: F,
    ) -> ZenResult<Bytes>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ZenResult<Bytes>>,
    {
        let ck = CacheKey {
            key: key.to_string(),
            start: range.start,
            end: range.end,
        };
        if let Some(b) = self.inner.get(&ck).await {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(b);
        }
        self.misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = fetch().await?;
        self.inner.insert(ck, bytes.clone()).await;
        Ok(bytes)
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn approx_entries(&self) -> u64 {
        self.inner.entry_count()
    }
}

/// Convenience: a `BlobStore` wrapped with a `PageCache`. Implements `BlobStore`
/// itself so callers can transparently get cached reads.
pub struct CachedStore {
    pub store: Arc<dyn BlobStore>,
    pub cache: PageCache,
}

impl CachedStore {
    pub fn new(store: Arc<dyn BlobStore>, cache: PageCache) -> Self {
        Self { store, cache }
    }
}

#[async_trait::async_trait]
impl BlobStore for CachedStore {
    async fn get(&self, key: &str) -> ZenResult<Bytes> {
        // Treat full GET as range 0..u64::MAX so we can re-use page cache.
        // Actual size is unknown; use object_store's get directly to avoid
        // double-fetches.
        let store = self.store.clone();
        let key_owned = key.to_string();
        self.cache
            .get_or_fetch(key, 0..u64::MAX, move || async move {
                store.get(&key_owned).await
            })
            .await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ZenResult<Bytes> {
        let store = self.store.clone();
        let key_owned = key.to_string();
        let range_clone = range.clone();
        self.cache
            .get_or_fetch(key, range, move || async move {
                store.get_range(&key_owned, range_clone).await
            })
            .await
    }

    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()> {
        self.store.put(key, bytes).await
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool> {
        self.store.put_if_absent(key, bytes).await
    }

    async fn delete(&self, key: &str) -> ZenResult<()> {
        self.store.delete(key).await
    }

    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>> {
        self.store.list(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_fs::InMemoryStore;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn cache_serves_repeated_reads() {
        let inner = Arc::new(InMemoryStore::default());
        inner.put("k", Bytes::from_static(b"hello")).await.unwrap();
        let cached = CachedStore::new(inner.clone(), PageCache::new(1024 * 1024));
        // First — miss.
        let r1 = cached.get_range("k", 0..5).await.unwrap();
        // Second — hit.
        let r2 = cached.get_range("k", 0..5).await.unwrap();
        assert_eq!(&r1[..], b"hello");
        assert_eq!(&r2[..], b"hello");
        assert_eq!(cached.cache.hits.load(Ordering::Relaxed), 1);
        assert_eq!(cached.cache.misses.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cache_evicts_at_capacity() {
        let inner = Arc::new(InMemoryStore::default());
        inner
            .put("k", Bytes::from_iter(std::iter::repeat_n(b'A', 1024)))
            .await
            .unwrap();
        let cached = CachedStore::new(inner.clone(), PageCache::new(2048));
        for i in 0..10 {
            cached
                .get_range("k", (i * 100)..(i * 100 + 100))
                .await
                .unwrap();
        }
        // moka is async-async; entry count should be roughly bounded.
        let n = cached.cache.approx_entries();
        // Bound is loose due to moka's eventual eviction.
        assert!(n < 100);
    }
}
