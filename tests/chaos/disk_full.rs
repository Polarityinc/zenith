//! Chaos: an out-of-space blob store. Writes must surface a structured
//! error, not panic. Successful writes preceding the failure must remain
//! intact and readable.

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use zen_common::{ZenError, ZenResult};
use zen_storage::BlobStore;

/// A wrapper that returns ENOSPC after `fail_after_n_writes` writes.
struct ChokingStore {
    inner: Arc<dyn BlobStore>,
    writes: AtomicUsize,
    fail_after: usize,
}

#[async_trait::async_trait]
impl BlobStore for ChokingStore {
    async fn get(&self, key: &str) -> ZenResult<Bytes> {
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, r: Range<u64>) -> ZenResult<Bytes> {
        self.inner.get_range(key, r).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()> {
        if self.writes.fetch_add(1, Ordering::SeqCst) >= self.fail_after {
            return Err(ZenError::storage("ENOSPC: simulated"));
        }
        self.inner.put(key, bytes).await
    }
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool> {
        if self.writes.fetch_add(1, Ordering::SeqCst) >= self.fail_after {
            return Err(ZenError::storage("ENOSPC: simulated"));
        }
        self.inner.put_if_absent(key, bytes).await
    }
    async fn delete(&self, key: &str) -> ZenResult<()> {
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn enospc_returns_error_not_panic() {
    use zen_storage::local_fs::InMemoryStore;
    let backing: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let store = ChokingStore {
        inner: backing,
        writes: AtomicUsize::new(0),
        fail_after: 2,
    };
    // First two writes succeed.
    store.put("a", Bytes::from_static(b"1")).await.unwrap();
    store.put("b", Bytes::from_static(b"2")).await.unwrap();
    // Third write fails with structured error (no panic).
    let r = store.put("c", Bytes::from_static(b"3")).await;
    assert!(r.is_err(), "third write should fail");
    let msg = format!("{}", r.unwrap_err());
    assert!(msg.contains("ENOSPC"), "expected ENOSPC message, got {msg}");
    // Pre-failure data is still readable.
    assert_eq!(&store.get("a").await.unwrap()[..], b"1");
    assert_eq!(&store.get("b").await.unwrap()[..], b"2");
}
