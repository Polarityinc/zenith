//! Single-flight request coalescing.
//!
//! When the executor fires N concurrent reads for the same `(key, range)`, we
//! want exactly one underlying GET. `RequestCoalescer` keeps a map of in-flight
//! futures and dedupes against it.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{watch, Mutex};

use zen_common::ZenResult;

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
struct K {
    key: String,
    start: u64,
    end: u64,
}

type Slot = watch::Receiver<Option<Result<Bytes, String>>>;

#[derive(Default, Clone)]
pub struct RequestCoalescer {
    inner: Arc<Mutex<HashMap<K, Slot>>>,
}

impl RequestCoalescer {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run<F, Fut>(&self, key: &str, range: Range<u64>, fetch: F) -> ZenResult<Bytes>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ZenResult<Bytes>>,
    {
        let k = K {
            key: key.to_string(),
            start: range.start,
            end: range.end,
        };
        // Try to claim or join.
        let mut rx_opt = {
            let mut g = self.inner.lock().await;
            if let Some(rx) = g.get(&k) {
                Some(rx.clone())
            } else {
                let (tx, rx) = watch::channel::<Option<Result<Bytes, String>>>(None);
                g.insert(k.clone(), rx.clone());
                drop(g);
                // We are the leader: run the fetch.
                let result = fetch().await;
                let to_send = match &result {
                    Ok(b) => Ok(b.clone()),
                    Err(e) => Err(format!("{e}")),
                };
                let _ = tx.send(Some(to_send));
                let mut g = self.inner.lock().await;
                g.remove(&k);
                return result;
            }
        };
        if let Some(rx) = rx_opt.as_mut() {
            // Wait for value.
            loop {
                if let Some(v) = rx.borrow().clone() {
                    return v.map_err(zen_common::ZenError::storage);
                }
                if rx.changed().await.is_err() {
                    return Err(zen_common::ZenError::storage("coalesce channel closed"));
                }
            }
        }
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn n_concurrent_dedupe_to_one_fetch() {
        let coalescer = RequestCoalescer::new();
        let counter = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let c = coalescer.clone();
            let cnt = counter.clone();
            handles.push(tokio::spawn(async move {
                c.run("foo", 0..16, move || {
                    let cnt = cnt.clone();
                    async move {
                        // Simulate slow fetch.
                        cnt.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Ok(Bytes::from_static(b"deadbeefdeadbeef"))
                    }
                })
                .await
            }));
        }
        for h in handles {
            let r = h.await.unwrap().unwrap();
            assert_eq!(&r[..], b"deadbeefdeadbeef");
        }
        // Should be 1 (or very close to 1 — there's a small race possible if
        // the leader finishes before others register; we accept up to 2).
        let n = counter.load(Ordering::Relaxed);
        assert!(n <= 2, "expected ≤2 fetches, got {n}");
    }
}
