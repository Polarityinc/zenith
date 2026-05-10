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

    /// 100 concurrent reads of the *same* (key, range) must collapse to a
    /// very small number of underlying fetches. We hold the leader's fetch
    /// open with a long sleep so all 99 followers race in and join the
    /// existing slot. We assert the count is small (≤ 3) rather than
    /// exactly 1: there's an inherent race between the leader removing its
    /// slot and the next would-be-leader checking the map. In practice the
    /// fetch should complete with 1 callee, but ≤3 keeps the test stable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_hundred_concurrent_collapse_to_one_fetch() {
        let coalescer = RequestCoalescer::new();
        let counter = Arc::new(AtomicU32::new(0));
        let n_callers = 100u32;

        let mut handles = Vec::new();
        for _ in 0..n_callers {
            let c = coalescer.clone();
            let cnt = counter.clone();
            handles.push(tokio::spawn(async move {
                c.run("k", 0..8, move || {
                    let cnt = cnt.clone();
                    async move {
                        cnt.fetch_add(1, Ordering::SeqCst);
                        // Hold the leader's fetch long enough for all
                        // followers to register against the slot. 100 ms
                        // is well above the time it takes 100 tasks to
                        // schedule on a 4-thread runtime.
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        Ok(Bytes::from_static(b"payload8b"))
                    }
                })
                .await
            }));
        }
        for h in handles {
            let r = h.await.unwrap().unwrap();
            assert_eq!(&r[..], b"payload8b");
        }
        let fetches = counter.load(Ordering::SeqCst);
        assert!(
            fetches <= 3,
            "100 concurrent reads must collapse to a tiny number, got {fetches}"
        );
    }

    /// Different (key, range) pairs must NOT collapse: the coalescer keys on
    /// the full tuple, so each distinct key gets its own fetch.
    #[tokio::test]
    async fn different_keys_do_not_collapse() {
        let coalescer = RequestCoalescer::new();
        let counter = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for i in 0..5u32 {
            let c = coalescer.clone();
            let cnt = counter.clone();
            let key = format!("k{i}");
            handles.push(tokio::spawn(async move {
                c.run(&key, 0..16, move || {
                    let cnt = cnt.clone();
                    async move {
                        cnt.fetch_add(1, Ordering::Relaxed);
                        Ok(Bytes::from_static(b"x"))
                    }
                })
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }

    /// Different ranges of the SAME key are also distinct.
    #[tokio::test]
    async fn same_key_different_ranges_do_not_collapse() {
        let coalescer = RequestCoalescer::new();
        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for i in 0..4u64 {
            let c = coalescer.clone();
            let cnt = counter.clone();
            handles.push(tokio::spawn(async move {
                c.run("same-key", i * 100..(i + 1) * 100, move || {
                    let cnt = cnt.clone();
                    async move {
                        cnt.fetch_add(1, Ordering::Relaxed);
                        Ok(Bytes::from_static(b"x"))
                    }
                })
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    /// A failed underlying fetch must propagate the error to the leader and
    /// to every coalesced follower. The follower's error string carries the
    /// formatted message of the leader's error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_fetch_propagates_to_all_waiters() {
        use zen_common::ZenError;

        let coalescer = RequestCoalescer::new();
        let n = 16u32;

        let mut handles = Vec::new();
        for _ in 0..n {
            let c = coalescer.clone();
            handles.push(tokio::spawn(async move {
                c.run("kerr", 0..8, move || async move {
                    // Hold the leader open long enough for the followers
                    // to join, then fail the fetch.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Err::<Bytes, _>(ZenError::storage("io broken pipe"))
                })
                .await
            }));
        }
        let mut errors_seen = 0;
        for h in handles {
            let r = h.await.unwrap();
            let e = r.unwrap_err();
            // Followers wrap the message via storage(...); leader returns
            // the original. Both render with "io broken pipe" inside.
            assert!(
                format!("{e}").contains("io broken pipe"),
                "expected the source error to surface, got {e}"
            );
            errors_seen += 1;
        }
        assert_eq!(errors_seen, n);
    }

    /// After a request completes, the coalescer's internal map is empty —
    /// it must not retain references to bytes from past calls.
    #[tokio::test]
    async fn coalescer_does_not_leak_completed_entries() {
        let coalescer = RequestCoalescer::new();
        for i in 0..32u64 {
            let _ = coalescer
                .run(&format!("k{i}"), 0..16, || async {
                    Ok(Bytes::from_static(b"sixteen-bytes-ok"))
                })
                .await
                .unwrap();
        }
        // After all calls drain, the inner map must be empty.
        let g = coalescer.inner.lock().await;
        assert!(
            g.is_empty(),
            "coalescer leaked {} entries after drain",
            g.len()
        );
    }
}
