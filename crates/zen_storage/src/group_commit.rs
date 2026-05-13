//! Group-commit fsync coordinator.
//!
//! When N concurrent writers each want a durable write, naive `f.sync_all()`
//! per-call costs 5–15 ms each on macOS APFS / Linux ext4. The kernel
//! coalesces some of this internally but the workload still pays a
//! measurable tax under contention.
//!
//! The coordinator amortizes that tax. Writers call [`Coordinator::wait`]
//! after their `flush()` returns. The first writer becomes the **leader**
//! — installs an in-flight "barrier" window, sleeps a tiny coalesce
//! window so concurrent late arrivers can join, then issues one
//! `sync_all()`. Late arrivers see the open window and **subscribe** to
//! a `broadcast` channel; when the leader finishes they all receive the
//! same Ok/Err result without doing their own fsync.
//!
//! On ext4 with `data=ordered` (Linux default) and APFS, an fsync on
//! file A causes the journal to flush, which makes file B's writes
//! durable too — provided B's data was already in the page cache (its
//! `flush()` returned). Group commit relies on this property; ALL
//! waiters MUST `flush()` before calling `wait()`.
//!
//! **Correctness**: subscribers acquire their broadcast `Receiver` while
//! holding the state read-lock, and the leader holds the state write-lock
//! when broadcasting + tearing down. Rust's `RwLock` mutual exclusion
//! between readers and writers means a subscriber can never miss the
//! broadcast.

use std::sync::Arc;
use std::time::Duration;

use tokio::fs::File;
use tokio::sync::{broadcast, RwLock};

use zen_common::{ZenError, ZenResult};

/// Result published to followers. `Arc` so we can ship a single
/// allocation through `broadcast` to N subscribers cheaply.
type SharedResult = Arc<Result<(), String>>;

#[derive(Default)]
pub struct Coordinator {
    state: RwLock<Option<Arc<Window>>>,
    coalesce_window: Duration,
}

struct Window {
    tx: broadcast::Sender<SharedResult>,
}

impl Coordinator {
    pub fn new(coalesce_window: Duration) -> Self {
        Self {
            state: RwLock::new(None),
            coalesce_window,
        }
    }

    /// Default coordinator with a 200 µs coalesce window.
    pub fn default_coordinator() -> Self {
        Self::new(Duration::from_micros(200))
    }

    /// Issue a durable barrier covering `f`'s data. Joins the in-flight
    /// barrier if one is open; otherwise becomes leader and runs the
    /// fsync after a brief coalesce window.
    // `f` is mut so the leader can call `sync_all`; clippy can't see
    // through the early-return branch where it's used.
    #[allow(unused_mut)]
    pub async fn wait(&self, mut f: File) -> ZenResult<()> {
        // Fast follow path: subscribe under the read lock.
        {
            let g = self.state.read().await;
            if let Some(window) = g.as_ref() {
                let mut rx = window.tx.subscribe();
                drop(g);
                return match rx.recv().await {
                    Ok(arc) => match arc.as_ref() {
                        Ok(()) => Ok(()),
                        Err(s) => Err(ZenError::storage(s.clone())),
                    },
                    Err(_) => Err(ZenError::storage(
                        "group commit: leader dropped before broadcasting (shouldn't happen)",
                    )),
                };
            }
        }

        // Try to become leader.
        let window = {
            let mut g = self.state.write().await;
            if let Some(w) = g.as_ref() {
                // Race: someone became leader while we were upgrading
                // locks. Subscribe and follow.
                let mut rx = w.tx.subscribe();
                drop(g);
                return match rx.recv().await {
                    Ok(arc) => match arc.as_ref() {
                        Ok(()) => Ok(()),
                        Err(s) => Err(ZenError::storage(s.clone())),
                    },
                    Err(_) => Err(ZenError::storage("group commit: race-loser receive failed")),
                };
            }
            // Channel capacity 1 is plenty — we send exactly one message.
            let (tx, _) = broadcast::channel(1);
            let w = Arc::new(Window { tx });
            *g = Some(w.clone());
            w
        };

        // Brief coalesce window — late writers reach `subscribe()`
        // during this sleep and ride the same barrier.
        if !self.coalesce_window.is_zero() {
            tokio::time::sleep(self.coalesce_window).await;
        }

        // Leader's fsync.
        let raw_result = f
            .sync_all()
            .await
            .map_err(|e| format!("group-commit fsync: {e}"));
        let shared: SharedResult = Arc::new(raw_result.clone());

        // Broadcast first, THEN tear down the state. Followers who
        // subscribed under the read lock are guaranteed to see this
        // send because the channel doesn't drop until the leader's
        // `window` Arc drops at the end of this function — *after*
        // the broadcast.
        let _ = window.tx.send(shared);

        // Close the window so the next writer becomes a fresh leader.
        {
            let mut g = self.state.write().await;
            *g = None;
        }

        raw_result.map_err(ZenError::storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::fs::OpenOptions;

    /// Concurrent writers all succeed (durability respected). 32 of
    /// them — one becomes leader, 31 follow via the broadcast channel.
    #[tokio::test]
    async fn concurrent_commits_succeed() {
        let dir = TempDir::new().unwrap();
        let coord = Arc::new(Coordinator::default_coordinator());
        let success = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..32 {
            let coord = coord.clone();
            let path = dir.path().join(format!("f{i}"));
            let success = success.clone();
            handles.push(tokio::spawn(async move {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
                    .await
                    .unwrap();
                use tokio::io::AsyncWriteExt;
                f.write_all(format!("hello {i}").as_bytes()).await.unwrap();
                f.flush().await.unwrap();
                if coord.wait(f).await.is_ok() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(success.load(Ordering::Relaxed), 32);
    }

    /// Sequential commits work in isolation.
    #[tokio::test]
    async fn single_commit_works() {
        let dir = TempDir::new().unwrap();
        let coord = Coordinator::new(Duration::from_micros(0));
        let path = dir.path().join("solo");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await
            .unwrap();
        use tokio::io::AsyncWriteExt;
        f.write_all(b"x").await.unwrap();
        f.flush().await.unwrap();
        coord.wait(f).await.unwrap();
    }

    /// Two sequential commits — the second must NOT block on the first
    /// (the first's window must close cleanly).
    #[tokio::test]
    async fn back_to_back_commits() {
        let dir = TempDir::new().unwrap();
        let coord = Coordinator::default_coordinator();
        for i in 0..3 {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(dir.path().join(format!("f{i}")))
                .await
                .unwrap();
            use tokio::io::AsyncWriteExt;
            f.write_all(b"x").await.unwrap();
            f.flush().await.unwrap();
            coord.wait(f).await.unwrap();
        }
    }
}
