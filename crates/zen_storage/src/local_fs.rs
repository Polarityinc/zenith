//! Local-filesystem `BlobStore` implementations: `LocalFsStore` (on-disk) and
//! `InMemoryStore` (RAM only, used in tests).

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

use zen_common::{ZenError, ZenResult};

use crate::store::BlobStore;

pub struct LocalFsStore {
    root: PathBuf,
    /// When false, skip `fsync` on writes. Default is **true** — durability
    /// is the safe choice. To opt out (for ephemeral dev / CI runs that
    /// prize speed over crash-safety), construct with
    /// [`LocalFsStore::new_unsafe_fast`] or set `ZEN_UNSAFE_FAST=1`.
    pub durable: bool,
    /// Group-commit coordinator. When N concurrent durable writes are
    /// in flight, only the first issues `sync_all()`; the rest piggyback
    /// on the same kernel journal-flush barrier. Recovers most of the
    /// fsync-default-on regression that durability_bench measured.
    group: Arc<crate::group_commit::Coordinator>,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> ZenResult<Self> {
        Self::with_durable(root, default_durable())
    }

    /// Explicit opt-in for non-durable writes. Caller acknowledges that
    /// crashes between WAL flush and disk-cache flush can lose committed
    /// rows. Use only when the data is reproducible (tests / benchmarks).
    pub fn new_unsafe_fast(root: impl Into<PathBuf>) -> ZenResult<Self> {
        Self::with_durable(root, false)
    }

    fn with_durable(root: impl Into<PathBuf>, durable: bool) -> ZenResult<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| ZenError::storage(format!("mkdir {root:?}: {e}")))?;
        Ok(Self {
            root,
            durable,
            group: Arc::new(crate::group_commit::Coordinator::default_coordinator()),
        })
    }

    fn path_for(&self, key: &str) -> PathBuf {
        // Keys can contain '/'; we mirror them into the filesystem.
        self.root.join(key.trim_start_matches('/'))
    }
}

/// Default durability policy: ON unless `ZEN_UNSAFE_FAST=1` is explicitly
/// set in the environment. The previous default (OFF unless `ZEN_FS_DURABLE=1`)
/// was a silent-data-loss landmine; the polarity is now reversed so the
/// safe choice is the implicit one.
fn default_durable() -> bool {
    std::env::var("ZEN_UNSAFE_FAST").ok().as_deref() != Some("1")
}

#[async_trait]
impl BlobStore for LocalFsStore {
    async fn get(&self, key: &str) -> ZenResult<Bytes> {
        let p = self.path_for(key);
        let bytes = fs::read(&p)
            .await
            .map_err(|e| ZenError::not_found(format!("get {key}: {e}")))?;
        Ok(Bytes::from(bytes))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ZenResult<Bytes> {
        let p = self.path_for(key);
        let mut f = fs::File::open(&p)
            .await
            .map_err(|e| ZenError::not_found(format!("get_range {key}: {e}")))?;
        let len = (range.end - range.start) as usize;
        // tokio doesn't expose pread; seek + read is fine.
        use tokio::io::AsyncSeekExt;
        f.seek(std::io::SeekFrom::Start(range.start))
            .await
            .map_err(|e| ZenError::storage(format!("seek {key}: {e}")))?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)
            .await
            .map_err(|e| ZenError::storage(format!("read_exact {key}: {e}")))?;
        Ok(Bytes::from(buf))
    }

    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()> {
        let p = self.path_for(key);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| ZenError::storage(format!("mkdir {parent:?}: {e}")))?;
        }
        // Atomic create: write to .tmp, fsync (if durable), then rename.
        // Pre-fix this method silently dropped the fsync regardless of
        // `self.durable`, which lost compactor-written segments on crash.
        let tmp = p.with_extension("tmp.zen-write");
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .map_err(|e| ZenError::storage(format!("open tmp {key}: {e}")))?;
        use tokio::io::AsyncWriteExt;
        f.write_all(bytes.as_ref())
            .await
            .map_err(|e| ZenError::storage(format!("write tmp {key}: {e}")))?;
        f.flush()
            .await
            .map_err(|e| ZenError::storage(format!("flush tmp {key}: {e}")))?;
        if self.durable {
            // Group commit: concurrent durable writes share one syscall.
            self.group.wait(f).await?;
        } else {
            // Drop without fsync — speed mode.
            drop(f);
        }
        fs::rename(&tmp, &p)
            .await
            .map_err(|e| ZenError::storage(format!("rename {key}: {e}")))?;
        // For full durability of the rename itself we'd fsync the parent
        // dir. Skipped here because it adds a syscall per write; rename
        // crash-recovery is well-understood (orphaned `.tmp.zen-write`
        // files are pruned at startup by the GC).
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool> {
        let p = self.path_for(key);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| ZenError::storage(format!("mkdir {parent:?}: {e}")))?;
        }
        // Use OpenOptions with create_new for atomic-or-fail.
        let mut opts = tokio::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        match opts.open(&p).await {
            Ok(mut f) => {
                use tokio::io::AsyncWriteExt;
                f.write_all(bytes.as_ref())
                    .await
                    .map_err(|e| ZenError::storage(format!("write {key}: {e}")))?;
                f.flush()
                    .await
                    .map_err(|e| ZenError::storage(format!("flush {key}: {e}")))?;
                if self.durable {
                    self.group.wait(f).await?;
                }
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(ZenError::storage(format!("create_new {key}: {e}"))),
        }
    }

    async fn delete(&self, key: &str) -> ZenResult<()> {
        let p = self.path_for(key);
        match fs::remove_file(&p).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ZenError::storage(format!("rm {key}: {e}"))),
        }
    }

    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>> {
        let mut out = Vec::new();
        let prefix_path = self.path_for(prefix);
        // Walk under prefix_path; if it's a file, add it; if it's a directory, walk.
        let root = self.root.clone();
        if !prefix_path.exists() {
            // Try treating prefix as a string prefix at root.
            let mut stack = vec![root.clone()];
            while let Some(d) = stack.pop() {
                let mut rd = match fs::read_dir(&d).await {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                while let Ok(Some(ent)) = rd.next_entry().await {
                    let p = ent.path();
                    if p.is_dir() {
                        stack.push(p);
                    } else if let Ok(rel) = p.strip_prefix(&root) {
                        let s = rel.to_string_lossy().to_string();
                        if s.starts_with(prefix.trim_start_matches('/')) {
                            out.push(s);
                        }
                    }
                }
            }
        } else if prefix_path.is_file() {
            if let Ok(rel) = prefix_path.strip_prefix(&self.root) {
                out.push(rel.to_string_lossy().to_string());
            }
        } else {
            // Walk the directory.
            let mut stack = vec![prefix_path];
            while let Some(d) = stack.pop() {
                let mut rd = match fs::read_dir(&d).await {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                while let Ok(Some(ent)) = rd.next_entry().await {
                    let p = ent.path();
                    if p.is_dir() {
                        stack.push(p);
                    } else if let Ok(rel) = p.strip_prefix(&self.root) {
                        out.push(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

/// In-memory blob store. For tests and the `memory` storage backend.
#[derive(Default, Clone)]
pub struct InMemoryStore {
    map: Arc<RwLock<HashMap<String, Bytes>>>,
}

#[async_trait]
impl BlobStore for InMemoryStore {
    async fn get(&self, key: &str) -> ZenResult<Bytes> {
        let g = self.map.read().await;
        g.get(key)
            .cloned()
            .ok_or_else(|| ZenError::not_found(format!("get {key}")))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ZenResult<Bytes> {
        let g = self.map.read().await;
        let b = g
            .get(key)
            .ok_or_else(|| ZenError::not_found(format!("get_range {key}")))?;
        let s = range.start as usize;
        let e = (range.end as usize).min(b.len());
        if s > b.len() {
            return Err(ZenError::storage("range start past end"));
        }
        Ok(b.slice(s..e))
    }

    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()> {
        let mut g = self.map.write().await;
        g.insert(key.to_string(), bytes);
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool> {
        let mut g = self.map.write().await;
        if g.contains_key(key) {
            return Ok(false);
        }
        g.insert(key.to_string(), bytes);
        Ok(true)
    }

    async fn delete(&self, key: &str) -> ZenResult<()> {
        let mut g = self.map.write().await;
        g.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>> {
        let g = self.map.read().await;
        let mut out: Vec<String> = g
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn put_get_roundtrip() {
        let dir = TempDir::new().unwrap();
        let s = LocalFsStore::new(dir.path()).unwrap();
        s.put("a/b/c", Bytes::from_static(b"hello")).await.unwrap();
        let r = s.get("a/b/c").await.unwrap();
        assert_eq!(&r[..], b"hello");
    }

    #[tokio::test]
    async fn get_range_works() {
        let dir = TempDir::new().unwrap();
        let s = LocalFsStore::new(dir.path()).unwrap();
        s.put("k", Bytes::from_static(b"abcdefghij")).await.unwrap();
        let r = s.get_range("k", 3..7).await.unwrap();
        assert_eq!(&r[..], b"defg");
    }

    #[tokio::test]
    async fn put_if_absent_no_overwrite() {
        let dir = TempDir::new().unwrap();
        let s = LocalFsStore::new(dir.path()).unwrap();
        let ok1 = s.put_if_absent("k", Bytes::from_static(b"first")).await.unwrap();
        let ok2 = s.put_if_absent("k", Bytes::from_static(b"second")).await.unwrap();
        assert!(ok1);
        assert!(!ok2);
        let r = s.get("k").await.unwrap();
        assert_eq!(&r[..], b"first");
    }

    #[tokio::test]
    async fn delete_idempotent() {
        let dir = TempDir::new().unwrap();
        let s = LocalFsStore::new(dir.path()).unwrap();
        s.put("k", Bytes::from_static(b"x")).await.unwrap();
        s.delete("k").await.unwrap();
        s.delete("k").await.unwrap(); // idempotent
        assert!(s.get("k").await.is_err());
    }

    #[tokio::test]
    async fn list_prefix() {
        let dir = TempDir::new().unwrap();
        let s = LocalFsStore::new(dir.path()).unwrap();
        s.put("a/x", Bytes::from_static(b"1")).await.unwrap();
        s.put("a/y", Bytes::from_static(b"2")).await.unwrap();
        s.put("b/z", Bytes::from_static(b"3")).await.unwrap();
        let mut keys = s.list("a/").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a/x".to_string(), "a/y".to_string()]);
    }

    #[tokio::test]
    async fn in_memory_basics() {
        let s = InMemoryStore::default();
        s.put("k", Bytes::from_static(b"hello")).await.unwrap();
        assert_eq!(&s.get("k").await.unwrap()[..], b"hello");
        assert_eq!(&s.get_range("k", 1..4).await.unwrap()[..], b"ell");
        assert!(!s.put_if_absent("k", Bytes::from_static(b"x")).await.unwrap());
        assert!(s.put_if_absent("k2", Bytes::from_static(b"y")).await.unwrap());
        let mut keys = s.list("").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["k".to_string(), "k2".to_string()]);
    }
}
