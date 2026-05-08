//! `EncryptingStore` — transparent envelope encryption around any inner
//! `BlobStore`. Opt-in via `cfg.crypto.enabled`. When enabled:
//!
//! - `put` and `put_if_absent` envelope-encrypt with a fresh per-blob
//!   data key, wrapped by the configured root key.
//! - `get` detects the `ZENV` magic and decrypts; legacy unencrypted
//!   blobs (e.g. older segments) pass through unchanged.
//! - `get_range` falls back to a full `get` + slice, since AEAD prevents
//!   safe partial reads. Today this is fine because segment + WAL paths
//!   already use full `get()`; ranged reads exist as an interface but
//!   aren't on the hot path.
//!
//! Performance: AES-256-GCM with hardware AES-NI / NEON crypto-extensions
//! runs at ~10 GB/s — at or above NVMe sequential read bandwidth. Cold
//! reads pay <5 % overhead; warm reads (already decrypted in
//! `SegmentCache`) pay 0 %.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use zen_common::{ZenError, ZenResult};
use zen_crypto::{decrypt, encrypt, envelope::is_encrypted, RootKey};

use crate::store::BlobStore;

pub struct EncryptingStore {
    inner: Arc<dyn BlobStore>,
    root: Arc<dyn RootKey>,
}

impl EncryptingStore {
    pub fn new(inner: Arc<dyn BlobStore>, root: Arc<dyn RootKey>) -> Self {
        Self { inner, root }
    }
}

#[async_trait]
impl BlobStore for EncryptingStore {
    async fn get(&self, key: &str) -> ZenResult<Bytes> {
        let bytes = self.inner.get(key).await?;
        if is_encrypted(&bytes) {
            let plain = decrypt(&bytes, &*self.root)
                .map_err(|e| ZenError::storage(format!("decrypt {key}: {e}")))?;
            Ok(Bytes::from(plain))
        } else {
            // Legacy unencrypted blob — pass through. This is what makes
            // the rollout backward-compatible: existing data keeps
            // working, new writes are encrypted.
            Ok(bytes)
        }
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ZenResult<Bytes> {
        // AEAD over the whole blob means we can't safely return a partial
        // ciphertext slice — fetch all, decrypt, slice. Today this isn't
        // hot: segment + WAL readers use `get()` for the whole blob.
        let plain = self.get(key).await?;
        let s = range.start as usize;
        let e = (range.end as usize).min(plain.len());
        if s > plain.len() {
            return Err(ZenError::storage("range start past end"));
        }
        Ok(plain.slice(s..e))
    }

    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()> {
        let ct = encrypt(&bytes, &*self.root)
            .map_err(|e| ZenError::storage(format!("encrypt {key}: {e}")))?;
        self.inner.put(key, Bytes::from(ct)).await
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool> {
        let ct = encrypt(&bytes, &*self.root)
            .map_err(|e| ZenError::storage(format!("encrypt {key}: {e}")))?;
        self.inner.put_if_absent(key, Bytes::from(ct)).await
    }

    async fn delete(&self, key: &str) -> ZenResult<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>> {
        self.inner.list(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_fs::InMemoryStore;
    use zen_crypto::root::StaticRootKey;

    fn rk() -> Arc<dyn RootKey> {
        Arc::new(StaticRootKey::new([0x42; 32]))
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let backing: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let store = EncryptingStore::new(backing.clone(), rk());
        store
            .put("k", Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        // Cipher bytes are different from plaintext — confirm via raw read.
        let raw = backing.get("k").await.unwrap();
        assert_ne!(&raw[..], b"hello world");
        assert!(zen_crypto::envelope::is_encrypted(&raw));
        // Reading back via the wrapper decrypts.
        let pt = store.get("k").await.unwrap();
        assert_eq!(&pt[..], b"hello world");
    }

    #[tokio::test]
    async fn legacy_unencrypted_passes_through() {
        let backing: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        backing
            .put("legacy", Bytes::from_static(b"plaintext blob"))
            .await
            .unwrap();
        let store = EncryptingStore::new(backing, rk());
        let r = store.get("legacy").await.unwrap();
        assert_eq!(&r[..], b"plaintext blob");
    }

    #[tokio::test]
    async fn get_range_works() {
        let backing: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let store = EncryptingStore::new(backing, rk());
        store
            .put("k", Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
        let r = store.get_range("k", 3..7).await.unwrap();
        assert_eq!(&r[..], b"defg");
    }
}
