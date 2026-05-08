//! Object-storage facade and NVMe page cache.

pub mod store;
pub mod local_fs;
pub mod cache;
pub mod coalesce;
pub mod group_commit;
pub mod encrypting;

pub use store::{BlobStore, BlobError};
pub use local_fs::LocalFsStore;
pub use cache::PageCache;
pub use coalesce::RequestCoalescer;

use std::sync::Arc;

use zen_common::{Config, ZenError, ZenResult};

/// Build a `BlobStore` from a `StorageConfig`. Currently supports `fs` directly
/// and falls back to `object_store` for `s3`/`gcs`/`azure`/`memory` backends.
///
/// When `cfg.crypto.enabled` is true, the resulting store is wrapped in
/// an [`encrypting::EncryptingStore`] so every put/get goes through
/// envelope encryption transparently. Legacy (unencrypted) blobs still
/// read fine — `EncryptingStore::get` detects the `ZENV` magic and
/// falls back when absent.
pub async fn open_blob_store(cfg: &Config) -> ZenResult<Arc<dyn BlobStore>> {
    let inner: Arc<dyn BlobStore> = match cfg.storage.backend.as_str() {
        "fs" => Arc::new(LocalFsStore::new(&cfg.storage.fs_root)?),
        "memory" => Arc::new(local_fs::InMemoryStore::default()),
        "s3" => {
            use object_store::aws::AmazonS3Builder;
            let mut b = AmazonS3Builder::from_env()
                .with_region(cfg.storage.region.clone().unwrap_or_default());
            if let Some(bucket) = &cfg.storage.bucket {
                b = b.with_bucket_name(bucket);
            }
            if let Some(endpoint) = &cfg.storage.endpoint {
                b = b.with_endpoint(endpoint).with_allow_http(true);
            }
            if let (Some(ak), Some(sk)) = (&cfg.storage.access_key, &cfg.storage.secret_key) {
                b = b.with_access_key_id(ak).with_secret_access_key(sk);
            }
            let s = b
                .build()
                .map_err(|e| ZenError::storage(format!("s3 builder: {e}")))?;
            Arc::new(store::ObjectStoreBlob::new(Arc::new(s)))
        }
        other => return Err(ZenError::storage(format!("unsupported backend: {other}"))),
    };

    if cfg.crypto.enabled {
        let key_bytes = load_root_key(&cfg.crypto.root_key_path)?;
        let root: Arc<dyn zen_crypto::RootKey> =
            Arc::new(zen_crypto::root::StaticRootKey::new(key_bytes));
        Ok(Arc::new(encrypting::EncryptingStore::new(inner, root)))
    } else {
        Ok(inner)
    }
}

fn load_root_key(path: &str) -> ZenResult<[u8; 32]> {
    if path.is_empty() {
        return Err(ZenError::storage(
            "crypto.enabled=true requires crypto.root_key_path",
        ));
    }
    let raw = std::fs::read(path)
        .map_err(|e| ZenError::storage(format!("read root key {path}: {e}")))?;
    if raw.len() == 32 {
        let mut out = [0u8; 32];
        out.copy_from_slice(&raw);
        return Ok(out);
    }
    // Hex form — strip whitespace, decode.
    let trimmed: String = raw
        .iter()
        .filter(|b| !b.is_ascii_whitespace())
        .map(|b| *b as char)
        .collect();
    if trimmed.len() == 64 {
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&trimmed[i * 2..i * 2 + 2], 16)
                .map_err(|e| ZenError::storage(format!("bad hex root key: {e}")))?;
        }
        return Ok(out);
    }
    Err(ZenError::storage(format!(
        "root key file must be 32 bytes raw or 64 hex chars; got {} bytes",
        raw.len()
    )))
}
