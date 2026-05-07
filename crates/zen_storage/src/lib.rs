//! Object-storage facade and NVMe page cache.

pub mod store;
pub mod local_fs;
pub mod cache;
pub mod coalesce;

pub use store::{BlobStore, BlobError};
pub use local_fs::LocalFsStore;
pub use cache::PageCache;
pub use coalesce::RequestCoalescer;

use std::sync::Arc;

use zen_common::{Config, ZenError, ZenResult};

/// Build a `BlobStore` from a `StorageConfig`. Currently supports `fs` directly
/// and falls back to `object_store` for `s3`/`gcs`/`azure`/`memory` backends.
pub async fn open_blob_store(cfg: &Config) -> ZenResult<Arc<dyn BlobStore>> {
    match cfg.storage.backend.as_str() {
        "fs" => Ok(Arc::new(LocalFsStore::new(&cfg.storage.fs_root)?)),
        "memory" => Ok(Arc::new(local_fs::InMemoryStore::default())),
        // For S3/GCS/Azure we delegate to object_store via a wrapper.
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
            Ok(Arc::new(store::ObjectStoreBlob::new(Arc::new(s))))
        }
        other => Err(ZenError::storage(format!("unsupported backend: {other}"))),
    }
}
