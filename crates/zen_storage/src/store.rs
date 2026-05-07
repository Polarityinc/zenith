//! `BlobStore`: tiny portable object-storage trait.
//!
//! We don't expose `object_store::ObjectStore` directly because (a) we want
//! `put_if_absent` everywhere, (b) we want CRC validation on bytes, and (c) we
//! want to control how range coalescing happens at our layer rather than the
//! library's. The trait is small, and the impls are short.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutPayload};
use thiserror::Error;

use zen_common::{ZenError, ZenResult};

#[derive(Debug, Error)]
pub enum BlobError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("io: {0}")]
    Io(String),
}

impl From<BlobError> for ZenError {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::NotFound(s) => ZenError::not_found(s),
            BlobError::Conflict(s) => ZenError::conflict(s),
            BlobError::Io(s) => ZenError::storage(s),
        }
    }
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Get a full object as bytes.
    async fn get(&self, key: &str) -> ZenResult<Bytes>;

    /// Get a contiguous byte range from an object. The range is half-open `[start..end)`.
    async fn get_range(&self, key: &str, range: Range<u64>) -> ZenResult<Bytes>;

    /// Put a new object. Overwrites if the key exists.
    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()>;

    /// Put a new object only if the key does not exist. Returns `true` on success,
    /// `false` if the key already exists. Used as the WAL fence.
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool>;

    /// Delete an object. Idempotent.
    async fn delete(&self, key: &str) -> ZenResult<()>;

    /// List object keys with a given prefix.
    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>>;
}

/// Wrapper that adapts an `object_store::ObjectStore` to `BlobStore`. Not used
/// for local filesystem (which has its own faster impl), but used for S3 / GCS
/// / Azure.
pub struct ObjectStoreBlob {
    inner: Arc<dyn ObjectStore>,
}

impl ObjectStoreBlob {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl BlobStore for ObjectStoreBlob {
    async fn get(&self, key: &str) -> ZenResult<Bytes> {
        let p = Path::from(key);
        let r = self
            .inner
            .get(&p)
            .await
            .map_err(|e| ZenError::storage(format!("get {key}: {e}")))?;
        Ok(r.bytes()
            .await
            .map_err(|e| ZenError::storage(format!("get bytes {key}: {e}")))?)
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ZenResult<Bytes> {
        let p = Path::from(key);
        let usize_range = (range.start as usize)..(range.end as usize);
        Ok(self
            .inner
            .get_range(&p, usize_range)
            .await
            .map_err(|e| ZenError::storage(format!("get_range {key}: {e}")))?)
    }

    async fn put(&self, key: &str, bytes: Bytes) -> ZenResult<()> {
        let p = Path::from(key);
        self.inner
            .put(&p, PutPayload::from(bytes))
            .await
            .map_err(|e| ZenError::storage(format!("put {key}: {e}")))?;
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> ZenResult<bool> {
        let p = Path::from(key);
        let res = self
            .inner
            .put_opts(
                &p,
                PutPayload::from(bytes),
                object_store::PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = format!("{e}");
                if msg.to_ascii_lowercase().contains("already exists")
                    || msg.contains("PreconditionFailed")
                {
                    Ok(false)
                } else {
                    Err(ZenError::storage(format!("put_if_absent {key}: {e}")))
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> ZenResult<()> {
        let p = Path::from(key);
        match self.inner.delete(&p).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(ZenError::storage(format!("delete {key}: {e}"))),
        }
    }

    async fn list(&self, prefix: &str) -> ZenResult<Vec<String>> {
        let p = Path::from(prefix);
        let mut out = Vec::new();
        let mut s = self.inner.list(Some(&p));
        while let Some(meta) = s
            .try_next()
            .await
            .map_err(|e| ZenError::storage(format!("list {prefix}: {e}")))?
        {
            out.push(meta.location.to_string());
        }
        Ok(out)
    }
}
