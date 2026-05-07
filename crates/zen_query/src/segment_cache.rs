//! Cache of parsed `SegmentReader`s keyed by object key. Without this, every
//! query re-fetches and re-parses each segment's footer + metadata from object
//! storage, which dominates latency on small queries.

use std::sync::Arc;

use moka::future::Cache;

use zen_common::ZenResult;
use zen_format::SegmentReader;
use zen_storage::BlobStore;

#[derive(Clone)]
pub struct SegmentCache {
    inner: Cache<String, Arc<SegmentReader>>,
}

impl SegmentCache {
    pub fn new(max_segments: u64) -> Self {
        Self {
            inner: Cache::builder().max_capacity(max_segments).build(),
        }
    }

    pub async fn get_or_load(
        &self,
        key: &str,
        store: Arc<dyn BlobStore>,
    ) -> ZenResult<Arc<SegmentReader>> {
        if let Some(r) = self.inner.get(key).await {
            return Ok(r);
        }
        let bytes = store.get(key).await?;
        let reader = Arc::new(SegmentReader::from_bytes(bytes.to_vec())?);
        self.inner.insert(key.to_string(), reader.clone()).await;
        Ok(reader)
    }
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::new(256)
    }
}
