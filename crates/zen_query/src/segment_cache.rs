//! Cache of parsed `SegmentReader`s + derived structures (posting lists, FTS
//! handles). Without this, every query re-fetches and re-parses each segment's
//! footer + metadata, AND re-deserializes posting lists. With it, all of that
//! is a one-time cost amortized across all queries against the same segment.

use std::collections::HashMap;
use std::sync::Arc;

use moka::future::Cache;
use parking_lot::RwLock;
use roaring::RoaringBitmap;

use zen_common::ZenResult;
use zen_format::SegmentReader;
use zen_index::PostingMap;
use zen_storage::BlobStore;

/// Cached segment data: parsed reader plus lazy posting-map / FTS / JSON-path
/// caches. Without these, every query pays the cost of deserializing and (in
/// the FTS case) reopening Tantivy indexes against the inline blob.
pub struct SegmentExtras {
    pub reader: Arc<SegmentReader>,
    postings: RwLock<HashMap<(u32, u32), Arc<PostingMap>>>,
    posting_results: RwLock<HashMap<(u32, u32, u64), Arc<RoaringBitmap>>>,
    fts: RwLock<HashMap<u32, Arc<zen_fts::FtsHandle>>>,
    jsonpath: RwLock<HashMap<u32, Arc<zen_jsonpath::JsonPathIndex>>>,
    fts_results: RwLock<HashMap<(u32, u64, u64), Arc<RoaringBitmap>>>,
    jsonpath_results: RwLock<HashMap<(u32, u64, u64), Arc<RoaringBitmap>>>,
}

impl SegmentExtras {
    pub fn new(reader: Arc<SegmentReader>) -> Self {
        Self {
            reader,
            postings: RwLock::new(HashMap::new()),
            posting_results: RwLock::new(HashMap::new()),
            fts: RwLock::new(HashMap::new()),
            jsonpath: RwLock::new(HashMap::new()),
            fts_results: RwLock::new(HashMap::new()),
            jsonpath_results: RwLock::new(HashMap::new()),
        }
    }

    pub fn fts_handle(&self, rg_idx: u32) -> Option<Arc<zen_fts::FtsHandle>> {
        if let Some(h) = self.fts.read().get(&rg_idx) {
            return Some(h.clone());
        }
        let rg = self.reader.hotcache.row_groups.get(rg_idx as usize)?;
        let entry = rg
            .columns
            .iter()
            .find(|c| c.fts_offset.is_some() && c.fts_length.is_some())?;
        let off = entry.fts_offset?;
        let len = entry.fts_length? as usize;
        let inline_base = self.reader.footer.inline_indexes_offset as usize;
        let start = inline_base + off as usize;
        let end = start + len;
        if self.reader.bytes.len() < end {
            return None;
        }
        let blob = &self.reader.bytes[start..end];
        let handle = zen_fts::open_fts_index(blob).ok()?;
        let h = Arc::new(handle);
        self.fts.write().insert(rg_idx, h.clone());
        Some(h)
    }

    pub fn fts_search_cached(
        &self,
        rg_idx: u32,
        column: &str,
        query: &str,
    ) -> Option<Arc<RoaringBitmap>> {
        let key = (
            rg_idx,
            xxhash_rust::xxh3::xxh3_64(column.as_bytes()),
            xxhash_rust::xxh3::xxh3_64(query.as_bytes()),
        );
        if let Some(bm) = self.fts_results.read().get(&key) {
            return Some(bm.clone());
        }
        let handle = self.fts_handle(rg_idx)?;
        let q = zen_fts::FtsQuery {
            field: Some(column),
            query,
            limit: 100_000,
        };
        let bm = handle.search_to_bitmap(&q).ok()?;
        let bm = Arc::new(bm);
        self.fts_results.write().insert(key, bm.clone());
        Some(bm)
    }

    pub fn jsonpath_index(&self, rg_idx: u32) -> Option<Arc<zen_jsonpath::JsonPathIndex>> {
        if let Some(h) = self.jsonpath.read().get(&rg_idx) {
            return Some(h.clone());
        }
        let rg = self.reader.hotcache.row_groups.get(rg_idx as usize)?;
        let entry = rg
            .columns
            .iter()
            .find(|c| c.jsonpath_offset.is_some() && c.jsonpath_length.is_some())?;
        let off = entry.jsonpath_offset?;
        let len = entry.jsonpath_length? as usize;
        let inline_base = self.reader.footer.inline_indexes_offset as usize;
        let start = inline_base + off as usize;
        let end = start + len;
        if self.reader.bytes.len() < end {
            return None;
        }
        let bytes = &self.reader.bytes[start..end];
        let idx = zen_jsonpath::JsonPathIndex::deserialize(bytes).ok()?;
        let arc = Arc::new(idx);
        self.jsonpath.write().insert(rg_idx, arc.clone());
        Some(arc)
    }

    pub fn jsonpath_lookup_cached(
        &self,
        rg_idx: u32,
        path: &str,
        value: &str,
    ) -> Option<Arc<RoaringBitmap>> {
        let key = (
            rg_idx,
            xxhash_rust::xxh3::xxh3_64(path.as_bytes()),
            xxhash_rust::xxh3::xxh3_64(value.as_bytes()),
        );
        if let Some(bm) = self.jsonpath_results.read().get(&key) {
            return Some(bm.clone());
        }
        let idx = self.jsonpath_index(rg_idx)?;
        if !idx.knows_path(path) {
            return None;
        }
        let bm = idx.lookup(path, value).cloned().unwrap_or_default();
        let bm = Arc::new(bm);
        self.jsonpath_results.write().insert(key, bm.clone());
        Some(bm)
    }

    /// Get a posting map for `(rg_idx, column_idx)`, deserializing on first
    /// access and caching for subsequent calls.
    pub fn posting_map(&self, rg_idx: u32, column_idx: u32) -> Option<Arc<PostingMap>> {
        if let Some(pm) = self.postings.read().get(&(rg_idx, column_idx)) {
            return Some(pm.clone());
        }
        let rg = self.reader.hotcache.row_groups.get(rg_idx as usize)?;
        let entry = rg.columns.iter().find(|c| c.column_idx == column_idx)?;
        let local_off = entry.posting_offset?;
        let len = entry.posting_length? as usize;
        let inline_base = self.reader.footer.inline_indexes_offset as usize;
        let start = inline_base + local_off as usize;
        let end = start + len;
        if self.reader.bytes.len() < end {
            return None;
        }
        let bytes = &self.reader.bytes[start..end];
        let pm = PostingMap::deserialize(bytes).ok()?;
        let pm = Arc::new(pm);
        self.postings
            .write()
            .insert((rg_idx, column_idx), pm.clone());
        Some(pm)
    }

    /// Cached row mask for `(rg_idx, column_idx, value_hash)`.
    pub fn posting_lookup_cached(
        &self,
        rg_idx: u32,
        column_idx: u32,
        value: &[u8],
    ) -> Option<Arc<RoaringBitmap>> {
        let h = xxhash_rust::xxh3::xxh3_64(value);
        if let Some(bm) = self.posting_results.read().get(&(rg_idx, column_idx, h)) {
            return Some(bm.clone());
        }
        let pm = self.posting_map(rg_idx, column_idx)?;
        let bm = pm
            .get(value)
            .map(|pl| pl.bitmap.clone())
            .unwrap_or_default();
        let bm = Arc::new(bm);
        self.posting_results
            .write()
            .insert((rg_idx, column_idx, h), bm.clone());
        Some(bm)
    }
}

#[derive(Clone)]
pub struct SegmentCache {
    inner: Cache<String, Arc<SegmentExtras>>,
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
    ) -> ZenResult<Arc<SegmentExtras>> {
        if let Some(r) = self.inner.get(key).await {
            return Ok(r);
        }
        let bytes = store.get(key).await?;
        let reader = Arc::new(SegmentReader::from_bytes(bytes.to_vec())?);
        let extras = Arc::new(SegmentExtras::new(reader));
        self.inner.insert(key.to_string(), extras.clone()).await;
        Ok(extras)
    }
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::new(256)
    }
}
