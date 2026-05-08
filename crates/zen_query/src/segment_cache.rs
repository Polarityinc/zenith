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
/// caches. Per-segment caches are bounded by row-group count (small N).
/// Per-(rg, query) result caches are bounded by `MAX_RESULT_ENTRIES` so that
/// adversarial query streams can't grow them unboundedly. Limits are
/// generous (32K entries) to keep eviction rare.
type ResultCache = RwLock<HashMap<(u32, u64, u64, usize), Arc<RoaringBitmap>>>;
pub struct SegmentExtras {
    pub reader: Arc<SegmentReader>,
    postings: RwLock<HashMap<(u32, u32), Arc<PostingMap>>>,
    posting_results: RwLock<HashMap<(u32, u32, u64), Arc<RoaringBitmap>>>,
    fts: RwLock<HashMap<u32, Arc<zen_fts::FtsHandle>>>,
    jsonpath: RwLock<HashMap<u32, Arc<zen_jsonpath::JsonPathIndex>>>,
    fts_results: ResultCache,
    jsonpath_results: ResultCache,
}

const MAX_RESULT_ENTRIES: usize = 32_768;
const FULL_RESULT_LIMIT: usize = usize::MAX;

fn limited_bitmap_clone(bitmap: &RoaringBitmap, limit: Option<usize>) -> RoaringBitmap {
    let Some(limit) = limit else {
        return bitmap.clone();
    };
    let mut out = RoaringBitmap::new();
    for row in bitmap.iter().take(limit) {
        out.push(row);
    }
    out
}

fn cap_insert<K: Eq + std::hash::Hash + Clone, V>(map: &RwLock<HashMap<K, V>>, k: K, v: V) {
    let mut g = map.write();
    if g.len() >= MAX_RESULT_ENTRIES {
        // Drop one arbitrary key (cheap; we don't need true LRU here).
        if let Some(evict) = g.keys().next().cloned() {
            g.remove(&evict);
        }
    }
    g.insert(k, v);
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
        limit: usize,
    ) -> Option<Arc<RoaringBitmap>> {
        let key = (
            rg_idx,
            xxhash_rust::xxh3::xxh3_64(column.as_bytes()),
            xxhash_rust::xxh3::xxh3_64(query.as_bytes()),
            limit,
        );
        if let Some(bm) = self.fts_results.read().get(&key) {
            return Some(bm.clone());
        }
        let handle = self.fts_handle(rg_idx)?;
        let q = zen_fts::FtsQuery {
            field: Some(column),
            query,
            limit,
        };
        let bm = handle.search_to_bitmap(&q).ok()?;
        let bm = Arc::new(bm);
        cap_insert(&self.fts_results, key, bm.clone());
        Some(bm)
    }

    pub fn jsonpath_index(&self, rg_idx: u32) -> Option<Arc<zen_jsonpath::JsonPathIndex>> {
        if let Some(h) = self.jsonpath.read().get(&rg_idx) {
            return Some(h.clone());
        }
        let bytes = self.jsonpath_blob(rg_idx)?;
        let idx = zen_jsonpath::JsonPathIndex::deserialize(bytes).ok()?;
        let arc = Arc::new(idx);
        self.jsonpath.write().insert(rg_idx, arc.clone());
        Some(arc)
    }

    fn jsonpath_blob(&self, rg_idx: u32) -> Option<&[u8]> {
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
        Some(&self.reader.bytes[start..end])
    }

    pub fn jsonpath_lookup_cached(
        &self,
        rg_idx: u32,
        path: &str,
        value: &str,
        limit: Option<usize>,
    ) -> Option<Arc<RoaringBitmap>> {
        let limit_key = limit.unwrap_or(FULL_RESULT_LIMIT);
        let key = (
            rg_idx,
            xxhash_rust::xxh3::xxh3_64(path.as_bytes()),
            xxhash_rust::xxh3::xxh3_64(value.as_bytes()),
            limit_key,
        );
        if let Some(bm) = self.jsonpath_results.read().get(&key) {
            return Some(bm.clone());
        }
        if limit.is_some() {
            let full_key = (key.0, key.1, key.2, FULL_RESULT_LIMIT);
            if let Some(full) = self.jsonpath_results.read().get(&full_key) {
                let bm = Arc::new(limited_bitmap_clone(full, limit));
                cap_insert(&self.jsonpath_results, key, bm.clone());
                return Some(bm);
            }
        }
        let bm = if let Some(idx) = self.jsonpath.read().get(&rg_idx) {
            if !idx.knows_path(path) {
                return None;
            }
            idx.lookup(path, value)
                .map(|bm| limited_bitmap_clone(bm, limit))
                .unwrap_or_default()
        } else {
            let bytes = self.jsonpath_blob(rg_idx)?;
            let bm = zen_jsonpath::JsonPathIndex::lookup_serialized(bytes, path, value).ok()??;
            limited_bitmap_clone(&bm, limit)
        };
        let bm = Arc::new(bm);
        cap_insert(&self.jsonpath_results, key, bm.clone());
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
        let key = (rg_idx, column_idx, h);
        if let Some(bm) = self.posting_results.read().get(&key) {
            return Some(bm.clone());
        }
        let pm = self.posting_map(rg_idx, column_idx)?;
        let bm = pm
            .get(value)
            .map(|pl| pl.bitmap.clone())
            .unwrap_or_default();
        let bm = Arc::new(bm);
        cap_insert(&self.posting_results, key, bm.clone());
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
