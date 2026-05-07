//! "Hotcache" — a single contiguous block at a known offset in the segment
//! that contains everything a reader needs to plan a query without reading the
//! row groups: zone maps, posting list locations, FTS roots, HNSW headers.
//!
//! The reader fetches the hotcache in one ranged GET on cold open, then makes
//! decisions about which row groups to read further.

use serde::{Deserialize, Serialize};

use zen_index::ZoneMap;

use crate::row_group::RowGroupHeader;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ColumnHotcacheEntry {
    pub column_idx: u32,
    pub zone_map: ZoneMap,
    /// Absolute offset (within the segment) of the posting list serialized blob, if any.
    pub posting_offset: Option<u64>,
    pub posting_length: Option<u32>,
    /// Absolute offset of FTS index blob, if any.
    pub fts_offset: Option<u64>,
    pub fts_length: Option<u32>,
    /// Absolute offset of JSON-path posting blob, if any.
    pub jsonpath_offset: Option<u64>,
    pub jsonpath_length: Option<u32>,
    /// Absolute offset of HNSW graph blob, if any.
    pub hnsw_offset: Option<u64>,
    pub hnsw_length: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowGroupHotcacheEntry {
    pub row_group_idx: u32,
    pub header: RowGroupHeader,
    pub columns: Vec<ColumnHotcacheEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Hotcache {
    pub row_groups: Vec<RowGroupHotcacheEntry>,
}

impl Hotcache {
    pub fn new() -> Self {
        Self::default()
    }
}
