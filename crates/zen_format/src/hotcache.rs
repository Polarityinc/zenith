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

#[cfg(test)]
mod tests {
    use zen_index::ZoneMap;

    use crate::row_group::{ColumnPageDescriptor, RowGroupHeader};

    use super::*;

    /// Synthesize a `ColumnHotcacheEntry` with predictable contents for tests.
    fn make_entry(col: u32) -> ColumnHotcacheEntry {
        ColumnHotcacheEntry {
            column_idx: col,
            zone_map: ZoneMap::from_i64(&[col as i64, col as i64 + 1, col as i64 + 2], 0),
            posting_offset: Some(1024),
            posting_length: Some(64),
            fts_offset: None,
            fts_length: None,
            jsonpath_offset: Some(2048),
            jsonpath_length: Some(128),
            hnsw_offset: None,
            hnsw_length: None,
        }
    }

    /// Synthesize a small `RowGroupHeader` for hotcache entries.
    fn make_header(rows: u32) -> RowGroupHeader {
        RowGroupHeader {
            row_count: rows,
            total_bytes: 256,
            columns: vec![ColumnPageDescriptor {
                column_idx: 0,
                encoding: 0,
                page_offset: 100,
                page_length: 64,
                uncompressed_size: 200,
            }],
        }
    }

    /// An empty `Hotcache::new` serializes to a small bincode blob and roundtrips.
    #[test]
    fn empty_hotcache_serialize_small() {
        let hc = Hotcache::new();
        let bytes = bincode::serialize(&hc).expect("serialize empty hotcache");
        // Empty Vec encodes as just a length, so this should be tiny.
        assert!(
            bytes.len() < 200,
            "empty hotcache serialized to {} bytes (>=200)",
            bytes.len()
        );
        let back: Hotcache = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back, hc);
        assert!(back.row_groups.is_empty());
    }

    /// A `Hotcache` with a few column entries roundtrips via bincode unchanged.
    #[test]
    fn hotcache_with_columns_roundtrip() {
        let entries: Vec<ColumnHotcacheEntry> = (0..4).map(make_entry).collect();
        let mut hc = Hotcache::new();
        hc.row_groups.push(RowGroupHotcacheEntry {
            row_group_idx: 0,
            header: make_header(8),
            columns: entries.clone(),
        });
        let bytes = bincode::serialize(&hc).expect("serialize");
        let back: Hotcache = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back, hc);
        assert_eq!(back.row_groups[0].columns.len(), entries.len());
        assert_eq!(back.row_groups[0].columns[2], entries[2]);
    }

    /// Bincode deserialization of a garbage-byte payload returns `Err`, never panics.
    #[test]
    fn hotcache_deserialize_garbage_is_err() {
        let garbage: Vec<u8> = (0..128).map(|i| (i * 31) as u8).collect();
        let r: Result<Hotcache, _> = bincode::deserialize(&garbage);
        assert!(r.is_err(), "expected deserialize Err on garbage");
    }

    /// Empty bytes also fail deserialize cleanly (no panic).
    #[test]
    fn hotcache_deserialize_empty_is_err() {
        let r: Result<Hotcache, _> = bincode::deserialize(&[]);
        assert!(r.is_err(), "expected deserialize Err on empty input");
    }

    proptest::proptest! {
        /// `Hotcache` with arbitrary numbers of entries roundtrips identically.
        #[test]
        fn arbitrary_entry_counts_roundtrip(
            n_groups in 0usize..4,
            n_columns in 0usize..16,
        ) {
            let mut hc = Hotcache::new();
            for g in 0..n_groups {
                let cols: Vec<ColumnHotcacheEntry> =
                    (0..n_columns).map(|c| make_entry(c as u32)).collect();
                hc.row_groups.push(RowGroupHotcacheEntry {
                    row_group_idx: g as u32,
                    header: make_header((g + 1) as u32),
                    columns: cols,
                });
            }
            let bytes = bincode::serialize(&hc).expect("serialize");
            let back: Hotcache = bincode::deserialize(&bytes).expect("deserialize");
            proptest::prop_assert_eq!(back, hc);
        }
    }
}
