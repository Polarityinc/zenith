//! Segment footer.
//!
//! The footer is fixed-size when serialized to bincode and lives directly
//! before the magic trailer. To open a cold segment, a reader fetches the last
//! N bytes of the file (a "tail GET"), reads the footer, and from there knows
//! where the hotcache lives — one more ranged GET fetches the hotcache.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Footer {
    pub format_version: u32,
    pub metadata_offset: u64,
    pub metadata_length: u32,
    pub row_group_payload_offset: u64,
    pub row_group_payload_length: u64,
    pub inline_indexes_offset: u64,
    pub inline_indexes_length: u64,
    pub hotcache_offset: u64,
    pub hotcache_length: u64,
    /// CRC32 of (metadata bytes + row-group payload + inline indexes + hotcache).
    pub content_crc32: u32,
}
