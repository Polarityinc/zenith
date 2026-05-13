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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `Footer` with all fields populated to non-default values.
    fn make_footer() -> Footer {
        Footer {
            format_version: 1,
            metadata_offset: 12,
            metadata_length: 200,
            row_group_payload_offset: 220,
            row_group_payload_length: 1024,
            inline_indexes_offset: 1244,
            inline_indexes_length: 32,
            hotcache_offset: 1276,
            hotcache_length: 96,
            content_crc32: 0xDEAD_BEEF,
        }
    }

    /// Footer struct round-trips through bincode unchanged.
    #[test]
    fn footer_bincode_roundtrip() {
        let f = make_footer();
        let bytes = bincode::serialize(&f).expect("serialize");
        let f2: Footer = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(f, f2);
    }

    /// Footer serializes to a fixed-shape blob whose size equals the sum of its
    /// fields under bincode's default `varint = false` settings: 4 + 8 + 4 + 8
    /// + 8 + 8 + 8 + 8 + 8 + 4 = 68 bytes.
    #[test]
    fn footer_bincode_size_matches_field_sum() {
        let f = make_footer();
        let bytes = bincode::serialize(&f).expect("serialize");
        // 4 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 8 + 4 = 68
        assert_eq!(bytes.len(), 68, "expected 68-byte fixed-size footer");
    }

    /// All offset/length fields roundtrip under boundary u64 values without
    /// truncation. The reader does its own bounds-checking on these offsets,
    /// so the footer itself just needs to preserve every bit.
    #[test]
    fn footer_preserves_high_offset_values() {
        let f = Footer {
            format_version: 1,
            metadata_offset: u64::MAX - 4,
            metadata_length: u32::MAX,
            row_group_payload_offset: u64::MAX / 2,
            row_group_payload_length: u64::MAX / 4,
            inline_indexes_offset: u64::MAX / 8,
            inline_indexes_length: u64::MAX / 16,
            hotcache_offset: u64::MAX / 32,
            hotcache_length: 0,
            content_crc32: u32::MAX,
        };
        let bytes = bincode::serialize(&f).expect("serialize");
        let f2: Footer = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(f, f2);
        // The reader performs bounds checks against the actual segment length;
        // see `SegmentReader::from_bytes` (e.g. `m_body_end > bytes.len()`).
    }

    /// A 68-byte serialized footer can be deserialized from a longer buffer by
    /// slicing to the known length — the segment reader uses an explicit
    /// `footer_len` from the trailer to do exactly this.
    #[test]
    fn footer_deserializes_from_known_length_prefix() {
        let f = make_footer();
        let mut bytes = bincode::serialize(&f).expect("serialize");
        let original_len = bytes.len();
        bytes.extend_from_slice(b"trailing-junk");
        // The reader always slices to the recorded footer length before calling
        // bincode (see `SegmentReader::from_bytes`).
        let f2: Footer = bincode::deserialize(&bytes[..original_len]).expect("prefix deserialize");
        assert_eq!(f, f2);
    }
}
