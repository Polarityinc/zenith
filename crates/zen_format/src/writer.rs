//! Segment writer.
//!
//! Builds an immutable segment by accumulating row groups, then writing them
//! out in the order: magic + metadata + RG payloads + inline indexes + hotcache + footer + trailer.
//!
//! Inline indexes are passed in pre-built (e.g. by the compactor, which uses
//! `zen_index` and `zen_fts` to construct posting lists / FTS / etc.).

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher as Crc32;

use zen_common::ZenError;

use crate::footer::Footer;
use crate::hotcache::Hotcache;
use crate::magic::{FORMAT_VERSION, MAGIC_HEADER, MAGIC_TRAILER};
use crate::meta::SegmentMetadata;
use crate::row_group::RowGroupHeader;

/// Builder accumulator for the writer.
pub struct SegmentWriter {
    pub metadata: SegmentMetadata,
    /// Each tuple is (header, payload bytes).
    row_groups: Vec<(RowGroupHeader, Vec<u8>)>,
    /// Inline indexes block (posting lists, FTS, HNSW, JSON-path) supplied by caller.
    inline_indexes: Vec<u8>,
    hotcache: Hotcache,
}

impl SegmentWriter {
    pub fn new(metadata: SegmentMetadata) -> Self {
        Self {
            metadata,
            row_groups: Vec::new(),
            inline_indexes: Vec::new(),
            hotcache: Hotcache::new(),
        }
    }

    pub fn add_row_group(&mut self, header: RowGroupHeader, payload: Vec<u8>) {
        self.row_groups.push((header, payload));
    }

    pub fn set_inline_indexes(&mut self, bytes: Vec<u8>) {
        self.inline_indexes = bytes;
    }

    pub fn set_hotcache(&mut self, hc: Hotcache) {
        self.hotcache = hc;
    }

    /// Finalize the segment to bytes.
    pub fn finish(mut self) -> Result<Bytes, ZenError> {
        // Layout:
        //
        // [0..8)         MAGIC_HEADER
        // [8..12)        format version
        // [12..16)       metadata length (u32)
        // [16..16+ml)    metadata bytes
        // [meta_end..)   row group descriptors length (u32) + bincode-serialized Vec<RowGroupHeader>
        // ...            row group payloads (concatenated)
        // ...            inline indexes
        // ...            hotcache
        // ...            footer length (u32) + bincode footer
        // [last 8 bytes] MAGIC_TRAILER

        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(MAGIC_HEADER);
        buf.put_u32_le(FORMAT_VERSION);

        // Update metadata row count.
        self.metadata.row_count = self
            .row_groups
            .iter()
            .map(|(h, _)| h.row_count as u64)
            .sum();

        let metadata_offset = buf.len() as u64;
        let meta_bytes = bincode::serialize(&self.metadata)
            .map_err(|e| ZenError::format(format!("metadata serialize: {e}")))?;
        buf.put_u32_le(meta_bytes.len() as u32);
        buf.put_slice(&meta_bytes);
        let metadata_end = buf.len();
        let metadata_length = (metadata_end as u64 - metadata_offset) as u32;

        // Adjust descriptors to absolute offsets.
        let row_group_payload_offset = buf.len() as u64
            // we'll write the headers right after, then payloads
            ;

        // Write row-group headers prefix-length first, then the headers.
        let n_rg = self.row_groups.len() as u32;
        let mut rg_headers = Vec::with_capacity(self.row_groups.len());
        // Compute payload positions
        // Headers reference absolute offsets, so first compute total header bytes,
        // then payloads start there.
        let mut hdrs_bytes: Vec<Vec<u8>> = Vec::with_capacity(self.row_groups.len());
        for (header, _) in &self.row_groups {
            // Placeholder — we'll re-encode after offsets are known.
            hdrs_bytes.push(
                bincode::serialize(header)
                    .map_err(|e| ZenError::format(format!("row group header serialize: {e}")))?,
            );
        }
        // Total bytes for `n_rg(u32) + Σ(header_len(u32) + header_bytes)`
        let mut headers_block_len: u64 = 4; // n_rg
        for h in &hdrs_bytes {
            headers_block_len += 4 + h.len() as u64;
        }
        let payloads_start = buf.len() as u64 + headers_block_len;

        // Now adjust each header's offsets to absolute.
        let mut payload_cursor = payloads_start;
        for (header, payload) in &mut self.row_groups {
            for desc in &mut header.columns {
                desc.page_offset += payload_cursor;
            }
            payload_cursor += payload.len() as u64;
            rg_headers.push(header.clone());
        }
        // Re-serialize headers after adjustment.
        hdrs_bytes.clear();
        for h in &rg_headers {
            hdrs_bytes.push(
                bincode::serialize(h)
                    .map_err(|e| ZenError::format(format!("row group header reserialize: {e}")))?,
            );
        }

        // Write headers block.
        buf.put_u32_le(n_rg);
        for h in &hdrs_bytes {
            buf.put_u32_le(h.len() as u32);
            buf.put_slice(h);
        }
        debug_assert_eq!(buf.len() as u64, payloads_start);

        // Write payloads.
        for (_, payload) in &self.row_groups {
            buf.put_slice(payload);
        }
        let row_group_payload_length = (buf.len() as u64) - row_group_payload_offset;

        // Write inline indexes.
        let inline_indexes_offset = buf.len() as u64;
        buf.put_slice(&self.inline_indexes);
        let inline_indexes_length = (buf.len() as u64) - inline_indexes_offset;

        // Write hotcache.
        let hc_bytes = bincode::serialize(&self.hotcache)
            .map_err(|e| ZenError::format(format!("hotcache serialize: {e}")))?;
        let hotcache_offset = buf.len() as u64;
        buf.put_slice(&hc_bytes);
        let hotcache_length = hc_bytes.len() as u64;

        // CRC32 over content (metadata bytes onward, excluding headers/footer/trailer).
        let mut hasher = Crc32::new();
        let content_start = metadata_offset as usize;
        let content_end = buf.len();
        hasher.update(&buf[content_start..content_end]);
        let content_crc32 = hasher.finalize();

        // Footer
        let footer = Footer {
            format_version: FORMAT_VERSION,
            metadata_offset,
            metadata_length,
            row_group_payload_offset,
            row_group_payload_length,
            inline_indexes_offset,
            inline_indexes_length,
            hotcache_offset,
            hotcache_length,
            content_crc32,
        };
        let footer_bytes = bincode::serialize(&footer)
            .map_err(|e| ZenError::format(format!("footer serialize: {e}")))?;
        buf.put_slice(&footer_bytes);
        buf.put_u32_le(footer_bytes.len() as u32);
        buf.put_slice(MAGIC_TRAILER);
        Ok(buf.freeze())
    }
}
