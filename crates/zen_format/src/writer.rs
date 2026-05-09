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

#[cfg(test)]
mod tests {
    use zen_common::{CommitId, PartitionId, SchemaFingerprint, SpanId, TenantId, TraceId};
    use zen_index::ZoneMap;

    use crate::hotcache::{ColumnHotcacheEntry, Hotcache, RowGroupHotcacheEntry};
    use crate::page::{encode_page, ColumnValues, PageEncoding};
    use crate::reader::SegmentReader;
    use crate::row_group::RowGroupBuilder;

    use super::*;

    /// Build a vanilla `SegmentMetadata` for tests; no observations recorded.
    fn make_meta() -> SegmentMetadata {
        SegmentMetadata::new(
            1,
            TenantId(1),
            PartitionId(0),
            SchemaFingerprint(0xABCD),
            vec!["start_time_ms".into(), "model".into()],
            vec!["start_time_ms".into()],
        )
    }

    /// Build one trivial row group: column 0 is i64, column 1 is dict-encoded strings.
    fn make_row_group(rows: &[(i64, &[u8])]) -> (RowGroupHeader, Vec<u8>) {
        let mut rgb = RowGroupBuilder::new(rows.len() as u32);
        let times: Vec<i64> = rows.iter().map(|(t, _)| *t).collect();
        let (e, b) = encode_page(ColumnValues::I64(times), PageEncoding::For).expect("For encode");
        rgb.add_page(0, e, b.to_vec(), 8 * rows.len() as u64);
        let models: Vec<Vec<u8>> = rows.iter().map(|(_, m)| m.to_vec()).collect();
        let (e, b) = encode_page(ColumnValues::StringsOwned(models), PageEncoding::Dict)
            .expect("Dict encode");
        rgb.add_page(1, e, b.to_vec(), 32);
        let (payload, header) = rgb.finish();
        (header, payload)
    }

    /// `SegmentWriter::new` + `add_row_group` + `finish` produces bytes the reader
    /// opens with the expected row count.
    #[test]
    fn writer_finish_opens_in_reader() {
        let mut writer = SegmentWriter::new(make_meta());
        let (header, payload) = make_row_group(&[(1000, b"gpt-4o"), (2000, b"haiku")]);
        writer.add_row_group(header, payload);
        let bytes = writer.finish().expect("finish").to_vec();
        let r = SegmentReader::from_bytes(bytes).expect("reader open");
        assert_eq!(r.row_group_count(), 1);
        assert_eq!(r.metadata.row_count, 2);
    }

    /// An empty writer (no row groups) finishes successfully and the reader sees zero RGs.
    #[test]
    fn writer_empty_segment_finishes() {
        let writer = SegmentWriter::new(make_meta());
        let bytes = writer.finish().expect("finish empty").to_vec();
        let r = SegmentReader::from_bytes(bytes).expect("reader open");
        assert_eq!(r.row_group_count(), 0);
        assert_eq!(r.metadata.row_count, 0);
    }

    /// Writing 3 row groups makes all of them addressable via `row_group(0..N)`.
    #[test]
    fn writer_multiple_row_groups_reachable() {
        let mut writer = SegmentWriter::new(make_meta());
        for batch in [
            &[(100i64, &b"a"[..]), (101, &b"b"[..])][..],
            &[(200, &b"c"[..])][..],
            &[(300, &b"d"[..]), (301, &b"e"[..]), (302, &b"f"[..])][..],
        ] {
            let (h, p) = make_row_group(batch);
            writer.add_row_group(h, p);
        }
        let bytes = writer.finish().expect("finish").to_vec();
        let r = SegmentReader::from_bytes(bytes).expect("open");
        assert_eq!(r.row_group_count(), 3);
        for i in 0..3 {
            let rg = r.row_group(i).expect("row_group lookup");
            assert!(rg.header.row_count >= 1);
        }
        // Total rows match the sum.
        assert_eq!(r.metadata.row_count, 2 + 1 + 3);
    }

    /// `set_inline_indexes` survives roundtripping through the writer/reader:
    /// the bytes appear at the offset/length recorded in the footer.
    #[test]
    fn writer_inline_indexes_roundtrip() {
        let mut writer = SegmentWriter::new(make_meta());
        let (h, p) = make_row_group(&[(1, b"x")]);
        writer.add_row_group(h, p);
        let blob = b"\xDE\xAD\xBE\xEF--inline-indexes-blob--".to_vec();
        writer.set_inline_indexes(blob.clone());
        let bytes = writer.finish().expect("finish").to_vec();
        let r = SegmentReader::from_bytes(bytes).expect("open");
        let off = r.footer.inline_indexes_offset as usize;
        let len = r.footer.inline_indexes_length as usize;
        assert_eq!(len, blob.len(), "inline_indexes length mismatch");
        assert_eq!(&r.bytes[off..off + len], blob.as_slice());
    }

    /// A non-default `Hotcache` set on the writer is readable via the reader's
    /// `hotcache` field.
    #[test]
    fn writer_hotcache_is_readable() {
        let mut writer = SegmentWriter::new(make_meta());
        let (h, p) = make_row_group(&[(1, b"y")]);
        writer.add_row_group(h.clone(), p);
        let entry = ColumnHotcacheEntry {
            column_idx: 0,
            zone_map: ZoneMap::from_i64(&[1, 2, 3], 0),
            posting_offset: Some(42),
            posting_length: Some(7),
            fts_offset: None,
            fts_length: None,
            jsonpath_offset: None,
            jsonpath_length: None,
            hnsw_offset: None,
            hnsw_length: None,
        };
        let mut hc = Hotcache::new();
        hc.row_groups.push(RowGroupHotcacheEntry {
            row_group_idx: 0,
            header: h,
            columns: vec![entry.clone()],
        });
        writer.set_hotcache(hc);
        let bytes = writer.finish().expect("finish").to_vec();
        let r = SegmentReader::from_bytes(bytes).expect("open");
        assert_eq!(r.hotcache.row_groups.len(), 1);
        assert_eq!(r.hotcache.row_groups[0].columns[0], entry);
    }

    /// Metadata observations recorded before `finish` survive into the reader's
    /// `SegmentMetadata`.
    #[test]
    fn writer_metadata_observations_preserved() {
        let mut meta = make_meta();
        meta.observe_time(50);
        meta.observe_time(2000);
        meta.observe_commit(CommitId(3));
        meta.observe_commit(CommitId(99));
        meta.observe_trace_id(TraceId([0x10; 16]));
        meta.observe_trace_id(TraceId([0x80; 16]));
        meta.observe_span_id(SpanId([0x00; 16]));
        meta.observe_span_id(SpanId([0xFF; 16]));
        let mut writer = SegmentWriter::new(meta);
        let (h, p) = make_row_group(&[(60, b"a"), (1900, b"b")]);
        writer.add_row_group(h, p);
        let bytes = writer.finish().expect("finish").to_vec();
        let r = SegmentReader::from_bytes(bytes).expect("open");
        assert_eq!(r.metadata.time_min_ms, 50);
        assert_eq!(r.metadata.time_max_ms, 2000);
        assert_eq!(r.metadata.commit_id_min, CommitId(3));
        assert_eq!(r.metadata.commit_id_max, CommitId(99));
        assert_eq!(r.metadata.trace_id_min, TraceId([0x10; 16]));
        assert_eq!(r.metadata.trace_id_max, TraceId([0x80; 16]));
        assert_eq!(r.metadata.span_id_min, SpanId([0x00; 16]));
        assert_eq!(r.metadata.span_id_max, SpanId([0xFF; 16]));
    }

    /// The writer's footer CRC32 is non-zero and the same content produced
    /// twice yields identical bytes (deterministic emit).
    #[test]
    fn writer_finish_is_deterministic() {
        let make = || {
            let mut w = SegmentWriter::new(make_meta());
            let (h, p) = make_row_group(&[(10, b"q"), (20, b"r")]);
            w.add_row_group(h, p);
            w.finish().expect("finish").to_vec()
        };
        let a = make();
        let b = make();
        assert_eq!(a, b, "writer must emit deterministic bytes");
        let r = SegmentReader::from_bytes(a).expect("open");
        assert!(
            r.footer.content_crc32 != 0,
            "expected a non-zero content CRC32"
        );
    }
}
