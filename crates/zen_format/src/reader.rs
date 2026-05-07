//! Segment reader.
//!
//! In production this drives async ranged-GETs against object storage. Here
//! we expose a synchronous in-memory reader and a `from_bytes` constructor;
//! the storage layer composes this with `object_store` for real cold reads.

use bytes::Buf;

use zen_common::ZenError;

use crate::footer::Footer;
use crate::hotcache::Hotcache;
use crate::magic::{FORMAT_VERSION, MAGIC_HEADER, MAGIC_TRAILER};
use crate::meta::SegmentMetadata;
use crate::page::{decode_one_row, decode_page, ColumnValues, PageEncoding, PageView, RowValue};
use crate::row_group::{RowGroupHeader, RowGroupReader};

pub struct SegmentReader {
    pub bytes: Vec<u8>,
    pub footer: Footer,
    pub metadata: SegmentMetadata,
    pub row_groups: Vec<RowGroupHeader>,
    pub hotcache: Hotcache,
}

impl SegmentReader {
    /// Open from a fully-loaded byte buffer.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ZenError> {
        if bytes.len() < MAGIC_HEADER.len() + MAGIC_TRAILER.len() + 4 + 8 {
            return Err(ZenError::format("segment too short"));
        }
        if &bytes[..MAGIC_HEADER.len()] != MAGIC_HEADER {
            return Err(ZenError::format("bad magic header"));
        }
        if &bytes[bytes.len() - MAGIC_TRAILER.len()..] != MAGIC_TRAILER {
            return Err(ZenError::format("bad magic trailer"));
        }

        // Footer length is right before the trailer (u32 le).
        let trailer_off = bytes.len() - MAGIC_TRAILER.len();
        let footer_len_off = trailer_off - 4;
        let footer_len = u32::from_le_bytes(bytes[footer_len_off..trailer_off].try_into().unwrap())
            as usize;
        if footer_len_off < footer_len {
            return Err(ZenError::format("footer length too large"));
        }
        let footer_bytes = &bytes[(footer_len_off - footer_len)..footer_len_off];
        let footer: Footer = bincode::deserialize(footer_bytes)
            .map_err(|e| ZenError::format(format!("footer deserialize: {e}")))?;
        if footer.format_version != FORMAT_VERSION {
            return Err(ZenError::format(format!(
                "format version mismatch: got {} expected {}",
                footer.format_version, FORMAT_VERSION
            )));
        }

        // Metadata
        let m_start = footer.metadata_offset as usize;
        if bytes.len() < m_start + 4 {
            return Err(ZenError::format("metadata length truncated"));
        }
        let m_len_bytes = &bytes[m_start..m_start + 4];
        let m_len =
            u32::from_le_bytes(m_len_bytes.try_into().unwrap()) as usize;
        let metadata: SegmentMetadata = bincode::deserialize(&bytes[m_start + 4..m_start + 4 + m_len])
            .map_err(|e| ZenError::format(format!("metadata deserialize: {e}")))?;

        // Row group headers
        let mut p = footer.row_group_payload_offset as usize;
        if bytes.len() < p + 4 {
            return Err(ZenError::format("rg headers count truncated"));
        }
        let n_rg = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        let mut row_groups = Vec::with_capacity(n_rg);
        for _ in 0..n_rg {
            if bytes.len() < p + 4 {
                return Err(ZenError::format("rg header length truncated"));
            }
            let l = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            if bytes.len() < p + l {
                return Err(ZenError::format("rg header body truncated"));
            }
            let h: RowGroupHeader = bincode::deserialize(&bytes[p..p + l])
                .map_err(|e| ZenError::format(format!("rg header deserialize: {e}")))?;
            p += l;
            row_groups.push(h);
        }

        // Hotcache
        let hc_off = footer.hotcache_offset as usize;
        let hc_end = hc_off + footer.hotcache_length as usize;
        let hotcache: Hotcache = if footer.hotcache_length > 0 {
            bincode::deserialize(&bytes[hc_off..hc_end])
                .map_err(|e| ZenError::format(format!("hotcache deserialize: {e}")))?
        } else {
            Hotcache::default()
        };

        Ok(Self {
            bytes,
            footer,
            metadata,
            row_groups,
            hotcache,
        })
    }

    pub fn row_group_count(&self) -> usize {
        self.row_groups.len()
    }

    pub fn row_group(&self, idx: usize) -> Result<RowGroupReader, ZenError> {
        let h = self
            .row_groups
            .get(idx)
            .ok_or_else(|| ZenError::format(format!("row group {idx} out of range")))?;
        Ok(RowGroupReader::new(h.clone()))
    }

    /// Slice a column's page bytes from the segment buffer.
    pub fn column_page_bytes(&self, rg_idx: usize, column_idx: u32) -> Result<&[u8], ZenError> {
        let rgh = self
            .row_groups
            .get(rg_idx)
            .ok_or_else(|| ZenError::format(format!("row group {rg_idx} out of range")))?;
        let desc = rgh
            .descriptor_for_column(column_idx)
            .ok_or_else(|| ZenError::format(format!("column {column_idx} not in rg {rg_idx}")))?;
        let off = desc.page_offset as usize;
        let len = desc.page_length as usize;
        if self.bytes.len() < off + len {
            return Err(ZenError::format("column page bytes out of range"));
        }
        Ok(&self.bytes[off..off + len])
    }

    pub fn column_encoding(&self, rg_idx: usize, column_idx: u32) -> Result<PageEncoding, ZenError> {
        let rgh = self
            .row_groups
            .get(rg_idx)
            .ok_or_else(|| ZenError::format(format!("row group {rg_idx} out of range")))?;
        let desc = rgh
            .descriptor_for_column(column_idx)
            .ok_or_else(|| ZenError::format(format!("column {column_idx} not in rg {rg_idx}")))?;
        PageEncoding::try_from_u8(desc.encoding)
    }

    /// Decode a full column page in a row group.
    pub fn read_column(&self, rg_idx: usize, column_idx: u32) -> Result<ColumnValues<'static>, ZenError> {
        let bytes = self.column_page_bytes(rg_idx, column_idx)?;
        let enc = self.column_encoding(rg_idx, column_idx)?;
        decode_page(enc, bytes)
    }

    /// Decode a single row from a column page (late materialization).
    pub fn read_row(
        &self,
        rg_idx: usize,
        column_idx: u32,
        row_in_rg: usize,
    ) -> Result<RowValue, ZenError> {
        let bytes = self.column_page_bytes(rg_idx, column_idx)?;
        let enc = self.column_encoding(rg_idx, column_idx)?;
        decode_one_row(enc, bytes, row_in_rg)
    }

    /// Open a page view for a (row-group, column). The view amortizes
    /// per-page setup; call `row(i)` repeatedly to decode many rows cheaply.
    pub fn open_page<'a>(&'a self, rg_idx: usize, column_idx: u32) -> Result<PageView<'a>, ZenError> {
        let bytes = self.column_page_bytes(rg_idx, column_idx)?;
        let enc = self.column_encoding(rg_idx, column_idx)?;
        PageView::open(enc, bytes)
    }

    /// Late-materialize a set of rows from one column. Opens the page once
    /// and decodes only the requested rows.
    pub fn read_rows(
        &self,
        rg_idx: usize,
        column_idx: u32,
        rows: &[usize],
    ) -> Result<Vec<RowValue>, ZenError> {
        let view = self.open_page(rg_idx, column_idx)?;
        let mut out = Vec::with_capacity(rows.len());
        for &i in rows {
            out.push(view.row(i)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use zen_common::{CommitId, PartitionId, SchemaFingerprint, SpanId, TenantId, TraceId};

    use crate::page::{encode_page, ColumnValues, PageEncoding};
    use crate::row_group::RowGroupBuilder;
    use crate::writer::SegmentWriter;

    use super::*;

    fn build_simple_segment() -> Vec<u8> {
        let fp = SchemaFingerprint(0x1234);
        let mut meta = SegmentMetadata::new(
            1,
            TenantId(7),
            PartitionId(0),
            fp,
            vec!["trace_id".into(), "start_time_ms".into(), "model".into(), "prompt".into()],
            vec!["trace_id".into(), "start_time_ms".into()],
        );
        meta.observe_time(1000);
        meta.observe_time(2000);
        meta.observe_commit(CommitId(1));
        meta.observe_commit(CommitId(99));
        meta.observe_trace_id(TraceId([0x10; 16]));
        meta.observe_trace_id(TraceId([0x20; 16]));
        meta.observe_span_id(SpanId([0x00; 16]));
        meta.observe_span_id(SpanId([0xFF; 16]));

        let mut writer = SegmentWriter::new(meta);

        let mut rgb = RowGroupBuilder::new(3);
        // Column 0: trace_ids (Fixed16)
        let trace_ids: Vec<[u8; 16]> = vec![[0x10; 16], [0x15; 16], [0x20; 16]];
        let (e, b) =
            encode_page(ColumnValues::Fixed16(trace_ids), PageEncoding::FixedRaw).unwrap();
        rgb.add_page(0, e, b.to_vec(), 48);
        // Column 1: start_time_ms (For)
        let times: Vec<i64> = vec![1000, 1500, 2000];
        let (e, b) = encode_page(ColumnValues::I64(times), PageEncoding::For).unwrap();
        rgb.add_page(1, e, b.to_vec(), 24);
        // Column 2: model (Dict)
        let models: Vec<Vec<u8>> = vec![b"gpt-4o".to_vec(), b"haiku".to_vec(), b"gpt-4o".to_vec()];
        let (e, b) =
            encode_page(ColumnValues::StringsOwned(models), PageEncoding::Dict).unwrap();
        rgb.add_page(2, e, b.to_vec(), 30);
        // Column 3: prompt (FsstWithOffsets)
        let prompts: Vec<Vec<u8>> = vec![
            b"the quick brown fox".to_vec(),
            b"out of memory at allocator".to_vec(),
            b"summarize the previous conversation".to_vec(),
        ];
        let (e, b) = encode_page(
            ColumnValues::StringsOwned(prompts),
            PageEncoding::FsstWithOffsets,
        )
        .unwrap();
        rgb.add_page(3, e, b.to_vec(), 200);

        let (payload, header) = rgb.finish();
        writer.add_row_group(header, payload);
        writer.finish().unwrap().to_vec()
    }

    #[test]
    fn write_then_read_segment() {
        let bytes = build_simple_segment();
        let r = SegmentReader::from_bytes(bytes).unwrap();
        assert_eq!(r.row_group_count(), 1);
        assert_eq!(r.metadata.tenant_id.0, 7);
        assert_eq!(r.metadata.row_count, 3);
        assert_eq!(r.row_groups[0].row_count, 3);
    }

    #[test]
    fn read_column_full_page() {
        let bytes = build_simple_segment();
        let r = SegmentReader::from_bytes(bytes).unwrap();

        let trace_ids = r.read_column(0, 0).unwrap();
        match trace_ids {
            ColumnValues::Fixed16(v) => {
                assert_eq!(v.len(), 3);
                assert_eq!(v[0], [0x10; 16]);
                assert_eq!(v[2], [0x20; 16]);
            }
            _ => panic!(),
        }

        let times = r.read_column(0, 1).unwrap();
        match times {
            ColumnValues::I64(v) => assert_eq!(v, vec![1000, 1500, 2000]),
            _ => panic!(),
        }

        let prompts = r.read_column(0, 3).unwrap();
        match prompts {
            ColumnValues::StringsOwned(v) => {
                assert_eq!(v[0], b"the quick brown fox");
                assert_eq!(v[2], b"summarize the previous conversation");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn late_materialize_one_row() {
        // The big win: decode prompt at row 1 alone.
        let bytes = build_simple_segment();
        let r = SegmentReader::from_bytes(bytes).unwrap();

        let row = r.read_row(0, 3, 1).unwrap();
        match row {
            RowValue::Bytes(b) => assert_eq!(b, b"out of memory at allocator"),
            _ => panic!(),
        }
        let row = r.read_row(0, 1, 2).unwrap();
        match row {
            RowValue::I64(t) => assert_eq!(t, 2000),
            _ => panic!(),
        }
    }

    #[test]
    fn rejects_corrupted_magic() {
        let mut bytes = build_simple_segment();
        bytes[0] = 0;
        assert!(SegmentReader::from_bytes(bytes).is_err());
    }

    #[test]
    fn round_trip_byte_identity() {
        // Two writes of the same logical content should produce identical bytes.
        // (Hashmap iteration in PostingMap could perturb this — segments don't include
        // posting lists in this minimal test, so it's deterministic.)
        let a = build_simple_segment();
        let b = build_simple_segment();
        assert_eq!(a, b);
    }
}
