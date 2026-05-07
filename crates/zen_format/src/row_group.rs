//! Row groups within a segment.
//!
//! A row group is a horizontal slice of the segment with one page per column,
//! laid out in PAX (Partition Attributes Across) order. The on-disk layout is:
//!
//! ```text
//! RG header bytes (bincode):
//!   row_count: u32
//!   total_bytes: u64
//!   per-column descriptors: (encoding, page_offset, page_length)
//! Page payloads: each column's page (contiguous within the row group)
//! ```
//!
//! The header is written to the segment's "row group descriptors" inline area.
//! Each page payload is at an absolute offset in the segment.

use serde::{Deserialize, Serialize};

use zen_common::ZenError;

use crate::page::PageEncoding;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ColumnPageDescriptor {
    /// Index into `SegmentMetadata::column_names`.
    pub column_idx: u32,
    pub encoding: u8,
    /// Absolute offset within the segment file.
    pub page_offset: u64,
    pub page_length: u32,
    /// Approximate uncompressed byte count.
    pub uncompressed_size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowGroupHeader {
    pub row_count: u32,
    pub total_bytes: u64,
    pub columns: Vec<ColumnPageDescriptor>,
}

impl RowGroupHeader {
    pub fn new(row_count: u32) -> Self {
        Self {
            row_count,
            total_bytes: 0,
            columns: Vec::new(),
        }
    }

    pub fn descriptor_for_column(&self, column_idx: u32) -> Option<&ColumnPageDescriptor> {
        self.columns.iter().find(|d| d.column_idx == column_idx)
    }
}

/// In-memory builder for a single row group. Collects page bytes and tracks
/// offsets relative to the row-group start; the segment writer offsets these
/// into absolute positions when writing.
pub struct RowGroupBuilder {
    pub row_count: u32,
    pub buffers: Vec<(u32, PageEncoding, Vec<u8>, u64)>, // (column_idx, enc, bytes, uncompressed)
}

impl RowGroupBuilder {
    pub fn new(row_count: u32) -> Self {
        Self {
            row_count,
            buffers: Vec::new(),
        }
    }

    pub fn add_page(
        &mut self,
        column_idx: u32,
        encoding: PageEncoding,
        bytes: Vec<u8>,
        uncompressed_size: u64,
    ) {
        self.buffers.push((column_idx, encoding, bytes, uncompressed_size));
    }

    /// Finalize: returns (concatenated page payloads, header).
    /// Offsets in the header are *relative* to the start of the row-group's
    /// payload region. The segment writer adjusts them to absolute offsets.
    pub fn finish(self) -> (Vec<u8>, RowGroupHeader) {
        let mut payload: Vec<u8> = Vec::new();
        let mut columns: Vec<ColumnPageDescriptor> = Vec::with_capacity(self.buffers.len());
        for (col_idx, enc, bytes, uncomp) in self.buffers {
            let off = payload.len() as u64;
            let len = bytes.len() as u32;
            payload.extend_from_slice(&bytes);
            columns.push(ColumnPageDescriptor {
                column_idx: col_idx,
                encoding: enc.as_u8(),
                page_offset: off, // relative; adjusted by segment writer
                page_length: len,
                uncompressed_size: uncomp,
            });
        }
        let header = RowGroupHeader {
            row_count: self.row_count,
            total_bytes: payload.len() as u64,
            columns,
        };
        (payload, header)
    }
}

/// Reader for a row group given its header and a `Bytes`-like accessor for
/// the row-group's payload region.
pub struct RowGroupReader {
    pub header: RowGroupHeader,
}

impl RowGroupReader {
    pub fn new(header: RowGroupHeader) -> Self {
        Self { header }
    }

    /// Look up a column page descriptor by column index.
    pub fn column_descriptor(&self, column_idx: u32) -> Result<&ColumnPageDescriptor, ZenError> {
        self.header
            .descriptor_for_column(column_idx)
            .ok_or_else(|| ZenError::format(format!("column {column_idx} not in row group")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_finish_offsets_relative() {
        let mut b = RowGroupBuilder::new(100);
        b.add_page(0, PageEncoding::For, vec![0u8; 32], 800);
        b.add_page(1, PageEncoding::Dict, vec![0u8; 64], 1600);
        b.add_page(2, PageEncoding::FsstWithOffsets, vec![0u8; 128], 4096);

        let (payload, header) = b.finish();
        assert_eq!(payload.len(), 32 + 64 + 128);
        assert_eq!(header.row_count, 100);
        assert_eq!(header.columns[0].page_offset, 0);
        assert_eq!(header.columns[1].page_offset, 32);
        assert_eq!(header.columns[2].page_offset, 32 + 64);
    }

    #[test]
    fn descriptor_lookup() {
        let mut b = RowGroupBuilder::new(10);
        b.add_page(7, PageEncoding::For, vec![0u8; 8], 80);
        let (_, header) = b.finish();
        assert!(header.descriptor_for_column(7).is_some());
        assert!(header.descriptor_for_column(8).is_none());
    }
}
