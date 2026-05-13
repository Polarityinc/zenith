//! FSST (Fast Static Symbol Table) compression for wide string columns.
//!
//! At compaction time, we train a per-page symbol table on a sample of the data,
//! then compress every row through the compressor. Each row is stored
//! independently — the page format includes a per-row offset directory so we
//! can decode row N alone in O(symbol-table-lookup * |row|) without touching
//! other rows. This is the foundation of late materialization.
//!
//! Layout of an FSST-compressed page payload (excluding the page header that
//! `zen_format` writes around it):
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │ symbol_count: u8                                         │
//! │ symbols:    Symbol[symbol_count]      (8 bytes each)     │
//! │ lengths:    u8[symbol_count]                             │
//! │ offsets:    u32[row_count + 1]   into compressed_payload │
//! │ compressed_payload: bytes                                │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! The symbol table is small (≤ 255 × 9 bytes ≈ 2.3 KB), so we ship it inline
//! per page. Pages are independently decodable.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fsst::{Compressor, Symbol};

use zen_common::ZenError;

#[derive(Debug)]
pub struct FsstHeader {
    pub row_count: u32,
    pub symbol_count: u8,
}

/// Owned FSST encoder/decoder for a column page.
pub struct FsstCompressor {
    pub compressor: Compressor,
}

impl FsstCompressor {
    /// Train a symbol table on a sample of input rows.
    pub fn train(samples: &[&[u8]]) -> Self {
        let v: Vec<&[u8]> = samples.to_vec();
        let compressor = Compressor::train(&v);
        Self { compressor }
    }

    /// Compress a column page given a slice of `(row_bytes)`. Empty rows compress to a
    /// 0-length region in the payload.
    ///
    /// Returns the encoded page (symbol table + offsets + compressed payload).
    pub fn encode_page(&self, rows: &[&[u8]]) -> Bytes {
        let mut payload: Vec<u8> = Vec::with_capacity(rows.iter().map(|r| r.len()).sum::<usize>());
        let mut offsets: Vec<u32> = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for r in rows {
            if !r.is_empty() {
                let mut buf = self.compressor.compress(r);
                payload.append(&mut buf);
            }
            offsets.push(payload.len() as u32);
        }

        let symbols = self.compressor.symbol_table();
        let lengths = self.compressor.symbol_lengths();
        let symbol_count = symbols.len() as u8;

        let mut out = BytesMut::with_capacity(
            1 + symbols.len() * 8 + lengths.len() + offsets.len() * 4 + payload.len(),
        );
        out.put_u8(symbol_count);
        for s in symbols {
            out.put_u64_le(s.to_u64());
        }
        out.put_slice(lengths);
        out.put_u32_le(rows.len() as u32);
        for o in &offsets {
            out.put_u32_le(*o);
        }
        out.put_slice(&payload);
        out.freeze()
    }

    /// Decode the symbol-table portion of a page payload, returning a (compressor, header,
    /// offsets, payload-start-offset).
    pub fn open(page: &[u8]) -> Result<DecodedFsst<'_>, ZenError> {
        if page.is_empty() {
            return Err(ZenError::format("empty FSST page"));
        }
        let mut p = page;
        let symbol_count = p.get_u8();
        if p.remaining() < symbol_count as usize * 9 + 4 {
            return Err(ZenError::format("FSST page truncated in symbol table"));
        }
        let mut symbols = Vec::with_capacity(symbol_count as usize);
        for _ in 0..symbol_count {
            symbols.push(Symbol::from_slice(&p[..8].try_into().unwrap()));
            p.advance(8);
        }
        let mut lengths = Vec::with_capacity(symbol_count as usize);
        for _ in 0..symbol_count {
            lengths.push(p.get_u8());
        }
        let row_count = p.get_u32_le();
        if p.remaining() < (row_count as usize + 1) * 4 {
            return Err(ZenError::format("FSST page truncated in offsets"));
        }
        let mut offsets = Vec::with_capacity(row_count as usize + 1);
        for _ in 0..=row_count {
            offsets.push(p.get_u32_le());
        }
        // p now points at compressed payload
        let payload = p;
        let compressor = Compressor::rebuild_from(symbols, lengths);
        Ok(DecodedFsst {
            compressor,
            header: FsstHeader {
                row_count,
                symbol_count,
            },
            offsets,
            payload,
        })
    }
}

/// Decoded view of an FSST page. Holds the rebuilt compressor, the offsets, and
/// the raw compressed payload. `decode_row` decodes a single row.
pub struct DecodedFsst<'a> {
    pub compressor: Compressor,
    pub header: FsstHeader,
    pub offsets: Vec<u32>,
    pub payload: &'a [u8],
}

impl<'a> DecodedFsst<'a> {
    pub fn row_count(&self) -> usize {
        self.header.row_count as usize
    }

    /// Decode row `idx` into a fresh `Vec<u8>`.
    pub fn decode_row(&self, idx: usize) -> Result<Vec<u8>, ZenError> {
        if idx >= self.row_count() {
            return Err(ZenError::format(format!(
                "row index {idx} >= row count {}",
                self.row_count()
            )));
        }
        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;
        if start == end {
            return Ok(Vec::new());
        }
        if end > self.payload.len() {
            return Err(ZenError::format(
                "FSST row offset out of bounds for payload",
            ));
        }
        let codes = &self.payload[start..end];
        let dec = self.compressor.decompressor();
        Ok(dec.decompress(codes))
    }

    /// Decode a contiguous range `[start..end)` of rows.
    pub fn decode_range(&self, start: usize, end: usize) -> Result<Vec<Vec<u8>>, ZenError> {
        let end = end.min(self.row_count());
        if start >= end {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(end - start);
        for i in start..end {
            out.push(self.decode_row(i)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_corpus() -> Vec<&'static [u8]> {
        vec![
            b"the quick brown fox jumps over the lazy dog".as_slice(),
            b"out of memory error in compaction worker".as_slice(),
            b"rate limit exceeded for tier free; please upgrade".as_slice(),
            b"finished tool call get_user_orders successfully".as_slice(),
            b"the model returned an unexpected response shape".as_slice(),
            b"pre-flight checks passed; entering main loop".as_slice(),
            b"the quick brown fox jumps over the lazy fox".as_slice(),
            b"out of memory while allocating a 16MB chunk".as_slice(),
            b"out of memory error in retrieval cache".as_slice(),
            b"finished tool call get_user_orders with 0 rows".as_slice(),
        ]
    }

    #[test]
    fn roundtrip_small_corpus() {
        let rows = sample_corpus();
        let comp = FsstCompressor::train(&rows);
        let page = comp.encode_page(&rows);
        let view = FsstCompressor::open(&page).unwrap();
        assert_eq!(view.row_count(), rows.len());
        for (i, expected) in rows.iter().enumerate() {
            assert_eq!(view.decode_row(i).unwrap(), *expected);
        }
    }

    #[test]
    fn empty_rows_roundtrip() {
        let rows: Vec<&[u8]> = vec![b"hello", b"", b"world", b"", b""];
        let comp = FsstCompressor::train(&rows);
        let page = comp.encode_page(&rows);
        let view = FsstCompressor::open(&page).unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(view.decode_row(i).unwrap(), *r);
        }
    }

    #[test]
    fn compresses_repetitive_text() {
        // FSST should beat raw on repetitive English-like text.
        let mut rows: Vec<&[u8]> = Vec::new();
        for _ in 0..200 {
            rows.extend(sample_corpus());
        }
        let raw_bytes: usize = rows.iter().map(|r| r.len()).sum();
        let comp = FsstCompressor::train(&rows);
        let page = comp.encode_page(&rows);
        let ratio = raw_bytes as f64 / page.len() as f64;
        // 1.5x or better on this corpus is realistic for pure FSST.
        assert!(
            ratio > 1.5,
            "FSST compression ratio {:.2}x not > 1.5x",
            ratio
        );
    }

    #[test]
    fn random_range_decode() {
        let rows = sample_corpus();
        let comp = FsstCompressor::train(&rows);
        let page = comp.encode_page(&rows);
        let view = FsstCompressor::open(&page).unwrap();
        let r = view.decode_range(2, 5).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0], rows[2]);
        assert_eq!(r[1], rows[3]);
        assert_eq!(r[2], rows[4]);
    }
}
