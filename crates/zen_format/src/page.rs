//! Per-column pages within a row group.
//!
//! A page is the smallest independently decodable unit. For the wide string
//! columns we ship a per-row offset directory inside the page, so the executor
//! can decode row N alone in microseconds without touching other rows.
//!
//! Encodings cover every column type listed in `Schema::spans_v1`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use zen_common::ZenError;
use zen_compress::{
    for_decompress, for_encode, gorilla_decompress, gorilla_encode, rle_decompress, rle_encode,
    zstd_compress, zstd_decompress, DictBuilder, DictDecoder, FsstCompressor,
};

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum PageEncoding {
    Raw = 0,
    /// Used for `prompt`, `completion`, `tool_io_text`. Ships symbol table + per-row offsets.
    FsstWithOffsets = 1,
    /// ZSTD-wrapped binary blob (e.g. `metadata` JSON).
    Zstd = 2,
    /// Gorilla XOR for f64 streams.
    Gorilla = 3,
    /// Frame-of-Reference + bit-pack for i64.
    For = 4,
    /// RLE for high-repetition i64.
    Rle = 5,
    /// Dictionary keys + LZ4 wrap, for low-cardinality strings.
    Dict = 6,
    /// Fixed-width binary (e.g. TraceId/SpanId) — concatenated raw.
    FixedRaw = 7,
}

impl PageEncoding {
    pub fn try_from_u8(x: u8) -> Result<Self, ZenError> {
        Ok(match x {
            0 => Self::Raw,
            1 => Self::FsstWithOffsets,
            2 => Self::Zstd,
            3 => Self::Gorilla,
            4 => Self::For,
            5 => Self::Rle,
            6 => Self::Dict,
            7 => Self::FixedRaw,
            other => return Err(ZenError::format(format!("unknown page encoding {other}"))),
        })
    }
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Untyped column buffer used to hand data into / out of the page codec.
#[derive(Clone, Debug)]
pub enum ColumnValues<'a> {
    /// One byte slice per row (variable length).
    Strings(Vec<&'a [u8]>),
    /// Owned variant returned from decoders.
    StringsOwned(Vec<Vec<u8>>),
    /// i64 column.
    I64(Vec<i64>),
    /// u32 column.
    U32(Vec<u32>),
    /// f64 column.
    F64(Vec<f64>),
    /// Fixed 16-byte column (TraceId/SpanId).
    Fixed16(Vec<[u8; 16]>),
    /// Raw bytes column (variable length).
    Bytes(Vec<&'a [u8]>),
    /// Owned bytes column.
    BytesOwned(Vec<Vec<u8>>),
}

impl<'a> ColumnValues<'a> {
    pub fn row_count(&self) -> usize {
        match self {
            Self::Strings(v) => v.len(),
            Self::StringsOwned(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::U32(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::Fixed16(v) => v.len(),
            Self::Bytes(v) => v.len(),
            Self::BytesOwned(v) => v.len(),
        }
    }
}

/// Encode a column page. Returns the encoded bytes plus the actual encoding
/// used. The caller passes a hint, but `encode_page` may downgrade to a more
/// general encoding if the data doesn't fit the hint.
pub fn encode_page(
    values: ColumnValues<'_>,
    hint: PageEncoding,
) -> Result<(PageEncoding, Bytes), ZenError> {
    match (hint, values) {
        (PageEncoding::FsstWithOffsets, ColumnValues::Strings(v)) => {
            let comp = FsstCompressor::train(&v);
            Ok((PageEncoding::FsstWithOffsets, comp.encode_page(&v)))
        }
        (PageEncoding::FsstWithOffsets, ColumnValues::StringsOwned(v)) => {
            let refs: Vec<&[u8]> = v.iter().map(|s| s.as_slice()).collect();
            let comp = FsstCompressor::train(&refs);
            Ok((PageEncoding::FsstWithOffsets, comp.encode_page(&refs)))
        }
        (PageEncoding::Dict, ColumnValues::Strings(v)) => {
            let mut b = DictBuilder::new();
            for s in &v {
                b.push(s);
            }
            Ok((PageEncoding::Dict, b.finish()?))
        }
        (PageEncoding::Dict, ColumnValues::StringsOwned(v)) => {
            let mut b = DictBuilder::new();
            for s in &v {
                b.push(s);
            }
            Ok((PageEncoding::Dict, b.finish()?))
        }
        (PageEncoding::For, ColumnValues::I64(v)) => Ok((PageEncoding::For, for_encode(&v))),
        (PageEncoding::Rle, ColumnValues::I64(v)) => Ok((PageEncoding::Rle, rle_encode(&v))),
        (PageEncoding::Gorilla, ColumnValues::F64(v)) => {
            Ok((PageEncoding::Gorilla, gorilla_encode(&v)?))
        }
        (PageEncoding::Zstd, ColumnValues::Bytes(v)) => {
            let mut buf = BytesMut::new();
            buf.put_u32_le(v.len() as u32);
            for b in &v {
                buf.put_u32_le(b.len() as u32);
                buf.put_slice(b);
            }
            Ok((PageEncoding::Zstd, zstd_compress(&buf, 3)?))
        }
        (PageEncoding::Zstd, ColumnValues::BytesOwned(v)) => {
            let mut buf = BytesMut::new();
            buf.put_u32_le(v.len() as u32);
            for b in &v {
                buf.put_u32_le(b.len() as u32);
                buf.put_slice(b);
            }
            Ok((PageEncoding::Zstd, zstd_compress(&buf, 3)?))
        }
        (PageEncoding::FixedRaw, ColumnValues::Fixed16(v)) => {
            let mut out = BytesMut::with_capacity(4 + v.len() * 16);
            out.put_u32_le(v.len() as u32);
            for fx in &v {
                out.put_slice(fx);
            }
            Ok((PageEncoding::FixedRaw, out.freeze()))
        }
        (PageEncoding::Raw, ColumnValues::Bytes(v)) => {
            let mut out = BytesMut::new();
            out.put_u32_le(v.len() as u32);
            for b in &v {
                out.put_u32_le(b.len() as u32);
                out.put_slice(b);
            }
            Ok((PageEncoding::Raw, out.freeze()))
        }
        (PageEncoding::Raw, ColumnValues::BytesOwned(v)) => {
            let mut out = BytesMut::new();
            out.put_u32_le(v.len() as u32);
            for b in &v {
                out.put_u32_le(b.len() as u32);
                out.put_slice(b);
            }
            Ok((PageEncoding::Raw, out.freeze()))
        }
        (PageEncoding::For, ColumnValues::U32(v)) => {
            // U32 → i64 lossless promotion for FoR.
            let i: Vec<i64> = v.into_iter().map(|x| x as i64).collect();
            Ok((PageEncoding::For, for_encode(&i)))
        }
        (h, v) => Err(ZenError::format(format!(
            "unsupported encoding/values combination: {:?} / {} rows",
            h,
            v.row_count()
        ))),
    }
}

/// Decode a page back into owned column values. Variants are returned as
/// `*Owned` so the caller need not lifetime-track the source bytes.
pub fn decode_page(enc: PageEncoding, page: &[u8]) -> Result<ColumnValues<'static>, ZenError> {
    match enc {
        PageEncoding::FsstWithOffsets => {
            let view = FsstCompressor::open(page)?;
            let n = view.row_count();
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(view.decode_row(i)?);
            }
            Ok(ColumnValues::StringsOwned(out))
        }
        PageEncoding::Dict => {
            let dec = DictDecoder::open(page)?;
            let mut out = Vec::with_capacity(dec.row_count);
            for i in 0..dec.row_count {
                out.push(dec.row(i)?.to_vec());
            }
            Ok(ColumnValues::StringsOwned(out))
        }
        PageEncoding::For => {
            let v = for_decompress(page)?;
            Ok(ColumnValues::I64(v))
        }
        PageEncoding::Rle => {
            let v = rle_decompress(page)?;
            Ok(ColumnValues::I64(v))
        }
        PageEncoding::Gorilla => {
            let v = gorilla_decompress(page)?;
            Ok(ColumnValues::F64(v))
        }
        PageEncoding::Zstd => {
            let raw = zstd_decompress(page)?;
            if raw.len() < 4 {
                return Err(ZenError::format("zstd page header truncated"));
            }
            let n = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
            // SECURITY: bound the row count from a network-controlled
            // segment so a crafted page can't cause an unbounded
            // `Vec::with_capacity` allocation. Real row groups are
            // ≤ 16 K rows; 1 M is generous slack.
            if n > MAX_PAGE_ROWS {
                return Err(ZenError::format(format!(
                    "zstd page row count {n} > {MAX_PAGE_ROWS} max"
                )));
            }
            let mut p = &raw[4..];
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                if p.len() < 4 {
                    return Err(ZenError::format("zstd page truncated"));
                }
                let l = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
                p = &p[4..];
                if p.len() < l {
                    return Err(ZenError::format("zstd page body truncated"));
                }
                out.push(p[..l].to_vec());
                p = &p[l..];
            }
            Ok(ColumnValues::BytesOwned(out))
        }
        PageEncoding::FixedRaw => {
            let mut p = page;
            if p.remaining() < 4 {
                return Err(ZenError::format("FixedRaw header truncated"));
            }
            let n = p.get_u32_le() as usize;
            // SECURITY: cap row count, and use checked_mul on the
            // bounds calculation. Without this a crafted u32 close to
            // u32::MAX wraps when multiplied by 16, the bounds check
            // passes, and the subsequent slice/copy panics or reads
            // out-of-bounds.
            if n > MAX_PAGE_ROWS {
                return Err(ZenError::format(format!(
                    "FixedRaw row count {n} > {MAX_PAGE_ROWS} max"
                )));
            }
            let needed = n
                .checked_mul(16)
                .ok_or_else(|| ZenError::format("FixedRaw size overflow"))?;
            if p.len() < needed {
                return Err(ZenError::format("FixedRaw page truncated"));
            }
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let mut a = [0u8; 16];
                a.copy_from_slice(&p[..16]);
                p.advance(16);
                out.push(a);
            }
            Ok(ColumnValues::Fixed16(out))
        }
        PageEncoding::Raw => {
            let mut p = page;
            if p.remaining() < 4 {
                return Err(ZenError::format("Raw header truncated"));
            }
            let n = p.get_u32_le() as usize;
            if n > MAX_PAGE_ROWS {
                return Err(ZenError::format(format!(
                    "Raw row count {n} > {MAX_PAGE_ROWS} max"
                )));
            }
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                if p.remaining() < 4 {
                    return Err(ZenError::format("raw page truncated"));
                }
                let l = p.get_u32_le() as usize;
                if p.remaining() < l {
                    return Err(ZenError::format("raw page body truncated"));
                }
                out.push(p[..l].to_vec());
                p.advance(l);
            }
            Ok(ColumnValues::BytesOwned(out))
        }
    }
}

/// Maximum row count we'll honour from a page header. Real row groups
/// cap at 16 K rows; this is generous slack so honest data still fits
/// while a crafted u32 close to u32::MAX is rejected before any
/// allocation happens.
pub(crate) const MAX_PAGE_ROWS: usize = 1_000_000;

/// Decode just one row index from a page, without materializing all rows. Used
/// by the executor for late materialization.
pub fn decode_one_row(enc: PageEncoding, page: &[u8], idx: usize) -> Result<RowValue, ZenError> {
    match enc {
        PageEncoding::FsstWithOffsets => {
            let view = FsstCompressor::open(page)?;
            Ok(RowValue::Bytes(view.decode_row(idx)?))
        }
        PageEncoding::Dict => {
            let dec = DictDecoder::open(page)?;
            Ok(RowValue::Bytes(dec.row(idx)?.to_vec()))
        }
        PageEncoding::For => {
            let v = for_decompress(page)?;
            Ok(RowValue::I64(*v.get(idx).ok_or_else(|| {
                ZenError::format(format!("row idx {idx} out of bounds for FoR page"))
            })?))
        }
        PageEncoding::Rle => {
            let v = rle_decompress(page)?;
            Ok(RowValue::I64(*v.get(idx).ok_or_else(|| {
                ZenError::format(format!("row idx {idx} out of bounds for RLE page"))
            })?))
        }
        PageEncoding::Gorilla => {
            let v = gorilla_decompress(page)?;
            Ok(RowValue::F64(*v.get(idx).ok_or_else(|| {
                ZenError::format(format!("row idx {idx} out of bounds for Gorilla page"))
            })?))
        }
        PageEncoding::Zstd => {
            let raw = zstd_decompress(page)?;
            let (n, mut p) = (
                u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize,
                &raw[4..],
            );
            for i in 0..n {
                if p.len() < 4 {
                    return Err(ZenError::format("zstd one-row truncated"));
                }
                let l = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
                p = &p[4..];
                if i == idx {
                    return Ok(RowValue::Bytes(p[..l].to_vec()));
                }
                p = &p[l..];
            }
            Err(ZenError::format(format!("zstd row idx {idx} not found")))
        }
        PageEncoding::FixedRaw => {
            let mut p = page;
            let n = p.get_u32_le() as usize;
            if idx >= n {
                return Err(ZenError::format("FixedRaw row idx oob"));
            }
            let off = idx * 16;
            let bytes = &p[off..off + 16];
            Ok(RowValue::Fixed16(bytes.try_into().unwrap()))
        }
        PageEncoding::Raw => {
            let mut p = page;
            let n = p.get_u32_le() as usize;
            for i in 0..n {
                let l = p.get_u32_le() as usize;
                if i == idx {
                    return Ok(RowValue::Bytes(p[..l].to_vec()));
                }
                p.advance(l);
            }
            Err(ZenError::format(format!("raw row idx {idx} not found")))
        }
    }
}

#[derive(Clone, Debug)]
pub enum RowValue {
    Bytes(Vec<u8>),
    I64(i64),
    F64(f64),
    Fixed16([u8; 16]),
}

/// Opened page view: amortizes per-page setup across multiple row reads. The
/// scan operator opens one of these per (row-group, column) and then decodes
/// only the rows that survived its filter.
pub enum PageView<'a> {
    Fsst(zen_compress::fsst::DecodedFsst<'a>),
    Dict(zen_compress::DictDecoder),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Fixed16Page { page: &'a [u8], n: usize },
    Bytes(Vec<Vec<u8>>),
}

impl<'a> PageView<'a> {
    pub fn open(enc: PageEncoding, page: &'a [u8]) -> Result<Self, ZenError> {
        Ok(match enc {
            PageEncoding::FsstWithOffsets => PageView::Fsst(FsstCompressor::open(page)?),
            PageEncoding::Dict => PageView::Dict(DictDecoder::open(page)?),
            PageEncoding::For => PageView::I64(for_decompress(page)?),
            PageEncoding::Rle => PageView::I64(rle_decompress(page)?),
            PageEncoding::Gorilla => PageView::F64(gorilla_decompress(page)?),
            PageEncoding::FixedRaw => {
                let mut p = page;
                let n = p.get_u32_le() as usize;
                PageView::Fixed16Page { page: p, n }
            }
            PageEncoding::Zstd => {
                // Decompress once, then we hold the rows.
                let raw = zstd_decompress(page)?;
                let mut p: &[u8] = &raw;
                let n = p.get_u32_le() as usize;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = p.get_u32_le() as usize;
                    out.push(p[..l].to_vec());
                    p = &p[l..];
                }
                PageView::Bytes(out)
            }
            PageEncoding::Raw => {
                let mut p = page;
                let n = p.get_u32_le() as usize;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = p.get_u32_le() as usize;
                    out.push(p[..l].to_vec());
                    p = &p[l..];
                }
                PageView::Bytes(out)
            }
        })
    }

    pub fn row_count(&self) -> usize {
        match self {
            PageView::Fsst(v) => v.row_count(),
            PageView::Dict(d) => d.row_count,
            PageView::I64(v) => v.len(),
            PageView::F64(v) => v.len(),
            PageView::Fixed16Page { n, .. } => *n,
            PageView::Bytes(v) => v.len(),
        }
    }

    pub fn row(&self, idx: usize) -> Result<RowValue, ZenError> {
        match self {
            PageView::Fsst(v) => Ok(RowValue::Bytes(v.decode_row(idx)?)),
            PageView::Dict(d) => Ok(RowValue::Bytes(d.row(idx)?.to_vec())),
            PageView::I64(v) => Ok(RowValue::I64(*v.get(idx).ok_or_else(|| {
                ZenError::format(format!("row {idx} oob (i64 page len {})", v.len()))
            })?)),
            PageView::F64(v) => Ok(RowValue::F64(*v.get(idx).ok_or_else(|| {
                ZenError::format(format!("row {idx} oob (f64 page len {})", v.len()))
            })?)),
            PageView::Fixed16Page { page, n } => {
                if idx >= *n {
                    return Err(ZenError::format("row idx oob"));
                }
                let off = idx * 16;
                let mut a = [0u8; 16];
                a.copy_from_slice(&page[off..off + 16]);
                Ok(RowValue::Fixed16(a))
            }
            PageView::Bytes(v) => Ok(RowValue::Bytes(
                v.get(idx)
                    .ok_or_else(|| ZenError::format(format!("row {idx} oob (bytes page)")))?
                    .clone(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsst_with_offsets_roundtrip() {
        let rows: Vec<Vec<u8>> = vec![
            b"hello world".to_vec(),
            b"out of memory error".to_vec(),
            b"the quick brown fox jumps".to_vec(),
        ];
        let cv = ColumnValues::StringsOwned(rows.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::FsstWithOffsets).unwrap();
        assert_eq!(enc, PageEncoding::FsstWithOffsets);
        let decoded = decode_page(enc, &bytes).unwrap();
        match decoded {
            ColumnValues::StringsOwned(v) => {
                assert_eq!(v, rows);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn dict_roundtrip() {
        let rows: Vec<Vec<u8>> = vec![b"gpt-4o".to_vec(), b"haiku".to_vec(), b"gpt-4o".to_vec()];
        let cv = ColumnValues::StringsOwned(rows.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::Dict).unwrap();
        let decoded = decode_page(enc, &bytes).unwrap();
        match decoded {
            ColumnValues::StringsOwned(v) => {
                assert_eq!(v, rows);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn for_i64_roundtrip() {
        let v = vec![100i64, 101, 102, 200, 50];
        let cv = ColumnValues::I64(v.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::For).unwrap();
        let decoded = decode_page(enc, &bytes).unwrap();
        match decoded {
            ColumnValues::I64(d) => assert_eq!(d, v),
            _ => panic!(),
        }
    }

    #[test]
    fn fixed16_roundtrip() {
        let v: Vec<[u8; 16]> = (0..10).map(|i| [i as u8; 16]).collect();
        let cv = ColumnValues::Fixed16(v.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::FixedRaw).unwrap();
        let decoded = decode_page(enc, &bytes).unwrap();
        match decoded {
            ColumnValues::Fixed16(d) => assert_eq!(d, v),
            _ => panic!(),
        }
    }

    #[test]
    fn decode_one_row_late_mat() {
        let rows: Vec<Vec<u8>> = (0..1000).map(|i| format!("row-{i}").into_bytes()).collect();
        let cv = ColumnValues::StringsOwned(rows.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::FsstWithOffsets).unwrap();
        // Pull row 427 alone.
        let v = decode_one_row(enc, &bytes, 427).unwrap();
        match v {
            RowValue::Bytes(b) => assert_eq!(b, rows[427]),
            _ => panic!(),
        }
    }

    #[test]
    fn zstd_byte_pages_roundtrip() {
        let rows: Vec<Vec<u8>> = (0..50)
            .map(|i| format!("{{\"id\":{i}}}").into_bytes())
            .collect();
        let cv = ColumnValues::BytesOwned(rows.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::Zstd).unwrap();
        let decoded = decode_page(enc, &bytes).unwrap();
        match decoded {
            ColumnValues::BytesOwned(v) => assert_eq!(v, rows),
            _ => panic!(),
        }
    }
}
