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

    /// Raw `BytesOwned` pages encode and decode byte-for-byte.
    #[test]
    fn raw_byte_pages_roundtrip() {
        let rows: Vec<Vec<u8>> = vec![
            b"alpha".to_vec(),
            Vec::new(),
            b"".to_vec(),
            b"the third row has spaces".to_vec(),
        ];
        let cv = ColumnValues::BytesOwned(rows.clone());
        let (enc, bytes) = encode_page(cv, PageEncoding::Raw).expect("Raw encode");
        assert_eq!(enc, PageEncoding::Raw);
        match decode_page(enc, &bytes).expect("Raw decode") {
            ColumnValues::BytesOwned(v) => assert_eq!(v, rows),
            _ => panic!("expected BytesOwned"),
        }
    }

    /// RLE i64 pages roundtrip correctly even with long runs.
    #[test]
    fn rle_i64_roundtrip() {
        let mut v: Vec<i64> = vec![5; 20];
        v.extend(vec![7; 3]);
        v.extend(vec![5; 100]);
        let (enc, bytes) =
            encode_page(ColumnValues::I64(v.clone()), PageEncoding::Rle).expect("Rle encode");
        assert_eq!(enc, PageEncoding::Rle);
        match decode_page(enc, &bytes).expect("Rle decode") {
            ColumnValues::I64(d) => assert_eq!(d, v),
            _ => panic!("expected I64"),
        }
    }

    /// Gorilla-encoded f64 pages roundtrip.
    #[test]
    fn gorilla_f64_roundtrip() {
        let v: Vec<f64> = vec![1.0, 1.5, 1.55, 1.555, 2.0, -2.5, 0.0];
        let (enc, bytes) = encode_page(ColumnValues::F64(v.clone()), PageEncoding::Gorilla)
            .expect("Gorilla encode");
        assert_eq!(enc, PageEncoding::Gorilla);
        match decode_page(enc, &bytes).expect("Gorilla decode") {
            ColumnValues::F64(d) => assert_eq!(d, v),
            _ => panic!("expected F64"),
        }
    }

    /// U32 column values widen to i64 losslessly through the For encoder.
    #[test]
    fn u32_for_promotes_to_i64() {
        let v: Vec<u32> = vec![0, 100, 5000, u32::MAX, 42];
        let (enc, bytes) =
            encode_page(ColumnValues::U32(v.clone()), PageEncoding::For).expect("For/U32 encode");
        assert_eq!(enc, PageEncoding::For);
        match decode_page(enc, &bytes).expect("For decode") {
            ColumnValues::I64(d) => {
                let expected: Vec<i64> = v.into_iter().map(|x| x as i64).collect();
                assert_eq!(d, expected);
            }
            _ => panic!("expected I64"),
        }
    }

    /// FixedRaw with a header claiming N rows but a payload missing all rows
    /// returns a structured error rather than panicking on the slice.
    #[test]
    fn fixed_raw_rejects_truncated_body() {
        // Header says 10 rows of 16 bytes (160 bytes needed) but payload is empty.
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u32.to_le_bytes());
        let err = decode_page(PageEncoding::FixedRaw, &buf).expect_err("expected truncation error");
        let msg = format!("{err}");
        assert!(
            msg.contains("FixedRaw page truncated"),
            "expected truncation message, got: {msg}"
        );
    }

    /// FixedRaw must reject row counts above MAX_PAGE_ROWS without allocating.
    #[test]
    fn fixed_raw_rejects_oversized_row_count() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&((MAX_PAGE_ROWS + 1) as u32).to_le_bytes());
        let err =
            decode_page(PageEncoding::FixedRaw, &buf).expect_err("expected row-count cap error");
        let msg = format!("{err}");
        assert!(
            msg.contains(&format!("> {MAX_PAGE_ROWS} max")),
            "expected MAX_PAGE_ROWS cap message, got: {msg}"
        );
    }

    /// Raw encoding rejects row counts above MAX_PAGE_ROWS before allocating.
    #[test]
    fn raw_rejects_oversized_row_count() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&((MAX_PAGE_ROWS as u32) + 1).to_le_bytes());
        let err = decode_page(PageEncoding::Raw, &buf).expect_err("expected Raw cap error");
        let msg = format!("{err}");
        assert!(
            msg.contains(&format!("> {MAX_PAGE_ROWS} max")),
            "expected MAX_PAGE_ROWS cap message, got: {msg}"
        );
    }

    /// Zstd encoding rejects row counts above MAX_PAGE_ROWS in the inner header.
    #[test]
    fn zstd_rejects_oversized_row_count() {
        // Build an inner Zstd payload whose header claims too many rows, then
        // ZSTD-compress it so decode_page reaches the row-count check.
        let mut inner = Vec::new();
        inner.extend_from_slice(&((MAX_PAGE_ROWS as u32) + 1).to_le_bytes());
        let compressed = zen_compress::zstd_compress(&inner, 3).expect("zstd compress");
        let err = decode_page(PageEncoding::Zstd, &compressed).expect_err("expected Zstd cap");
        let msg = format!("{err}");
        assert!(
            msg.contains(&format!("> {MAX_PAGE_ROWS} max")),
            "expected MAX_PAGE_ROWS cap message, got: {msg}"
        );
    }

    /// FixedRaw with `n = u32::MAX` must reject via the row-count cap (the
    /// `checked_mul(16)` guard would also catch wrap-around if the cap were lifted).
    #[test]
    fn fixed_raw_rejects_u32_max_without_overflow() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_page(PageEncoding::FixedRaw, &buf).expect_err("expected error");
        // Must not panic / wrap to a small number; we accept either the row
        // cap or the overflow guard since both protect against the same class
        // of crafted input.
        let msg = format!("{err}");
        assert!(
            msg.contains("max") || msg.contains("overflow"),
            "expected cap/overflow message, got: {msg}"
        );
    }

    /// Empty FixedRaw header (n=0) decodes to an empty Fixed16 vec, not a panic.
    #[test]
    fn fixed_raw_empty_decodes_to_empty() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes());
        match decode_page(PageEncoding::FixedRaw, &buf).expect("empty FixedRaw") {
            ColumnValues::Fixed16(v) => assert!(v.is_empty(), "expected empty Fixed16"),
            _ => panic!("expected Fixed16"),
        }
    }

    /// A zero-length input page returns a structured "header truncated" error,
    /// never a panic, for both Raw and FixedRaw.
    #[test]
    fn empty_input_returns_structured_error() {
        let err_raw = decode_page(PageEncoding::Raw, &[]).expect_err("Raw empty");
        assert!(
            format!("{err_raw}").contains("truncated"),
            "expected truncation error for empty Raw"
        );
        let err_fx = decode_page(PageEncoding::FixedRaw, &[]).expect_err("FixedRaw empty");
        assert!(
            format!("{err_fx}").contains("truncated"),
            "expected truncation error for empty FixedRaw"
        );
    }

    /// `decode_one_row` returns the same value as `decode_page`'s nth element
    /// for Raw, FixedRaw, For, and Rle encodings.
    #[test]
    fn decode_one_row_matches_decode_page() {
        // Raw bytes
        let rows: Vec<Vec<u8>> = (0..8).map(|i| format!("r{i}").into_bytes()).collect();
        let (enc, bytes) =
            encode_page(ColumnValues::BytesOwned(rows.clone()), PageEncoding::Raw).unwrap();
        let full = decode_page(enc, &bytes).unwrap();
        let one = decode_one_row(enc, &bytes, 5).unwrap();
        match (full, one) {
            (ColumnValues::BytesOwned(all), RowValue::Bytes(r5)) => assert_eq!(r5, all[5]),
            _ => panic!("Raw mismatch"),
        }
        // FixedRaw
        let fx: Vec<[u8; 16]> = (0..6).map(|i| [i as u8; 16]).collect();
        let (enc, bytes) =
            encode_page(ColumnValues::Fixed16(fx.clone()), PageEncoding::FixedRaw).unwrap();
        let one = decode_one_row(enc, &bytes, 3).unwrap();
        match one {
            RowValue::Fixed16(r) => assert_eq!(r, fx[3]),
            _ => panic!("FixedRaw mismatch"),
        }
        // For
        let v = vec![10i64, 20, 30, 40];
        let (enc, bytes) = encode_page(ColumnValues::I64(v.clone()), PageEncoding::For).unwrap();
        let one = decode_one_row(enc, &bytes, 2).unwrap();
        match one {
            RowValue::I64(r) => assert_eq!(r, v[2]),
            _ => panic!("For mismatch"),
        }
        // Rle
        let v = vec![7i64; 16];
        let (enc, bytes) = encode_page(ColumnValues::I64(v.clone()), PageEncoding::Rle).unwrap();
        let one = decode_one_row(enc, &bytes, 9).unwrap();
        match one {
            RowValue::I64(r) => assert_eq!(r, v[9]),
            _ => panic!("Rle mismatch"),
        }
    }

    /// `PageEncoding::try_from_u8` rejects unknown tags with a structured error.
    #[test]
    fn page_encoding_try_from_unknown_byte() {
        let err = PageEncoding::try_from_u8(99).expect_err("expected unknown encoding error");
        assert!(format!("{err}").contains("unknown page encoding"));
    }

    proptest::proptest! {
        /// Arbitrary `BytesOwned` vectors must roundtrip byte-for-byte through
        /// both the Raw and Zstd encoders. This guards regressions in the page
        /// codecs as the production code evolves.
        #[test]
        fn arbitrary_bytes_owned_roundtrip(
            rows in proptest::collection::vec(
                proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
                1..32,
            ),
        ) {
            // Raw
            let (enc, bytes) = encode_page(
                ColumnValues::BytesOwned(rows.clone()),
                PageEncoding::Raw,
            ).expect("Raw encode");
            let decoded = decode_page(enc, &bytes).expect("Raw decode");
            match decoded {
                ColumnValues::BytesOwned(v) => proptest::prop_assert_eq!(&v, &rows),
                _ => proptest::prop_assert!(false, "Raw decoded wrong variant"),
            }
            // Zstd
            let (enc, bytes) = encode_page(
                ColumnValues::BytesOwned(rows.clone()),
                PageEncoding::Zstd,
            ).expect("Zstd encode");
            let decoded = decode_page(enc, &bytes).expect("Zstd decode");
            match decoded {
                ColumnValues::BytesOwned(v) => proptest::prop_assert_eq!(&v, &rows),
                _ => proptest::prop_assert!(false, "Zstd decoded wrong variant"),
            }
        }
    }
}
