//! Frame-of-Reference (FoR) + bit-pack for i64 / u32 / u64 columns.
//!
//! For a value slice, find min, subtract it from every value to get a non-negative
//! delta, then bit-pack the deltas using the smallest width that fits the largest
//! delta. Layout:
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │ count: u32 little-endian        │
//! │ width: u8 (number of bits)      │
//! │ frame: i64 little-endian (min)  │
//! │ packed: bytes                    │
//! └─────────────────────────────────┘
//! ```
//!
//! `width=0` is the "all values equal frame" case — the packed area is zero bytes.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use zen_common::ZenError;

pub fn for_encode(values: &[i64]) -> Bytes {
    if values.is_empty() {
        let mut b = BytesMut::with_capacity(13);
        b.put_u32_le(0);
        b.put_u8(0);
        b.put_i64_le(0);
        return b.freeze();
    }
    let frame = *values.iter().min().unwrap();
    let max_delta: u64 = values.iter().map(|v| (*v - frame) as u64).max().unwrap_or(0);
    let width = if max_delta == 0 {
        0
    } else {
        64 - max_delta.leading_zeros() as u8
    };

    let mut b = BytesMut::with_capacity(13 + (values.len() * width as usize + 7) / 8);
    b.put_u32_le(values.len() as u32);
    b.put_u8(width);
    b.put_i64_le(frame);

    if width == 0 {
        return b.freeze();
    }

    let mut acc: u64 = 0;
    let mut acc_bits: u8 = 0;
    for v in values {
        let d = (*v - frame) as u64;
        acc |= d << acc_bits;
        acc_bits += width;
        while acc_bits >= 8 {
            b.put_u8((acc & 0xFF) as u8);
            acc >>= 8;
            acc_bits -= 8;
        }
    }
    if acc_bits > 0 {
        b.put_u8((acc & 0xFF) as u8);
    }
    b.freeze()
}

pub fn for_decompress(input: &[u8]) -> Result<Vec<i64>, ZenError> {
    if input.len() < 13 {
        return Err(ZenError::compress("FoR input too short"));
    }
    let mut p = input;
    let count = p.get_u32_le() as usize;
    let width = p.get_u8();
    let frame = p.get_i64_le();

    if count == 0 {
        return Ok(Vec::new());
    }
    if width == 0 {
        return Ok(vec![frame; count]);
    }

    let needed_bytes = (count * width as usize + 7) / 8;
    if p.len() < needed_bytes {
        return Err(ZenError::compress("FoR packed area truncated"));
    }
    let packed = &p[..needed_bytes];
    let mut acc: u64 = 0;
    let mut acc_bits: u8 = 0;
    let mask = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
    let mut out = Vec::with_capacity(count);
    let mut bi = 0usize;
    for _ in 0..count {
        while acc_bits < width {
            if bi >= packed.len() {
                return Err(ZenError::compress("FoR EOF mid-value"));
            }
            acc |= (packed[bi] as u64) << acc_bits;
            bi += 1;
            acc_bits += 8;
        }
        let d = acc & mask;
        acc >>= width;
        acc_bits -= width;
        out.push(frame + d as i64);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn empty_roundtrip() {
        let b = for_encode(&[]);
        let v = for_decompress(&b).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn all_equal_uses_zero_width() {
        let v = vec![42i64; 100];
        let b = for_encode(&v);
        // 4 + 1 + 8 = 13 bytes header + zero packed bytes
        assert_eq!(b.len(), 13);
        let d = for_decompress(&b).unwrap();
        assert_eq!(d, v);
    }

    #[test]
    fn monotonic_compresses_well() {
        let v: Vec<i64> = (0..10_000).collect();
        let b = for_encode(&v);
        // 14 bits per value max; 10K * 14 / 8 = ~17 KB, plus 13 bytes header.
        assert!(b.len() < v.len() * 2);
        let d = for_decompress(&b).unwrap();
        assert_eq!(d, v);
    }

    #[test]
    fn negative_values_roundtrip() {
        let v: Vec<i64> = vec![-100, -50, 0, 50, 100, 200];
        let b = for_encode(&v);
        let d = for_decompress(&b).unwrap();
        assert_eq!(d, v);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn proptest_roundtrip(v in proptest::collection::vec(-1_000_000_i64..1_000_000_i64, 0..500)) {
            let b = for_encode(&v);
            let d = for_decompress(&b).unwrap();
            prop_assert_eq!(d, v);
        }
    }
}
