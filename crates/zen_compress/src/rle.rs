//! Run-length encoding for high-repetition columns.
//!
//! Encodes a slice of i64 as `(value, run_length)` pairs. Frame-of-Reference
//! handles "monotonic with small deltas" well; RLE handles "long runs of the
//! same value". Many spans share the same status / span_type / model in a row
//! group, so RLE is ideal there.
//!
//! Format:
//! ```text
//! count: u32 le
//! pairs: { value: i64 le, run: u32 le } * pair_count
//! ```
//!
//! `run` is the number of repetitions. Sum of `run` equals `count`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use zen_common::ZenError;

pub fn rle_encode(values: &[i64]) -> Bytes {
    let mut buf = BytesMut::with_capacity(4 + values.len() * 12);
    buf.put_u32_le(values.len() as u32);
    if values.is_empty() {
        return buf.freeze();
    }
    let mut current = values[0];
    let mut run = 1u32;
    for &v in &values[1..] {
        if v == current && run < u32::MAX {
            run += 1;
        } else {
            buf.put_i64_le(current);
            buf.put_u32_le(run);
            current = v;
            run = 1;
        }
    }
    buf.put_i64_le(current);
    buf.put_u32_le(run);
    buf.freeze()
}

pub fn rle_decompress(input: &[u8]) -> Result<Vec<i64>, ZenError> {
    if input.len() < 4 {
        return Err(ZenError::compress("RLE input too short"));
    }
    let mut p = input;
    let count = p.get_u32_le() as usize;
    let mut out = Vec::with_capacity(count);
    while !p.is_empty() {
        if p.remaining() < 12 {
            return Err(ZenError::compress("RLE truncated mid-pair"));
        }
        let value = p.get_i64_le();
        let run = p.get_u32_le() as usize;
        out.extend(std::iter::repeat(value).take(run));
    }
    if out.len() != count {
        return Err(ZenError::compress(format!(
            "RLE expected {count} values, got {}",
            out.len()
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn empty_roundtrip() {
        let b = rle_encode(&[]);
        let v = rle_decompress(&b).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn long_run_compresses() {
        let v = vec![7i64; 10_000];
        let b = rle_encode(&v);
        assert_eq!(b.len(), 4 + 12);
        let d = rle_decompress(&b).unwrap();
        assert_eq!(d, v);
    }

    #[test]
    fn distinct_values_no_compression() {
        let v: Vec<i64> = (0..100).collect();
        let b = rle_encode(&v);
        // 4 (count) + 100 * 12 (pairs)
        assert_eq!(b.len(), 4 + 100 * 12);
        let d = rle_decompress(&b).unwrap();
        assert_eq!(d, v);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn proptest_roundtrip(v in proptest::collection::vec(-100_i64..100_i64, 0..500)) {
            let b = rle_encode(&v);
            let d = rle_decompress(&b).unwrap();
            prop_assert_eq!(d, v);
        }
    }
}
