//! Per-(row-group, column) zone maps.
//!
//! Tracks min, max, null_count, and an HLL-style approximate distinct count.
//! These ride in the segment hotcache so the executor can prune row groups
//! before any payload bytes are read.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use zen_common::ZenError;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ZoneMapValue {
    Empty,
    I64 {
        min: i64,
        max: i64,
    },
    U64 {
        min: u64,
        max: u64,
    },
    F64 {
        min: f64,
        max: f64,
    },
    /// Stored as canonical bytes; for utf8 columns this is the raw utf8 bytes.
    Bytes {
        min: Vec<u8>,
        max: Vec<u8>,
    },
    /// Used for fixed-width binary columns (TraceId, SpanId).
    Fixed {
        min: Vec<u8>,
        max: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ZoneMap {
    pub value: ZoneMapValue,
    pub null_count: u32,
    pub row_count: u32,
    /// Approximate distinct count, computed during build via `HllSketch`.
    pub distinct_estimate: u32,
}

impl Default for ZoneMap {
    fn default() -> Self {
        Self {
            value: ZoneMapValue::Empty,
            null_count: 0,
            row_count: 0,
            distinct_estimate: 0,
        }
    }
}

impl ZoneMap {
    pub fn from_i64(values: &[i64], nulls: u32) -> Self {
        if values.is_empty() {
            return Self {
                value: ZoneMapValue::Empty,
                null_count: nulls,
                row_count: nulls,
                distinct_estimate: 0,
            };
        }
        let mut min = values[0];
        let mut max = values[0];
        for &v in &values[1..] {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
        Self {
            value: ZoneMapValue::I64 { min, max },
            null_count: nulls,
            row_count: values.len() as u32 + nulls,
            distinct_estimate: hll_distinct_i64(values),
        }
    }

    pub fn from_u64(values: &[u64], nulls: u32) -> Self {
        if values.is_empty() {
            return Self {
                value: ZoneMapValue::Empty,
                null_count: nulls,
                row_count: nulls,
                distinct_estimate: 0,
            };
        }
        let mut min = values[0];
        let mut max = values[0];
        for &v in &values[1..] {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
        Self {
            value: ZoneMapValue::U64 { min, max },
            null_count: nulls,
            row_count: values.len() as u32 + nulls,
            distinct_estimate: hll_distinct_u64(values),
        }
    }

    pub fn from_f64(values: &[f64], nulls: u32) -> Self {
        if values.is_empty() {
            return Self {
                value: ZoneMapValue::Empty,
                null_count: nulls,
                row_count: nulls,
                distinct_estimate: 0,
            };
        }
        let mut min = values[0];
        let mut max = values[0];
        for &v in &values[1..] {
            if !v.is_nan() {
                if v < min {
                    min = v;
                }
                if v > max {
                    max = v;
                }
            }
        }
        Self {
            value: ZoneMapValue::F64 { min, max },
            null_count: nulls,
            row_count: values.len() as u32 + nulls,
            distinct_estimate: hll_distinct_u64(
                &values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            ),
        }
    }

    pub fn from_bytes(values: &[&[u8]], nulls: u32) -> Self {
        if values.is_empty() {
            return Self {
                value: ZoneMapValue::Empty,
                null_count: nulls,
                row_count: nulls,
                distinct_estimate: 0,
            };
        }
        let mut min: &[u8] = values[0];
        let mut max: &[u8] = values[0];
        for v in &values[1..] {
            if v < &min {
                min = v;
            }
            if v > &max {
                max = v;
            }
        }
        Self {
            value: ZoneMapValue::Bytes {
                min: min.to_vec(),
                max: max.to_vec(),
            },
            null_count: nulls,
            row_count: values.len() as u32 + nulls,
            distinct_estimate: hll_distinct_bytes(values),
        }
    }

    /// Returns true if `value` lies within `[min, max]` for an i64 column.
    pub fn maybe_contains_i64(&self, value: i64) -> bool {
        match &self.value {
            ZoneMapValue::I64 { min, max } => *min <= value && value <= *max,
            ZoneMapValue::Empty => false,
            _ => true, // unsupported combination; conservative
        }
    }

    pub fn maybe_contains_bytes(&self, value: &[u8]) -> bool {
        match &self.value {
            ZoneMapValue::Bytes { min, max } => value >= min.as_slice() && value <= max.as_slice(),
            ZoneMapValue::Empty => false,
            _ => true,
        }
    }

    pub fn serialize(&self) -> Result<Bytes, ZenError> {
        let s = serde_json::to_vec(self)
            .map_err(|e| ZenError::format(format!("zonemap serialize: {e}")))?;
        let mut out = BytesMut::with_capacity(4 + s.len());
        out.put_u32_le(s.len() as u32);
        out.put_slice(&s);
        Ok(out.freeze())
    }

    pub fn deserialize(input: &[u8]) -> Result<(Self, usize), ZenError> {
        if input.len() < 4 {
            return Err(ZenError::format("zonemap header truncated"));
        }
        let mut p = input;
        let len = p.get_u32_le() as usize;
        if p.len() < len {
            return Err(ZenError::format("zonemap body truncated"));
        }
        let zm: ZoneMap = serde_json::from_slice(&p[..len])
            .map_err(|e| ZenError::format(format!("zonemap deserialize: {e}")))?;
        Ok((zm, 4 + len))
    }
}

// ---- minimal HLL distinct estimation ---------------------------------------------------
// We use a cheap 1024-bucket HLL over xxh3_64 hashes. For the kinds of column
// cardinalities we see (≤ tens of millions), this is accurate to ~3% and uses
// only 1 KB of state per call.

fn hll_estimate(buckets: &[u8]) -> u32 {
    let m = buckets.len() as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m); // for m=1024
    let mut sum = 0.0;
    let mut zeros = 0u32;
    for &b in buckets {
        sum += 1.0 / (1u64 << b) as f64;
        if b == 0 {
            zeros += 1;
        }
    }
    let raw = alpha * m * m / sum;
    let result = if zeros > 0 && raw < 2.5 * m {
        // Linear counting for small cardinality.
        m * (m / zeros as f64).ln()
    } else {
        raw
    };
    result.round().min(u32::MAX as f64) as u32
}

fn hll_observe(buckets: &mut [u8], hash: u64) {
    let m_log2 = (buckets.len() as u64).trailing_zeros() as u8;
    let lower_bits = 64 - m_log2;
    let bucket = (hash >> lower_bits) as usize;
    let lower = if lower_bits == 64 {
        hash
    } else {
        hash & ((1u64 << lower_bits) - 1)
    };
    // rho = (position of leftmost 1 in lower, 1-indexed) within the `lower_bits`-bit window.
    // If lower is all zero, rho = lower_bits + 1.
    let aligned = if lower == 0 {
        0u64
    } else {
        lower << (64 - lower_bits)
    };
    let lz = aligned.leading_zeros() as u8;
    let rho = lz.min(lower_bits) + 1;
    if buckets[bucket] < rho {
        buckets[bucket] = rho;
    }
}

fn hll_distinct_i64(values: &[i64]) -> u32 {
    let mut buckets = vec![0u8; 1024];
    for &v in values {
        let h = xxhash_rust::xxh3::xxh3_64(&v.to_le_bytes());
        hll_observe(&mut buckets, h);
    }
    hll_estimate(&buckets)
}

fn hll_distinct_u64(values: &[u64]) -> u32 {
    let mut buckets = vec![0u8; 1024];
    for &v in values {
        let h = xxhash_rust::xxh3::xxh3_64(&v.to_le_bytes());
        hll_observe(&mut buckets, h);
    }
    hll_estimate(&buckets)
}

fn hll_distinct_bytes(values: &[&[u8]]) -> u32 {
    let mut buckets = vec![0u8; 1024];
    for v in values {
        let h = xxhash_rust::xxh3::xxh3_64(v);
        hll_observe(&mut buckets, h);
    }
    hll_estimate(&buckets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i64_zonemap_min_max() {
        let v = vec![3i64, 1, 4, 1, 5, 9, 2, 6];
        let zm = ZoneMap::from_i64(&v, 0);
        match zm.value {
            ZoneMapValue::I64 { min, max } => {
                assert_eq!(min, 1);
                assert_eq!(max, 9);
            }
            _ => panic!("wrong variant"),
        }
        assert!(zm.maybe_contains_i64(5));
        assert!(!zm.maybe_contains_i64(0));
        assert!(!zm.maybe_contains_i64(10));
    }

    #[test]
    fn bytes_zonemap_min_max() {
        let v: Vec<&[u8]> = vec![b"banana", b"apple", b"cherry"];
        let zm = ZoneMap::from_bytes(&v, 0);
        match &zm.value {
            ZoneMapValue::Bytes { min, max } => {
                assert_eq!(min, b"apple");
                assert_eq!(max, b"cherry");
            }
            _ => panic!("wrong variant"),
        }
        assert!(zm.maybe_contains_bytes(b"banana"));
        assert!(!zm.maybe_contains_bytes(b"zucchini"));
    }

    #[test]
    fn empty_zonemap() {
        let zm = ZoneMap::from_i64(&[], 5);
        assert_eq!(zm.value, ZoneMapValue::Empty);
        assert_eq!(zm.null_count, 5);
        assert!(!zm.maybe_contains_i64(0));
    }

    #[test]
    fn serialize_roundtrip() {
        let zm = ZoneMap::from_i64(&[1, 2, 3, 4, 5], 2);
        let bytes = zm.serialize().unwrap();
        let (zm2, consumed) = ZoneMap::deserialize(&bytes).unwrap();
        assert_eq!(zm.value, zm2.value);
        assert_eq!(zm.null_count, zm2.null_count);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn hll_distinct_ballpark() {
        let mut v: Vec<u64> = Vec::new();
        for i in 0..10_000u64 {
            v.push(i);
        }
        let est = hll_distinct_u64(&v);
        // 1024-bucket HLL should be accurate to ~5% on 10K distinct.
        let err = (est as f64 - 10_000.0).abs() / 10_000.0;
        assert!(err < 0.10, "HLL error {err} for 10K distinct (est={est})");
    }

    #[test]
    fn f64_zonemap_ignores_nan() {
        let v = vec![1.0, 2.0, f64::NAN, 3.0, -1.0];
        let zm = ZoneMap::from_f64(&v, 0);
        match zm.value {
            ZoneMapValue::F64 { min, max } => {
                assert_eq!(min, -1.0);
                assert_eq!(max, 3.0);
            }
            _ => panic!("wrong variant"),
        }
    }
}
