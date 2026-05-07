//! Simple split-block Bloom filter for probabilistic membership tests.
//!
//! Used for high-cardinality columns (e.g. `request_id`) where a full posting
//! list would be too large but we still want fast negative responses.
//!
//! The Bloom is a fixed-size bit array with 4 hash functions derived from
//! `xxh3_64(value)`. Sized for ~1% false positive rate at the given expected
//! count.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use zen_common::ZenError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BloomFilter {
    bits: Vec<u8>,
    /// Number of hash functions.
    k: u8,
}

impl BloomFilter {
    /// Build a Bloom filter sized for `n` elements at ~`fpr` false positive rate.
    pub fn new(expected_n: usize, fpr: f64) -> Self {
        let n = expected_n.max(1);
        let m_bits = ((-(n as f64) * fpr.ln()) / (std::f64::consts::LN_2.powi(2))).ceil() as usize;
        let m_bits = m_bits.next_power_of_two().max(64);
        let k = (((m_bits as f64) / (n as f64)) * std::f64::consts::LN_2).ceil() as u8;
        let k = k.clamp(2, 8);
        Self {
            bits: vec![0u8; m_bits / 8],
            k,
        }
    }

    pub fn capacity_bits(&self) -> usize {
        self.bits.len() * 8
    }

    pub fn insert(&mut self, value: &[u8]) {
        let m = self.capacity_bits();
        let h = xxh3_64(value);
        let mut state = h;
        for _ in 0..self.k {
            let bit = (state as usize) % m;
            self.bits[bit / 8] |= 1 << (bit % 8);
            state = state.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(13).wrapping_add(0xC2B2_AE3D_27D4_EB4F);
        }
    }

    pub fn contains(&self, value: &[u8]) -> bool {
        let m = self.capacity_bits();
        let h = xxh3_64(value);
        let mut state = h;
        for _ in 0..self.k {
            let bit = (state as usize) % m;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
            state = state.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(13).wrapping_add(0xC2B2_AE3D_27D4_EB4F);
        }
        true
    }

    pub fn serialize(&self) -> Result<Bytes, ZenError> {
        let mut out = BytesMut::with_capacity(8 + self.bits.len());
        out.put_u32_le(self.bits.len() as u32);
        out.put_u8(self.k);
        out.put_slice(&self.bits);
        Ok(out.freeze())
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, ZenError> {
        if input.len() < 5 {
            return Err(ZenError::format("bloom header truncated"));
        }
        let mut p = input;
        let m = p.get_u32_le() as usize;
        let k = p.get_u8();
        if p.len() < m {
            return Err(ZenError::format("bloom body truncated"));
        }
        let mut bits = vec![0u8; m];
        bits.copy_from_slice(&p[..m]);
        Ok(Self { bits, k })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(10_000, 0.01);
        for i in 0..10_000 {
            bf.insert(format!("key{i}").as_bytes());
        }
        for i in 0..10_000 {
            assert!(bf.contains(format!("key{i}").as_bytes()));
        }
    }

    #[test]
    fn fpr_acceptable() {
        let mut bf = BloomFilter::new(10_000, 0.01);
        for i in 0..10_000 {
            bf.insert(format!("key{i}").as_bytes());
        }
        // Test 10_000 absent keys; expect ~ <2% false positives.
        let mut fp = 0;
        for i in 10_000..20_000 {
            if bf.contains(format!("key{i}").as_bytes()) {
                fp += 1;
            }
        }
        assert!(fp < 200, "fpr too high: {fp}");
    }

    #[test]
    fn serialize_roundtrip() {
        let mut bf = BloomFilter::new(1000, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");
        let bytes = bf.serialize().unwrap();
        let bf2 = BloomFilter::deserialize(&bytes).unwrap();
        assert!(bf2.contains(b"hello"));
        assert!(bf2.contains(b"world"));
    }
}
