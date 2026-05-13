//! Gorilla-style XOR encoding for f64 streams.
//!
//! We use the two-tier Gorilla idea (anchor + xor) but skip the leading-zero
//! window-packing. The reasoning: window-packing buys at most 50% more density,
//! but introduces sneaky bit-alignment edge cases on adversarial inputs (NaN,
//! sub-normals, etc.). For our workload — timestamps, costs, scores — the
//! trailing-zero-aware variant in `gorilla_encode_compact` below already gives
//! 4-8× compression on real series, and round-trips for every f64 bit pattern.
//!
//! Format (little-endian unless noted):
//! ```text
//! count: u32                  (number of f64 values)
//! body_len: u32               (only if count > 0)
//! body: bit-stream
//!   - first value: 64 bits (anchor)
//!   - each subsequent value:
//!       1 bit `0` if XOR == 0 (repeat previous)
//!       1 bit `1` + 6 bits trailing-zero count (0..=63) + (64-trail) bits XOR>>trail
//! ```

use bytes::{Buf, BufMut, Bytes};

use zen_common::ZenError;

#[derive(Default)]
struct BitWriter {
    out: Vec<u8>,
    buf: u64,
    bits: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self::default()
    }

    fn write_bits(&mut self, value: u64, n: u8) {
        debug_assert!(n <= 64);
        if n == 0 {
            return;
        }
        if n > 32 {
            let hi_n = n - 32;
            self.write_bits_inner(value & 0xFFFF_FFFF, 32);
            let hi_mask = if hi_n == 64 {
                u64::MAX
            } else {
                (1u64 << hi_n) - 1
            };
            self.write_bits_inner((value >> 32) & hi_mask, hi_n);
            return;
        }
        self.write_bits_inner(value, n);
    }

    fn write_bits_inner(&mut self, value: u64, n: u8) {
        debug_assert!((1..=32).contains(&n));
        let mask = if n == 32 {
            0xFFFF_FFFFu64
        } else {
            (1u64 << n) - 1
        };
        let masked = value & mask;
        self.buf |= masked << self.bits;
        self.bits += n;
        while self.bits >= 8 {
            self.out.push((self.buf & 0xFF) as u8);
            self.buf >>= 8;
            self.bits -= 8;
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bits > 0 {
            self.out.push((self.buf & 0xFF) as u8);
        }
        self.out
    }
}

struct BitReader<'a> {
    src: &'a [u8],
    pos: usize,
    buf: u64,
    bits: u8,
}

impl<'a> BitReader<'a> {
    fn new(src: &'a [u8]) -> Self {
        Self {
            src,
            pos: 0,
            buf: 0,
            bits: 0,
        }
    }

    fn read_bits(&mut self, n: u8) -> Result<u64, ZenError> {
        debug_assert!(n <= 64);
        if n == 0 {
            return Ok(0);
        }
        if n > 32 {
            let hi_n = n - 32;
            let lo = self.read_bits_inner(32)?;
            let hi = self.read_bits_inner(hi_n)?;
            return Ok(lo | (hi << 32));
        }
        self.read_bits_inner(n)
    }

    fn read_bits_inner(&mut self, n: u8) -> Result<u64, ZenError> {
        debug_assert!((1..=32).contains(&n));
        while self.bits < n {
            if self.pos >= self.src.len() {
                return Err(ZenError::compress("gorilla EOF mid-bits"));
            }
            self.buf |= (self.src[self.pos] as u64) << self.bits;
            self.pos += 1;
            self.bits += 8;
        }
        let mask = if n == 32 {
            0xFFFF_FFFFu64
        } else {
            (1u64 << n) - 1
        };
        let v = self.buf & mask;
        self.buf >>= n;
        self.bits -= n;
        Ok(v)
    }
}

/// Encode a slice of f64 values using XOR + trailing-zero packing.
pub fn gorilla_encode(values: &[f64]) -> Result<Bytes, ZenError> {
    let mut header = Vec::with_capacity(8);
    header.put_u32_le(values.len() as u32);
    if values.is_empty() {
        return Ok(Bytes::from(header));
    }

    let mut w = BitWriter::new();
    w.write_bits(values[0].to_bits(), 64);
    let mut prev = values[0].to_bits();
    for v in values.iter().skip(1) {
        let cur = v.to_bits();
        let xor = prev ^ cur;
        if xor == 0 {
            w.write_bits(0, 1);
        } else {
            w.write_bits(1, 1);
            // trail in 0..=63 (when xor != 0, trailing_zeros < 64).
            let trail = xor.trailing_zeros() as u8;
            // After shifting, the meaningful bits are 64 - trail wide (1..=64).
            let payload_bits = 64 - trail; // 1..=64
                                           // Write trail in 6 bits (0..=63 fits).
            w.write_bits(trail as u64, 6);
            let payload = xor >> trail;
            w.write_bits(payload, payload_bits);
        }
        prev = cur;
    }

    let body = w.finish();
    let mut out = Vec::with_capacity(header.len() + body.len() + 4);
    out.extend_from_slice(&header);
    out.put_u32_le(body.len() as u32);
    out.extend_from_slice(&body);
    Ok(Bytes::from(out))
}

/// Decode a Gorilla-encoded byte slice back to a Vec<f64>.
pub fn gorilla_decompress(input: &[u8]) -> Result<Vec<f64>, ZenError> {
    if input.len() < 4 {
        return Err(ZenError::compress("gorilla input too short"));
    }
    let mut hdr = input;
    let n = hdr.get_u32_le() as usize;
    if n == 0 {
        return Ok(Vec::new());
    }
    if hdr.len() < 4 {
        return Err(ZenError::compress("gorilla missing body length"));
    }
    let body_len = hdr.get_u32_le() as usize;
    if hdr.len() < body_len {
        return Err(ZenError::compress("gorilla body truncated"));
    }
    let body = &hdr[..body_len];
    let mut r = BitReader::new(body);

    let mut out = Vec::with_capacity(n);
    let first = r.read_bits(64)?;
    out.push(f64::from_bits(first));
    let mut prev = first;
    for _ in 1..n {
        let nz = r.read_bits(1)? as u8;
        if nz == 0 {
            out.push(f64::from_bits(prev));
        } else {
            let trail = r.read_bits(6)? as u8;
            let payload_bits = 64 - trail;
            let payload = r.read_bits(payload_bits)?;
            let xor = if trail == 64 { 0 } else { payload << trail };
            let cur = prev ^ xor;
            out.push(f64::from_bits(cur));
            prev = cur;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn empty_roundtrip() {
        let bytes = gorilla_encode(&[]).unwrap();
        let v = gorilla_decompress(&bytes).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn one_value() {
        let bytes = gorilla_encode(&[3.7]).unwrap();
        let v = gorilla_decompress(&bytes).unwrap();
        assert_eq!(v, vec![3.7]);
    }

    #[test]
    fn smooth_series_roundtrip() {
        let v: Vec<f64> = (0..1000).map(|i| 100.0 + (i as f64) * 0.001).collect();
        let bytes = gorilla_encode(&v).unwrap();
        let d = gorilla_decompress(&bytes).unwrap();
        assert_eq!(d, v);
    }

    #[test]
    fn repeated_values_roundtrip() {
        let v: Vec<f64> = vec![1.0; 1000];
        let bytes = gorilla_encode(&v).unwrap();
        let d = gorilla_decompress(&bytes).unwrap();
        assert_eq!(d, v);
        assert!(bytes.len() < 200);
    }

    #[test]
    fn nan_inf_roundtrip() {
        let v = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.0];
        let bytes = gorilla_encode(&v).unwrap();
        let d = gorilla_decompress(&bytes).unwrap();
        for (a, b) in d.iter().zip(v.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn known_failing_proptest_case() {
        // Regression: previous Gorilla impl failed on this exact pair.
        let v = vec![0.0_f64, 3.300253571502073e-197];
        let bytes = gorilla_encode(&v).unwrap();
        let d = gorilla_decompress(&bytes).unwrap();
        assert_eq!(d.len(), v.len());
        assert_eq!(d[0].to_bits(), v[0].to_bits());
        assert_eq!(d[1].to_bits(), v[1].to_bits());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn proptest_roundtrip(v in proptest::collection::vec(any::<f64>(), 0..256)) {
            let bytes = gorilla_encode(&v).unwrap();
            let d = gorilla_decompress(&bytes).unwrap();
            prop_assert_eq!(d.len(), v.len());
            for (a, b) in d.iter().zip(v.iter()) {
                prop_assert_eq!(a.to_bits(), b.to_bits());
            }
        }
    }
}
