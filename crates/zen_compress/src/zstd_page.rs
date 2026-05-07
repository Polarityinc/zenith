//! Thin ZSTD wrapper for page-level compression. Used after FSST as a second-pass
//! wrap and as a standalone codec for already-binary-ish data (e.g. metadata JSON).

use bytes::Bytes;
use zen_common::ZenError;

/// Default compression level. 3 is a good speed/ratio tradeoff and the default
/// level used by Brainstore, Husky, and other observability backends.
pub const DEFAULT_LEVEL: i32 = 3;

pub fn zstd_compress(input: &[u8], level: i32) -> Result<Bytes, ZenError> {
    zstd::stream::encode_all(input, level)
        .map(Bytes::from)
        .map_err(|e| ZenError::compress(format!("zstd encode: {e}")))
}

pub fn zstd_decompress(input: &[u8]) -> Result<Bytes, ZenError> {
    zstd::stream::decode_all(input)
        .map(Bytes::from)
        .map_err(|e| ZenError::compress(format!("zstd decode: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_random_bytes() {
        let mut data = Vec::with_capacity(8192);
        for i in 0..8192 {
            data.push((i % 251) as u8);
        }
        let c = zstd_compress(&data, 3).unwrap();
        let d = zstd_decompress(&c).unwrap();
        assert_eq!(&d[..], &data[..]);
    }

    #[test]
    fn empty_roundtrip() {
        let c = zstd_compress(&[], 3).unwrap();
        let d = zstd_decompress(&c).unwrap();
        assert!(d.is_empty());
    }

    #[test]
    fn compresses_repetitive_bytes() {
        let data = vec![b'A'; 4096];
        let c = zstd_compress(&data, 3).unwrap();
        assert!(c.len() < data.len() / 10);
    }

    #[test]
    fn invalid_input_errors() {
        let bad = vec![0xFF, 0xFE, 0xFD, 0xFC];
        assert!(zstd_decompress(&bad).is_err());
    }
}
