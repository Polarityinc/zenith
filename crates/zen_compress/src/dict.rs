//! Dictionary encoding for low-cardinality string columns. The encoder
//! emits a sorted dictionary plus a key array (u8 / u16 / u32 depending on
//! cardinality). The keys can then be LZ4-compressed since they often have
//! long runs.
//!
//! Format:
//! ```text
//! count_rows: u32 le
//! dict_size:  u32 le
//! key_width:  u8 (1 = u8, 2 = u16, 4 = u32)
//! dict_bytes: u32 le
//! dict:       length-prefixed entries (u16 le len + bytes) * dict_size
//! key_bytes:  u32 le (compressed length)
//! key_block:  LZ4-frame compressed key array
//! ```

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use zen_common::ZenError;

pub struct DictBuilder {
    map: HashMap<Vec<u8>, u32>,
    dict: Vec<Vec<u8>>,
    keys: Vec<u32>,
}

impl Default for DictBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DictBuilder {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            dict: Vec::new(),
            keys: Vec::new(),
        }
    }

    pub fn push(&mut self, value: &[u8]) {
        let id = match self.map.get(value) {
            Some(&id) => id,
            None => {
                let id = self.dict.len() as u32;
                self.dict.push(value.to_vec());
                self.map.insert(value.to_vec(), id);
                id
            }
        };
        self.keys.push(id);
    }

    pub fn dict_size(&self) -> usize {
        self.dict.len()
    }

    pub fn finish(self) -> Result<Bytes, ZenError> {
        let row_count = self.keys.len();
        let dict_size = self.dict.len();
        let key_width: u8 = if dict_size <= u8::MAX as usize + 1 {
            1
        } else if dict_size <= u16::MAX as usize + 1 {
            2
        } else {
            4
        };

        // Serialize dict
        let mut dict_buf = Vec::with_capacity(dict_size * 16);
        for entry in &self.dict {
            if entry.len() > u16::MAX as usize {
                return Err(ZenError::compress(format!(
                    "dict entry length {} exceeds u16",
                    entry.len()
                )));
            }
            dict_buf.put_u16_le(entry.len() as u16);
            dict_buf.put_slice(entry);
        }

        // Serialize raw keys
        let mut raw_keys = Vec::with_capacity(row_count * key_width as usize);
        for k in &self.keys {
            match key_width {
                1 => raw_keys.put_u8(*k as u8),
                2 => raw_keys.put_u16_le(*k as u16),
                4 => raw_keys.put_u32_le(*k),
                _ => unreachable!(),
            }
        }
        let compressed = lz4_flex::compress_prepend_size(&raw_keys);

        let mut out = BytesMut::with_capacity(
            4 + 4 + 1 + 4 + dict_buf.len() + 4 + compressed.len(),
        );
        out.put_u32_le(row_count as u32);
        out.put_u32_le(dict_size as u32);
        out.put_u8(key_width);
        out.put_u32_le(dict_buf.len() as u32);
        out.put_slice(&dict_buf);
        out.put_u32_le(compressed.len() as u32);
        out.put_slice(&compressed);
        Ok(out.freeze())
    }
}

pub struct DictDecoder {
    pub row_count: usize,
    pub dict: Vec<Vec<u8>>,
    pub keys: Vec<u32>,
}

impl DictDecoder {
    pub fn open(input: &[u8]) -> Result<Self, ZenError> {
        if input.len() < 13 {
            return Err(ZenError::compress("dict input too short"));
        }
        let mut p = input;
        let row_count = p.get_u32_le() as usize;
        let dict_size = p.get_u32_le() as usize;
        let key_width = p.get_u8();
        let dict_bytes = p.get_u32_le() as usize;
        if p.len() < dict_bytes + 4 {
            return Err(ZenError::compress("dict bytes truncated"));
        }
        let mut dp = &p[..dict_bytes];
        p.advance(dict_bytes);
        let mut dict = Vec::with_capacity(dict_size);
        for _ in 0..dict_size {
            if dp.remaining() < 2 {
                return Err(ZenError::compress("dict entry truncated"));
            }
            let len = dp.get_u16_le() as usize;
            if dp.remaining() < len {
                return Err(ZenError::compress("dict entry body truncated"));
            }
            let mut entry = vec![0u8; len];
            entry.copy_from_slice(&dp[..len]);
            dp.advance(len);
            dict.push(entry);
        }
        let compressed_len = p.get_u32_le() as usize;
        if p.len() < compressed_len {
            return Err(ZenError::compress("dict key block truncated"));
        }
        let raw_keys = lz4_flex::decompress_size_prepended(&p[..compressed_len])
            .map_err(|e| ZenError::compress(format!("lz4 decompress: {e}")))?;
        let mut kp: &[u8] = &raw_keys;
        let mut keys = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let k = match key_width {
                1 => {
                    if kp.is_empty() {
                        return Err(ZenError::compress("dict keys truncated"));
                    }
                    let v = kp[0] as u32;
                    kp = &kp[1..];
                    v
                }
                2 => {
                    if kp.len() < 2 {
                        return Err(ZenError::compress("dict keys truncated"));
                    }
                    let v = u16::from_le_bytes(kp[..2].try_into().unwrap()) as u32;
                    kp = &kp[2..];
                    v
                }
                4 => {
                    if kp.len() < 4 {
                        return Err(ZenError::compress("dict keys truncated"));
                    }
                    let v = u32::from_le_bytes(kp[..4].try_into().unwrap());
                    kp = &kp[4..];
                    v
                }
                w => return Err(ZenError::compress(format!("unsupported key width {w}"))),
            };
            keys.push(k);
        }
        Ok(Self {
            row_count,
            dict,
            keys,
        })
    }

    pub fn row(&self, idx: usize) -> Result<&[u8], ZenError> {
        if idx >= self.row_count {
            return Err(ZenError::compress("dict row idx out of range"));
        }
        let key = self.keys[idx] as usize;
        if key >= self.dict.len() {
            return Err(ZenError::compress("dict key out of range"));
        }
        Ok(&self.dict[key])
    }

    pub fn iter_rows(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.keys.iter().map(move |k| self.dict[*k as usize].as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_roundtrip() {
        let mut b = DictBuilder::new();
        for v in &["gpt-4o", "claude-sonnet-4-7", "gpt-4o", "gpt-4o", "gpt-5-mini"] {
            b.push(v.as_bytes());
        }
        let bytes = b.finish().unwrap();
        let dec = DictDecoder::open(&bytes).unwrap();
        assert_eq!(dec.row_count, 5);
        assert_eq!(dec.dict.len(), 3);
        assert_eq!(dec.row(0).unwrap(), b"gpt-4o");
        assert_eq!(dec.row(1).unwrap(), b"claude-sonnet-4-7");
        assert_eq!(dec.row(2).unwrap(), b"gpt-4o");
    }

    #[test]
    fn high_cardinality_uses_u32() {
        let mut b = DictBuilder::new();
        for i in 0..70_000 {
            b.push(format!("v{i}").as_bytes());
        }
        let bytes = b.finish().unwrap();
        let dec = DictDecoder::open(&bytes).unwrap();
        assert_eq!(dec.dict.len(), 70_000);
    }

    #[test]
    fn empty_roundtrip() {
        let b = DictBuilder::new();
        let bytes = b.finish().unwrap();
        let dec = DictDecoder::open(&bytes).unwrap();
        assert_eq!(dec.row_count, 0);
        assert!(dec.dict.is_empty());
    }
}
