//! Roaring posting lists used to index low- and medium-cardinality columns.
//!
//! At segment-build time, the compactor groups row indices by `(column,
//! value_hash)`, builds one `RoaringBitmap` per group, and serializes them
//! into the segment's inline-indexes section. At query time, the executor
//! reads the requested posting lists and ANDs / ORs them to produce a final
//! row mask.

use std::collections::HashMap;
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use roaring::RoaringBitmap;
use xxhash_rust::xxh3::xxh3_64;

use zen_common::ZenError;

/// One posting list, keyed implicitly by the position in the parent map.
#[derive(Default, Clone)]
pub struct PostingList {
    pub bitmap: RoaringBitmap,
}

impl PostingList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, row: u32) {
        self.bitmap.insert(row);
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    pub fn serialize(&self) -> Result<Bytes, ZenError> {
        let mut buf = Vec::with_capacity(self.bitmap.serialized_size());
        self.bitmap
            .serialize_into(&mut buf)
            .map_err(|e| ZenError::format(format!("roaring serialize: {e}")))?;
        Ok(Bytes::from(buf))
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, ZenError> {
        let bitmap = RoaringBitmap::deserialize_from(Cursor::new(input))
            .map_err(|e| ZenError::format(format!("roaring deserialize: {e}")))?;
        Ok(Self { bitmap })
    }

    /// Bitwise AND in place with another posting list.
    pub fn and_assign(&mut self, other: &PostingList) {
        self.bitmap &= &other.bitmap;
    }

    /// Bitwise OR in place with another posting list.
    pub fn or_assign(&mut self, other: &PostingList) {
        self.bitmap |= &other.bitmap;
    }

    /// Iterate the row indices in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.bitmap.iter()
    }
}

/// Map from a 64-bit value hash to a posting list. Stored on disk as a length-
/// prefixed sequence of (hash, serialized_bitmap_bytes).
#[derive(Default, Clone)]
pub struct PostingMap {
    /// `xxh3_64(value)` → posting list.
    pub by_hash: HashMap<u64, PostingList>,
}

impl PostingMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push `row` onto the posting list for `value`. The byte slice must be
    /// the canonical encoding of the value (e.g. UTF-8 for strings).
    pub fn insert(&mut self, value: &[u8], row: u32) {
        let h = xxh3_64(value);
        self.by_hash.entry(h).or_default().push(row);
    }

    pub fn get(&self, value: &[u8]) -> Option<&PostingList> {
        self.by_hash.get(&xxh3_64(value))
    }

    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty()
    }

    /// Serialize the entire map. Format:
    /// ```text
    /// num_lists: u32 le
    /// for each list:
    ///   hash: u64 le
    ///   bitmap_size: u32 le
    ///   bitmap_bytes: <bitmap_size bytes>
    /// ```
    pub fn serialize(&self) -> Result<Bytes, ZenError> {
        let mut out = BytesMut::new();
        out.put_u32_le(self.by_hash.len() as u32);

        let mut hashes: Vec<u64> = self.by_hash.keys().copied().collect();
        hashes.sort_unstable();

        for h in hashes {
            let pl = &self.by_hash[&h];
            let bytes = pl.serialize()?;
            out.put_u64_le(h);
            out.put_u32_le(bytes.len() as u32);
            out.put_slice(&bytes);
        }
        Ok(out.freeze())
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, ZenError> {
        let mut p = input;
        if p.remaining() < 4 {
            return Err(ZenError::format("posting map header truncated"));
        }
        let num_lists = p.get_u32_le() as usize;
        let mut by_hash = HashMap::with_capacity(num_lists);
        for _ in 0..num_lists {
            if p.remaining() < 12 {
                return Err(ZenError::format("posting map entry header truncated"));
            }
            let h = p.get_u64_le();
            let size = p.get_u32_le() as usize;
            if p.remaining() < size {
                return Err(ZenError::format("posting map bitmap truncated"));
            }
            let pl = PostingList::deserialize(&p[..size])?;
            p.advance(size);
            by_hash.insert(h, pl);
        }
        Ok(Self { by_hash })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posting_list_roundtrip() {
        let mut pl = PostingList::new();
        for i in &[3u32, 5, 7, 11, 13, 17, 100, 1000, 2_000_000] {
            pl.push(*i);
        }
        let bytes = pl.serialize().unwrap();
        let pl2 = PostingList::deserialize(&bytes).unwrap();
        assert_eq!(pl2.cardinality(), 9);
        let v: Vec<u32> = pl2.iter().collect();
        assert_eq!(v, vec![3, 5, 7, 11, 13, 17, 100, 1000, 2_000_000]);
    }

    #[test]
    fn posting_map_index_and_serialize() {
        let values = ["gpt-4o", "claude", "gpt-4o", "haiku", "claude", "gpt-4o"];
        let mut m = PostingMap::new();
        for (i, v) in values.iter().enumerate() {
            m.insert(v.as_bytes(), i as u32);
        }
        assert_eq!(m.len(), 3);
        assert_eq!(m.get(b"gpt-4o").unwrap().cardinality(), 3);

        let bytes = m.serialize().unwrap();
        let m2 = PostingMap::deserialize(&bytes).unwrap();
        assert_eq!(m2.len(), 3);
        let pl = m2.get(b"gpt-4o").unwrap();
        let rows: Vec<u32> = pl.iter().collect();
        assert_eq!(rows, vec![0, 2, 5]);
    }

    #[test]
    fn and_or_correct() {
        let mut a = PostingList::new();
        for i in &[1u32, 2, 3, 4, 5] {
            a.push(*i);
        }
        let mut b = PostingList::new();
        for i in &[3u32, 4, 5, 6, 7] {
            b.push(*i);
        }
        let mut c = a.clone();
        c.and_assign(&b);
        let v: Vec<u32> = c.iter().collect();
        assert_eq!(v, vec![3, 4, 5]);
        let mut d = a.clone();
        d.or_assign(&b);
        let v: Vec<u32> = d.iter().collect();
        assert_eq!(v, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn matches_naive_scan_for_low_cardinality() {
        let n = 100_000u32;
        let mut m = PostingMap::new();
        // 5 distinct values, deterministic distribution.
        let pool = ["A", "B", "C", "D", "E"];
        for i in 0..n {
            let val = pool[(i as usize) % pool.len()];
            m.insert(val.as_bytes(), i);
        }

        // Naive: collect all rows where value == "C".
        let naive: Vec<u32> = (0..n).filter(|i| (*i as usize) % pool.len() == 2).collect();
        let pl = m.get(b"C").unwrap();
        let posting: Vec<u32> = pl.iter().collect();
        assert_eq!(naive, posting);
    }
}
