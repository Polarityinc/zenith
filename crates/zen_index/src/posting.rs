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
    use proptest::prelude::*;
    use std::collections::{BTreeSet, HashSet};

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

    // ---- Additional coverage ------------------------------------------------

    #[test]
    fn posting_map_lookup_returns_expected_bitmap() {
        let pairs: &[(&[u8], u32)] = &[
            (b"alpha", 1),
            (b"beta", 2),
            (b"alpha", 3),
            (b"gamma", 4),
            (b"alpha", 5),
            (b"beta", 6),
        ];
        let mut m = PostingMap::new();
        for (val, row) in pairs {
            m.insert(val, *row);
        }
        let alpha = m.get(b"alpha").expect("alpha posting list missing");
        let alpha_rows: Vec<u32> = alpha.iter().collect();
        assert_eq!(alpha_rows, vec![1, 3, 5]);

        let beta = m.get(b"beta").expect("beta posting list missing");
        let beta_rows: Vec<u32> = beta.iter().collect();
        assert_eq!(beta_rows, vec![2, 6]);

        let gamma = m.get(b"gamma").expect("gamma posting list missing");
        let gamma_rows: Vec<u32> = gamma.iter().collect();
        assert_eq!(gamma_rows, vec![4]);
    }

    #[test]
    fn posting_list_roundtrip_for_serialize_deserialize() {
        let mut pl = PostingList::new();
        for i in 0..1024u32 {
            if i % 7 == 0 {
                pl.push(i);
            }
        }
        let original: Vec<u32> = pl.iter().collect();
        let bytes = pl.serialize().expect("serialize");
        let pl2 = PostingList::deserialize(&bytes).expect("deserialize");
        let restored: Vec<u32> = pl2.iter().collect();
        assert_eq!(original, restored);
        assert_eq!(pl.cardinality(), pl2.cardinality());
    }

    #[test]
    fn posting_list_intersection_is_correct() {
        let mut a = PostingList::new();
        for r in [1u32, 2, 3, 4, 5, 100, 101] {
            a.push(r);
        }
        let mut b = PostingList::new();
        for r in [3u32, 5, 7, 100, 200] {
            b.push(r);
        }
        let mut intersection = a.clone();
        intersection.and_assign(&b);
        let v: Vec<u32> = intersection.iter().collect();
        assert_eq!(v, vec![3, 5, 100]);

        // Symmetric: B AND A is the same set.
        let mut symmetric = b.clone();
        symmetric.and_assign(&a);
        let v2: Vec<u32> = symmetric.iter().collect();
        assert_eq!(v2, vec![3, 5, 100]);
    }

    #[test]
    fn posting_list_union_is_correct() {
        let mut a = PostingList::new();
        for r in [1u32, 3, 5, 7] {
            a.push(r);
        }
        let mut b = PostingList::new();
        for r in [2u32, 3, 4, 5, 6] {
            b.push(r);
        }
        let mut union = a.clone();
        union.or_assign(&b);
        let v: Vec<u32> = union.iter().collect();
        assert_eq!(v, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn posting_map_empty_lookup_returns_none() {
        let m = PostingMap::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
        assert!(m.get(b"never_inserted").is_none());

        // Serializing then deserializing the empty map is still empty.
        let bytes = m.serialize().expect("serialize empty");
        let m2 = PostingMap::deserialize(&bytes).expect("deserialize empty");
        assert!(m2.is_empty());
        assert!(m2.get(b"never_inserted").is_none());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        // Build a PostingMap for a small dictionary of random tags using
        // 1..1024 random row indices, then verify the lookup output for
        // every tag matches the naive groupby.
        #[test]
        fn proptest_random_inserts_match_naive_groupby(
            entries in proptest::collection::vec((0u8..8u8, 0u32..10_000u32), 1..1024)
        ) {
            let mut m = PostingMap::new();
            // Naive ground truth.
            let mut naive: std::collections::HashMap<u8, BTreeSet<u32>> = Default::default();
            for (tag, row) in &entries {
                let value = format!("tag-{tag}");
                m.insert(value.as_bytes(), *row);
                naive.entry(*tag).or_default().insert(*row);
            }

            // Map's distinct value count <= 8 (one per tag).
            prop_assert!(m.len() <= 8);

            for (tag, expected_rows) in &naive {
                let value = format!("tag-{tag}");
                let pl = m.get(value.as_bytes())
                    .unwrap_or_else(|| panic!("missing tag {tag}"));
                let got_rows: HashSet<u32> = pl.iter().collect();
                let exp_rows: HashSet<u32> = expected_rows.iter().copied().collect();
                prop_assert_eq!(got_rows, exp_rows);
            }

            // Round trip through serialize / deserialize must preserve everything.
            let bytes = m.serialize().expect("serialize");
            let m2 = PostingMap::deserialize(&bytes).expect("deserialize");
            prop_assert_eq!(m.len(), m2.len());
            for (tag, expected_rows) in &naive {
                let value = format!("tag-{tag}");
                let pl = m2.get(value.as_bytes()).unwrap();
                let got_rows: HashSet<u32> = pl.iter().collect();
                let exp_rows: HashSet<u32> = expected_rows.iter().copied().collect();
                prop_assert_eq!(got_rows, exp_rows);
            }
        }
    }
}
