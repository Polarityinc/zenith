//! Per-row-group sparse index that lives in the catalog. For every row group
//! we record the (min,max) of `trace_id` and `start_time_ms` and the
//! `(min_commit_id, max_commit_id)` window. The catalog uses this to prune row
//! groups before any segment is opened.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use zen_common::ZenError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowGroupKey {
    pub min_trace_id: [u8; 16],
    pub max_trace_id: [u8; 16],
    pub min_start_time: i64,
    pub max_start_time: i64,
    pub min_commit_id: u64,
    pub max_commit_id: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SparseRowGroupIndex {
    pub entries: Vec<RowGroupKey>,
}

impl SparseRowGroupIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, k: RowGroupKey) {
        self.entries.push(k);
    }

    /// Return the indices of row groups whose `start_time` range overlaps `[lo, hi]`.
    pub fn time_range_hits(&self, lo: i64, hi: i64) -> Vec<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, k)| k.max_start_time >= lo && k.min_start_time <= hi)
            .map(|(i, _)| i)
            .collect()
    }

    /// Return the indices of row groups whose trace-id range may contain `tid`.
    pub fn trace_id_hits(&self, tid: &[u8; 16]) -> Vec<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, k)| &k.min_trace_id <= tid && tid <= &k.max_trace_id)
            .map(|(i, _)| i)
            .collect()
    }

    pub fn serialize(&self) -> Result<Bytes, ZenError> {
        let mut out = BytesMut::with_capacity(self.entries.len() * 64);
        out.put_u32_le(self.entries.len() as u32);
        for k in &self.entries {
            out.put_slice(&k.min_trace_id);
            out.put_slice(&k.max_trace_id);
            out.put_i64_le(k.min_start_time);
            out.put_i64_le(k.max_start_time);
            out.put_u64_le(k.min_commit_id);
            out.put_u64_le(k.max_commit_id);
        }
        Ok(out.freeze())
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, ZenError> {
        if input.len() < 4 {
            return Err(ZenError::format("sparse index header truncated"));
        }
        let mut p = input;
        let n = p.get_u32_le() as usize;
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            if p.remaining() < 64 {
                return Err(ZenError::format("sparse index entry truncated"));
            }
            let mut min_trace_id = [0u8; 16];
            let mut max_trace_id = [0u8; 16];
            min_trace_id.copy_from_slice(&p[..16]);
            p.advance(16);
            max_trace_id.copy_from_slice(&p[..16]);
            p.advance(16);
            let min_start_time = p.get_i64_le();
            let max_start_time = p.get_i64_le();
            let min_commit_id = p.get_u64_le();
            let max_commit_id = p.get_u64_le();
            entries.push(RowGroupKey {
                min_trace_id,
                max_trace_id,
                min_start_time,
                max_start_time,
                min_commit_id,
                max_commit_id,
            });
        }
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(min_tid: u8, max_tid: u8, lo: i64, hi: i64) -> RowGroupKey {
        let mut min = [0u8; 16];
        let mut max = [0u8; 16];
        min[0] = min_tid;
        max[0] = max_tid;
        RowGroupKey {
            min_trace_id: min,
            max_trace_id: max,
            min_start_time: lo,
            max_start_time: hi,
            min_commit_id: 0,
            max_commit_id: 0,
        }
    }

    #[test]
    fn time_range_pruning() {
        let mut idx = SparseRowGroupIndex::new();
        idx.push(k(0, 1, 1000, 2000));
        idx.push(k(2, 3, 3000, 4000));
        idx.push(k(4, 5, 5000, 6000));

        assert_eq!(idx.time_range_hits(2500, 3500), vec![1]);
        assert_eq!(idx.time_range_hits(0, 1000), vec![0]);
        assert_eq!(idx.time_range_hits(0, 7000), vec![0, 1, 2]);
        assert_eq!(idx.time_range_hits(2100, 2999), Vec::<usize>::new());
    }

    #[test]
    fn trace_id_pruning() {
        let mut idx = SparseRowGroupIndex::new();
        idx.push(k(0, 1, 0, 0));
        idx.push(k(2, 3, 0, 0));

        let mut tid = [0u8; 16];
        tid[0] = 1;
        assert_eq!(idx.trace_id_hits(&tid), vec![0]);
        tid[0] = 3;
        assert_eq!(idx.trace_id_hits(&tid), vec![1]);
        tid[0] = 4;
        assert_eq!(idx.trace_id_hits(&tid), Vec::<usize>::new());
    }

    #[test]
    fn serialize_roundtrip() {
        let mut idx = SparseRowGroupIndex::new();
        idx.push(k(0, 1, 1000, 2000));
        idx.push(k(2, 3, 3000, 4000));
        let bytes = idx.serialize().unwrap();
        let idx2 = SparseRowGroupIndex::deserialize(&bytes).unwrap();
        assert_eq!(idx.entries, idx2.entries);
    }
}
