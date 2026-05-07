//! Open and query a vector index blob. We rebuild the HNSW from raw vectors
//! at open time. This makes cold open O(n_vectors * log) but the resulting
//! handle is stable and simpler than juggling `hnsw_rs::HnswIo` lifetimes.

use hnsw_rs::anndists::dist::DistL2;
use hnsw_rs::hnsw::Hnsw;
use roaring::RoaringBitmap;

use zen_common::{ZenError, ZenResult};

use crate::build::parse_blob;

#[derive(Clone, Debug)]
pub struct KnnHit {
    pub row_idx: u32,
    pub distance: f32,
}

pub struct HnswHandle {
    pub hnsw: Hnsw<'static, f32, DistL2>,
    pub dimensions: usize,
    pub n_vectors: usize,
    /// All vectors retained for brute-force hybrid path.
    pub all_vectors: Vec<(u32, Vec<f32>)>,
}

pub fn open_hnsw_index(blob: &[u8]) -> ZenResult<HnswHandle> {
    let (header, rids, vectors) = parse_blob(blob)?;

    let hnsw = Hnsw::<f32, DistL2>::new(
        header.m,
        header.max_elements.max(vectors.len()),
        16,
        header.ef_construction,
        DistL2 {},
    );
    for (i, v) in vectors.iter().enumerate() {
        hnsw.insert((v.as_slice(), rids[i] as usize));
    }
    let all_vectors: Vec<(u32, Vec<f32>)> = rids.iter().zip(vectors.into_iter()).map(|(r, v)| (*r, v)).collect();
    Ok(HnswHandle {
        hnsw,
        dimensions: header.dimensions,
        n_vectors: header.n,
        all_vectors,
    })
}

#[derive(Clone, Debug)]
pub struct HybridResult {
    pub hits: Vec<KnnHit>,
    pub used_brute_force: bool,
}

impl HnswHandle {
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> ZenResult<Vec<KnnHit>> {
        if query.len() != self.dimensions {
            return Err(ZenError::invalid(format!(
                "query dim {} != index dim {}",
                query.len(),
                self.dimensions
            )));
        }
        let neighbours = self.hnsw.search(query, k, ef);
        Ok(neighbours
            .into_iter()
            .map(|n| KnnHit {
                row_idx: n.get_origin_id() as u32,
                distance: n.distance,
            })
            .collect())
    }

    pub fn hybrid_search(
        &self,
        query: &[f32],
        mask: &RoaringBitmap,
        k: usize,
        ef: usize,
    ) -> ZenResult<HybridResult> {
        let n = self.n_vectors as u64;
        let m = mask.len();
        let selective_threshold = (n / 100).max(1);

        if m <= selective_threshold {
            // Brute force.
            let mut scored: Vec<KnnHit> = self
                .all_vectors
                .iter()
                .filter(|(rid, _)| mask.contains(*rid))
                .map(|(rid, v)| KnnHit {
                    row_idx: *rid,
                    distance: l2(query, v),
                })
                .collect();
            scored.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
            scored.truncate(k);
            return Ok(HybridResult {
                hits: scored,
                used_brute_force: true,
            });
        }
        let larger_k = (k * 8).max(64);
        let neighbours = self.hnsw.search(query, larger_k, ef.max(larger_k));
        let mut hits: Vec<KnnHit> = neighbours
            .into_iter()
            .filter_map(|n| {
                let rid = n.get_origin_id() as u32;
                if mask.contains(rid) {
                    Some(KnnHit {
                        row_idx: rid,
                        distance: n.distance,
                    })
                } else {
                    None
                }
            })
            .collect();
        hits.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(k);
        Ok(HybridResult {
            hits,
            used_brute_force: false,
        })
    }
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{build_hnsw_index, BuildOptions};
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn random_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..n)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
            .collect()
    }

    #[test]
    fn build_open_search_roundtrip() {
        let dim = 64;
        let n = 200;
        let vectors = random_vectors(n, dim, 11);
        let row_indices: Vec<u32> = (0..n as u32).collect();
        let opts = BuildOptions {
            dimensions: dim,
            m: 8,
            ef_construction: 64,
            max_elements: n,
        };
        let res = build_hnsw_index(&vectors, &row_indices, &opts).unwrap();
        let handle = open_hnsw_index(&res.blob).unwrap();
        let q = vectors[7].clone();
        let hits = handle.search(&q, 5, 32).unwrap();
        assert_eq!(hits[0].row_idx, 7);
        assert!(hits[0].distance < 1e-3);
    }

    #[test]
    fn recall_at_10_above_threshold() {
        let dim = 64;
        let n = 1_000;
        let vectors = random_vectors(n, dim, 7);
        let row_indices: Vec<u32> = (0..n as u32).collect();
        let opts = BuildOptions {
            dimensions: dim,
            m: 16,
            ef_construction: 200,
            max_elements: n,
        };
        let res = build_hnsw_index(&vectors, &row_indices, &opts).unwrap();
        let handle = open_hnsw_index(&res.blob).unwrap();

        let mut total_recall = 0.0;
        let k = 10;
        for q_id in 0..30 {
            let q = vectors[q_id].clone();
            let hits = handle.search(&q, k, 64).unwrap();
            let mut bf: Vec<(usize, f32)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (i, l2(&q, v)))
                .collect();
            bf.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            bf.truncate(k);
            let bf_set: std::collections::HashSet<u32> =
                bf.iter().map(|(i, _)| *i as u32).collect();
            let hit_set: std::collections::HashSet<u32> =
                hits.iter().map(|h| h.row_idx).collect();
            let common = bf_set.intersection(&hit_set).count();
            total_recall += common as f64 / k as f64;
        }
        let recall = total_recall / 30.0;
        assert!(recall >= 0.85, "recall@10 too low: {recall}");
    }

    #[test]
    fn hybrid_with_selective_filter() {
        let dim = 32;
        let n = 500;
        let vectors = random_vectors(n, dim, 99);
        let row_indices: Vec<u32> = (0..n as u32).collect();
        let opts = BuildOptions {
            dimensions: dim,
            m: 8,
            ef_construction: 64,
            max_elements: n,
        };
        let res = build_hnsw_index(&vectors, &row_indices, &opts).unwrap();
        let handle = open_hnsw_index(&res.blob).unwrap();

        let q = vectors[42].clone();
        let mut mask = RoaringBitmap::new();
        mask.insert(40);
        mask.insert(42);
        mask.insert(45);

        let res = handle.hybrid_search(&q, &mask, 5, 32).unwrap();
        assert!(res.used_brute_force);
        assert_eq!(res.hits.len(), 3);
        assert_eq!(res.hits[0].row_idx, 42);
    }
}
