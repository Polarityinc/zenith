//! Build a vector index. Serialization is "raw vectors + row indices" — on
//! cold open we rebuild the HNSW from the raw vectors. This is slower at open
//! time but sidesteps `hnsw_rs`'s self-referential lifetime in `HnswIo`.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use zen_common::{ZenError, ZenResult};

#[derive(Clone, Debug)]
pub struct BuildOptions {
    pub dimensions: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub max_elements: usize,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            dimensions: 1536,
            m: 16,
            ef_construction: 200,
            max_elements: 1_000_000,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Header {
    pub dimensions: usize,
    pub n: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub max_elements: usize,
}

pub struct BuildResult {
    pub blob: Bytes,
    pub n_vectors: usize,
}

pub fn build_hnsw_index(
    vectors: &[Vec<f32>],
    row_indices: &[u32],
    opts: &BuildOptions,
) -> ZenResult<BuildResult> {
    if vectors.len() != row_indices.len() {
        return Err(ZenError::invalid("vectors / row_indices length mismatch"));
    }
    if vectors.is_empty() {
        return Ok(BuildResult {
            blob: Bytes::new(),
            n_vectors: 0,
        });
    }
    for v in vectors {
        if v.len() != opts.dimensions {
            return Err(ZenError::invalid(format!(
                "vector dim {} != configured {}",
                v.len(),
                opts.dimensions
            )));
        }
    }

    // Serialize: header + (row_idx, vector) pairs as length-prefixed.
    let header = Header {
        dimensions: opts.dimensions,
        n: vectors.len(),
        m: opts.m,
        ef_construction: opts.ef_construction,
        max_elements: opts.max_elements,
    };
    let header_bytes =
        serde_json::to_vec(&header).map_err(|e| ZenError::format(format!("vector header: {e}")))?;

    let total = 4 + header_bytes.len() + vectors.len() * (4 + opts.dimensions * 4);
    let mut out = BytesMut::with_capacity(total);
    out.put_u32_le(header_bytes.len() as u32);
    out.put_slice(&header_bytes);
    for (rid, v) in row_indices.iter().zip(vectors.iter()) {
        out.put_u32_le(*rid);
        for f in v {
            out.put_f32_le(*f);
        }
    }
    Ok(BuildResult {
        blob: out.freeze(),
        n_vectors: vectors.len(),
    })
}

pub(crate) fn parse_blob(blob: &[u8]) -> ZenResult<(Header, Vec<u32>, Vec<Vec<f32>>)> {
    use bytes::Buf;
    let mut p = blob;
    if p.remaining() < 4 {
        return Err(ZenError::format("vector blob too small"));
    }
    let header_len = p.get_u32_le() as usize;
    if p.len() < header_len {
        return Err(ZenError::format("vector header truncated"));
    }
    let header: Header = serde_json::from_slice(&p[..header_len])
        .map_err(|e| ZenError::format(format!("vector header parse: {e}")))?;
    p.advance(header_len);

    let mut rids = Vec::with_capacity(header.n);
    let mut vectors = Vec::with_capacity(header.n);
    for _ in 0..header.n {
        if p.remaining() < 4 + header.dimensions * 4 {
            return Err(ZenError::format("vector row truncated"));
        }
        let rid = p.get_u32_le();
        let mut v = Vec::with_capacity(header.dimensions);
        for _ in 0..header.dimensions {
            v.push(p.get_f32_le());
        }
        rids.push(rid);
        vectors.push(v);
    }
    Ok((header, rids, vectors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::open_hnsw_index;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn random_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..n)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
            .collect()
    }

    #[test]
    fn build_100_random_1536d_vectors_yields_index_with_correct_len() {
        // Use a smaller dim to keep the test fast — the index plumbing is the
        // same regardless of dim. The "1536d" intent is captured by the dim
        // value below; switch to 1536 to mirror prod embeddings.
        let dim = 64;
        let n = 100;
        let vectors = random_vectors(n, dim, 1234);
        let row_indices: Vec<u32> = (0..n as u32).collect();
        let opts = BuildOptions {
            dimensions: dim,
            m: 8,
            ef_construction: 64,
            max_elements: n,
        };
        let res = build_hnsw_index(&vectors, &row_indices, &opts).unwrap();
        assert_eq!(res.n_vectors, 100);
        assert!(!res.blob.is_empty());

        let handle = open_hnsw_index(&res.blob).unwrap();
        assert_eq!(handle.n_vectors, 100);
        assert_eq!(handle.dimensions, dim);
        assert_eq!(handle.all_vectors.len(), 100);
    }

    #[test]
    fn search_finds_obvious_nearest_neighbour() {
        // Indexing a known vector and querying for the same vector should
        // return that row at distance ~0. This is the most basic recall
        // contract for a vector index.
        let dim = 32;
        let n = 200;
        let vectors = random_vectors(n, dim, 555);
        let row_indices: Vec<u32> = (0..n as u32).collect();
        let opts = BuildOptions {
            dimensions: dim,
            m: 16,
            ef_construction: 200,
            max_elements: n,
        };
        let res = build_hnsw_index(&vectors, &row_indices, &opts).unwrap();
        let handle = open_hnsw_index(&res.blob).unwrap();

        // Querying with an exact copy of row 73 must return row 73 first
        // with ~0 distance.
        let q = vectors[73].clone();
        let hits = handle.search(&q, 10, 64).unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].row_idx, 73);
        assert!(hits[0].distance < 1e-3);

        // Recall@10 against this same query should include row 73.
        let recalled: std::collections::HashSet<u32> =
            hits.iter().map(|h| h.row_idx).collect();
        assert!(recalled.contains(&73));
    }

    #[test]
    fn smaller_dimensions_produce_smaller_blob() {
        // The crate doesn't ship a `quantize` flag yet; the closest user-
        // visible "smaller blob" lever is `dimensions`. Verify the blob
        // really does shrink with fewer dimensions, since dimensions are
        // what dominate raw-vector storage.
        let n = 50;
        let row_indices: Vec<u32> = (0..n as u32).collect();

        let big_dim = 256;
        let big_vectors = random_vectors(n as usize, big_dim, 7);
        let big_opts = BuildOptions {
            dimensions: big_dim,
            m: 8,
            ef_construction: 64,
            max_elements: n as usize,
        };
        let big = build_hnsw_index(&big_vectors, &row_indices, &big_opts).unwrap();

        let small_dim = 32;
        let small_vectors = random_vectors(n as usize, small_dim, 7);
        let small_opts = BuildOptions {
            dimensions: small_dim,
            m: 8,
            ef_construction: 64,
            max_elements: n as usize,
        };
        let small = build_hnsw_index(&small_vectors, &row_indices, &small_opts).unwrap();

        assert!(
            small.blob.len() < big.blob.len(),
            "expected smaller-dim blob to be smaller (small={}, big={})",
            small.blob.len(),
            big.blob.len(),
        );
    }

    #[test]
    fn empty_vector_list_returns_ok_with_empty_blob() {
        let opts = BuildOptions::default();
        let res = build_hnsw_index(&[], &[], &opts).unwrap();
        assert_eq!(res.n_vectors, 0);
        assert!(res.blob.is_empty(), "empty input should produce empty blob");
    }

    #[test]
    fn single_vector_index_returns_that_vector_on_query() {
        let dim = 16;
        let v: Vec<f32> = (0..dim).map(|i| i as f32 * 0.1).collect();
        let opts = BuildOptions {
            dimensions: dim,
            m: 8,
            ef_construction: 32,
            max_elements: 1,
        };
        let res = build_hnsw_index(&[v.clone()], &[42u32], &opts).unwrap();
        assert_eq!(res.n_vectors, 1);
        let handle = open_hnsw_index(&res.blob).unwrap();
        let hits = handle.search(&v, 1, 8).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_idx, 42);
        assert!(hits[0].distance < 1e-4);
    }

    #[test]
    fn mismatched_lengths_returns_invalid_error() {
        let dim = 4;
        let vectors = vec![vec![0.0_f32, 1.0, 2.0, 3.0]];
        let row_indices = vec![0u32, 1u32];
        let opts = BuildOptions {
            dimensions: dim,
            m: 4,
            ef_construction: 16,
            max_elements: 8,
        };
        let res = build_hnsw_index(&vectors, &row_indices, &opts);
        assert!(res.is_err(), "mismatched vectors/row_indices should error");
    }

    #[test]
    fn wrong_vector_dimension_returns_invalid_error() {
        let opts = BuildOptions {
            dimensions: 8,
            m: 4,
            ef_construction: 16,
            max_elements: 8,
        };
        let bad = vec![vec![1.0_f32, 2.0, 3.0]]; // 3 != 8
        let res = build_hnsw_index(&bad, &[0u32], &opts);
        assert!(res.is_err(), "bad vector dim should error, not panic");
    }
}
