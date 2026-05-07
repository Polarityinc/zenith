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
    let header_bytes = serde_json::to_vec(&header)
        .map_err(|e| ZenError::format(format!("vector header: {e}")))?;

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
