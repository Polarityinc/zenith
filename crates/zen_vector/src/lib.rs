//! Per-segment HNSW vector index with hybrid filter+KNN.
//!
//! At compaction we build an HNSW graph over the segment's `embedding` column
//! using `hnsw_rs`, dump it to two files, and bundle those into a single byte
//! blob for embedding in the segment.
//!
//! At query time:
//!  - Pure KNN: `Hnsw::search`.
//!  - Hybrid (filter + KNN): if filter is broad (≥1% of segment), use
//!    `Hnsw::search_filter`. If filter is very selective (<1%), fall back to
//!    brute-force over the surviving rows.

pub mod build;
pub mod query;

pub use build::{build_hnsw_index, BuildOptions, BuildResult};
pub use query::{open_hnsw_index, HnswHandle, HybridResult, KnnHit};
