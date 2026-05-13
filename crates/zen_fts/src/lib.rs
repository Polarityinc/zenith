//! Tantivy-as-a-library FTS that embeds the index inline in our segment file.
//!
//! At compaction we build a per-segment Tantivy index over the configured text
//! columns (default: prompt, completion, tool_io_text), serialize the entire
//! Tantivy directory into one byte blob, and stash that blob in the segment's
//! inline-indexes section. At query time we reverse the serialization, open
//! the index against a temp dir, and run BM25 queries that produce
//! `RoaringBitmap`s of segment row indices.

pub mod build;
pub mod query;

pub use build::{build_fts_index, BuildOptions, BuildResult};
pub use query::{open_fts_index, FtsHandle, FtsQuery};
