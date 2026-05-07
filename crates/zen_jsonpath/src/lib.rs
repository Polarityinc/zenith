//! JSON-path indexing and scan fallback.
//!
//! At compaction we walk a sample of JSON documents to discover frequently-occurring
//! paths (e.g. `metadata.user_id`, `metadata.output.steps[].name`). For each
//! path that meets the presence threshold, we build a roaring posting list
//! keyed on `(path_id, value_hash)` for scalars; a Bloom filter when the path's
//! cardinality is too high.
//!
//! At query time, an indexed path resolves to a posting list lookup. An
//! unindexed path falls back to a vectorized scan over the raw JSON (we still
//! beat naive parse-the-whole-blob).

pub mod discovery;
pub mod index;
pub mod scan;

pub use discovery::{discover_paths, DiscoveredPath, DiscoveryConfig};
pub use index::{JsonPathIndex, JsonPathIndexBuilder};
pub use scan::scan_path_eq;
