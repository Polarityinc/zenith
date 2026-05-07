//! Compactor: merges unsealed WAL files into compacted segments with trace-
//! locality. The trace-locality invariant — every span of a trace lands in one
//! row group — is the second moat. It makes trace-load queries one ranged GET.

pub mod merge;
pub mod build;
pub mod runner;

pub use merge::{merge_wals, MergedRows};
pub use build::{build_segment_from_rows, BuildOptions};
pub use runner::{compact_partition, CompactionStats};
