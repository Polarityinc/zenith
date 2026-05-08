//! Compactor: merges unsealed WAL files into compacted segments with trace-
//! locality. The trace-locality invariant — every span of a trace lands in one
//! row group — is the second moat. It makes trace-load queries one ranged GET.

pub mod build;
pub mod merge;
pub mod runner;

pub use build::{build_segment_from_iter, build_segment_from_rows, BuildOptions};
pub use merge::{merge_wals, MergedRows};
pub use runner::{compact_full, compact_partition, CompactionStats};
