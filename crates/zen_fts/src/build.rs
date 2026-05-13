//! Build a Tantivy index for the rows of one segment, then serialize the index
//! directory into a single contiguous byte blob.
//!
//! Serialization format:
//! ```text
//! [u32 le manifest_len][manifest_bytes][file_1_bytes ...][file_n_bytes]
//! ```
//! Manifest is JSON: `Vec<{name: String, offset: u64, length: u64}>`.

use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tantivy::schema::{Schema as TSchema, FAST, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};
use tempfile::TempDir;

use zen_common::{ZenError, ZenResult};

#[derive(Clone, Debug)]
pub struct BuildOptions {
    /// Field names being indexed for FTS, in priority order.
    pub field_names: Vec<String>,
    /// Memory budget for the writer (bytes). Tantivy default is 50 MB.
    pub writer_memory_bytes: usize,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            field_names: vec!["prompt".into(), "completion".into(), "tool_io_text".into()],
            writer_memory_bytes: 50_000_000,
        }
    }
}

pub struct BuildResult {
    /// Serialized blob suitable for embedding in a segment.
    pub blob: Bytes,
    pub doc_count: usize,
}

/// Field accessor: per row, returns `Option<&str>` for each of the configured fields,
/// in the same order as `BuildOptions::field_names`.
pub trait FieldAccessor {
    fn field(&self, row: usize, field_idx: usize) -> Option<&str>;
    fn row_count(&self) -> usize;
}

#[derive(Serialize, Deserialize)]
struct ManifestEntry {
    name: String,
    offset: u64,
    length: u64,
}

pub fn build_fts_index<A: FieldAccessor>(
    accessor: &A,
    opts: &BuildOptions,
) -> ZenResult<BuildResult> {
    if opts.field_names.is_empty() {
        return Err(ZenError::invalid("FTS requires at least one field"));
    }
    let mut sb = TSchema::builder();
    let mut field_handles = Vec::new();
    for name in &opts.field_names {
        let f = sb.add_text_field(name, TEXT);
        field_handles.push(f);
    }
    // Add a synthetic row index so BM25 result → row mask. FAST lets the
    // query path read row ids from Tantivy's columnstore instead of fetching
    // and decoding stored documents for every hit.
    let row_idx_field = sb.add_u64_field("__row_idx", FAST | STORED);
    let schema = sb.build();

    // Write to a temp dir.
    let dir = TempDir::new().map_err(|e| ZenError::format(format!("temp dir: {e}")))?;
    let index = Index::create_in_dir(dir.path(), schema.clone())
        .map_err(|e| ZenError::format(format!("create index: {e}")))?;
    let mut writer: IndexWriter = index
        .writer(opts.writer_memory_bytes)
        .map_err(|e| ZenError::format(format!("writer: {e}")))?;

    let n = accessor.row_count();
    for row in 0..n {
        let mut td = tantivy::TantivyDocument::default();
        td.add_u64(row_idx_field, row as u64);
        for (fi, fh) in field_handles.iter().enumerate() {
            if let Some(text) = accessor.field(row, fi) {
                td.add_text(*fh, text);
            }
        }
        writer
            .add_document(td)
            .map_err(|e| ZenError::format(format!("add doc: {e}")))?;
    }
    writer
        .commit()
        .map_err(|e| ZenError::format(format!("commit: {e}")))?;
    drop(writer);

    let blob = serialize_dir(dir.path())?;
    Ok(BuildResult { blob, doc_count: n })
}

pub fn serialize_dir(path: &Path) -> ZenResult<Bytes> {
    // Collect files (one level deep is sufficient for Tantivy index dirs).
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let mut stack = vec![path.to_path_buf()];
    while let Some(d) = stack.pop() {
        for ent in
            std::fs::read_dir(&d).map_err(|e| ZenError::format(format!("read_dir {d:?}: {e}")))?
        {
            let ent = ent.map_err(|e| ZenError::format(format!("dir entry: {e}")))?;
            let p = ent.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                let rel = p
                    .strip_prefix(path)
                    .map_err(|e| ZenError::format(format!("strip_prefix: {e}")))?
                    .to_string_lossy()
                    .into_owned();
                let bytes = std::fs::read(&p)
                    .map_err(|e| ZenError::format(format!("read file {p:?}: {e}")))?;
                entries.push((rel, bytes));
            }
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Build manifest + payload
    let mut manifest = Vec::with_capacity(entries.len());
    let mut payload = Vec::new();
    for (name, bytes) in entries {
        if is_tantivy_lock_file(&name) {
            continue;
        }
        manifest.push(ManifestEntry {
            name,
            offset: payload.len() as u64,
            length: bytes.len() as u64,
        });
        payload.extend_from_slice(&bytes);
    }
    let manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|e| ZenError::format(format!("manifest serialize: {e}")))?;

    let mut out = BytesMut::with_capacity(4 + manifest_bytes.len() + payload.len());
    out.put_u32_le(manifest_bytes.len() as u32);
    out.put_slice(&manifest_bytes);
    out.put_slice(&payload);
    Ok(out.freeze())
}

fn is_tantivy_lock_file(path: &str) -> bool {
    matches!(path, ".tantivy-writer.lock" | ".tantivy-meta.lock")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{open_fts_index, FtsQuery};

    /// Two-field corpus: column 0 is "prompt", column 1 is "completion".
    struct TwoFieldCorpus {
        rows: Vec<(Option<String>, Option<String>)>,
    }
    impl FieldAccessor for TwoFieldCorpus {
        fn field(&self, row: usize, field_idx: usize) -> Option<&str> {
            let r = &self.rows[row];
            match field_idx {
                0 => r.0.as_deref(),
                1 => r.1.as_deref(),
                _ => None,
            }
        }
        fn row_count(&self) -> usize {
            self.rows.len()
        }
    }

    /// Single-field corpus, useful for the multi-field-vs-single-field tests.
    struct SingleField {
        rows: Vec<String>,
    }
    impl FieldAccessor for SingleField {
        fn field(&self, row: usize, field_idx: usize) -> Option<&str> {
            if field_idx != 0 {
                return None;
            }
            Some(&self.rows[row])
        }
        fn row_count(&self) -> usize {
            self.rows.len()
        }
    }

    fn fifty_doc_corpus() -> SingleField {
        // 50 docs, every 5th one carries the marker word "alpha" so we can
        // verify search after reopen; everything else is filler.
        let rows: Vec<String> = (0..50)
            .map(|i| {
                if i % 5 == 0 {
                    format!("doc {i} contains alpha word")
                } else {
                    format!("doc {i} contains beta filler")
                }
            })
            .collect();
        SingleField { rows }
    }

    #[test]
    fn build_50_docs_returns_nonempty_blob() {
        let acc = fifty_doc_corpus();
        let opts = BuildOptions {
            field_names: vec!["prompt".into()],
            writer_memory_bytes: 15_000_000,
        };
        let res = build_fts_index(&acc, &opts).expect("build fts");
        assert_eq!(res.doc_count, 50);
        assert!(
            !res.blob.is_empty(),
            "serialized FTS blob must be non-empty"
        );
        assert!(
            res.blob.len() > 200,
            "blob should at least contain a manifest + segment files"
        );
    }

    #[test]
    fn reopened_blob_supports_search() {
        let acc = fifty_doc_corpus();
        let opts = BuildOptions {
            field_names: vec!["prompt".into()],
            writer_memory_bytes: 15_000_000,
        };
        let res = build_fts_index(&acc, &opts).expect("build");
        let handle = open_fts_index(&res.blob).expect("open");

        let q = FtsQuery {
            field: Some("prompt"),
            query: "alpha",
            limit: 100,
        };
        let bm = handle.search_to_bitmap(&q).expect("search");
        // Every 5th row was marked; expect 10 hits over 50 docs.
        let mut hits: Vec<u32> = bm.iter().collect();
        hits.sort_unstable();
        assert_eq!(hits.len(), 10);
        assert_eq!(hits, vec![0, 5, 10, 15, 20, 25, 30, 35, 40, 45]);
    }

    #[test]
    fn empty_doc_list_builds_valid_empty_index() {
        let acc = SingleField { rows: vec![] };
        let opts = BuildOptions {
            field_names: vec!["prompt".into()],
            writer_memory_bytes: 15_000_000,
        };
        let res = build_fts_index(&acc, &opts).expect("build empty");
        assert_eq!(res.doc_count, 0);
        assert!(!res.blob.is_empty(), "even empty index has manifest bytes");

        // The blob must be openable, and any query yields zero hits.
        let handle = open_fts_index(&res.blob).expect("open empty");
        let q = FtsQuery {
            field: Some("prompt"),
            query: "anything",
            limit: 10,
        };
        let bm = handle.search_to_bitmap(&q).expect("search empty");
        assert_eq!(bm.len(), 0);
    }

    #[test]
    fn multi_field_index_makes_each_field_searchable() {
        // Distinct vocabulary in each field so we can confirm each field is
        // independently queryable after build + reopen.
        let acc = TwoFieldCorpus {
            rows: vec![
                (
                    Some("prompt mentions ratelimit".into()),
                    Some("completion mentions oom".into()),
                ),
                (
                    Some("prompt about latency".into()),
                    Some("completion about throughput".into()),
                ),
                (
                    Some("prompt about ratelimit again".into()),
                    Some("completion about latency".into()),
                ),
            ],
        };
        let opts = BuildOptions {
            field_names: vec!["prompt".into(), "completion".into()],
            writer_memory_bytes: 15_000_000,
        };
        let res = build_fts_index(&acc, &opts).expect("build");
        let handle = open_fts_index(&res.blob).expect("open");
        // Both field names visible.
        assert!(handle.field_names.contains(&"prompt".to_string()));
        assert!(handle.field_names.contains(&"completion".to_string()));

        // Search "ratelimit" in prompt: rows 0 and 2.
        let q1 = FtsQuery {
            field: Some("prompt"),
            query: "ratelimit",
            limit: 10,
        };
        let mut h1: Vec<u32> = handle
            .search_to_bitmap(&q1)
            .expect("search prompt")
            .iter()
            .collect();
        h1.sort_unstable();
        assert_eq!(h1, vec![0, 2]);

        // Search "oom" in completion: row 0 only.
        let q2 = FtsQuery {
            field: Some("completion"),
            query: "oom",
            limit: 10,
        };
        let h2: Vec<u32> = handle
            .search_to_bitmap(&q2)
            .expect("search completion")
            .iter()
            .collect();
        assert_eq!(h2, vec![0]);
    }

    #[test]
    fn default_build_options_are_sensible() {
        let opts = BuildOptions::default();
        assert!(
            !opts.field_names.is_empty(),
            "default field_names must be non-empty"
        );
        // The default schema includes "prompt" and "completion" by name.
        assert!(opts.field_names.iter().any(|f| f == "prompt"));
        assert!(opts.field_names.iter().any(|f| f == "completion"));
        assert!(
            opts.writer_memory_bytes >= 15_000_000,
            "writer memory must clear Tantivy's per-thread minimum"
        );
    }

    #[test]
    fn missing_field_names_returns_error() {
        let acc = SingleField {
            rows: vec!["only doc".into()],
        };
        let opts = BuildOptions {
            field_names: vec![],
            writer_memory_bytes: 15_000_000,
        };
        let r = build_fts_index(&acc, &opts);
        assert!(r.is_err(), "empty field_names must fail fast");
    }

    #[test]
    fn serialize_dir_round_trips_through_open() {
        // Build through the public path; verify that what `serialize_dir`
        // produces is parseable by `open_fts_index`. This guards against
        // manifest format drift.
        let acc = fifty_doc_corpus();
        let opts = BuildOptions {
            field_names: vec!["prompt".into()],
            writer_memory_bytes: 15_000_000,
        };
        let res = build_fts_index(&acc, &opts).expect("build");
        // Manifest length is the first u32 LE.
        assert!(res.blob.len() > 4);
        let manifest_len =
            u32::from_le_bytes([res.blob[0], res.blob[1], res.blob[2], res.blob[3]]) as usize;
        assert!(manifest_len > 0);
        assert!(4 + manifest_len <= res.blob.len(), "manifest must fit");

        // And it opens cleanly.
        let _handle = open_fts_index(&res.blob).expect("open after build");
    }
}
