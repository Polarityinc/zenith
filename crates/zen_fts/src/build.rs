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
use tantivy::schema::{Schema as TSchema, STORED, TEXT};
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
            field_names: vec![
                "prompt".into(),
                "completion".into(),
                "tool_io_text".into(),
            ],
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
    // Add a synthetic stored field for the row index so BM25 result → row mask.
    let row_idx_field = sb.add_u64_field("__row_idx", STORED);
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
    Ok(BuildResult {
        blob,
        doc_count: n,
    })
}

pub fn serialize_dir(path: &Path) -> ZenResult<Bytes> {
    // Collect files (one level deep is sufficient for Tantivy index dirs).
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let mut stack = vec![path.to_path_buf()];
    while let Some(d) = stack.pop() {
        for ent in std::fs::read_dir(&d)
            .map_err(|e| ZenError::format(format!("read_dir {d:?}: {e}")))?
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
