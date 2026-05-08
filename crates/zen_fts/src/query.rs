//! Open a serialized Tantivy index blob and run queries against it.


use bytes::Buf;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use tantivy::collector::TopDocs;
use tantivy::query::{Query, QueryParser};
use tantivy::schema::Field;
use tantivy::{Index, IndexReader, ReloadPolicy};
use tempfile::TempDir;

use zen_common::{ZenError, ZenResult};

#[derive(Serialize, Deserialize)]
struct ManifestEntry {
    name: String,
    offset: u64,
    length: u64,
}

pub struct FtsHandle {
    pub index: Index,
    pub reader: IndexReader,
    pub field_names: Vec<String>,
    pub fields: Vec<Field>,
    pub row_idx_field: Field,
    /// Held to keep the temp directory alive.
    _dir: TempDir,
}

pub fn open_fts_index(blob: &[u8]) -> ZenResult<FtsHandle> {
    if blob.len() < 4 {
        return Err(ZenError::format("FTS blob too small"));
    }
    let mut p = blob;
    let manifest_len = p.get_u32_le() as usize;
    if p.len() < manifest_len {
        return Err(ZenError::format("FTS manifest truncated"));
    }
    let manifest: Vec<ManifestEntry> = serde_json::from_slice(&p[..manifest_len])
        .map_err(|e| ZenError::format(format!("manifest parse: {e}")))?;
    let payload = &p[manifest_len..];

    let dir = TempDir::new().map_err(|e| ZenError::format(format!("temp dir: {e}")))?;
    for ent in &manifest {
        let path = dir.path().join(&ent.name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| ZenError::format(format!("mkdir: {e}")))?;
        }
        let s = ent.offset as usize;
        let e = (ent.offset + ent.length) as usize;
        if payload.len() < e {
            return Err(ZenError::format("FTS payload truncated"));
        }
        std::fs::write(&path, &payload[s..e])
            .map_err(|err| ZenError::format(format!("write file {path:?}: {err}")))?;
    }
    let index = Index::open_in_dir(dir.path())
        .map_err(|e| ZenError::format(format!("open index: {e}")))?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(|e| ZenError::format(format!("reader: {e}")))?;
    // Recover field handles by name from the schema.
    let schema = index.schema();
    let mut field_names = Vec::new();
    let mut fields = Vec::new();
    let mut row_idx_field: Option<Field> = None;
    for (f, fe) in schema.fields() {
        let name = fe.name().to_string();
        if name == "__row_idx" {
            row_idx_field = Some(f);
        } else {
            field_names.push(name);
            fields.push(f);
        }
    }
    let row_idx_field = row_idx_field
        .ok_or_else(|| ZenError::format("FTS index missing __row_idx field"))?;
    Ok(FtsHandle {
        index,
        reader,
        field_names,
        fields,
        row_idx_field,
        _dir: dir,
    })
}

#[derive(Clone, Debug)]
pub struct FtsQuery<'a> {
    /// Specific field name to query. If None, queries all fields.
    pub field: Option<&'a str>,
    /// User-supplied query string (may include phrase, AND/OR, etc.).
    pub query: &'a str,
    /// Max docs to return; the executor only needs the row mask, so use a high
    /// number (or `usize::MAX`).
    pub limit: usize,
}

impl FtsHandle {
    /// Run a query and return the matching segment row indices.
    pub fn search_to_bitmap(&self, q: &FtsQuery<'_>) -> ZenResult<RoaringBitmap> {
        let searcher = self.reader.searcher();

        let parser = if let Some(field_name) = q.field {
            let f = self
                .index
                .schema()
                .get_field(field_name)
                .map_err(|e| ZenError::format(format!("get_field {field_name}: {e}")))?;
            QueryParser::for_index(&self.index, vec![f])
        } else {
            QueryParser::for_index(&self.index, self.fields.clone())
        };
        let query: Box<dyn Query> = parser
            .parse_query(q.query)
            .map_err(|e| ZenError::format(format!("parse query: {e}")))?;

        let top = TopDocs::with_limit(q.limit);
        let hits = searcher
            .search(&query, &top)
            .map_err(|e| ZenError::format(format!("search: {e}")))?;

        let mut bm = RoaringBitmap::new();
        for (_, doc_addr) in hits {
            let doc: tantivy::TantivyDocument = searcher
                .doc(doc_addr)
                .map_err(|e| ZenError::format(format!("retrieve doc: {e}")))?;
            if let Some(v) = doc.get_first(self.row_idx_field) {
                if let Some(u) = extract_u64_from_value(v) {
                    bm.insert(u as u32);
                }
            }
        }
        Ok(bm)
    }
}

fn extract_u64_from_value(v: &tantivy::schema::OwnedValue) -> Option<u64> {
    match v {
        tantivy::schema::OwnedValue::U64(u) => Some(*u),
        tantivy::schema::OwnedValue::I64(i) => Some(*i as u64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{build_fts_index, BuildOptions, FieldAccessor};

    struct StaticAccessor {
        rows: Vec<Vec<Option<String>>>,
    }
    impl FieldAccessor for StaticAccessor {
        fn field(&self, row: usize, field_idx: usize) -> Option<&str> {
            self.rows[row][field_idx].as_deref()
        }
        fn row_count(&self) -> usize {
            self.rows.len()
        }
    }

    fn corpus() -> StaticAccessor {
        StaticAccessor {
            rows: vec![
                vec![Some("the quick brown fox".into()), Some("jumped over".into()), None],
                vec![Some("out of memory error".into()), Some("on the gpu".into()), None],
                vec![Some("rate limit exceeded".into()), Some("for tier free".into()), None],
                vec![Some("hello world".into()), Some("greetings".into()), None],
                vec![Some("out of memory while".into()), Some("allocating".into()), None],
            ],
        }
    }

    #[test]
    fn build_serialize_open_search() {
        let acc = corpus();
        let opts = BuildOptions {
            field_names: vec!["prompt".into(), "completion".into(), "tool_io_text".into()],
            writer_memory_bytes: 15_000_000,
        };
        let res = build_fts_index(&acc, &opts).unwrap();
        assert_eq!(res.doc_count, 5);
        let handle = open_fts_index(&res.blob).unwrap();
        let q = FtsQuery {
            field: Some("prompt"),
            query: "memory",
            limit: 10,
        };
        let bm = handle.search_to_bitmap(&q).unwrap();
        // Rows 1 and 4 mention "memory" in prompt.
        let v: Vec<u32> = bm.iter().collect();
        let mut s = v.clone();
        s.sort_unstable();
        assert_eq!(s, vec![1, 4]);
    }

    #[test]
    fn phrase_query_works() {
        let acc = corpus();
        let opts = BuildOptions::default();
        let res = build_fts_index(&acc, &opts).unwrap();
        let handle = open_fts_index(&res.blob).unwrap();
        let q = FtsQuery {
            field: Some("prompt"),
            query: "\"out of memory\"",
            limit: 10,
        };
        let bm = handle.search_to_bitmap(&q).unwrap();
        let mut v: Vec<u32> = bm.iter().collect();
        v.sort_unstable();
        assert_eq!(v, vec![1, 4]);
    }

    #[test]
    fn no_match_returns_empty() {
        let acc = corpus();
        let opts = BuildOptions::default();
        let res = build_fts_index(&acc, &opts).unwrap();
        let handle = open_fts_index(&res.blob).unwrap();
        let q = FtsQuery {
            field: Some("prompt"),
            query: "thisbeenseennowhere",
            limit: 10,
        };
        let bm = handle.search_to_bitmap(&q).unwrap();
        assert_eq!(bm.len(), 0);
    }
}
