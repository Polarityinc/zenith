//! JSON-path posting index.

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use roaring::RoaringBitmap;
use serde_json::Value;
use xxhash_rust::xxh3::xxh3_64;

use zen_common::{ZenError, ZenResult};

use crate::discovery::{walk, DiscoveredPath};

pub struct JsonPathIndexBuilder {
    /// `path_id` → posting maps from `value_hash` → row mask.
    pub paths: Vec<String>,
    pub posting: HashMap<u32, HashMap<u64, RoaringBitmap>>,
}

impl JsonPathIndexBuilder {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            paths,
            posting: HashMap::new(),
        }
    }

    /// Push row `row` for json `v`. Only paths in `self.paths` are indexed.
    pub fn push_row(&mut self, row: u32, v: &Value) {
        let want: std::collections::HashSet<&str> =
            self.paths.iter().map(|s| s.as_str()).collect();
        let paths_clone = self.paths.clone();
        let mut local: Vec<(String, Option<String>)> = Vec::new();
        walk(v, "", 0, 8, &mut |path, scalar| {
            if want.contains(path) {
                local.push((path.to_string(), scalar.map(str::to_string)));
            }
        });
        for (path, scalar) in local {
            if let Some(s) = scalar {
                let path_id = paths_clone.iter().position(|p| p == &path).unwrap() as u32;
                let h = xxh3_64(s.as_bytes());
                self.posting
                    .entry(path_id)
                    .or_default()
                    .entry(h)
                    .or_default()
                    .insert(row);
            }
        }
    }

    pub fn finish(self) -> JsonPathIndex {
        JsonPathIndex {
            paths: self.paths,
            posting: self.posting,
        }
    }
}

#[derive(Default)]
pub struct JsonPathIndex {
    pub paths: Vec<String>,
    pub posting: HashMap<u32, HashMap<u64, RoaringBitmap>>,
}

impl JsonPathIndex {
    pub fn lookup(&self, path: &str, value: &str) -> Option<&RoaringBitmap> {
        let path_id = self.paths.iter().position(|p| p == path)? as u32;
        self.posting.get(&path_id)?.get(&xxh3_64(value.as_bytes()))
    }

    pub fn knows_path(&self, path: &str) -> bool {
        self.paths.iter().any(|p| p == path)
    }

    pub fn serialize(&self) -> ZenResult<Bytes> {
        let mut out = BytesMut::new();
        // Path dictionary.
        out.put_u32_le(self.paths.len() as u32);
        for p in &self.paths {
            let pb = p.as_bytes();
            out.put_u32_le(pb.len() as u32);
            out.put_slice(pb);
        }
        // Posting lists.
        out.put_u32_le(self.posting.len() as u32);
        let mut keys: Vec<&u32> = self.posting.keys().collect();
        keys.sort_unstable();
        for path_id in keys {
            let m = &self.posting[path_id];
            out.put_u32_le(*path_id);
            out.put_u32_le(m.len() as u32);
            let mut hkeys: Vec<&u64> = m.keys().collect();
            hkeys.sort_unstable();
            for h in hkeys {
                let bm = &m[h];
                let mut buf = Vec::with_capacity(bm.serialized_size());
                bm.serialize_into(&mut buf)
                    .map_err(|e| ZenError::format(format!("roaring serialize: {e}")))?;
                out.put_u64_le(*h);
                out.put_u32_le(buf.len() as u32);
                out.put_slice(&buf);
            }
        }
        Ok(out.freeze())
    }

    pub fn deserialize(input: &[u8]) -> ZenResult<Self> {
        let mut p = input;
        if p.remaining() < 4 {
            return Err(ZenError::format("json path index header truncated"));
        }
        let np = p.get_u32_le() as usize;
        let mut paths = Vec::with_capacity(np);
        for _ in 0..np {
            if p.remaining() < 4 {
                return Err(ZenError::format("path entry truncated"));
            }
            let l = p.get_u32_le() as usize;
            if p.remaining() < l {
                return Err(ZenError::format("path body truncated"));
            }
            let s = std::str::from_utf8(&p[..l])
                .map_err(|e| ZenError::format(format!("path utf8: {e}")))?
                .to_string();
            p.advance(l);
            paths.push(s);
        }
        if p.remaining() < 4 {
            return Err(ZenError::format("posting count truncated"));
        }
        let n_lists = p.get_u32_le() as usize;
        let mut posting: HashMap<u32, HashMap<u64, RoaringBitmap>> = HashMap::new();
        for _ in 0..n_lists {
            if p.remaining() < 8 {
                return Err(ZenError::format("posting header truncated"));
            }
            let pid = p.get_u32_le();
            let n_h = p.get_u32_le() as usize;
            let mut m: HashMap<u64, RoaringBitmap> = HashMap::with_capacity(n_h);
            for _ in 0..n_h {
                if p.remaining() < 12 {
                    return Err(ZenError::format("hash entry header truncated"));
                }
                let h = p.get_u64_le();
                let l = p.get_u32_le() as usize;
                if p.remaining() < l {
                    return Err(ZenError::format("hash entry body truncated"));
                }
                let bm = RoaringBitmap::deserialize_from(std::io::Cursor::new(&p[..l]))
                    .map_err(|e| ZenError::format(format!("roaring deserialize: {e}")))?;
                p.advance(l);
                m.insert(h, bm);
            }
            posting.insert(pid, m);
        }
        Ok(Self { paths, posting })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_lookup_serialize() {
        let paths = vec!["user_id".to_string(), "output.steps[*].name".to_string()];
        let mut b = JsonPathIndexBuilder::new(paths);
        for i in 0..100u32 {
            let v = json!({
                "user_id": format!("u-{}", i % 5),
                "output": { "steps": [{"name": if i % 2 == 0 {"router"} else {"summarize"}}] }
            });
            b.push_row(i, &v);
        }
        let idx = b.finish();
        let bm = idx.lookup("user_id", "u-3").unwrap();
        // i % 5 == 3 → i = 3, 8, 13, ..., 98 (20 values)
        assert_eq!(bm.len(), 20);

        let bytes = idx.serialize().unwrap();
        let idx2 = JsonPathIndex::deserialize(&bytes).unwrap();
        let bm2 = idx2.lookup("user_id", "u-3").unwrap();
        assert_eq!(bm2.len(), 20);

        let names = idx2.lookup("output.steps[*].name", "router").unwrap();
        assert_eq!(names.len(), 50);
    }

    #[test]
    fn unindexed_path_returns_none() {
        let idx = JsonPathIndexBuilder::new(vec!["user_id".into()]).finish();
        assert!(idx.lookup("metadata.flag", "yes").is_none());
        assert!(!idx.knows_path("metadata.flag"));
    }
}
