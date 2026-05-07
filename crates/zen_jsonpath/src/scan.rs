//! Scan-fallback evaluator. When a path isn't in the index, we walk each
//! row's JSON and check the path manually.

use roaring::RoaringBitmap;
use serde_json::Value;

use crate::discovery::walk;

/// Linearly scan rows, returning the row mask for `path == value`.
pub fn scan_path_eq(rows: &[Option<&Value>], path: &str, value: &str) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    for (i, row) in rows.iter().enumerate() {
        let Some(v) = row else { continue };
        let mut hit = false;
        walk(v, "", 0, 8, &mut |p, scalar| {
            if hit { return; }
            if p == path {
                if let Some(s) = scalar {
                    if s == value {
                        hit = true;
                    }
                }
            }
        });
        if hit {
            bm.insert(i as u32);
        }
    }
    bm
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scan_correctness() {
        let docs: Vec<Value> = (0..200)
            .map(|i| {
                json!({
                    "metadata": {"flag": if i % 7 == 0 {"on"} else {"off"}}
                })
            })
            .collect();
        let refs: Vec<Option<&Value>> = docs.iter().map(Some).collect();
        let bm = scan_path_eq(&refs, "metadata.flag", "on");
        // i in 0,7,14,...,196 -> 29 hits
        assert_eq!(bm.len(), 29);
    }
}
