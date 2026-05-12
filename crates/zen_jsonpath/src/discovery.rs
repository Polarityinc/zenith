//! Walk JSON documents to discover paths and their value distributions.
//!
//! Path syntax: dot-segmented keys, with `[*]` for arrays. We don't index
//! per-array-element values; we record that the path exists and use the
//! flattened set of values for posting construction. Example: a JSON document
//! `{"output": {"steps": [{"name":"router"}, {"name":"summarize"}]}}` produces:
//!   - `output.steps[*].name` → ["router", "summarize"]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    pub sample_size: u32,
    pub min_presence_pct: f64,
    pub max_paths: u32,
    /// Maximum nesting depth we record.
    pub max_depth: u32,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            sample_size: 10_000,
            min_presence_pct: 1.0,
            max_paths: 256,
            max_depth: 6,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveredPath {
    pub path: String,
    /// Number of documents the path appeared in.
    pub presence_count: u32,
    /// Approximate distinct count of scalar values across all documents.
    pub distinct_estimate: u32,
}

pub fn discover_paths<'a, I: IntoIterator<Item = &'a Value>>(
    samples: I,
    cfg: &DiscoveryConfig,
) -> Vec<DiscoveredPath> {
    let samples: Vec<&Value> = samples.into_iter().collect();
    let total = samples.len() as u32;
    let mut path_count: HashMap<String, u32> = HashMap::new();
    // distinct value tracker per path (we use a tiny hashset bound for memory).
    let mut path_values: HashMap<String, std::collections::HashSet<u64>> = HashMap::new();

    for v in &samples {
        let mut seen: std::collections::HashSet<String> = Default::default();
        walk(v, "", 0, cfg.max_depth, &mut |path, scalar| {
            if seen.insert(path.to_string()) {
                *path_count.entry(path.to_string()).or_insert(0) += 1;
            }
            if let Some(s) = scalar {
                let h = xxhash_rust::xxh3::xxh3_64(s.as_bytes());
                let entry = path_values.entry(path.to_string()).or_default();
                if entry.len() < 4096 {
                    entry.insert(h);
                }
            }
        });
    }

    let min_count = ((total as f64) * cfg.min_presence_pct / 100.0)
        .max(1.0)
        .round() as u32;
    let mut out: Vec<DiscoveredPath> = path_count
        .into_iter()
        .filter(|(_, c)| *c >= min_count)
        .map(|(path, c)| DiscoveredPath {
            distinct_estimate: path_values.get(&path).map(|s| s.len() as u32).unwrap_or(0),
            path,
            presence_count: c,
        })
        .collect();
    out.sort_by(|a, b| b.presence_count.cmp(&a.presence_count));
    out.truncate(cfg.max_paths as usize);
    out
}

/// Walk a value, calling `f(path, scalar_string?)`. Scalars are the leaves; for
/// objects/arrays we still call f to record the "path exists" event, but with
/// `scalar = None`.
pub fn walk<F: FnMut(&str, Option<&str>)>(
    v: &Value,
    prefix: &str,
    depth: u32,
    max_depth: u32,
    f: &mut F,
) {
    if depth > max_depth {
        return;
    }
    match v {
        Value::Object(map) => {
            for (k, sub) in map {
                let mut p = prefix.to_string();
                if !p.is_empty() {
                    p.push('.');
                }
                p.push_str(k);
                walk(sub, &p, depth + 1, max_depth, f);
            }
        }
        Value::Array(items) => {
            let p = format!("{prefix}[*]");
            // Path event for the array itself.
            f(&p, None);
            for item in items {
                walk(item, &p, depth + 1, max_depth, f);
            }
        }
        Value::String(s) => f(prefix, Some(s)),
        Value::Bool(b) => f(prefix, Some(if *b { "true" } else { "false" })),
        Value::Number(n) => {
            let s = n.to_string();
            f(prefix, Some(&s));
        }
        Value::Null => f(prefix, Some("null")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn finds_common_paths() {
        let docs: Vec<Value> = (0..1000)
            .map(|i| {
                json!({
                    "user_id": format!("u-{}", i % 50),
                    "output": { "steps": [{"name": if i % 2 == 0 {"router"} else {"summarize"}}] }
                })
            })
            .collect();
        let cfg = DiscoveryConfig::default();
        let paths = discover_paths(docs.iter(), &cfg);
        let names: Vec<&str> = paths.iter().map(|p| p.path.as_str()).collect();
        assert!(names.contains(&"user_id"));
        assert!(names.contains(&"output.steps[*].name"));
    }

    #[test]
    fn respects_min_presence() {
        let mut docs = Vec::new();
        for _ in 0..1000 {
            docs.push(json!({"a": 1}));
        }
        for _ in 0..2 {
            docs.push(json!({"rare": "x"}));
        }
        let cfg = DiscoveryConfig {
            min_presence_pct: 5.0,
            ..Default::default()
        };
        let paths = discover_paths(docs.iter(), &cfg);
        assert!(paths.iter().any(|p| p.path == "a"));
        assert!(!paths.iter().any(|p| p.path == "rare"));
    }

    // ---- Additional coverage ------------------------------------------------

    #[test]
    fn flat_object_returns_top_level_paths() {
        // A single document with several top-level scalar keys.
        let doc = json!({
            "user_id": "u-1",
            "trace_id": "t-1",
            "model": "gpt-4o",
        });
        let cfg = DiscoveryConfig::default();
        let paths = discover_paths(std::iter::once(&doc), &cfg);
        let names: std::collections::HashSet<&str> =
            paths.iter().map(|p| p.path.as_str()).collect();
        assert!(names.contains("user_id"));
        assert!(names.contains("trace_id"));
        assert!(names.contains("model"));
        // Each appeared in exactly one document.
        for p in &paths {
            assert_eq!(p.presence_count, 1);
        }
    }

    #[test]
    fn nested_object_discovers_dot_path() {
        let doc = json!({"a": {"b": 1}});
        let cfg = DiscoveryConfig::default();
        let paths = discover_paths(std::iter::once(&doc), &cfg);
        let names: std::collections::HashSet<&str> =
            paths.iter().map(|p| p.path.as_str()).collect();
        assert!(names.contains("a.b"), "missing a.b — got {names:?}");
    }

    #[test]
    fn arrays_use_star_segment() {
        // The walker uses `[*]` (not `[]`) to denote "any element of an array".
        let doc = json!({
            "output": {
                "steps": [
                    {"name": "router"},
                    {"name": "summarize"},
                ],
            },
        });
        let cfg = DiscoveryConfig::default();
        let paths = discover_paths(std::iter::once(&doc), &cfg);
        let names: std::collections::HashSet<&str> =
            paths.iter().map(|p| p.path.as_str()).collect();
        assert!(
            names.contains("output.steps[*]"),
            "missing output.steps[*] — got {names:?}"
        );
        assert!(
            names.contains("output.steps[*].name"),
            "missing output.steps[*].name — got {names:?}"
        );
        // Distinct values found at the leaf path: "router" and "summarize".
        let p = paths
            .iter()
            .find(|p| p.path == "output.steps[*].name")
            .unwrap();
        assert_eq!(p.distinct_estimate, 2);
    }

    #[test]
    fn min_presence_threshold_drops_rare_paths_in_10k_sample() {
        // 10k docs all carry `common`; only a handful carry `rare`.
        let mut docs = Vec::with_capacity(10_000);
        for i in 0..10_000u32 {
            if i < 50 {
                // ~0.5% of the corpus carries the rare path.
                docs.push(json!({"common": i, "rare": "x"}));
            } else {
                docs.push(json!({"common": i}));
            }
        }
        let cfg = DiscoveryConfig {
            sample_size: 10_000,
            min_presence_pct: 5.0,
            ..Default::default()
        };
        let paths = discover_paths(docs.iter(), &cfg);
        assert!(paths.iter().any(|p| p.path == "common"));
        assert!(
            !paths.iter().any(|p| p.path == "rare"),
            "rare path (~0.5%) should be dropped under 5% threshold"
        );
    }

    #[test]
    fn empty_object_returns_no_paths() {
        let doc = json!({});
        let cfg = DiscoveryConfig::default();
        let paths = discover_paths(std::iter::once(&doc), &cfg);
        assert!(
            paths.is_empty(),
            "expected zero paths for empty object — got {paths:?}"
        );
    }

    #[test]
    fn extremely_deep_nesting_does_not_panic() {
        // Build a 50-level nested object: {"a": {"a": {"a": ... 1 ... }}}
        let mut v: Value = json!(1);
        for _ in 0..50 {
            v = json!({"a": v});
        }
        let cfg = DiscoveryConfig {
            // Bump max_depth so we exercise the recursion past the default 6.
            max_depth: 100,
            ..Default::default()
        };
        // Important: must not panic / overflow / hang.
        let paths = discover_paths(std::iter::once(&v), &cfg);
        // The walker only records the deepest scalar leaf, so we expect a
        // single path made of 50 "a" segments. The key signal is "did we
        // return without panicking?"
        assert!(
            !paths.is_empty(),
            "deep object should produce at least one discovered path"
        );
        let deepest = paths
            .iter()
            .map(|p| p.path.matches('.').count())
            .max()
            .unwrap();
        assert!(deepest <= 50, "depth must be bounded by structure");

        // With the default (small) max_depth, the recursion still doesn't
        // blow up — it just truncates by returning early on too-deep nodes.
        let cfg_default = DiscoveryConfig::default();
        let truncated = discover_paths(std::iter::once(&v), &cfg_default);
        // No leaf scalar is reachable within max_depth=6, so the result is
        // empty; what matters is no panic.
        for p in &truncated {
            let segs = p.path.matches('.').count() + 1;
            assert!(
                segs as u32 <= cfg_default.max_depth + 1,
                "path {} exceeds max_depth {}",
                p.path,
                cfg_default.max_depth
            );
        }
    }
}
