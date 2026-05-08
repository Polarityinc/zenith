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
}
