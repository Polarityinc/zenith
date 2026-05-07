//! Naive reference implementation used as a correctness oracle.
//!
//! Given the same set of `SpanRecord`s, this returns the same answer that the
//! real executor must produce. We use this in differential tests to catch any
//! optimizer / encoding / pruning bug.

use zen_common::SpanRecord;

pub fn naive_filter_count(rows: &[SpanRecord], model: &str, status: &str) -> usize {
    rows.iter()
        .filter(|r| r.model.as_deref() == Some(model) && r.status.as_deref() == Some(status))
        .count()
}

pub fn naive_count_by_model(rows: &[SpanRecord]) -> std::collections::BTreeMap<String, usize> {
    let mut out = std::collections::BTreeMap::new();
    for r in rows {
        if let Some(m) = &r.model {
            *out.entry(m.clone()).or_insert(0) += 1;
        }
    }
    out
}

pub fn naive_text_match_count(rows: &[SpanRecord], needle: &str) -> usize {
    rows.iter()
        .filter(|r| r.prompt.as_deref().map(|p| p.contains(needle)).unwrap_or(false))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zen_common::{PartitionId, SpanRecord, TenantId};

    fn fixture() -> Vec<SpanRecord> {
        let mut v = Vec::new();
        for i in 0..100 {
            let mut r = SpanRecord::new(TenantId(0), PartitionId(0));
            r.model = Some(if i % 3 == 0 { "gpt-4o" } else { "haiku" }.into());
            r.status = Some(if i == 17 { "error" } else { "ok" }.into());
            r.prompt = Some(if i == 42 { "out of memory error".into() } else { "no error".into() });
            v.push(r);
        }
        v
    }

    #[test]
    fn naive_oracle_matches_known_truth() {
        let rows = fixture();
        // 100/3 rounded up = 34 gpt-4o rows.
        let gpt = rows.iter().filter(|r| r.model.as_deref() == Some("gpt-4o")).count();
        assert_eq!(gpt, 34);
        assert_eq!(naive_filter_count(&rows, "haiku", "error"), 1);
        assert_eq!(naive_count_by_model(&rows).get("haiku"), Some(&66));
        assert_eq!(naive_text_match_count(&rows, "out of memory"), 1);
    }
}
