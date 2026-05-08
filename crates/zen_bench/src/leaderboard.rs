//! Leaderboard generator: compares current bench results vs Brainstore
//! published numbers and writes Markdown.

use std::collections::HashMap;

use crate::run::BenchResult;

pub struct Leaderboard;

impl Leaderboard {
    pub fn render(results: &[BenchResult]) -> String {
        // Brainstore reference numbers (mar/dec 2025) as a constant table.
        // We compare the closest Mac measurement to the Linux Brainstore number.
        let mut bs: HashMap<&str, (f64, f64)> = HashMap::new();
        // (p50_us, p95_us)
        bs.insert("span_load", (549_000.0, 549_000.0));
        bs.insert("B3_fts_common_term", (240_000.0, 401_000.0));
        bs.insert("write_flush", (1_780_000.0, 1_780_000.0));

        let mut s = String::new();
        s.push_str("# ZenithDB Mac Leaderboard\n\n");
        s.push_str("| Benchmark | p50 (µs) | p95 (µs) | p99 (µs) | samples | Brainstore Linux p95 (µs, ref) |\n");
        s.push_str("|---|---:|---:|---:|---:|---:|\n");
        for r in results {
            let bs_p95 = bs
                .get(r.name.as_str())
                .map(|(_, p)| format!("{:.0}", p))
                .unwrap_or_else(|| "(no ref)".into());
            s.push_str(&format!(
                "| {} | {:.0} | {:.0} | {:.0} | {} | {} |\n",
                r.name, r.p50_us, r.p95_us, r.p99_us, r.n, bs_p95
            ));
        }
        s.push_str(
            "\n## Notes\n\n\
             - Brainstore figures are from the March/December 2025 announcements on c7gd.* instances (Linux io_uring).\n\
             - These results were measured on Apple M4 Pro / macOS 26 / tokio. We do not run on Brainstore's hardware,\n\
             so absolute numbers are not directly comparable. Trends and ratios are.\n\
             - The 5 'moat' design choices (PAX with per-row offset directories, trace-locality compaction,\n\
             late materialization, Tantivy-as-a-library, and WAL on object storage) are all in effect here.\n",
        );
        s
    }
}
