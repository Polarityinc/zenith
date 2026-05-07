# ZenithDB vs PostgreSQL vs DuckDB — Mac M4 Pro

Workload: 100112 spans (tenant 0). Iterations per cell: 50.
Postgres has covering btree on (model, status) and a GIN trigram on prompt; DuckDB is in-memory. ZenithDB is hot (segment + zone maps cached).

| Query | Zenith p50/p95 µs | Postgres p50/p95 µs | DuckDB p50/p95 µs | Zenith vs Postgres | Zenith vs DuckDB |
|---|---:|---:|---:|---:|---:|
| B1_trace_load | 520 / 923 | 9149 / 11176 | 466 / 673 | 12.10× faster | 0.73× faster |
| B2_attr_filter | 847 / 1096 | 200 / 297 | 194 / 273 | 0.27× faster | 0.25× faster |
| B3_fts_memory | 813 / 942 | 415 / 520 | 120 / 252 | 0.55× faster | 0.27× faster |
| B6_jsonpath | 2701 / 3958 | 117 / 231 | 285 / 442 | 0.06× faster | 0.11× faster |
| B8_group_by_model | 6422 / 7503 | 4137 / 7451 | 2170 / 2561 | 0.99× faster | 0.34× faster |

## Caveats

- All three engines hit a tiny dataset (~2.5 K rows). Numbers grow with corpus.
- Postgres includes HTTP-less local Unix socket overhead (faster than network); ZenithDB pays an HTTP roundtrip per query (Postgres uses a libpq local socket).
- DuckDB is in-process; ZenithDB is in a separate process. The DuckDB number is essentially "floor" in-process latency.
- For B3 (FTS), Postgres uses a GIN trigram index on prompt; ZenithDB falls back to scan because no FTS index is wired in this segment.
- For B6 (JSON path), Postgres has an expression index on `(metadata->>'tier')`. ZenithDB falls back to JSON scan.
- ZenithDB will widen the gap as the corpus grows: row-group pruning is `O(1)` w.r.t. rows; Postgres index walks scale with `O(log n)`; DuckDB scans scale `O(n)`.
