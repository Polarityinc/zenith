# Brainstore-style benchmark: ZenithDB vs PostgreSQL vs DuckDB

Corpus: 50,000 docs × ~25 KB ≈ 1.2 GB raw (scaled down from Brainstore's 3.9 M × 25 KB ≈ 100 GB).

Apple M4 Pro (24 GB RAM) · macOS 26 · Tokio runtime.

| Test | Zenith p95 | Postgres p95 | DuckDB p95 |
|---|---:|---:|---:|
| Span load (trace inspect) | 1.1 ms | 0.6 ms | 0.3 ms |
| Full-text search 'memory' | 0.9 ms | 7.6 ms | 1.0 ms |
| Write flush (100 × 100 KB) | 38.5 ms | 299.6 ms | 107.5 ms |
| Write visible (read-after-flush) | 0.0 ms | 299.6 ms (sync) | 107.5 ms (sync) |

## Brainstore reference numbers (their March 2025 post)

| Test | Brainstore | 'Popular DW' | Competitor |
|---|---:|---:|---:|
| Span load | 549 ms | 679 ms | 1,160 ms |
| FTS 'memory' | 240 ms | 78,963 ms | 20,789 ms |
| Write flush | 1,780 ms | 331 ms | 4,176 ms |
| Write visible | 1,780 ms | 2,678 ms | 10,412 ms |

*Brainstore numbers are at 3.9 M × 25 KB on c7gd.8xlarge. Our numbers are scaled down; see corpus size above.*
