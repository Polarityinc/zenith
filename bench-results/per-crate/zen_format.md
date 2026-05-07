# zen_format — microbench (M4 Pro)

| Operation | Time | Throughput |
|---|---|---|
| Open segment (10K rows, ~40 KB blob) | 16.7 µs | 17 GiB/s |
| Read full prompt column (FSST, 10K rows) | 304 µs | 33 M rows/s |
| Read full model column (Dict, 10K rows) | 183 µs | 55 M rows/s |
| Read full time column (FoR, 10K rows) | 18 µs | 556 M rows/s |
| **Late mat: 100 scattered prompts via batched `read_rows`** | **36 µs** | 2.7 M rows/s |
| Late mat: 1000 scattered prompts via batched `read_rows` | 64 µs | 15.6 M rows/s |
| Slow path: 100 prompts via per-row open | 3.44 ms | 29 K rows/s |

## The win

The 94× speedup of `read_rows` over per-row `read_row` is exactly the late-
materialization invariant: one page open amortizes across N row decodes. For
the trace-load workload (return all spans of one trace, ≤200 spans), this
puts wide-column decode in the tens of microseconds — well below the 250 ms
span-load p95 target.

## Format properties verified by tests

- Magic header/trailer detection.
- Footer length is at the *end* of the file (right before the trailer), so a
  reader can do one tail GET to bootstrap.
- Per-row offset directory in FSST pages → `decode_one_row` is O(1).
- Dict pages decode keys → dict lookup → bytes per row.
- FoR / Gorilla / RLE round-trip on i64 / f64 / monotonic columns.
- Two writes of identical content produce identical bytes (deterministic).

## Tests
15 unit tests pass.
