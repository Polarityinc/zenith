# zen_common — microbench (M4 Pro)

| bench | time |
|---|---|
| trace_id_to_string_then_parse | ~210-260 ns |
| span_id_to_string_then_parse | ~210-260 ns |
| schema_fingerprint_spans_v1 | 1.84 µs |
| span_record_default | 124.7 ns |
| schema_fingerprint_eq | 0.42 ns |

Notes: foundation types are not on any hot path. Schema fingerprint is computed
once per writer setup and then cached. ID parse cost is dominated by the Crockford
base32 decode in `ulid::Ulid::from_string`.

Tests: 18 unit tests pass.
