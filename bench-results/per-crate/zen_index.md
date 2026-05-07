# zen_index — microbench (M4 Pro)

| Operation | Throughput | Time |
|---|---|---|
| PostingMap insert (1M rows, 8 distinct values) | 50 M elem/s | 19.9 ms total |
| PostingList AND (1M rows × 1M rows, roaring) | 94 G elem/s | **10.6 µs** |
| Bloom filter insert (100K) | 27 M elem/s | 37 ns / insert |
| Bloom filter contains (100K hits) | 25 M elem/s | 40 ns / check |

## Notes

- The roaring AND is the critical path for multi-predicate queries
  (`status='error' AND model='gpt-4o'`). At 10.6 µs / 1M rows, ANDing dozens of
  posting lists across a typical query is sub-millisecond.
- Bloom is for high-cardinality columns where we don't want to ship full
  posting lists. 40 ns per check means a 100-row negative-lookup batch costs
  4 µs — fast enough to use as a pre-filter.

## Tests
16 unit tests pass. HLL distinct estimation accurate to ~5% on 10K cardinality.
