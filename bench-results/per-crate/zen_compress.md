# zen_compress — microbench (M4 Pro)

| Encoding | Encode | Decode |
|---|---|---|
| FSST (2048 natural-text rows) | 834 MiB/s | **26 ns / row** • 2.1 GiB/s bulk |
| ZSTD (64 KB, level 3) | 540 MiB/s | 980 MiB/s |
| Gorilla (16K smooth f64) | 369 MiB/s | 725 MiB/s |
| Frame-of-Reference + bit-pack (16K monotonic i64) | 4.34 GiB/s | 5.16 GiB/s |
| RLE (16K, run-heavy) | 16.9 GiB/s | 16.0 GiB/s |
| Dict (16K low-card strings) | 339 MiB/s | 7.4 GiB/s |

## Notes

- The **single-row FSST decode at ~26 ns** is the win for late materialization.
  Decoding 100 surviving rows of a 10K-row page costs ~2.6 µs.
- ZSTD level 3 is the right tradeoff: 540 MiB/s encode is faster than the WAL
  flush rate target.
- Gorilla on truly smooth series (timestamps) compresses ~5× when paired with
  pre-deltifying. Encode/decode throughput is solid.
- FoR + RLE saturate cache bandwidth.

## Tests
27 unit + property-test rounds pass. Includes a regression for the proptest
case `[0.0, 3.300253571502073e-197]` that broke the previous Gorilla impl.
