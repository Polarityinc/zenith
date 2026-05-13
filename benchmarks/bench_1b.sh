#!/usr/bin/env bash
# benchmarks/bench_1b.sh
#
# Recreate the canonical ZenithDB 1-billion-row benchmark.
#
# What it does:
#   1. Generates a synthetic AI-trace corpus in chunks (default 5M rows per
#      chunk * 200 chunks = 1B rows). Each chunk is written to disk, loaded
#      via HTTP ingest, then deleted so peak disk usage stays bounded.
#   2. Triggers compaction every COMPACT_EVERY chunks so segments stay sorted
#      by (trace_id, start_time, span_id) and trace-locality holds.
#   3. After the load phase, runs the canonical bench-run suite
#      (B2 attr filter, B3 FTS, B6 jsonpath, B8 group-by) and writes
#      p50/p95/p99 latencies to a JSON file under bench-results/.
#
# Pre-flight:
#   - cargo build --release -p zen_cli       (builds ./target/release/zen)
#   - zen serve --config examples/zenithdb.dev.toml   (in another shell)
#
# Environment overrides:
#   TARGET           server URL                       (default http://localhost:8080)
#   TOTAL_ROWS       total span count                 (default 1000000000)
#   CHUNK_ROWS       rows per generation chunk        (default 5000000)
#   CONCURRENCY      ingest concurrency               (default 32)
#   BATCH_SIZE       spans per ingest request         (default 500)
#   COMPACT_EVERY    compact every N chunks           (default 5)
#   TENANT_ID        tenant to compact + query        (default 0)
#   PARTITION_ID     partition to compact             (default 0)
#   SUITE_SECONDS    bench-run duration per query     (default 60)
#   WORK_DIR         scratch dir for chunk files      (default ./bench-1b-work)
#   OUTPUT           result JSON path                 (default bench-results/1b-<timestamp>.json)
#   ZEN_BIN          path to the zen binary           (default ./target/release/zen)
#
# Total elapsed depends entirely on hardware (NVMe + many cores helps).
# Plan for 12-48 hours on a single host. The script prints a running
# rate (rows/sec) and ETA so you can ctrl-C and tune CHUNK_ROWS /
# CONCURRENCY / BATCH_SIZE if ingest is bottlenecked.

set -euo pipefail

TARGET="${TARGET:-http://localhost:8080}"
TOTAL_ROWS="${TOTAL_ROWS:-1000000000}"
CHUNK_ROWS="${CHUNK_ROWS:-5000000}"
CONCURRENCY="${CONCURRENCY:-32}"
BATCH_SIZE="${BATCH_SIZE:-500}"
COMPACT_EVERY="${COMPACT_EVERY:-5}"
TENANT_ID="${TENANT_ID:-0}"
PARTITION_ID="${PARTITION_ID:-0}"
SUITE_SECONDS="${SUITE_SECONDS:-60}"
WORK_DIR="${WORK_DIR:-./bench-1b-work}"
ZEN_BIN="${ZEN_BIN:-./target/release/zen}"
TS="$(date +%Y%m%d-%H%M%S)"
OUTPUT="${OUTPUT:-bench-results/1b-${TS}.json}"

if [ ! -x "$ZEN_BIN" ]; then
  echo "FATAL: '$ZEN_BIN' not found or not executable." >&2
  echo "       Build it with: cargo build --release -p zen_cli" >&2
  exit 1
fi

if ! curl -sf "${TARGET}/v1/healthz" >/dev/null; then
  echo "FATAL: server not reachable at $TARGET." >&2
  echo "       Start it with: $ZEN_BIN serve --config examples/zenithdb.dev.toml" >&2
  exit 1
fi

mkdir -p "$WORK_DIR"
mkdir -p "$(dirname "$OUTPUT")"

NUM_CHUNKS=$(( (TOTAL_ROWS + CHUNK_ROWS - 1) / CHUNK_ROWS ))
echo "==============================================================="
echo " ZenithDB 1B-row benchmark"
echo "---------------------------------------------------------------"
echo " target          : $TARGET"
echo " total rows      : $TOTAL_ROWS"
echo " chunk rows      : $CHUNK_ROWS  ($NUM_CHUNKS chunks)"
echo " ingest          : batch=$BATCH_SIZE concurrency=$CONCURRENCY"
echo " compact         : every $COMPACT_EVERY chunks"
echo " tenant/partition: $TENANT_ID / $PARTITION_ID"
echo " suite duration  : ${SUITE_SECONDS}s per query"
echo " scratch dir     : $WORK_DIR"
echo " output          : $OUTPUT"
echo "==============================================================="

START_TS=$(date +%s)
TOTAL_LOADED=0

for ((i=1; i<=NUM_CHUNKS; i++)); do
  REMAINING=$(( TOTAL_ROWS - TOTAL_LOADED ))
  THIS=$(( REMAINING < CHUNK_ROWS ? REMAINING : CHUNK_ROWS ))
  CHUNK_FILE="$WORK_DIR/chunk-$(printf '%05d' "$i").json"

  echo "[$i/$NUM_CHUNKS] gen $THIS rows"
  "$ZEN_BIN" bench-gen --rows "$THIS" --output "$CHUNK_FILE"

  echo "[$i/$NUM_CHUNKS] load -> $TARGET"
  "$ZEN_BIN" bench-load \
    --input "$CHUNK_FILE" \
    --target "$TARGET" \
    --batch-size "$BATCH_SIZE" \
    --concurrency "$CONCURRENCY"

  rm -f "$CHUNK_FILE"
  TOTAL_LOADED=$(( TOTAL_LOADED + THIS ))

  if (( i % COMPACT_EVERY == 0 )) || (( i == NUM_CHUNKS )); then
    echo "[$i/$NUM_CHUNKS] compact tenant=$TENANT_ID partition=$PARTITION_ID"
    curl -sf -X POST "$TARGET/v1/compact" \
      -H 'content-type: application/json' \
      -d "{\"tenant_id\":$TENANT_ID,\"partition_id\":$PARTITION_ID}" \
      >/dev/null || echo "  warn: compact endpoint returned non-2xx (continuing)"
  fi

  ELAPSED=$(( $(date +%s) - START_TS ))
  RATE=$(( TOTAL_LOADED / (ELAPSED > 0 ? ELAPSED : 1) ))
  REMAINING_ROWS=$(( TOTAL_ROWS - TOTAL_LOADED ))
  ETA=$(( RATE > 0 ? REMAINING_ROWS / RATE : 0 ))
  printf "  loaded %d/%d (%.2f%%)  elapsed=%ds  rate=%d/s  eta=%dm\n" \
    "$TOTAL_LOADED" "$TOTAL_ROWS" \
    "$(awk "BEGIN{print $TOTAL_LOADED/$TOTAL_ROWS*100}")" \
    "$ELAPSED" "$RATE" "$(( ETA / 60 ))"
done

LOAD_ELAPSED=$(( $(date +%s) - START_TS ))
echo
echo "load phase complete in ${LOAD_ELAPSED}s ($(( LOAD_ELAPSED / 60 )) min)"
echo

# Optional segment summary (non-fatal if endpoint changes shape)
echo "segment summary (tenant=$TENANT_ID):"
curl -sf "$TARGET/v1/segments?tenant_id=$TENANT_ID" 2>/dev/null \
  | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    n = d.get('count', len(d.get('segments', [])))
    segs = d.get('segments', [])
    rows = sum(s.get('row_count', 0) for s in segs)
    bts  = sum(s.get('byte_count', 0) for s in segs)
    print(f'  segments     : {n}')
    print(f'  total rows   : {rows:,}')
    print(f'  total bytes  : {bts/1024/1024/1024:.2f} GiB')
except Exception as e:
    print(f'  (skipped: {e})')
" || echo "  (segments endpoint unavailable, skipping)"

echo
echo "running query suite for ${SUITE_SECONDS}s per query ..."
"$ZEN_BIN" bench-run \
  --target "$TARGET" \
  --seconds "$SUITE_SECONDS" \
  --concurrency 1 \
  --output "$OUTPUT"

echo
echo "==============================================================="
echo " done"
echo "---------------------------------------------------------------"
echo " load elapsed  : ${LOAD_ELAPSED}s ($(( LOAD_ELAPSED / 60 )) min)"
echo " result file   : $OUTPUT"
echo " compare with  : $ZEN_BIN bench-compare --candidate $OUTPUT"
echo "==============================================================="
