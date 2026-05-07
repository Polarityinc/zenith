#!/usr/bin/env bash
# Build a Zenith deployment with many segments.
# Splits the corpus into N batches; each batch is loaded then compacted into
# a separate segment, simulating natural segment growth over time.

set -euo pipefail

CORPUS="${CORPUS:-/tmp/zen-corpus-200k.json}"
TARGET="${TARGET:-http://localhost:8080}"
TENANT="${TENANT:-0}"
BATCH_SIZE_BYTES="${BATCH_SIZE_BYTES:-32_000_000}"

if [ ! -f "$CORPUS" ]; then
  echo "FATAL: corpus $CORPUS not found"
  exit 1
fi

echo "Splitting $CORPUS into batches and loading + compacting..."
SPLIT_DIR=$(mktemp -d)
trap "rm -rf $SPLIT_DIR" EXIT

# Pull spans, write 50 chunked files.
python3 -c "
import json, os, sys
with open('$CORPUS') as f:
    spans = json.load(f)
n = len(spans)
batches = 50
size = n // batches
for i in range(batches):
    chunk = spans[i*size:(i+1)*size]
    with open('$SPLIT_DIR/batch_%02d.json' % i, 'w') as f:
        json.dump(chunk, f)
print(f'wrote {batches} batches of ~{size} spans each')
"

i=0
for f in "$SPLIT_DIR"/batch_*.json; do
  i=$((i+1))
  ./target/release/zen bench-load --input "$f" --target "$TARGET" --batch-size 1000 --concurrency 8 2>&1 | tail -1
  curl -s -X POST "$TARGET/v1/compact" -H "content-type: application/json" \
    -d "{\"tenant_id\":$TENANT,\"partition_id\":0}" > /dev/null
  if [ $((i % 10)) -eq 0 ]; then
    echo "  ... $i/50 batches loaded + compacted"
  fi
done

echo "---"
curl -s "$TARGET/v1/segments?tenant_id=$TENANT" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f\"segments: {data['count']}\")
total_bytes = sum(s['byte_count'] for s in data['segments'])
total_rows = sum(s['row_count'] for s in data['segments'])
print(f'total rows: {total_rows:,}, total bytes: {total_bytes/1024/1024:.1f} MB')
"
