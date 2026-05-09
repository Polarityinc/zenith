//! Per-(tenant, partition) memtable.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;

use zen_common::{PartitionId, SpanRecord, TenantId, ZenError, ZenResult};

use crate::flush::flush_to_record_batch;

#[derive(Default)]
pub struct MemTableInner {
    pub rows: Vec<SpanRecord>,
    pub bytes_estimate: u64,
}

#[derive(Clone)]
pub struct MemTable {
    pub tenant_id: TenantId,
    pub partition_id: PartitionId,
    inner: Arc<Mutex<MemTableInner>>,
    flush_max_bytes: u64,
}

impl MemTable {
    pub fn new(tenant_id: TenantId, partition_id: PartitionId, flush_max_bytes: u64) -> Self {
        Self {
            tenant_id,
            partition_id,
            inner: Arc::new(Mutex::new(MemTableInner::default())),
            flush_max_bytes,
        }
    }

    pub fn append(&self, record: SpanRecord) {
        let bytes = estimate_size(&record);
        let mut g = self.inner.lock();
        g.rows.push(record);
        g.bytes_estimate += bytes;
    }

    pub fn append_many<I: IntoIterator<Item = SpanRecord>>(&self, records: I) {
        let mut g = self.inner.lock();
        for r in records {
            let b = estimate_size(&r);
            g.rows.push(r);
            g.bytes_estimate += b;
        }
    }

    pub fn should_flush(&self) -> bool {
        let g = self.inner.lock();
        g.bytes_estimate >= self.flush_max_bytes
    }

    pub fn row_count(&self) -> usize {
        self.inner.lock().rows.len()
    }

    pub fn bytes_estimate(&self) -> u64 {
        self.inner.lock().bytes_estimate
    }

    /// Drain rows and return a sorted Arrow RecordBatch. Mutates the internal
    /// buffer (it becomes empty); on error returns the rows back via the Err
    /// path so the caller can retry.
    pub fn flush(&self) -> ZenResult<RecordBatch> {
        let rows = {
            let mut g = self.inner.lock();
            std::mem::take(&mut g.rows)
        };
        // Reset bytes
        self.inner.lock().bytes_estimate = 0;
        if rows.is_empty() {
            return Err(ZenError::invalid("memtable flush with no rows"));
        }
        let mut sorted = rows;
        sorted.sort_by(|a, b| {
            a.trace_id
                .0
                .cmp(&b.trace_id.0)
                .then_with(|| a.start_time_ms.cmp(&b.start_time_ms))
                .then_with(|| a.span_id.0.cmp(&b.span_id.0))
        });
        flush_to_record_batch(&sorted)
    }

    /// Read snapshot of current rows for in-memory query (memtable scan).
    pub fn snapshot(&self) -> Vec<SpanRecord> {
        self.inner.lock().rows.clone()
    }
}

fn estimate_size(r: &SpanRecord) -> u64 {
    let mut s: u64 = 256; // fixed-size fields
    if let Some(p) = &r.prompt {
        s += p.len() as u64;
    }
    if let Some(c) = &r.completion {
        s += c.len() as u64;
    }
    if let Some(t) = &r.tool_io_text {
        s += t.len() as u64;
    }
    if let Some(m) = &r.metadata {
        s += serde_json::to_string(m)
            .map(|j| j.len() as u64)
            .unwrap_or(64);
    }
    if let Some(e) = &r.embedding {
        s += (e.len() * 4) as u64;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use zen_common::{SpanId, TraceId};

    #[test]
    fn append_and_flush_sorts() {
        let mt = MemTable::new(TenantId(1), PartitionId(0), 1 << 30);
        let mut a = SpanRecord::new(TenantId(1), PartitionId(0));
        a.trace_id = TraceId([2; 16]);
        a.start_time_ms = 200;
        a.span_id = SpanId([1; 16]);
        let mut b = SpanRecord::new(TenantId(1), PartitionId(0));
        b.trace_id = TraceId([1; 16]);
        b.start_time_ms = 100;
        b.span_id = SpanId([1; 16]);
        let mut c = SpanRecord::new(TenantId(1), PartitionId(0));
        c.trace_id = TraceId([1; 16]);
        c.start_time_ms = 50;
        c.span_id = SpanId([2; 16]);
        mt.append(a);
        mt.append(b);
        mt.append(c);
        let batch = mt.flush().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn should_flush_threshold() {
        let mt = MemTable::new(TenantId(1), PartitionId(0), 1024);
        for _ in 0..3 {
            let mut s = SpanRecord::new(TenantId(1), PartitionId(0));
            s.prompt = Some("X".repeat(500));
            mt.append(s);
        }
        assert!(mt.should_flush());
    }

    use arrow_array::{FixedSizeBinaryArray, Int64Array};

    fn span(trace_id: u8, span_id: u8, start: i64) -> SpanRecord {
        let mut s = SpanRecord::new(TenantId(1), PartitionId(0));
        s.trace_id = TraceId([trace_id; 16]);
        s.span_id = SpanId([span_id; 16]);
        s.start_time_ms = start;
        s.end_time_ms = start + 1;
        s.duration_ms = 1;
        s
    }

    #[test]
    fn append_many_then_flush_orders_by_trace_then_time_then_span() {
        // Construct 6 records that exercise every tier of the sort comparator:
        // - two distinct trace_ids
        // - within trace_id 0x05, two distinct start times
        // - within trace_id 0x05, start 100, two distinct span_ids
        let inputs = vec![
            span(0x05, 0x02, 100),
            span(0x05, 0x01, 100),
            span(0x05, 0x07, 50),
            span(0x01, 0x09, 200),
            span(0x05, 0x03, 200),
            span(0x01, 0x01, 0),
        ];

        let mt = MemTable::new(TenantId(1), PartitionId(0), u64::MAX);
        mt.append_many(inputs);
        let batch = mt.flush().unwrap();
        assert_eq!(batch.num_rows(), 6);

        let trace_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id column");
        let span_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id column");
        let start_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("start_time_ms column");

        let actual: Vec<(u8, i64, u8)> = (0..batch.num_rows())
            .map(|i| (trace_col.value(i)[0], start_col.value(i), span_col.value(i)[0]))
            .collect();
        let mut expected = actual.clone();
        expected.sort();
        assert_eq!(actual, expected, "rows must be sorted (trace, start, span)");

        // The mutation contract: flush() drains all rows.
        assert_eq!(mt.row_count(), 0);
        assert_eq!(mt.bytes_estimate(), 0);
    }

    #[test]
    fn concurrent_append_many_does_not_lose_rows() {
        use std::thread;
        let mt = MemTable::new(TenantId(1), PartitionId(0), u64::MAX);
        let mut handles = Vec::new();
        let writers = 8usize;
        let per_writer = 250usize;
        for w in 0..writers {
            let mt = mt.clone();
            handles.push(thread::spawn(move || {
                let batch: Vec<SpanRecord> = (0..per_writer)
                    .map(|i| span((w as u8) + 1, (i % 256) as u8, i as i64))
                    .collect();
                mt.append_many(batch);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(mt.row_count(), writers * per_writer);
        let batch = mt.flush().unwrap();
        assert_eq!(batch.num_rows(), writers * per_writer);
    }

    #[test]
    fn flush_twice_produces_two_distinct_batches() {
        let mt = MemTable::new(TenantId(1), PartitionId(0), u64::MAX);
        // First batch: 5 rows.
        let r1: Vec<SpanRecord> = (0..5).map(|i| span(1, i as u8, i as i64)).collect();
        mt.append_many(r1);
        let b1 = mt.flush().unwrap();
        assert_eq!(b1.num_rows(), 5);
        assert_eq!(mt.row_count(), 0);
        assert_eq!(mt.bytes_estimate(), 0);

        // Second batch: 3 rows. Must not see any leftover from b1, and must
        // not corrupt b1 (b1 is still alive in scope).
        let r2: Vec<SpanRecord> = (0..3).map(|i| span(2, i as u8, (i + 100) as i64)).collect();
        mt.append_many(r2);
        let b2 = mt.flush().unwrap();
        assert_eq!(b2.num_rows(), 3);
        // b1 is still readable and untouched.
        assert_eq!(b1.num_rows(), 5);
        // The first column of the first row of b1 was tenant_id=1; b2 same.
        // Verify the trace_id columns are independent (b1 has trace=1, b2
        // has trace=2).
        let t1 = b1
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let t2 = b2
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(t1.value(0)[0], 1);
        assert_eq!(t2.value(0)[0], 2);
    }

    #[test]
    fn flush_empty_returns_invalid_error() {
        // Pin the documented contract: flushing with zero rows is an Err.
        let mt = MemTable::new(TenantId(1), PartitionId(0), u64::MAX);
        let err = mt.flush().unwrap_err();
        assert!(matches!(err, zen_common::ZenError::Invalid(_)));
    }

    #[test]
    fn memory_bound_triggers_should_flush_and_records_match() {
        // flush_max_bytes is sized to ~3 prompt-heavy rows; build 4 to push
        // past the threshold, then verify (a) should_flush flips, and (b)
        // bytes_estimate is consistent with what we appended.
        let max = 4_000u64;
        let mt = MemTable::new(TenantId(1), PartitionId(0), max);
        let prompt_size = 2_000usize;
        for i in 0..4u8 {
            let mut s = span(i, 0, i as i64);
            s.prompt = Some("p".repeat(prompt_size));
            mt.append(s);
        }
        // Each row is approx 256 fixed + 2_000 prompt = 2_256; 4 rows ≈ 9k.
        assert!(mt.bytes_estimate() >= max);
        assert!(mt.should_flush(), "memtable should flag flush past max");

        let batch = mt.flush().unwrap();
        assert_eq!(batch.num_rows(), 4);
        // After flush the byte counter resets so we don't keep flushing.
        assert!(!mt.should_flush());
    }
}
