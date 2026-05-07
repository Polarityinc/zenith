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
    pub fn new(
        tenant_id: TenantId,
        partition_id: PartitionId,
        flush_max_bytes: u64,
    ) -> Self {
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
        s += serde_json::to_string(m).map(|j| j.len() as u64).unwrap_or(64);
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
}
