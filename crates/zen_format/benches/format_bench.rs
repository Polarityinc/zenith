use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use zen_common::{CommitId, PartitionId, SchemaFingerprint, SpanId, TenantId, TraceId};
use zen_format::{
    encode_page, ColumnValues, PageEncoding, RowGroupBuilder, SegmentMetadata, SegmentReader,
    SegmentWriter,
};

fn build_realistic_segment(rows: usize) -> Vec<u8> {
    let fp = SchemaFingerprint(0x1234);
    let mut meta = SegmentMetadata::new(
        1,
        TenantId(7),
        PartitionId(0),
        fp,
        vec![
            "trace_id".into(),
            "start_time_ms".into(),
            "model".into(),
            "prompt".into(),
        ],
        vec!["trace_id".into(), "start_time_ms".into()],
    );
    meta.observe_time(1_700_000_000_000);
    meta.observe_time(1_700_000_000_000 + rows as i64);
    meta.observe_commit(CommitId(1));
    meta.observe_commit(CommitId(rows as u64));
    meta.observe_trace_id(TraceId([0; 16]));
    meta.observe_trace_id(TraceId([0xFF; 16]));
    meta.observe_span_id(SpanId([0; 16]));
    meta.observe_span_id(SpanId([0xFF; 16]));

    let mut writer = SegmentWriter::new(meta);
    let mut rgb = RowGroupBuilder::new(rows as u32);

    // trace_ids
    let trace_ids: Vec<[u8; 16]> = (0..rows).map(|i| [(i % 256) as u8; 16]).collect();
    let (e, b) = encode_page(ColumnValues::Fixed16(trace_ids), PageEncoding::FixedRaw).unwrap();
    rgb.add_page(0, e, b.to_vec(), 16 * rows as u64);

    // start_time_ms
    let times: Vec<i64> = (0..rows as i64).map(|i| 1_700_000_000_000 + i * 1000).collect();
    let (e, b) = encode_page(ColumnValues::I64(times), PageEncoding::For).unwrap();
    rgb.add_page(1, e, b.to_vec(), 8 * rows as u64);

    // model (low-cardinality)
    let pool = ["gpt-4o", "claude-sonnet-4-7", "haiku-4-5", "gpt-5-mini"];
    let models: Vec<Vec<u8>> = (0..rows).map(|i| pool[i % pool.len()].as_bytes().to_vec()).collect();
    let (e, b) = encode_page(ColumnValues::StringsOwned(models), PageEncoding::Dict).unwrap();
    rgb.add_page(2, e, b.to_vec(), 8 * rows as u64);

    // prompts (FSST + per-row offsets)
    let prompt_pool: [&[u8]; 4] = [
        b"the quick brown fox jumps over the lazy dog",
        b"out of memory error in compaction worker",
        b"finished tool call get_user_orders successfully",
        b"received chunk 3 of 12 from streaming response",
    ];
    let prompts: Vec<Vec<u8>> = (0..rows).map(|i| prompt_pool[i % prompt_pool.len()].to_vec()).collect();
    let raw_prompt_bytes: u64 = prompts.iter().map(|p| p.len() as u64).sum();
    let (e, b) = encode_page(
        ColumnValues::StringsOwned(prompts),
        PageEncoding::FsstWithOffsets,
    )
    .unwrap();
    rgb.add_page(3, e, b.to_vec(), raw_prompt_bytes);

    let (payload, header) = rgb.finish();
    writer.add_row_group(header, payload);
    writer.finish().unwrap().to_vec()
}

fn bench_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_open");
    let bytes = build_realistic_segment(10_000);
    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("from_bytes_10k_rows", |b| {
        b.iter(|| black_box(SegmentReader::from_bytes(bytes.clone()).unwrap()));
    });
    group.finish();
}

fn bench_read_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_read_column");
    let bytes = build_realistic_segment(10_000);
    let reader = SegmentReader::from_bytes(bytes).unwrap();
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("read_full_prompt_column_10k", |b| {
        b.iter(|| black_box(reader.read_column(0, 3).unwrap()));
    });
    group.bench_function("read_full_model_column_10k", |b| {
        b.iter(|| black_box(reader.read_column(0, 2).unwrap()));
    });
    group.bench_function("read_full_time_column_10k", |b| {
        b.iter(|| black_box(reader.read_column(0, 1).unwrap()));
    });
    group.finish();
}

fn bench_late_mat(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_late_mat");
    let bytes = build_realistic_segment(10_000);
    let reader = SegmentReader::from_bytes(bytes).unwrap();
    group.bench_function("read_one_prompt_at_idx_5000_via_read_row", |b| {
        b.iter(|| black_box(reader.read_row(0, 3, 5000).unwrap()));
    });
    let indices: Vec<usize> = (0..100).map(|i| i * 100).collect();
    group.bench_function("read_100_scattered_prompts_via_read_rows", |b| {
        b.iter(|| black_box(reader.read_rows(0, 3, &indices).unwrap()));
    });
    let indices_1k: Vec<usize> = (0..1000).map(|i| i * 10).collect();
    group.bench_function("read_1000_scattered_prompts_via_read_rows", |b| {
        b.iter(|| black_box(reader.read_rows(0, 3, &indices_1k).unwrap()));
    });
    // For comparison, the slow path: open page per row.
    group.bench_function("read_100_scattered_prompts_via_per_row_open", |b| {
        b.iter(|| {
            for &i in &indices {
                black_box(reader.read_row(0, 3, i).unwrap());
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_open, bench_read_column, bench_late_mat);
criterion_main!(benches);
