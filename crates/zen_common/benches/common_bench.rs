use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use zen_common::{Schema, SchemaFingerprint, SpanId, SpanRecord, TenantId, PartitionId, TraceId};

fn bench_trace_id_roundtrip(c: &mut Criterion) {
    c.bench_function("trace_id_to_string_then_parse", |b| {
        b.iter_batched(
            TraceId::new_random,
            |id| {
                let s = id.to_string();
                let parsed: TraceId = s.parse().unwrap();
                black_box(parsed)
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_span_id_roundtrip(c: &mut Criterion) {
    c.bench_function("span_id_to_string_then_parse", |b| {
        b.iter_batched(
            SpanId::new_random,
            |id| {
                let s = id.to_string();
                let parsed: SpanId = s.parse().unwrap();
                black_box(parsed)
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_schema_fingerprint(c: &mut Criterion) {
    let schema = Schema::spans_v1();
    c.bench_function("schema_fingerprint_spans_v1", |b| {
        b.iter(|| black_box(schema.fingerprint()));
    });
}

fn bench_span_record_construction(c: &mut Criterion) {
    c.bench_function("span_record_default", |b| {
        b.iter(|| black_box(SpanRecord::new(TenantId(1), PartitionId(0))));
    });
}

fn bench_fingerprint_unchanged(c: &mut Criterion) {
    let s1 = Schema::spans_v1();
    let s2 = Schema::spans_v1();
    let f1 = s1.fingerprint();
    let f2 = s2.fingerprint();
    assert_eq!(f1, f2);
    c.bench_function("schema_fingerprint_eq", |b| {
        b.iter(|| black_box(f1 == f2));
    });
    let _ = SchemaFingerprint(0);
}

criterion_group!(
    benches,
    bench_trace_id_roundtrip,
    bench_span_id_roundtrip,
    bench_schema_fingerprint,
    bench_span_record_construction,
    bench_fingerprint_unchanged,
);
criterion_main!(benches);
