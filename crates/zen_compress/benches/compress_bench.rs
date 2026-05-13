use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

use zen_compress::{
    for_decompress, for_encode, gorilla_decompress, gorilla_encode, rle_decompress, rle_encode,
    zstd_compress, zstd_decompress, DictBuilder, DictDecoder, FsstCompressor,
};

fn natural_text_corpus() -> Vec<&'static [u8]> {
    vec![
        b"the quick brown fox jumps over the lazy dog".as_slice(),
        b"out of memory error in compaction worker".as_slice(),
        b"rate limit exceeded for tier free; please upgrade".as_slice(),
        b"finished tool call get_user_orders successfully".as_slice(),
        b"the model returned an unexpected response shape".as_slice(),
        b"pre-flight checks passed; entering main loop".as_slice(),
        b"the quick brown fox jumps over the lazy fox".as_slice(),
        b"out of memory while allocating a 16MB chunk".as_slice(),
        b"out of memory error in retrieval cache".as_slice(),
        b"finished tool call get_user_orders with 0 rows".as_slice(),
        b"begin agent step 47 with input from previous reasoning step".as_slice(),
        b"calling tool get_weather for location San Francisco".as_slice(),
        b"received chunk 3/12 from streaming response (89 tokens)".as_slice(),
        b"initiating retrieval over the support kb collection".as_slice(),
        b"the assistant produced a JSON output that did not parse".as_slice(),
        b"forwarding request to backend pool main with retry budget 3".as_slice(),
    ]
}

fn build_corpus(n: usize) -> Vec<&'static [u8]> {
    let base = natural_text_corpus();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(base[i % base.len()]);
    }
    out
}

fn bench_fsst(c: &mut Criterion) {
    let mut group = c.benchmark_group("fsst");
    let rows = build_corpus(2048);
    let raw_total: usize = rows.iter().map(|r| r.len()).sum();
    let comp = FsstCompressor::train(&rows);

    group.throughput(Throughput::Bytes(raw_total as u64));
    group.bench_function("encode_2048_rows", |b| {
        b.iter(|| black_box(comp.encode_page(&rows)));
    });

    let page = comp.encode_page(&rows);
    group.bench_function("decode_one_row_at_idx_1000", |b| {
        let view = FsstCompressor::open(&page).unwrap();
        b.iter(|| black_box(view.decode_row(1000).unwrap()));
    });

    group.bench_function("decode_all_rows", |b| {
        let view = FsstCompressor::open(&page).unwrap();
        b.iter(|| {
            for i in 0..view.row_count() {
                black_box(view.decode_row(i).unwrap());
            }
        });
    });
    group.finish();
}

fn bench_zstd(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd");
    let mut data = Vec::with_capacity(64 * 1024);
    let mut rng = StdRng::seed_from_u64(0xfeed_face);
    for _ in 0..(64 * 1024) {
        if rng.gen_bool(0.3) {
            data.push(rng.gen());
        } else {
            data.push(b'A' + (rng.gen_range(0..16) as u8));
        }
    }
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("encode_64kb_level3", |b| {
        b.iter(|| black_box(zstd_compress(&data, 3).unwrap()));
    });
    let comp = zstd_compress(&data, 3).unwrap();
    group.bench_function("decode_64kb", |b| {
        b.iter(|| black_box(zstd_decompress(&comp).unwrap()));
    });
    group.finish();
}

fn bench_gorilla(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla");
    let v: Vec<f64> = (0..16_384).map(|i| 100.0 + (i as f64) * 0.001).collect();
    group.throughput(Throughput::Bytes((v.len() * 8) as u64));
    group.bench_function("encode_16k_smooth", |b| {
        b.iter(|| black_box(gorilla_encode(&v).unwrap()));
    });
    let bytes = gorilla_encode(&v).unwrap();
    group.bench_function("decode_16k_smooth", |b| {
        b.iter(|| black_box(gorilla_decompress(&bytes).unwrap()));
    });
    group.finish();
}

fn bench_for(c: &mut Criterion) {
    let mut group = c.benchmark_group("for_bitpack");
    let v: Vec<i64> = (0..16_384).map(|i| 1_700_000_000_000 + i).collect();
    group.throughput(Throughput::Bytes((v.len() * 8) as u64));
    group.bench_function("encode_16k_monotonic", |b| {
        b.iter(|| black_box(for_encode(&v)));
    });
    let bytes = for_encode(&v);
    group.bench_function("decode_16k_monotonic", |b| {
        b.iter(|| black_box(for_decompress(&bytes).unwrap()));
    });
    group.finish();
}

fn bench_rle(c: &mut Criterion) {
    let mut group = c.benchmark_group("rle");
    let mut v: Vec<i64> = Vec::with_capacity(16_384);
    let mut rng = StdRng::seed_from_u64(7);
    while v.len() < 16_384 {
        let val = rng.gen_range(0..5);
        let run = rng.gen_range(20..200);
        for _ in 0..run.min(16_384 - v.len()) {
            v.push(val);
        }
    }
    group.throughput(Throughput::Bytes((v.len() * 8) as u64));
    group.bench_function("encode_16k_runs", |b| {
        b.iter(|| black_box(rle_encode(&v)));
    });
    let bytes = rle_encode(&v);
    group.bench_function("decode_16k_runs", |b| {
        b.iter(|| black_box(rle_decompress(&bytes).unwrap()));
    });
    group.finish();
}

fn bench_dict(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict");
    let models = [
        "gpt-4o",
        "claude-sonnet-4-7",
        "gpt-5-mini",
        "haiku-4-5",
        "o4-mini",
    ];
    let mut rng = StdRng::seed_from_u64(11);
    let n = 16_384;
    let raw_bytes: u64 = (0..n)
        .map(|_| models[rng.gen_range(0..models.len())].len() as u64)
        .sum();
    group.throughput(Throughput::Bytes(raw_bytes));
    group.bench_function("encode_16k_low_card", |b| {
        b.iter_batched(
            || StdRng::seed_from_u64(11),
            |mut rng| {
                let mut builder = DictBuilder::new();
                for _ in 0..n {
                    builder.push(models[rng.gen_range(0..models.len())].as_bytes());
                }
                black_box(builder.finish().unwrap())
            },
            BatchSize::SmallInput,
        );
    });
    let bytes = {
        let mut rng = StdRng::seed_from_u64(11);
        let mut b = DictBuilder::new();
        for _ in 0..n {
            b.push(models[rng.gen_range(0..models.len())].as_bytes());
        }
        b.finish().unwrap()
    };
    group.bench_function("decode_16k_low_card", |b| {
        b.iter(|| black_box(DictDecoder::open(&bytes).unwrap()));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_fsst,
    bench_zstd,
    bench_gorilla,
    bench_for,
    bench_rle,
    bench_dict,
);
criterion_main!(benches);
