use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use roaring::RoaringBitmap;

use zen_index::{BloomFilter, PostingList, PostingMap};

fn bench_posting_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("posting_build");
    let n = 1_000_000u32;
    let pool = ["A", "B", "C", "D", "E", "F", "G", "H"];
    let mut rng = StdRng::seed_from_u64(0xc0ffee);
    let values: Vec<&str> = (0..n).map(|_| pool[rng.gen_range(0..pool.len())]).collect();
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("insert_1M_8card", |b| {
        b.iter(|| {
            let mut m = PostingMap::new();
            for (i, v) in values.iter().enumerate() {
                m.insert(v.as_bytes(), i as u32);
            }
            black_box(m);
        });
    });
    group.finish();
}

fn bench_posting_intersect(c: &mut Criterion) {
    let mut group = c.benchmark_group("posting_intersect");
    let n = 1_000_000u32;
    let mut rng = StdRng::seed_from_u64(0xdeadbeef);
    let mut a = RoaringBitmap::new();
    let mut b = RoaringBitmap::new();
    for i in 0..n {
        if rng.gen_bool(0.4) {
            a.insert(i);
        }
        if rng.gen_bool(0.6) {
            b.insert(i);
        }
    }
    let pa = PostingList { bitmap: a };
    let pb = PostingList { bitmap: b };
    group.throughput(Throughput::Elements(pa.cardinality() + pb.cardinality()));
    group.bench_function("AND_1M_rows", |bb| {
        bb.iter(|| {
            let mut x = pa.clone();
            x.and_assign(&pb);
            black_box(x);
        });
    });
    group.finish();
}

fn bench_bloom(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom");
    let n = 100_000;
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("insert_100k", |b| {
        b.iter(|| {
            let mut bf = BloomFilter::new(n, 0.01);
            for i in 0..n {
                bf.insert(format!("k{i}").as_bytes());
            }
            black_box(bf);
        });
    });
    let mut bf = BloomFilter::new(n, 0.01);
    for i in 0..n {
        bf.insert(format!("k{i}").as_bytes());
    }
    group.bench_function("contains_100k_present", |b| {
        b.iter(|| {
            for i in 0..n {
                black_box(bf.contains(format!("k{i}").as_bytes()));
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_posting_build,
    bench_posting_intersect,
    bench_bloom
);
criterion_main!(benches);
