//! Microbenchmarks focused on MemHashIndex overflow bucket behavior.
//!
//! These benchmarks intentionally force many keys into the same base bucket to:
//! - Exercise overflow bucket allocation and traversal.
//! - Stress the "insert new key" path (no existing entry).

use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode,
    Throughput,
};

use oxifaster::index::{KeyHash, MemHashIndex, MemHashIndexConfig};

mod common_profiler;

fn build_hashes_same_bucket(n: usize, bucket_low_bits: u64) -> Vec<KeyHash> {
    // The tag is 14 bits derived from bits [48..62). Tags must be unique within a bucket chain,
    // otherwise MemHashIndex will detect a tag conflict and retry indefinitely.
    assert!(n <= (1 << KeyHash::TAG_BITS) as usize);

    (0..n)
        .map(|i| {
            let tag = i as u64;
            let hash = bucket_low_bits | (tag << 48);
            KeyHash::new(hash)
        })
        .collect()
}

fn bench_insert_same_bucket(c: &mut Criterion) {
    const TABLE_SIZE: u64 = 64;
    // Force all keys into the same base bucket: hash_table_index(size) uses low bits.
    const BUCKET_LOW_BITS: u64 = 0x2A;

    let mut group = c.benchmark_group("index/insert_same_bucket");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5));

    for n in [64usize, 256, 1024, 4096] {
        let hashes = build_hashes_same_bucket(n, BUCKET_LOW_BITS);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("n", n), |b| {
            b.iter_batched(
                || {
                    let mut index = MemHashIndex::new();
                    index.initialize(&MemHashIndexConfig::new(TABLE_SIZE));
                    index
                },
                |index| {
                    for h in &hashes {
                        let r = index.find_or_create_entry(black_box(*h));
                        black_box(&r);
                        assert!(r.atomic_entry.is_some());
                    }
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = common_profiler::configured_criterion();
    targets = bench_insert_same_bucket
);
criterion_main!(benches);
