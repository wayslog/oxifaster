//! Concurrent microbenchmarks focused on MemHashIndex overflow bucket behavior.
//!
//! These benchmarks force many keys into the same base bucket to:
//! - Exercise overflow bucket allocation and traversal under contention.
//! - Stress the "insert new key" path (no existing entry) with multiple threads.

use std::sync::Arc;
use std::time::Duration;

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, black_box, criterion_group,
    criterion_main,
};
use rayon::prelude::*;

use oxifaster::index::{KeyHash, MemHashIndex, MemHashIndexConfig};

mod common_profiler;

fn build_hashes_same_bucket_range(
    start_tag: usize,
    count: usize,
    bucket_low_bits: u64,
) -> Vec<KeyHash> {
    // The tag is 14 bits derived from bits [48..62). Tags must be unique within a bucket chain,
    // otherwise MemHashIndex will detect a tag conflict and retry indefinitely.
    assert!(start_tag + count <= (1 << KeyHash::TAG_BITS) as usize);

    (start_tag..start_tag + count)
        .map(|i| {
            let tag = i as u64;
            let hash = bucket_low_bits | (tag << 48);
            KeyHash::new(hash)
        })
        .collect()
}

fn bench_insert_same_bucket_concurrent(c: &mut Criterion) {
    const TABLE_SIZE: u64 = 64;
    // Force all keys into the same base bucket: hash_table_index(size) uses low bits.
    const BUCKET_LOW_BITS: u64 = 0x2A;
    const OPS_PER_THREAD: usize = 1024;

    let mut group = c.benchmark_group("index/insert_same_bucket_concurrent");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(30);

    for num_threads in [1usize, 2, 4, 8] {
        let ops_total = OPS_PER_THREAD * num_threads;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        let per_thread_hashes: Vec<Vec<KeyHash>> = (0..num_threads)
            .map(|thread_id| {
                build_hashes_same_bucket_range(
                    thread_id * OPS_PER_THREAD,
                    OPS_PER_THREAD,
                    BUCKET_LOW_BITS,
                )
            })
            .collect();

        group.throughput(Throughput::Elements(ops_total as u64));
        group.bench_function(BenchmarkId::new("threads", num_threads), |b| {
            b.iter_batched(
                || {
                    let mut index = MemHashIndex::new();
                    index.initialize(&MemHashIndexConfig::new(TABLE_SIZE).unwrap());
                    index
                },
                |index| {
                    let index = Arc::new(index);
                    pool.install(|| {
                        per_thread_hashes.par_iter().for_each(|hashes| {
                            for h in hashes {
                                let r = index.find_or_create_entry(black_box(*h));
                                black_box(&r);
                                assert!(r.atomic_entry.is_some());
                            }
                        });
                    });
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
    targets = bench_insert_same_bucket_concurrent
);
criterion_main!(benches);
