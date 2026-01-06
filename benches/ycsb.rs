//! YCSB-style benchmark for oxifaster
//!
//! This benchmark tests the performance of FasterKv under different workloads.

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::prelude::*;

use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

/// Create a test store with the given configuration
fn create_store(table_size: u64, memory_size: u64) -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size,
        log_memory_size: memory_size,
        page_size_bits: 22,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

/// Benchmark pure upsert performance
fn bench_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let store = create_store(1 << 20, 1 << 28);
    let mut session = store.start_session();
    let mut key = 0u64;

    group.bench_function("sequential", |b| {
        b.iter(|| {
            let status = session.upsert(black_box(key), black_box(key * 10));
            key += 1;
            status
        })
    });

    group.finish();
}

/// Benchmark pure read performance (after population)
fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let store = create_store(1 << 20, 1 << 28);
    let mut session = store.start_session();

    // Populate store
    let num_keys = 100_000u64;
    for i in 0..num_keys {
        session.upsert(i, i * 10);
    }

    let mut rng = rand::thread_rng();

    group.bench_function("random", |b| {
        b.iter(|| {
            let key = rng.gen_range(0..num_keys);

            session.read(black_box(&key))
        })
    });

    group.finish();
}

/// Benchmark mixed read/write workload (YCSB-A style: 50% read, 50% update)
fn bench_mixed_a(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_50_50");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let store = create_store(1 << 20, 1 << 28);
    let mut session = store.start_session();

    // Populate store
    let num_keys = 100_000u64;
    for i in 0..num_keys {
        session.upsert(i, i * 10);
    }

    let mut rng = rand::thread_rng();
    let mut op_counter = 0u64;

    group.bench_function("random_ops", |b| {
        b.iter(|| {
            let key = rng.gen_range(0..num_keys);
            if op_counter.is_multiple_of(2) {
                let _ = session.read(black_box(&key));
            } else {
                let _ = session.upsert(black_box(key), black_box(key * 100));
            }
            op_counter += 1;
        })
    });

    group.finish();
}

/// Benchmark read-heavy workload (YCSB-B style: 95% read, 5% update)
fn bench_mixed_b(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_95_5");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let store = create_store(1 << 20, 1 << 28);
    let mut session = store.start_session();

    // Populate store
    let num_keys = 100_000u64;
    for i in 0..num_keys {
        session.upsert(i, i * 10);
    }

    let mut rng = rand::thread_rng();

    group.bench_function("random_ops", |b| {
        b.iter(|| {
            let key = rng.gen_range(0..num_keys);
            if rng.gen_ratio(95, 100) {
                let _ = session.read(black_box(&key));
            } else {
                let _ = session.upsert(black_box(key), black_box(key * 100));
            }
        })
    });

    group.finish();
}

/// Benchmark write-heavy workload (YCSB-F style: 50% read, 50% RMW)
fn bench_rmw(c: &mut Criterion) {
    let mut group = c.benchmark_group("rmw");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let store = create_store(1 << 20, 1 << 28);
    let mut session = store.start_session();

    // Populate store
    let num_keys = 100_000u64;
    for i in 0..num_keys {
        session.upsert(i, i * 10);
    }

    let mut rng = rand::thread_rng();

    group.bench_function("random_increment", |b| {
        b.iter(|| {
            let key = rng.gen_range(0..num_keys);
            let _ = session.rmw(black_box(key), |v| {
                *v += 1;
                true
            });
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_upsert,
    bench_read,
    bench_mixed_a,
    bench_mixed_b,
    bench_rmw,
);

criterion_main!(benches);
