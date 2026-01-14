//! YCSB-style benchmark for oxifaster
//!
//! This benchmark tests the performance of FasterKv under different workloads:
//! - Basic single-threaded operations (upsert, read, mixed, RMW)
//! - Multi-threaded concurrent operations
//! - Large key/value operations using RawBytes (varlen, no envelope)
//! - Real disk I/O operations using FileSystemDisk

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::*;

use oxifaster::codec::RawBytes;
use oxifaster::device::{FileSystemDisk, NullDisk};
use oxifaster::store::{FasterKv, FasterKvConfig};

// =============================================================================
// Helper Functions
// =============================================================================

/// Create a test store with NullDisk (in-memory)
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

/// Create a test store with RawBytes key/value and NullDisk
fn create_raw_bytes_store(
    table_size: u64,
    memory_size: u64,
) -> Arc<FasterKv<RawBytes, RawBytes, NullDisk>> {
    let config = FasterKvConfig {
        table_size,
        log_memory_size: memory_size,
        page_size_bits: 22,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

/// Create a test store with FileSystemDisk (real disk I/O)
fn create_disk_store(
    table_size: u64,
    memory_size: u64,
    path: &std::path::Path,
) -> Arc<FasterKv<u64, u64, FileSystemDisk>> {
    let config = FasterKvConfig {
        table_size,
        log_memory_size: memory_size,
        page_size_bits: 20, // 1MB pages for disk
        mutable_fraction: 0.9,
    };
    let device = FileSystemDisk::single_file(path.join("data.db")).unwrap();
    Arc::new(FasterKv::new(config, device))
}

/// Generate random bytes of specified size
fn generate_random_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut data = vec![0u8; size];
    rng.fill_bytes(&mut data);
    data
}

// =============================================================================
// Basic Single-Threaded Benchmarks
// =============================================================================

/// Benchmark pure upsert performance
fn bench_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let store = create_store(1 << 20, 1 << 28);
    let mut session = store.start_session().unwrap();
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
    let mut session = store.start_session().unwrap();

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
    let mut session = store.start_session().unwrap();

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
    let mut session = store.start_session().unwrap();

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
    let mut session = store.start_session().unwrap();

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

// =============================================================================
// Multi-Threaded Concurrent Benchmarks
// =============================================================================

use rayon::prelude::*;

/// Benchmark concurrent read performance with multiple threads using rayon
fn bench_concurrent_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_read");
    group.measurement_time(Duration::from_secs(10));

    // Test with different thread counts
    for num_threads in [1, 2, 4, 8] {
        // Configure rayon thread pool for this test
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        // Create store inside the pool to ensure sessions are created in pool threads
        let store = pool.install(|| create_store(1 << 20, 1 << 28));

        // Populate store with initial data
        let num_keys = 100_000u64;
        pool.install(|| {
            let mut session = store.start_session().unwrap();
            for i in 0..num_keys {
                session.upsert(i, i * 10);
            }
        });

        let ops_per_thread = 10000u64;
        group.throughput(Throughput::Elements(ops_per_thread * num_threads as u64));

        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let total_ops = AtomicU64::new(0);
                    pool.install(|| {
                        (0..num_threads).into_par_iter().for_each(|_| {
                            let mut session = store.start_session().unwrap();
                            let mut rng = rand::thread_rng();
                            for _ in 0..ops_per_thread {
                                let key = rng.gen_range(0..num_keys);
                                let _ = session.read(black_box(&key));
                            }
                            total_ops.fetch_add(ops_per_thread, Ordering::Relaxed);
                        });
                    });
                    total_ops.load(Ordering::Relaxed)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent mixed read/write workload (80% read, 20% write) using rayon
fn bench_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed_80_20");
    group.measurement_time(Duration::from_secs(10));

    // Test with different thread counts
    for num_threads in [1, 2, 4, 8] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        let store = pool.install(|| create_store(1 << 20, 1 << 28));

        // Populate store with initial data
        let num_keys = 100_000u64;
        pool.install(|| {
            let mut session = store.start_session().unwrap();
            for i in 0..num_keys {
                session.upsert(i, i * 10);
            }
        });

        let ops_per_thread = 10000u64;
        group.throughput(Throughput::Elements(ops_per_thread * num_threads as u64));

        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let total_ops = AtomicU64::new(0);
                    pool.install(|| {
                        (0..num_threads).into_par_iter().for_each(|thread_id| {
                            let mut session = store.start_session().unwrap();
                            let mut rng = rand::thread_rng();
                            for i in 0..ops_per_thread {
                                let key = rng.gen_range(0..num_keys);
                                // 80% read, 20% write
                                if i.is_multiple_of(5) {
                                    let value = key * 10 + thread_id as u64;
                                    let _ = session.upsert(black_box(key), black_box(value));
                                } else {
                                    let _ = session.read(black_box(&key));
                                }
                            }
                            total_ops.fetch_add(ops_per_thread, Ordering::Relaxed);
                        });
                    });
                    total_ops.load(Ordering::Relaxed)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent write-heavy workload (100% upsert) using rayon
fn bench_concurrent_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_write");
    group.measurement_time(Duration::from_secs(10));

    // Test with different thread counts
    for num_threads in [1, 2, 4, 8] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        let store = pool.install(|| create_store(1 << 20, 1 << 28));

        let ops_per_thread = 10000u64;
        group.throughput(Throughput::Elements(ops_per_thread * num_threads as u64));

        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let total_ops = AtomicU64::new(0);
                    pool.install(|| {
                        (0..num_threads).into_par_iter().for_each(|thread_id| {
                            let mut session = store.start_session().unwrap();
                            let base_key = thread_id as u64 * ops_per_thread;
                            for i in 0..ops_per_thread {
                                let key = base_key + i;
                                let value = key * 10;
                                let _ = session.upsert(black_box(key), black_box(value));
                            }
                            total_ops.fetch_add(ops_per_thread, Ordering::Relaxed);
                        });
                    });
                    total_ops.load(Ordering::Relaxed)
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Large Key/Value Benchmarks (using RawBytes)
// =============================================================================

/// Benchmark read with 1KB values
fn bench_large_value_read_1kb(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_value_read_1kb");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(1024));

    // Small store: 64MB
    let store = create_raw_bytes_store(1 << 14, 1 << 26);
    let test_data = generate_random_bytes(1024);
    let num_keys = 500u64;

    // Populate
    {
        let mut session = store.start_session().unwrap();
        for i in 0..num_keys {
            let key = RawBytes::from(i.to_le_bytes().to_vec());
            let value = RawBytes::from(test_data.clone());
            session.upsert(key, value);
        }
    }

    group.bench_function("random", |b| {
        let mut session = store.start_session().unwrap();
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let key_num = rng.gen_range(0..num_keys);
            let key = RawBytes::from(key_num.to_le_bytes().to_vec());
            session.read(black_box(&key))
        })
    });

    group.finish();
}

/// Benchmark read with 4KB values
fn bench_large_value_read_4kb(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_value_read_4kb");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(4096));

    let store = create_raw_bytes_store(1 << 14, 1 << 26);
    let test_data = generate_random_bytes(4096);
    let num_keys = 500u64;

    {
        let mut session = store.start_session().unwrap();
        for i in 0..num_keys {
            let key = RawBytes::from(i.to_le_bytes().to_vec());
            let value = RawBytes::from(test_data.clone());
            session.upsert(key, value);
        }
    }

    group.bench_function("random", |b| {
        let mut session = store.start_session().unwrap();
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let key_num = rng.gen_range(0..num_keys);
            let key = RawBytes::from(key_num.to_le_bytes().to_vec());
            session.read(black_box(&key))
        })
    });

    group.finish();
}

/// Benchmark write with 1KB values using iter_batched to control memory
fn bench_large_value_write_1kb(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_value_write_1kb");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(1024));

    let store = create_raw_bytes_store(1 << 14, 1 << 26);
    let test_data: Arc<Vec<u8>> = Arc::new(generate_random_bytes(1024));
    let max_keys = 500u64;
    let key_counter = Arc::new(AtomicU64::new(0));

    group.bench_function("sequential", |b| {
        let mut session = store.start_session().unwrap();
        let test_data = test_data.clone();
        let key_counter = key_counter.clone();

        b.iter_batched(
            || {
                let key_num = key_counter.fetch_add(1, Ordering::Relaxed) % max_keys;
                let key = RawBytes::from(key_num.to_le_bytes().to_vec());
                let value = RawBytes::from((*test_data).clone());
                (key, value)
            },
            |(key, value)| session.upsert(black_box(key), black_box(value)),
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark write with 4KB values using iter_batched to control memory
fn bench_large_value_write_4kb(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_value_write_4kb");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(4096));

    let store = create_raw_bytes_store(1 << 14, 1 << 26);
    let test_data: Arc<Vec<u8>> = Arc::new(generate_random_bytes(4096));
    let max_keys = 500u64;
    let key_counter = Arc::new(AtomicU64::new(0));

    group.bench_function("sequential", |b| {
        let mut session = store.start_session().unwrap();
        let test_data = test_data.clone();
        let key_counter = key_counter.clone();

        b.iter_batched(
            || {
                let key_num = key_counter.fetch_add(1, Ordering::Relaxed) % max_keys;
                let key = RawBytes::from(key_num.to_le_bytes().to_vec());
                let value = RawBytes::from((*test_data).clone());
                (key, value)
            },
            |(key, value)| session.upsert(black_box(key), black_box(value)),
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// =============================================================================
// Real Disk I/O Benchmarks
// =============================================================================

/// Benchmark read performance with real disk I/O
fn bench_disk_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_read");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50); // Fewer samples due to slower disk I/O
    group.throughput(Throughput::Elements(1));

    // Create temporary directory for disk storage
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    // Use smaller memory to force more disk activity
    // 16MB memory means data will spill to disk
    let store = create_disk_store(1 << 18, 1 << 24, temp_dir.path());

    // Populate store with data
    let num_keys = 50_000u64;
    {
        let mut session = store.start_session().unwrap();
        for i in 0..num_keys {
            session.upsert(i, i * 10);
        }
    }

    let mut rng = rand::thread_rng();

    group.bench_function("random", |b| {
        let mut session = store.start_session().unwrap();

        b.iter(|| {
            let key = rng.gen_range(0..num_keys);
            session.read(black_box(&key))
        })
    });

    group.finish();
}

/// Benchmark write performance with real disk I/O
fn bench_disk_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_write");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50); // Fewer samples due to slower disk I/O
    group.throughput(Throughput::Elements(1));

    // Create temporary directory for disk storage
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    // Use smaller memory to force more disk activity
    let store = create_disk_store(1 << 18, 1 << 24, temp_dir.path());

    let mut key_counter = 0u64;

    group.bench_function("sequential", |b| {
        let mut session = store.start_session().unwrap();

        b.iter(|| {
            let status = session.upsert(black_box(key_counter), black_box(key_counter * 10));
            key_counter += 1;
            status
        })
    });

    group.finish();
}

/// Benchmark mixed workload with real disk I/O
fn bench_disk_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_mixed_80_20");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);
    group.throughput(Throughput::Elements(1));

    // Create temporary directory for disk storage
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    let store = create_disk_store(1 << 18, 1 << 24, temp_dir.path());

    // Populate store
    let num_keys = 50_000u64;
    {
        let mut session = store.start_session().unwrap();
        for i in 0..num_keys {
            session.upsert(i, i * 10);
        }
    }

    let mut rng = rand::thread_rng();
    let mut op_counter = 0u64;

    group.bench_function("random_ops", |b| {
        let mut session = store.start_session().unwrap();

        b.iter(|| {
            let key = rng.gen_range(0..num_keys);
            // 80% read, 20% write
            if op_counter.is_multiple_of(5) {
                let _ = session.upsert(black_box(key), black_box(key * 100));
            } else {
                let _ = session.read(black_box(&key));
            }
            op_counter += 1;
        })
    });

    group.finish();
}

// =============================================================================
// Benchmark Groups
// =============================================================================

// Basic single-threaded benchmarks (fast, default)
criterion_group!(
    basic_benches,
    bench_upsert,
    bench_read,
    bench_mixed_a,
    bench_mixed_b,
    bench_rmw,
);

// Concurrent multi-threaded benchmarks
criterion_group!(
    name = concurrent_benches;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(10));
    targets = bench_concurrent_read, bench_concurrent_mixed, bench_concurrent_write
);

// Large value benchmarks using RawBytes (separate functions to control memory)
criterion_group!(
    name = large_value_benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(3));
    targets = bench_large_value_read_1kb,
              bench_large_value_read_4kb,
              bench_large_value_write_1kb,
              bench_large_value_write_4kb
);

// Real disk I/O benchmarks
criterion_group!(
    name = disk_benches;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(10));
    targets = bench_disk_read, bench_disk_write, bench_disk_mixed
);

criterion_main!(
    basic_benches,
    concurrent_benches,
    large_value_benches,
    disk_benches
);
