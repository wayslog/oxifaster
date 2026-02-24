//! Lightweight performance profiling helper.
//!
//! This example is intentionally dependency-free (beyond existing project deps) and is meant to
//! provide a coarse "profile" signal when external profilers are unavailable.
//!
//! Run:
//!   cargo run --release --example perf_profile
//!
//! Profiling with `sample` (macOS):
//!   cargo build --release --example perf_profile
#![allow(clippy::collapsible_if)]
//!   ./target/release/examples/perf_profile --sleep-ms 2000 --seconds 10 &
//!   pid=$!
//!   sample $pid 10 1 -mayDie -file /tmp/oxifaster-perf/sample_perf_profile.txt
//!   wait $pid

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

#[derive(Clone, Copy, Debug)]
enum Workload {
    Mixed,
    Read,
    Upsert,
}

impl Workload {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "mixed" => Some(Self::Mixed),
            "read" => Some(Self::Read),
            "upsert" => Some(Self::Upsert),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Args {
    workload: Workload,
    threads: usize,
    seconds: u64,
    sleep_ms: u64,
    populate_keys: u64,
    phase_stats: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            workload: Workload::Mixed,
            threads: 1,
            seconds: 3,
            sleep_ms: 0,
            populate_keys: 100_000,
            phase_stats: false,
        }
    }
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--workload" => {
                if let Some(v) = it.next() {
                    if let Some(w) = Workload::parse(&v) {
                        args.workload = w;
                    }
                }
            }
            "--threads" => {
                if let Some(v) = it.next() {
                    if let Ok(n) = v.parse::<usize>() {
                        args.threads = n.max(1);
                    }
                }
            }
            "--seconds" => {
                if let Some(v) = it.next() {
                    if let Ok(n) = v.parse::<u64>() {
                        args.seconds = n.max(1);
                    }
                }
            }
            "--sleep-ms" => {
                if let Some(v) = it.next() {
                    if let Ok(n) = v.parse::<u64>() {
                        args.sleep_ms = n;
                    }
                }
            }
            "--populate-keys" => {
                if let Some(v) = it.next() {
                    if let Ok(n) = v.parse::<u64>() {
                        args.populate_keys = n.max(1);
                    }
                }
            }
            "--phase-stats" => {
                args.phase_stats = true;
            }
            _ => {}
        }
    }
    args
}

fn main() {
    let args = parse_args();

    let config = FasterKvConfig {
        table_size: 1 << 20,
        log_memory_size: 1 << 28,
        page_size_bits: 22,
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, NullDisk::new()).unwrap());
    store.enable_stats();
    if args.phase_stats {
        store.enable_phase_stats();
    }

    // Warm up and populate.
    let populate_keys = args.populate_keys;
    {
        let mut session = store.start_session().unwrap();
        for i in 0..populate_keys {
            session.upsert(i, i * 10);
        }
    }

    if args.sleep_ms > 0 {
        std::thread::sleep(Duration::from_millis(args.sleep_ms));
    }

    let before = store.stats_snapshot();
    let before_retries = store.operation_stats().retries.load(Ordering::Relaxed);

    let duration = Duration::from_secs(args.seconds);
    let start = Instant::now();
    {
        std::thread::scope(|s| {
            for thread_id in 0..args.threads {
                let store = Arc::clone(&store);
                s.spawn(move || {
                    let mut session = store.start_session().unwrap();
                    let mut seed = 0x1234_5678_9abc_def0u64 ^ (thread_id as u64);
                    let mut i = 0u64;
                    while start.elapsed() < duration {
                        // xorshift64*
                        seed ^= seed >> 12;
                        seed ^= seed << 25;
                        seed ^= seed >> 27;
                        seed = seed.wrapping_mul(0x2545F4914F6CDD1D);
                        let key = seed % populate_keys;

                        match args.workload {
                            Workload::Mixed => {
                                if i & 1 == 0 {
                                    let _ = session.read(&key);
                                } else {
                                    let _ =
                                        session.upsert(key, key.wrapping_mul(10).wrapping_add(i));
                                }
                            }
                            Workload::Read => {
                                let _ = session.read(&key);
                            }
                            Workload::Upsert => {
                                let _ = session.upsert(key, key.wrapping_mul(10).wrapping_add(i));
                            }
                        }

                        i = i.wrapping_add(1);
                    }
                });
            }
        });
    }
    let elapsed = start.elapsed();

    let after = store.stats_snapshot();
    let after_retries = store.operation_stats().retries.load(Ordering::Relaxed);
    let ops_delta = after
        .total_operations
        .saturating_sub(before.total_operations);
    let retries_delta = after_retries.saturating_sub(before_retries);
    let throughput = ops_delta as f64 / elapsed.as_secs_f64();

    println!("=== perf_profile ===");
    println!(
        "workload: {:?} threads: {} seconds: {}",
        args.workload, args.threads, args.seconds
    );
    println!("elapsed: {:?}", elapsed);
    println!("ops: {}", ops_delta);
    println!("throughput: {:.2} ops/s", throughput);
    println!("retries: {}", retries_delta);
    println!(
        "avg_latency (store-wide): {:?}",
        after.avg_latency.min(Duration::from_secs(10))
    );

    if args.phase_stats {
        let p = store.phase_stats_snapshot();
        println!("--- phase_stats (totals) ---");
        println!("upsert_ops: {}", p.upsert_ops);
        println!("read_ops: {}", p.read_ops);

        // Avoid division by zero for short/empty runs.
        let upsert_ops = p.upsert_ops.max(1) as u128;
        let read_ops = p.read_ops.max(1) as u128;

        println!("--- phase_stats (avg ns/op) ---");
        println!("upsert.hash: {}", (p.upsert_hash_ns as u128) / upsert_ops);
        println!("upsert.index: {}", (p.upsert_index_ns as u128) / upsert_ops);
        println!(
            "upsert.layout: {}",
            (p.upsert_layout_ns as u128) / upsert_ops
        );
        println!("upsert.alloc: {}", (p.upsert_alloc_ns as u128) / upsert_ops);
        println!("upsert.init: {}", (p.upsert_init_ns as u128) / upsert_ops);
        println!(
            "upsert.index_update: {}",
            (p.upsert_index_update_ns as u128) / upsert_ops
        );
        println!("read.hash: {}", (p.read_hash_ns as u128) / read_ops);
        println!("read.index: {}", (p.read_index_ns as u128) / read_ops);
        println!("read.chain: {}", (p.read_chain_ns as u128) / read_ops);
    }
}
