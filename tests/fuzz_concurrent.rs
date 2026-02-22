mod fuzz_util;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use rand::Rng;

fn small_store_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1 << 12,
        log_memory_size: 1 << 24,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    }
}

fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
    match r {
        Ok(v) => v,
        Err(s) => panic!("{ctx} failed: {s:?}"),
    }
}

fn worker_run(
    store: Arc<FasterKv<u64, u64, NullDisk>>,
    seed: u64,
    steps: usize,
    key_base: u64,
    key_space: u64,
) -> (u64, std::time::Duration) {
    let mut rng = fuzz_util::rng(seed);
    let strict = fuzz_util::strict();
    let started_at = Instant::now();

    let mut model = HashMap::<u64, u64>::new();
    let mut session = expect_ok("start_session", store.start_session());

    for _ in 0..steps {
        let op = rng.gen_range(0u8..=99);
        let key = key_base + fuzz_util::choose_key(&mut rng, key_space);

        match op {
            0..=29 => {
                let got = expect_ok("read", session.read(&key));
                if strict {
                    assert_eq!(got, model.get(&key).copied());
                }
            }
            30..=69 => {
                let value = fuzz_util::choose_value(&mut rng);
                let s = session.upsert(key, value);
                if s == Status::Ok {
                    model.insert(key, value);
                } else {
                    assert!(matches!(s, Status::OutOfMemory));
                }
            }
            70..=84 => {
                let s = session.delete(&key);
                match s {
                    Status::Ok => {
                        model.remove(&key);
                    }
                    Status::NotFound => {}
                    Status::OutOfMemory => {}
                    other => panic!("unexpected delete status: {other:?}"),
                }
            }
            _ => {
                let value = fuzz_util::choose_value(&mut rng);
                let s = session.conditional_insert(key, value);
                match s {
                    Status::Ok => {
                        model.insert(key, value);
                    }
                    Status::Aborted => {}
                    Status::OutOfMemory => {}
                    other => panic!("unexpected conditional_insert status: {other:?}"),
                }
            }
        }
    }

    // Verify the model via reads in the same thread/session.
    if strict {
        for (k, v) in model.iter().take(1_000) {
            let got = expect_ok("read_verify", session.read(k));
            assert_eq!(got, Some(*v));
        }
    }

    (steps as u64, started_at.elapsed())
}

#[test]
fn fuzz_concurrent_smoke_sessions() {
    let p = fuzz_util::params("concurrent_smoke", 5_000, 2_000);
    let threads = 4usize;
    let key_space_per_thread = p.key_space;

    let store =
        Arc::new(FasterKv::<u64, u64, _>::new(small_store_config(), NullDisk::new()).unwrap());

    let mut handles = Vec::new();
    for tid in 0..threads {
        let store = store.clone();
        let seed = p.seed ^ ((tid as u64) << 32);
        let steps = p.steps;
        let key_base = (tid as u64) * key_space_per_thread;
        handles.push(std::thread::spawn(move || {
            worker_run(store, seed, steps, key_base, key_space_per_thread)
        }));
    }

    let mut total_steps = 0u64;
    let mut max_elapsed = std::time::Duration::from_millis(0);
    for h in handles {
        let (steps, elapsed) = h.join().expect("thread join");
        total_steps += steps;
        max_elapsed = max_elapsed.max(elapsed);
    }

    if fuzz_util::verbose() {
        let snap = store.stats_snapshot();
        eprintln!(
            "fuzz_concurrent_smoke: threads={} total_steps={} max_elapsed_ms={} snap={{total_ops={}, peak_mem={}}}",
            threads,
            total_steps,
            max_elapsed.as_millis(),
            snap.total_operations,
            snap.peak_memory
        );
    }
}

#[test]
#[ignore]
fn fuzz_concurrent_stress_sessions() {
    let p = fuzz_util::params("concurrent_stress", 50_000, 10_000);
    let threads = 8usize;
    let key_space_per_thread = p.key_space;

    let store =
        Arc::new(FasterKv::<u64, u64, _>::new(small_store_config(), NullDisk::new()).unwrap());

    let mut handles = Vec::new();
    for tid in 0..threads {
        let store = store.clone();
        let seed = p.seed ^ ((tid as u64) << 32);
        let steps = p.steps;
        let key_base = (tid as u64) * key_space_per_thread;
        handles.push(std::thread::spawn(move || {
            worker_run(store, seed, steps, key_base, key_space_per_thread)
        }));
    }

    let mut total_steps = 0u64;
    let mut max_elapsed = std::time::Duration::from_millis(0);
    for h in handles {
        let (steps, elapsed) = h.join().expect("thread join");
        total_steps += steps;
        max_elapsed = max_elapsed.max(elapsed);
    }

    let snap = store.stats_snapshot();
    eprintln!(
        "fuzz_concurrent_stress: threads={} total_steps={} max_elapsed_ms={} snap={{total_ops={}, bytes_alloc={}, peak_mem={}}}",
        threads,
        total_steps,
        max_elapsed.as_millis(),
        snap.total_operations,
        snap.bytes_allocated,
        snap.peak_memory
    );
}
