mod fuzz_util;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use oxifaster::codec::Utf8;
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

fn rand_ascii_string(rng: &mut rand::rngs::StdRng, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let b = rng.gen_range(b'a'..=b'z');
        s.push(b as char);
    }
    s
}

#[derive(Debug, Default, Clone)]
struct VarlenReport {
    elapsed: std::time::Duration,
    reads: u64,
    upserts_ok: u64,
    deletes_ok: u64,
    deletes_not_found: u64,
    conditional_ok: u64,
    conditional_aborted: u64,
}

fn run_varlen_fuzz(
    store: &Arc<FasterKv<Utf8, Utf8, NullDisk>>,
    steps: usize,
    seed: u64,
) -> VarlenReport {
    let strict = fuzz_util::strict();
    let mut rng = fuzz_util::rng(seed);
    let started_at = Instant::now();
    let mut report = VarlenReport::default();

    let mut model = HashMap::<String, String>::new();
    let mut session = expect_ok("start_session", store.start_session());

    for i in 0..steps {
        let op = rng.gen_range(0u8..=99);

        // Mix a stable key-space (for hits) with occasional fresh keys/long keys (for varlen layout).
        let key = if rng.gen_ratio(7, 8) {
            let k = rng.gen_range(0u32..10_000);
            format!("k{:#x}", k)
        } else {
            rand_ascii_string(&mut rng, 1, 128)
        };

        match op {
            0..=29 => {
                let got = expect_ok("read", session.read(&Utf8::from(key.as_str())));
                let expected = model.get(&key).cloned().map(Utf8::from);
                assert_eq!(got, expected);
                report.reads += 1;
            }
            30..=74 => {
                let value = if rng.gen_ratio(3, 4) {
                    let v = rng.gen::<u64>();
                    format!("v{v}")
                } else {
                    rand_ascii_string(&mut rng, 0, 512)
                };
                let s = session.upsert(Utf8::from(key.as_str()), Utf8::from(value.as_str()));
                match s {
                    Status::Ok => {
                        model.insert(key, value);
                        report.upserts_ok += 1;
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected upsert status: {other:?}"),
                }
            }
            75..=89 => {
                let s = session.delete(&Utf8::from(key.as_str()));
                match s {
                    Status::Ok => {
                        model.remove(&key);
                        report.deletes_ok += 1;
                    }
                    Status::NotFound => {
                        report.deletes_not_found += 1;
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected delete status: {other:?}"),
                }

                if strict || (i % 256 == 0) {
                    let got =
                        expect_ok("read_after_delete", session.read(&Utf8::from(key.as_str())));
                    assert_eq!(got, None);
                }
            }
            _ => {
                let value = rand_ascii_string(&mut rng, 0, 256);
                let s = session
                    .conditional_insert(Utf8::from(key.as_str()), Utf8::from(value.as_str()));
                match s {
                    Status::Ok => {
                        model.insert(key, value);
                        report.conditional_ok += 1;
                    }
                    Status::Aborted => {
                        report.conditional_aborted += 1;
                        if strict || (i % 512 == 0) {
                            let got = expect_ok(
                                "read_after_conditional_aborted",
                                session.read(&Utf8::from(key.as_str())),
                            );
                            assert!(got.is_some(), "conditional_insert aborted but key missing");
                        }
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected conditional_insert status: {other:?}"),
                }
            }
        }
    }

    report.elapsed = started_at.elapsed();
    report
}

#[test]
fn fuzz_kv_varlen_smoke() {
    let p = fuzz_util::params("kv_varlen_smoke", 2_000, 1);
    let store = Arc::new(FasterKv::<Utf8, Utf8, _>::new(
        small_store_config(),
        NullDisk::new(),
    ));

    let report = run_varlen_fuzz(&store, p.steps, p.seed);
    if fuzz_util::verbose() {
        let snap = store.stats_snapshot();
        eprintln!(
            "fuzz_kv_varlen_smoke: steps={} elapsed_ms={} ops={} reads={} upserts_ok={} deletes_ok={} conditional_ok={} snap={{total_ops={}, bytes_alloc={}, peak_mem={}}}",
            p.steps,
            report.elapsed.as_millis(),
            snap.total_operations,
            report.reads,
            report.upserts_ok,
            report.deletes_ok,
            report.conditional_ok,
            snap.total_operations,
            snap.bytes_allocated,
            snap.peak_memory
        );
    }
}

#[test]
#[ignore]
fn fuzz_kv_varlen_stress() {
    let p = fuzz_util::params("kv_varlen_stress", 50_000, 1);
    let store = Arc::new(FasterKv::<Utf8, Utf8, _>::new(
        small_store_config(),
        NullDisk::new(),
    ));

    let report = run_varlen_fuzz(&store, p.steps, p.seed);
    let snap = store.stats_snapshot();
    eprintln!(
        "fuzz_kv_varlen_stress: steps={} elapsed_ms={} ops={} reads={} upserts_ok={} deletes_ok={} conditional_ok={} conditional_aborted={} snap={{bytes_alloc={}, peak_mem={}}}",
        p.steps,
        report.elapsed.as_millis(),
        snap.total_operations,
        report.reads,
        report.upserts_ok,
        report.deletes_ok,
        report.conditional_ok,
        report.conditional_aborted,
        snap.bytes_allocated,
        snap.peak_memory
    );
}
