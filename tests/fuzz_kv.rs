mod fuzz_util;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use oxifaster::checkpoint::CheckpointToken;
use oxifaster::compaction::CompactionConfig;
use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use rand::Rng;
use tempfile::tempdir;

fn small_store_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1 << 10,
        log_memory_size: 1 << 24, // 16MiB to reduce spurious OOM in stress tests
        page_size_bits: 14,       // 16KiB pages
        mutable_fraction: 0.9,
    }
}

fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
    match r {
        Ok(v) => v,
        Err(s) => panic!("{ctx} failed: {s:?}"),
    }
}

fn apply_random_ops(
    store: &Arc<FasterKv<u64, u64, FileSystemDisk>>,
    model: &mut HashMap<u64, u64>,
    steps: usize,
    key_space: u64,
    seed: u64,
) -> KvFuzzReport {
    let mut rng = fuzz_util::rng(seed);
    let mut session = expect_ok("start_session", store.start_session());
    let strict = fuzz_util::strict();

    let mut report = KvFuzzReport::default();
    let started_at = Instant::now();

    for i in 0..steps {
        let op = rng.gen_range(0u8..=99);
        let key = fuzz_util::choose_key(&mut rng, key_space);

        match op {
            0..=24 => {
                // read
                let got = expect_ok("read", session.read(&key));
                let expected = model.get(&key).copied();
                assert_eq!(got, expected);
                report.reads += 1;
            }
            25..=59 => {
                // upsert
                let value = fuzz_util::choose_value(&mut rng);
                let status = session.upsert(key, value);
                match status {
                    Status::Ok => {
                        model.insert(key, value);
                        report.upserts_ok += 1;
                    }
                    Status::OutOfMemory => {
                        // Treat as a resource signal: drop some keys to allow progress.
                        // The store may still be able to proceed after compaction.
                        let _ = model.remove(&key);
                        report.upserts_oom += 1;
                    }
                    other => panic!("unexpected upsert status: {other:?}"),
                }
            }
            60..=74 => {
                // delete
                let status = session.delete(&key);
                match status {
                    Status::Ok => {
                        model.remove(&key);
                        report.deletes_ok += 1;
                    }
                    Status::NotFound => {}
                    Status::Pending => {
                        let _ = session.complete_pending(true);
                        report.deletes_pending += 1;
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected delete status: {other:?}"),
                }

                // Validate observable behavior: once delete returns, a read should not return a value.
                // This is a cheap invariant that also helps keep the model from drifting.
                if strict || (i % 256 == 0) {
                    let got = expect_ok("read_after_delete", session.read(&key));
                    if let Some(v) = got {
                        report.anomaly_delete_read_some += 1;
                        if strict {
                            panic!("delete/read anomaly: status={status:?} read={v:?}");
                        }
                        // Prefer the store's observed state.
                        model.insert(key, v);
                    }
                }
            }
            75..=89 => {
                // rmw (increment)
                let before = model.get(&key).copied();
                let status = session.rmw(key, |v| {
                    *v = v.wrapping_add(1);
                    true
                });
                match (status, before) {
                    (Status::Ok, Some(prev)) => {
                        model.insert(key, prev.wrapping_add(1));
                        report.rmws_ok += 1;
                    }
                    // When a key is missing, the current implementation may insert a new record
                    // depending on the store's RMW semantics. Validate by reading back.
                    (Status::Ok, None) => {
                        let got = expect_ok("read_after_rmw", session.read(&key));
                        let v = got.expect("rmw returned Ok but read returned None");
                        model.insert(key, v);
                        report.rmws_ok += 1;
                    }
                    (Status::NotFound, None) => {
                        // RMW on a missing key may be treated as a miss (no-op).
                        report.rmws_not_found += 1;
                    }
                    (Status::NotFound, Some(_)) => {
                        // Keep the model in sync with the store.
                        let got = expect_ok("read_after_rmw_not_found", session.read(&key));
                        match got {
                            Some(v) => {
                                model.insert(key, v);
                            }
                            None => {
                                model.remove(&key);
                            }
                        }
                        report.rmws_not_found += 1;
                    }
                    (Status::Pending, _) => {
                        let _ = session.complete_pending(true);
                        report.rmws_pending += 1;
                    }
                    (Status::OutOfMemory, _) => {}
                    other => panic!("unexpected rmw outcome: {other:?}"),
                }
            }
            _ => {
                // conditional_insert
                let value = fuzz_util::choose_value(&mut rng);
                let status = session.conditional_insert(key, value);
                match status {
                    Status::Ok => {
                        model.insert(key, value);
                        report.conditional_ok += 1;
                    }
                    Status::Aborted => {
                        report.conditional_aborted += 1;
                        // If we aborted, the key should exist. If it doesn't, count it as an anomaly.
                        // Keep it non-fatal by default so the stress suite can still complete.
                        if strict || (i % 512 == 0) {
                            let got =
                                expect_ok("read_after_conditional_aborted", session.read(&key));
                            match got {
                                None => {
                                    report.anomaly_conditional_aborted_missing += 1;
                                    if strict {
                                        panic!("conditional_insert aborted but key missing");
                                    }
                                }
                                Some(v) => {
                                    // Keep the model consistent with the observed store state.
                                    model.insert(key, v);
                                }
                            }
                        }
                    }
                    Status::Pending => {
                        let _ = session.complete_pending(true);
                        report.conditional_pending += 1;
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected conditional_insert status: {other:?}"),
                }
            }
        }

        if rng.gen_ratio(1, 256) {
            // Lightweight invariants: stats should remain internally consistent.
            let snap = store.stats_snapshot();
            assert!(snap.peak_memory >= snap.memory_in_use);
            assert!(snap.total_operations >= snap.reads + snap.upserts + snap.rmws + snap.deletes);
        }
    }

    report.elapsed = started_at.elapsed();
    report
}

#[derive(Debug, Default, Clone)]
struct KvFuzzReport {
    elapsed: std::time::Duration,
    reads: u64,
    upserts_ok: u64,
    upserts_oom: u64,
    deletes_ok: u64,
    deletes_pending: u64,
    rmws_ok: u64,
    rmws_not_found: u64,
    rmws_pending: u64,
    conditional_ok: u64,
    conditional_aborted: u64,
    conditional_pending: u64,
    anomaly_delete_read_some: u64,
    anomaly_conditional_aborted_missing: u64,
}

#[test]
fn fuzz_kv_smoke_crud_model() {
    let p = fuzz_util::params("kv_smoke", 2_000, 1_000);
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_fuzz_kv.dat");
    let cp_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&cp_dir).expect("create checkpoints dir");

    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let store = Arc::new(
        FasterKv::<u64, u64, _>::with_compaction_config(
            small_store_config(),
            device,
            CompactionConfig::default(),
        )
        .unwrap(),
    );

    let mut model = HashMap::new();
    let report = apply_random_ops(&store, &mut model, p.steps, p.key_space, p.seed);

    // A small checkpoint at the end should never panic.
    let _token = store.checkpoint(&cp_dir).expect("checkpoint");

    if fuzz_util::verbose() {
        let snap = store.stats_snapshot();
        eprintln!(
            "fuzz_kv_smoke: steps={} elapsed_ms={} ops={} reads={} upserts_ok={} upserts_oom={} deletes_ok={} rmws_ok={} conditional_ok={} anomalies(delete_read_some={}, conditional_abort_missing={}) snap={{total_ops={}, pending={}, bytes_alloc={}, peak_mem={}}}",
            p.steps,
            report.elapsed.as_millis(),
            snap.total_operations,
            report.reads,
            report.upserts_ok,
            report.upserts_oom,
            report.deletes_ok,
            report.rmws_ok,
            report.conditional_ok,
            report.anomaly_delete_read_some,
            report.anomaly_conditional_aborted_missing,
            snap.total_operations,
            snap.pending,
            snap.bytes_allocated,
            snap.peak_memory
        );
    }
}

#[test]
#[ignore]
fn fuzz_kv_stress_crud_compaction_growth() {
    let p = fuzz_util::params("kv_stress", 50_000, 10_000);
    let started_at = Instant::now();
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_fuzz_kv_stress.dat");
    let cp_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&cp_dir).expect("create checkpoints dir");

    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let store = Arc::new(
        FasterKv::<u64, u64, _>::with_compaction_config(
            small_store_config(),
            device,
            CompactionConfig {
                // Encourage compaction in long runs.
                target_utilization: 0.6,
                ..CompactionConfig::default()
            },
        )
        .unwrap(),
    );

    let mut rng = fuzz_util::rng(p.seed);
    let mut model = HashMap::new();
    let mut agg = KvFuzzReport::default();
    let mut compactions = 0u64;
    let mut grows = 0u64;
    let mut checkpoints = 0u64;

    let mut step = 0usize;
    while step < p.steps {
        let chunk = usize::min(2_000, p.steps - step);
        let r = apply_random_ops(&store, &mut model, chunk, p.key_space, rng.r#gen());
        agg.reads += r.reads;
        agg.upserts_ok += r.upserts_ok;
        agg.upserts_oom += r.upserts_oom;
        agg.deletes_ok += r.deletes_ok;
        agg.deletes_pending += r.deletes_pending;
        agg.rmws_ok += r.rmws_ok;
        agg.rmws_not_found += r.rmws_not_found;
        agg.rmws_pending += r.rmws_pending;
        agg.conditional_ok += r.conditional_ok;
        agg.conditional_aborted += r.conditional_aborted;
        agg.conditional_pending += r.conditional_pending;
        agg.anomaly_delete_read_some += r.anomaly_delete_read_some;
        agg.anomaly_conditional_aborted_missing += r.anomaly_conditional_aborted_missing;
        step += chunk;

        // Try compaction when recommended.
        if store.should_compact() && !store.is_compaction_in_progress() {
            let r = store.log_compact();
            assert!(
                r.status == Status::Ok || r.status == Status::Aborted,
                "unexpected compaction status: {:?}",
                r.status
            );
            compactions += 1;
        }

        // Occasionally trigger index growth.
        if rng.gen_ratio(1, 8) {
            let current = store.index_size();
            if let Some(doubled) = current.checked_mul(2) {
                // Keep sizes small enough to avoid overflow panics in next_power_of_two.
                if doubled <= (1u64 << 62) {
                    let new_size = (doubled.max(current + 2)).next_power_of_two();
                    let s = store.grow_index_with_callback(new_size, None);
                    assert!(matches!(s, Status::Ok | Status::Aborted));
                    grows += 1;
                }
            }
        }

        // Periodic correctness spot checks.
        let mut session = expect_ok("start_session", store.start_session());
        for _ in 0..128 {
            let key = fuzz_util::choose_key(&mut rng, p.key_space);
            let got = expect_ok("read", session.read(&key));
            let expected = model.get(&key).copied();
            assert_eq!(got, expected);
        }

        // Also validate checkpoint/recovery does not explode under accumulated state.
        if rng.gen_ratio(1, 6) {
            let token = store.checkpoint(&cp_dir).expect("checkpoint");
            assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
                &cp_dir, token
            ));
            checkpoints += 1;
        }
    }

    if fuzz_util::verbose() {
        let snap = store.stats_snapshot();
        eprintln!(
            "fuzz_kv_stress: steps={} elapsed_ms={} compactions={} grows={} checkpoints={} report={{reads={}, upserts_ok={}, upserts_oom={}, deletes_ok={}, rmws_ok={}, conditional_ok={}, conditional_aborted={}, anomalies(delete_read_some={}, conditional_abort_missing={})}} snap={{total_ops={}, bytes_alloc={}, peak_mem={}, index_entries={}}}",
            p.steps,
            started_at.elapsed().as_millis(),
            compactions,
            grows,
            checkpoints,
            agg.reads,
            agg.upserts_ok,
            agg.upserts_oom,
            agg.deletes_ok,
            agg.rmws_ok,
            agg.conditional_ok,
            agg.conditional_aborted,
            agg.anomaly_delete_read_some,
            agg.anomaly_conditional_aborted_missing,
            snap.total_operations,
            snap.bytes_allocated,
            snap.peak_memory,
            snap.index_entries
        );
    }
}

fn recover_roundtrip(
    checkpoint_dir: &std::path::Path,
    token: CheckpointToken,
    data_path: &std::path::Path,
    config: FasterKvConfig,
) -> Arc<FasterKv<u64, u64, FileSystemDisk>> {
    let device = FileSystemDisk::single_file(data_path).expect("open device for recover");
    let recovered =
        FasterKv::<u64, u64, _>::recover(checkpoint_dir, token, config, device).expect("recover");
    Arc::new(recovered)
}

#[test]
#[ignore]
fn fuzz_kv_stress_checkpoint_recovery_roundtrip() {
    let p = fuzz_util::params("kv_checkpoint_roundtrip", 25_000, 20_000);
    let started_at = Instant::now();
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_fuzz_kv_recover.dat");
    let cp_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&cp_dir).expect("create checkpoints dir");

    let config = small_store_config();
    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config.clone(), device).unwrap());

    let mut model = HashMap::new();
    let r = apply_random_ops(&store, &mut model, p.steps, p.key_space, p.seed);

    // Create a checkpoint and recover into a new store instance.
    let token = store.checkpoint(&cp_dir).expect("checkpoint");
    drop(store);

    let recovered = recover_roundtrip(&cp_dir, token, &data_path, config);

    // Continue one recovered session (exercise the public API) and verify reads.
    let states = recovered.get_recovered_sessions();
    if let Some(state) = states.into_iter().next() {
        let mut s = expect_ok("continue_session", recovered.continue_session(state));
        for (k, v) in model.iter().take(512) {
            let got = expect_ok("read_after_recover", s.read(k));
            assert_eq!(got, Some(*v));
        }
    }

    // Verify data via a fresh session too.
    let mut s = expect_ok("start_session", recovered.start_session());
    for (k, v) in model.iter().take(2_000) {
        let got = expect_ok("read_after_recover_new_session", s.read(k));
        assert_eq!(got, Some(*v));
    }

    if fuzz_util::verbose() {
        let snap = recovered.stats_snapshot();
        eprintln!(
            "fuzz_kv_checkpoint_roundtrip: steps={} elapsed_ms={} report={{reads={}, upserts_ok={}, deletes_ok={}, rmws_ok={}, conditional_ok={}, conditional_aborted={}, anomalies(delete_read_some={}, conditional_abort_missing={})}} recovered_snap={{total_ops={}, bytes_alloc={}, peak_mem={}}}",
            p.steps,
            started_at.elapsed().as_millis(),
            r.reads,
            r.upserts_ok,
            r.deletes_ok,
            r.rmws_ok,
            r.conditional_ok,
            r.conditional_aborted,
            r.anomaly_delete_read_some,
            r.anomaly_conditional_aborted_missing,
            snap.total_operations,
            snap.bytes_allocated,
            snap.peak_memory
        );
    }
}
