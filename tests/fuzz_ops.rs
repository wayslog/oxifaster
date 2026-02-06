mod fuzz_util;

use std::collections::HashMap;
use std::sync::Arc;

use oxifaster::ops::{
    checkpoint_self_check, recover_latest_with_self_check, CheckpointSelfCheckOptions,
};
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::{device::FileSystemDisk, ops};
use rand::Rng;
use tempfile::tempdir;

fn small_store_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1 << 10,
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

fn write_some_data(
    store: &Arc<FasterKv<u64, u64, FileSystemDisk>>,
    seed: u64,
    steps: usize,
) -> HashMap<u64, u64> {
    let mut rng = fuzz_util::rng(seed);
    let mut model = HashMap::new();
    let mut session = expect_ok("start_session", store.start_session());
    for _ in 0..steps {
        let key = rng.gen_range(0u64..10_000);
        let value = fuzz_util::choose_value(&mut rng);
        let s = session.upsert(key, value);
        assert!(matches!(s, Status::Ok | Status::OutOfMemory));
        if s == Status::Ok {
            model.insert(key, value);
        }
        if rng.gen_ratio(1, 8) {
            let _ = session.read(&key);
        }
    }
    model
}

#[test]
fn fuzz_ops_smoke_self_check_and_recover_latest() {
    let p = fuzz_util::params("ops_smoke", 1_000, 10_000);
    let dir = tempdir().expect("tempdir");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).expect("create checkpoints");

    let data_path = dir.path().join("oxifaster_fuzz_ops.dat");
    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let store = Arc::new(FasterKv::<u64, u64, _>::new(small_store_config(), device));

    let _model = write_some_data(&store, p.seed, 2_000);

    // Create a few full checkpoints. `recover_latest` currently targets full checkpoints.
    let _base = store.checkpoint(&checkpoint_dir).expect("checkpoint base");
    let bad = store.checkpoint(&checkpoint_dir).expect("checkpoint bad");

    // Intentionally corrupt one directory by removing its log metadata.
    // Avoid corrupting the base checkpoint to keep at least one valid checkpoint.
    let bad_dir = checkpoint_dir.join(bad.to_string());
    let log_meta = bad_dir.join("log.meta");
    if log_meta.exists() {
        std::fs::remove_file(&log_meta).expect("remove log.meta to corrupt");
    }

    // Dry-run self check should report invalid but not remove.
    let report = checkpoint_self_check(
        &checkpoint_dir,
        CheckpointSelfCheckOptions {
            repair: true,
            dry_run: true,
            remove_invalid: true,
        },
    )
    .expect("self_check dry_run");
    assert!(report.total >= 1);
    assert!(report.invalid >= 1);
    assert!(report.dry_run);
    assert!(!report.repaired);
    assert!(!report.planned_removals.is_empty());

    // Repair mode should remove invalid directories.
    let report = checkpoint_self_check(
        &checkpoint_dir,
        CheckpointSelfCheckOptions {
            repair: true,
            dry_run: false,
            remove_invalid: true,
        },
    )
    .expect("self_check repair");
    assert!(!report.dry_run);
    assert!(report.repaired);
    assert!(!report.removed.is_empty());

    // Now recover_latest_with_self_check should succeed.
    drop(store);
    let device = FileSystemDisk::single_file(&data_path).expect("open device for recovery");
    let (_token, recovered, _report) = recover_latest_with_self_check::<u64, u64, _>(
        &checkpoint_dir,
        small_store_config(),
        device,
        Default::default(),
        None,
        CheckpointSelfCheckOptions {
            repair: false,
            dry_run: false,
            remove_invalid: true,
        },
    )
    .expect("recover_latest_with_self_check");

    // Ensure the recovered store can be used for basic reads/writes.
    let recovered = Arc::new(recovered);
    let mut session = expect_ok("start_session", recovered.start_session());
    let key = 1u64;
    assert!(matches!(
        session.upsert(key, 42),
        Status::Ok | Status::OutOfMemory
    ));
    let _ = session.read(&key);
}

#[test]
#[ignore]
fn fuzz_ops_stress_self_check_with_many_checkpoints() {
    let p = fuzz_util::params("ops_stress", 10_000, 10_000);
    let dir = tempdir().expect("tempdir");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).expect("create checkpoints");

    let data_path = dir.path().join("oxifaster_fuzz_ops_stress.dat");
    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let store = Arc::new(FasterKv::<u64, u64, _>::new(small_store_config(), device));

    let _model = write_some_data(&store, p.seed, p.steps);

    let mut rng = fuzz_util::rng(p.seed ^ 0xD00D_BEEF);
    let mut tokens = Vec::new();

    // Build a mixed set of checkpoints.
    for i in 0..64usize {
        let token = if i == 0 || rng.gen_ratio(1, 5) {
            store
                .checkpoint_full_snapshot(&checkpoint_dir)
                .expect("full snapshot")
        } else {
            store
                .checkpoint_incremental(&checkpoint_dir)
                .expect("incremental")
        };
        tokens.push(token);

        // Randomly corrupt some checkpoints.
        if rng.gen_ratio(1, 4) {
            let cp_dir = checkpoint_dir.join(token.to_string());
            let victim = if rng.gen_bool(0.5) {
                cp_dir.join("index.meta")
            } else {
                cp_dir.join("log.meta")
            };
            let _ = std::fs::remove_file(victim);
        }
    }

    // Self check should never panic and should be idempotent.
    let r1 = ops::checkpoint_self_check(
        &checkpoint_dir,
        CheckpointSelfCheckOptions {
            repair: true,
            dry_run: false,
            remove_invalid: true,
        },
    )
    .expect("self_check repair");
    let r2 = ops::checkpoint_self_check(
        &checkpoint_dir,
        CheckpointSelfCheckOptions {
            repair: true,
            dry_run: false,
            remove_invalid: true,
        },
    )
    .expect("self_check repair again");

    assert!(r2.invalid <= r1.invalid);
    assert!(r2.total <= r1.total);
}
