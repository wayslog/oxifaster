mod fuzz_util;

use std::collections::HashMap;
use std::sync::Arc;

use oxifaster::compaction::CompactionConfig;
use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::status::Status;
use oxifaster::store::{AsyncSession, FasterKv, FasterKvConfig};
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

async fn drive_async_ops(
    session: &mut AsyncSession<u64, u64, impl StorageDevice>,
    model: &mut HashMap<u64, u64>,
    steps: usize,
    key_space: u64,
    seed: u64,
    strict_no_pending: bool,
) {
    let mut rng = fuzz_util::rng(seed);

    for _ in 0..steps {
        let op = rng.gen_range(0u8..=99);
        let key = fuzz_util::choose_key(&mut rng, key_space);

        match op {
            0..=34 => {
                // read_async
                match session.read_async(&key).await {
                    Ok(v) => {
                        if strict_no_pending {
                            let expected = model.get(&key).copied();
                            assert_eq!(v, expected);
                        }
                    }
                    Err(Status::Pending) => {
                        if strict_no_pending {
                            panic!("unexpected pending read in strict mode");
                        }
                        // The async wrapper retries internally; if still pending, allow it.
                        // Force a sync pending completion attempt occasionally.
                        if rng.gen_ratio(1, 4) {
                            let _ = session.complete_pending(true);
                        }
                    }
                    Err(s) => panic!("unexpected read_async error: {s:?}"),
                }
            }
            35..=69 => {
                // upsert_async
                let value = fuzz_util::choose_value(&mut rng);
                let s = session.upsert_async(key, value).await;
                match s {
                    Status::Ok => {
                        model.insert(key, value);
                    }
                    Status::OutOfMemory => {}
                    Status::Pending => {
                        if strict_no_pending {
                            panic!("unexpected pending upsert in strict mode");
                        }
                        let _ = session.complete_pending(true);
                    }
                    other => panic!("unexpected upsert_async status: {other:?}"),
                }
            }
            70..=84 => {
                // delete_async
                let s = session.delete_async(&key).await;
                match s {
                    Status::Ok => {
                        model.remove(&key);
                        if strict_no_pending {
                            let got = session.read_async(&key).await.expect("read after delete");
                            assert_eq!(got, None);
                        }
                    }
                    Status::NotFound => {}
                    Status::Pending => {
                        if strict_no_pending {
                            panic!("unexpected pending delete in strict mode");
                        }
                        let _ = session.complete_pending(true);
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected delete_async status: {other:?}"),
                }
            }
            _ => {
                // rmw_async
                let before = model.get(&key).copied();
                let s = session
                    .rmw_async(key, |v| {
                        *v = v.wrapping_add(1);
                        true
                    })
                    .await;
                match (s, before) {
                    (Status::Ok, Some(prev)) => {
                        model.insert(key, prev.wrapping_add(1));
                    }
                    (Status::Ok, None) => match session.read_async(&key).await {
                        Ok(Some(v)) => {
                            model.insert(key, v);
                        }
                        Ok(None) => {}
                        Err(Status::Pending) => {}
                        Err(e) => panic!("read after rmw_async failed: {e:?}"),
                    },
                    (Status::NotFound, None) => {
                        // RMW on a missing key may be treated as a miss (no-op).
                    }
                    (Status::Pending, _) => {
                        if strict_no_pending {
                            panic!("unexpected pending rmw in strict mode");
                        }
                        let _ = session.complete_pending(true);
                    }
                    (Status::OutOfMemory, _) => {}
                    (other, _) => panic!("unexpected rmw_async status: {other:?}"),
                }
            }
        }

        if rng.gen_ratio(1, 128) {
            session.refresh();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fuzz_async_smoke_public_api() {
    let p = fuzz_util::params("async_smoke", 5_000, 5_000);
    let store = Arc::new(
        FasterKv::<u64, u64, _>::with_compaction_config(
            small_store_config(),
            NullDisk::new(),
            CompactionConfig::default(),
        )
        .unwrap(),
    );
    let mut session = store.start_async_session().expect("start_async_session");

    let mut model = HashMap::new();
    drive_async_ops(&mut session, &mut model, p.steps, p.key_space, p.seed, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn fuzz_async_stress_public_api() {
    let p = fuzz_util::params("async_stress", 50_000, 20_000);
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_fuzz_async_stress.dat");
    let device = FileSystemDisk::single_file(&data_path).expect("open device");

    let store = Arc::new(FasterKv::<u64, u64, _>::new(small_store_config(), device).unwrap());
    let mut session = store.start_async_session().expect("start_async_session");

    let mut model = HashMap::new();
    drive_async_ops(
        &mut session,
        &mut model,
        p.steps,
        p.key_space,
        p.seed,
        false,
    )
    .await;
}
