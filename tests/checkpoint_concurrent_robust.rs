//! Stress tests for concurrent checkpoint correctness

use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// 8 threads write concurrently while checkpoint runs.
/// After checkpoint, verify all readable keys have correct values.
#[test]
fn test_checkpoint_prefix_consistency() {
    let dir = tempdir().unwrap();
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::new(1 << 18, 1 << 26);
    let device = NullDisk::default();
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, device));

    let running = Arc::new(AtomicBool::new(true));
    let total_written = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8)
        .map(|t| {
            let s = store.clone();
            let r = running.clone();
            let tw = total_written.clone();
            std::thread::spawn(move || {
                let mut session = s.start_session().unwrap();
                let base = t * 100_000u64;
                let mut count = 0u64;
                while r.load(Ordering::Relaxed) {
                    let key = base + count;
                    session.upsert(key, key * 7 + 13);
                    count += 1;
                    if count.is_multiple_of(100) {
                        session.refresh();
                    }
                }
                tw.fetch_add(count, Ordering::Relaxed);
                session.end();
            })
        })
        .collect();

    std::thread::sleep(Duration::from_millis(200));

    let token = store
        .checkpoint(&checkpoint_dir)
        .expect("checkpoint under load should succeed");
    assert!(!token.is_nil());

    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let total = total_written.load(Ordering::Relaxed);
    assert!(total > 0, "writers should have written some data");

    // Verify: every readable key has correct value
    let mut session = store.start_session().unwrap();
    let mut verified = 0u64;
    for t in 0..8u64 {
        let base = t * 100_000;
        for i in 0..20_000u64 {
            let key = base + i;
            match session.read(&key) {
                Ok(Some(val)) => {
                    assert_eq!(val, key * 7 + 13, "key {key} has wrong value");
                    verified += 1;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }
    assert!(verified > 0, "should have verified at least some keys");
    session.end();
}

/// Multiple sequential checkpoints under continuous load.
#[test]
fn test_multiple_checkpoints_under_load() {
    let dir = tempdir().unwrap();
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::new(1 << 18, 1 << 26);
    let device = NullDisk::default();
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, device));

    let running = Arc::new(AtomicBool::new(true));

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let s = store.clone();
            let r = running.clone();
            std::thread::spawn(move || {
                let mut session = s.start_session().unwrap();
                let mut i = t * 100_000u64;
                while r.load(Ordering::Relaxed) {
                    session.upsert(i, i);
                    i += 1;
                    if i.is_multiple_of(50) {
                        session.refresh();
                    }
                }
                session.end();
            })
        })
        .collect();

    for cp_num in 0..3 {
        std::thread::sleep(Duration::from_millis(100));
        let token = store
            .checkpoint(&checkpoint_dir)
            .unwrap_or_else(|e| panic!("checkpoint {cp_num} failed: {e}"));
        assert!(!token.is_nil(), "checkpoint {cp_num} returned nil token");
    }

    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}
