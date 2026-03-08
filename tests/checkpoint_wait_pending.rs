//! Tests for WaitPending phase timeout and retry improvements.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn create_test_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1 << 16,
        log_memory_size: 1 << 20,
        page_size_bits: 12,
        mutable_fraction: 0.9,
    }
}

/// Checkpoint completes when all threads drain their pending IOs.
#[test]
fn test_checkpoint_wait_pending_drains() {
    let config = create_test_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let temp_dir = tempfile::tempdir().unwrap();

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let s = store.clone();
            std::thread::spawn(move || {
                let mut session = s.start_session().unwrap();
                for i in 0..1000u64 {
                    let status = session.upsert(i, i * 10);
                    assert_eq!(status, Status::Ok);
                    if i.is_multiple_of(100) {
                        session.refresh();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let token = store
        .checkpoint(temp_dir.path())
        .expect("checkpoint should succeed");
    assert!(!token.is_nil());
}

/// Checkpoint with concurrent writers completes within the timeout.
#[test]
fn test_checkpoint_concurrent_writers_complete() {
    let config = create_test_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let temp_dir = tempfile::tempdir().unwrap();

    let running = Arc::new(AtomicBool::new(true));

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let s = store.clone();
            let r = running.clone();
            std::thread::spawn(move || {
                let mut session = s.start_session().unwrap();
                let mut i = t * 10000u64;
                while r.load(Ordering::Relaxed) {
                    let _status = session.upsert(i, i * 10);
                    i += 1;
                    if i.is_multiple_of(50) {
                        session.refresh();
                    }
                }
            })
        })
        .collect();

    std::thread::sleep(Duration::from_millis(100));

    let start = Instant::now();
    let token = store
        .checkpoint(temp_dir.path())
        .expect("checkpoint should succeed");
    let elapsed = start.elapsed();
    assert!(!token.is_nil());
    assert!(elapsed < Duration::from_secs(30));

    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}
