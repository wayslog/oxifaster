//! Tests for WaitPending phase timeout and retry improvements.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig, LogCheckpointBackend};

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

/// FoldOver checkpoint completes and produces a valid token.
#[test]
fn test_foldover_checkpoint_basic() {
    let config = create_test_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let temp_dir = tempfile::tempdir().unwrap();

    // Insert some data
    {
        let mut session = store.start_session().unwrap();
        for i in 0..500u64 {
            let status = session.upsert(i, i * 10);
            assert_eq!(status, Status::Ok);
            if i.is_multiple_of(100) {
                session.refresh();
            }
        }
    }

    // Use FoldOver backend
    store.set_log_checkpoint_backend(LogCheckpointBackend::FoldOver);

    let token = store
        .checkpoint(temp_dir.path())
        .expect("foldover checkpoint should succeed");
    assert!(!token.is_nil());
}

/// Test that checkpoint and compaction can both complete when triggered concurrently.
#[test]
fn test_checkpoint_compaction_not_concurrent() {
    let config = create_test_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));

    {
        let mut session = store.start_session().unwrap();
        for i in 0..2000u64 {
            let status = session.upsert(i, i * 10);
            assert_eq!(status, Status::Ok);
            if i.is_multiple_of(100) {
                session.refresh();
            }
        }
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let s1 = store.clone();
    let s2 = store.clone();
    let cp_dir = temp_dir.path().to_path_buf();

    let h1 = std::thread::spawn(move || {
        let _ = s1.checkpoint(&cp_dir);
    });
    let h2 = std::thread::spawn(move || {
        let begin = s2.log_begin_address();
        s2.compact(begin);
    });

    h1.join().unwrap();
    h2.join().unwrap();
}
