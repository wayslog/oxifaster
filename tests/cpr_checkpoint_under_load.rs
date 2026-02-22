//! CPR integration tests.
//!
//! These tests focus on cooperative checkpoint progress (Workstream B):
//! - Checkpoints should not block new writes.
//! - WAIT_PENDING should wait only for the previous execution context to drain.

use std::sync::Arc;
use std::time::{Duration, Instant};

use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{
    CheckpointDurability, FasterKv, FasterKvConfig, LogCheckpointBackend, Phase,
};
use tempfile::tempdir;

fn create_test_store(path: &std::path::Path) -> Arc<FasterKv<u64, u64, FileSystemDisk>> {
    let device = FileSystemDisk::single_file(path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MiB
        page_size_bits: 14,       // 16 KiB
        mutable_fraction: 0.9,
    };
    Arc::new(FasterKv::new(config, device).unwrap())
}

#[test]
fn test_checkpoint_wait_pending_blocks_on_prev_context_only() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data.db");
    let checkpoint_dir = tempdir().unwrap();

    let store = create_test_store(&data_path);
    let mut session = store.start_session().unwrap();

    // Seed a record, then move it to the on-disk region so reads return Pending.
    assert_eq!(session.upsert(1, 100), Status::Ok);
    let tail = store.log_stats().tail_address;
    assert_eq!(store.flush_and_shift_head(tail), Status::Ok);
    assert_eq!(session.read(&1), Err(Status::Pending));

    let (tx, rx) = std::sync::mpsc::channel();
    let store_for_checkpoint = store.clone();
    let checkpoint_base = checkpoint_dir.path().to_path_buf();

    std::thread::spawn(move || {
        let result = store_for_checkpoint.checkpoint(&checkpoint_base);
        tx.send(result).unwrap();
    });

    // Wait until the checkpoint reaches WAIT_PENDING and stalls (because we still have prev pending
    // I/O + retry_requests).
    let start = Instant::now();
    loop {
        // Keep the session cooperating with CPR.
        session.refresh();

        if store.system_state().phase == Phase::WaitPending {
            break;
        }

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "checkpoint did not reach WAIT_PENDING in time (current={:?})",
            store.system_state().phase
        );

        std::thread::yield_now();
    }

    // Checkpoint should not block new writes while waiting.
    assert_eq!(session.upsert(3, 300), Status::Ok);

    // Drain the previous execution context: complete the I/O and let `complete_pending()`
    // drive the retry request to completion.
    assert!(session.complete_pending(true));
    assert_eq!(session.read(&1), Ok(Some(100)));
    session.refresh();

    // The checkpoint should complete shortly after the prev context drains.
    let start = Instant::now();
    let result = loop {
        session.refresh();
        match rx.try_recv() {
            Ok(result) => break result.unwrap(),
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                panic!("checkpoint thread exited unexpectedly");
            }
        }

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "checkpoint did not complete in time (current={:?})",
            store.system_state().phase
        );

        std::thread::yield_now();
    };

    assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
        checkpoint_dir.path(),
        result
    ));
    assert_eq!(store.system_state().phase, Phase::Rest);
}

#[test]
fn test_foldover_checkpoint_under_load_recovery_prefix() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data_foldover.db");
    let checkpoint_dir = tempdir().unwrap();

    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MiB
        page_size_bits: 14,       // 16 KiB
        mutable_fraction: 0.9,
    };

    let token = {
        let device = FileSystemDisk::single_file(&data_path).unwrap();
        let store = Arc::new(FasterKv::<u64, u64, _>::new(config.clone(), device).unwrap());
        store.set_log_checkpoint_backend(LogCheckpointBackend::FoldOver);
        store.set_checkpoint_durability(CheckpointDurability::FsyncOnCheckpoint);

        let mut session = store.start_session().unwrap();

        assert_eq!(session.upsert(1, 100), Status::Ok);
        let tail = store.log_stats().tail_address;
        assert_eq!(store.flush_and_shift_head(tail), Status::Ok);
        assert_eq!(session.read(&1), Err(Status::Pending));

        let (tx, rx) = std::sync::mpsc::channel();
        let store_for_checkpoint = store.clone();
        let checkpoint_base = checkpoint_dir.path().to_path_buf();

        std::thread::spawn(move || {
            let result = store_for_checkpoint.checkpoint(&checkpoint_base);
            tx.send(result).unwrap();
        });

        let start = Instant::now();
        loop {
            session.refresh();
            if store.system_state().phase == Phase::WaitPending {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "checkpoint did not reach WAIT_PENDING in time (current={:?})",
                store.system_state().phase
            );
            std::thread::yield_now();
        }

        // Writes continue while checkpoint waits.
        assert_eq!(session.upsert(3, 300), Status::Ok);

        // Drain the prev context so the fold-over checkpoint can finish.
        assert!(session.complete_pending(true));
        session.refresh();

        let start = Instant::now();
        loop {
            session.refresh();
            match rx.try_recv() {
                Ok(result) => break result.unwrap(),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("checkpoint thread exited unexpectedly");
                }
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "checkpoint did not complete in time (current={:?})",
                store.system_state().phase
            );
            std::thread::yield_now();
        }
    };

    assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
        checkpoint_dir.path(),
        token
    ));

    let recovered_device = FileSystemDisk::single_file(&data_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(checkpoint_dir.path(), token, config, recovered_device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // Key 1 existed before the checkpoint began, so it must be present after recovery.
    for _ in 0..16 {
        match session.read(&1) {
            Ok(Some(v)) => {
                assert_eq!(v, 100);
                break;
            }
            Ok(None) => panic!("expected key 1 to be present"),
            Err(Status::Pending) => assert!(session.complete_pending(true)),
            Err(s) => panic!("unexpected status: {s:?}"),
        }
    }

    // Key 3 was written after WAIT_PENDING, so it should not be part of the recovered prefix.
    for _ in 0..16 {
        match session.read(&3) {
            Ok(None) => break,
            Ok(Some(_)) => panic!("expected key 3 to be absent after prefix recovery"),
            Err(Status::Pending) => assert!(session.complete_pending(true)),
            Err(s) => panic!("unexpected status: {s:?}"),
        }
    }
}
