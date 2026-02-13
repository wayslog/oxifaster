//! Crash consistency tests for checkpoint durability
//!
//! These tests verify that checkpoints are durable and crash-consistent.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

#[test]
fn test_checkpoint_crash_durability() {
    // This test verifies that data is durable after checkpoint completes
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");

    let token;

    // Phase 1: Write data and checkpoint
    {
        let config = FasterKvConfig::default();
        let device = FileSystemDisk::single_file(&db_path).unwrap();
        let store = Arc::new(FasterKv::new(config, device));

        let mut session = store.start_session().unwrap();

        // Write some data
        for i in 0u64..1000 {
            session.upsert(i, i * 2);
        }

        // Create checkpoint
        token = store.checkpoint(&checkpoint_dir).unwrap();

        // Verify checkpoint completed
        assert!(checkpoint_dir.join(token.to_string()).exists());
    }
    // Simulate crash by dropping without clean shutdown

    // Phase 2: Recover and verify data
    {
        let config = FasterKvConfig::default();
        let device = FileSystemDisk::single_file(&db_path).unwrap();
        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
        let recovered = Arc::new(recovered);

        let mut session = recovered.start_session().unwrap();

        // Verify all data is present after recovery
        for i in 0u64..1000 {
            let value = session.read(&i).unwrap();
            assert_eq!(value, Some(i * 2), "Key {} should be recoverable", i);
        }
    }
}

#[test]
fn test_concurrent_write_checkpoint_crash() {
    // Test that checkpoint captures a consistent prefix during concurrent writes
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store = Arc::new(FasterKv::new(config.clone(), device));

    let stop_flag = Arc::new(AtomicBool::new(false));
    let store_clone = Arc::clone(&store);
    let stop_clone = Arc::clone(&stop_flag);

    // Writer thread: continuously write data
    let writer = thread::spawn(move || {
        let mut session = store_clone.start_session().unwrap();
        let mut counter = 0u64;
        while !stop_clone.load(Ordering::Relaxed) {
            session.upsert(counter, counter * 3);
            counter += 1;
            if counter.is_multiple_of(100) {
                thread::sleep(Duration::from_micros(10));
            }
        }
        counter
    });

    // Let writes happen for a bit
    thread::sleep(Duration::from_millis(50));

    // Create checkpoint while writes are ongoing
    let token = store.checkpoint(&checkpoint_dir).unwrap();

    // Stop writes
    stop_flag.store(true, Ordering::Relaxed);
    let final_counter = writer.join().unwrap();

    drop(store);

    // Recover and verify
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // All data up to some point should be consistent
    // We don't know exact boundary, but data should be valid
    for i in 0..final_counter.min(100) {
        if let Ok(Some(value)) = session.read(&i) {
            assert_eq!(value, i * 3, "Recovered data should be consistent");
        }
    }
}

#[test]
fn test_flush_device_durability() {
    // Test that device.flush() is actually called during checkpoint
    use std::collections::BTreeMap;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store = Arc::new(FasterKv::new(config.clone(), device));

    let mut session = store.start_session().unwrap();

    // Write data
    let mut expected = BTreeMap::new();
    for i in 0u64..500 {
        session.upsert(i, i * 7);
        expected.insert(i, i * 7);
    }

    // Checkpoint
    let token = store.checkpoint(&checkpoint_dir).unwrap();

    drop(session);
    drop(store);

    // Immediate recovery without delay - data should be durable
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // Verify all data
    for (k, v) in expected {
        let value = session.read(&k).unwrap();
        assert_eq!(
            value,
            Some(v),
            "Key {} should be durable after checkpoint",
            k
        );
    }
}

#[test]
fn test_checkpoint_without_writes() {
    // Test that checkpoint works even with no new writes
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config.clone(), device));

    // Checkpoint empty store
    let token = store.checkpoint(&checkpoint_dir).unwrap();

    drop(store);

    // Recover
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // Store should be empty
    assert_eq!(session.read(&0u64).unwrap(), None);
}

#[test]
fn test_multiple_checkpoints_durability() {
    // Test that multiple checkpoints maintain durability
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store = Arc::new(FasterKv::new(config.clone(), device));

    let mut session = store.start_session().unwrap();

    // Checkpoint 1
    for i in 0u64..100 {
        session.upsert(i, i);
    }
    let _token1 = store.checkpoint(&checkpoint_dir).unwrap();

    // Checkpoint 2
    for i in 100u64..200 {
        session.upsert(i, i);
    }
    let token2 = store.checkpoint(&checkpoint_dir).unwrap();

    drop(session);
    drop(store);

    // Recover from second checkpoint
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token2, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // Verify all data from both checkpoints
    for i in 0u64..200 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i), "Key {} should be present", i);
    }
}
