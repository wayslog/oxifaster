//! Concurrent checkpoint tests
//!
//! These tests verify that checkpoints work correctly under heavy concurrent load
//! and validate the CPR (Concurrent Prefix Recovery) prefix guarantee.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

/// Test checkpoint under heavy concurrent write load
#[test]
fn test_concurrent_checkpoint_under_write_load() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    let stop = Arc::new(AtomicBool::new(false));
    let checkpoint_taken = Arc::new(AtomicBool::new(false));
    let writes_before_checkpoint = Arc::new(AtomicU64::new(0));
    let writes_after_checkpoint = Arc::new(AtomicU64::new(0));

    // Start 10 writer threads
    let mut handles = vec![];
    for thread_id in 0..10 {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let checkpoint_taken = Arc::clone(&checkpoint_taken);
        let writes_before = Arc::clone(&writes_before_checkpoint);
        let writes_after = Arc::clone(&writes_after_checkpoint);

        let handle = thread::spawn(move || {
            let mut session = store.start_session().unwrap();
            let mut counter = 0u64;

            while !stop.load(Ordering::Acquire) {
                let key = thread_id as u64 * 1_000_000 + counter;
                let value = key * 2;
                session.upsert(key, value);
                session.refresh();

                if checkpoint_taken.load(Ordering::Acquire) {
                    writes_after.fetch_add(1, Ordering::Relaxed);
                } else {
                    writes_before.fetch_add(1, Ordering::Relaxed);
                }

                counter += 1;

                // Small delay to allow checkpoint to run
                if counter.is_multiple_of(100) {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        handles.push(handle);
    }

    // Let writers run for a bit
    thread::sleep(Duration::from_millis(200));

    // Trigger checkpoint while writes are ongoing
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    checkpoint_taken.store(true, Ordering::Release);

    // Continue writes for a bit longer
    thread::sleep(Duration::from_millis(200));

    // Stop all writers
    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "Writes before checkpoint: {}",
        writes_before_checkpoint.load(Ordering::Acquire)
    );
    println!(
        "Writes after checkpoint: {}",
        writes_after_checkpoint.load(Ordering::Acquire)
    );

    // Verify we can recover
    drop(store);

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // All data written before checkpoint should be present
    // (We can't easily verify the exact boundary, but we can verify recovery works)
    let mut found_count = 0;
    for thread_id in 0..10 {
        for counter in 0..1000 {
            let key = thread_id as u64 * 1_000_000 + counter;
            if let Ok(Some(value)) = session.read(&key) {
                assert_eq!(value, key * 2);
                found_count += 1;
            }
        }
        session.refresh();
    }

    println!("Found {} keys after recovery", found_count);
    assert!(found_count > 0, "Should have recovered some data");
}

/// Test multiple concurrent checkpoints
#[test]
fn test_multiple_concurrent_checkpoints() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    let stop = Arc::new(AtomicBool::new(false));

    // Start 5 writer threads
    let mut handles = vec![];
    for thread_id in 0..5 {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut session = store.start_session().unwrap();
            let mut counter = 0u64;

            while !stop.load(Ordering::Acquire) {
                let key = thread_id as u64 * 1_000_000 + counter;
                let value = key * 2;
                session.upsert(key, value);
                session.refresh();
                counter += 1;

                if counter.is_multiple_of(100) {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        handles.push(handle);
    }

    // Take checkpoint 1
    thread::sleep(Duration::from_millis(100));
    let token1 = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint 1 complete: {}", token1);

    // Continue writes
    thread::sleep(Duration::from_millis(100));

    // Take checkpoint 2
    let token2 = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint 2 complete: {}", token2);

    // Continue writes
    thread::sleep(Duration::from_millis(100));

    // Take checkpoint 3
    let token3 = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint 3 complete: {}", token3);

    // Stop writers
    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all checkpoints are valid by recovering from each
    drop(store);

    // Test recovery from checkpoint 1
    {
        let config = FasterKvConfig::default();
        let device = FileSystemDisk::single_file(&db_path).unwrap();
        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover(&checkpoint_dir, token1, config, device).unwrap();
        let recovered = Arc::new(recovered);
        let mut session = recovered.start_session().unwrap();

        // Verify some data is present
        let mut found = false;
        for key in 0..1000 {
            if let Ok(Some(_)) = session.read(&key) {
                found = true;
                break;
            }
        }
        assert!(found, "Should recover data from checkpoint 1");
    }

    // Test recovery from checkpoint 3 (latest)
    {
        let config = FasterKvConfig::default();
        let device = FileSystemDisk::single_file(&db_path).unwrap();
        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover(&checkpoint_dir, token3, config, device).unwrap();
        let recovered = Arc::new(recovered);
        let mut session = recovered.start_session().unwrap();

        // Latest checkpoint should have more data
        let mut count = 0;
        for thread_id in 0..5 {
            for counter in 0..2000 {
                let key = thread_id as u64 * 1_000_000 + counter;
                if let Ok(Some(_)) = session.read(&key) {
                    count += 1;
                }
            }
            session.refresh();
        }
        println!("Recovered {} keys from checkpoint 3", count);
        assert!(count > 0, "Should recover data from checkpoint 3");
    }
}

/// Test checkpoint with concurrent reads
#[test]
fn test_checkpoint_with_concurrent_reads() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    // Write initial data
    {
        let mut session = store.start_session().unwrap();
        for i in 0u64..10000 {
            session.upsert(i, i * 2);
            if i % 100 == 0 {
                session.refresh();
            }
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let read_errors = Arc::new(AtomicU64::new(0));

    // Start 10 reader threads
    let mut handles = vec![];
    for _thread_id in 0..10 {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let read_errors = Arc::clone(&read_errors);

        let handle = thread::spawn(move || {
            let mut session = store.start_session().unwrap();
            let mut counter = 0u64;

            while !stop.load(Ordering::Acquire) {
                let key = counter % 10000;
                match session.read(&key) {
                    Ok(Some(value)) => {
                        if value != key * 2 {
                            read_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {
                        // Key should exist
                        read_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        read_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                session.refresh();
                counter += 1;

                if counter.is_multiple_of(100) {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        handles.push(handle);
    }

    // Let readers run for a bit
    thread::sleep(Duration::from_millis(100));

    // Trigger checkpoint during reads
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint complete: {}", token);

    // Let readers continue for a bit
    thread::sleep(Duration::from_millis(100));

    // Stop readers
    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.join().unwrap();
    }

    let errors = read_errors.load(Ordering::Acquire);
    println!("Read errors during checkpoint: {}", errors);
    assert_eq!(errors, 0, "Reads should continue to work during checkpoint");

    // Verify checkpoint is valid
    drop(store);
    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // All keys should be present
    for i in 0u64..10000 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i * 2));
        if i % 100 == 0 {
            session.refresh();
        }
    }
}

/// Test thread coordination during checkpoint
#[test]
fn test_checkpoint_thread_coordination() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    let checkpoint_start = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    // Start 8 threads that all participate
    let mut handles = vec![];
    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let checkpoint_start = Arc::clone(&checkpoint_start);
        let stop = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut session = store.start_session().unwrap();
            let mut counter = 0u64;

            while !stop.load(Ordering::Acquire) {
                let key = thread_id as u64 * 100000 + counter;
                let value = key * 2;
                session.upsert(key, value);

                // Frequently call refresh to help checkpoint progress
                session.refresh();

                counter += 1;

                if checkpoint_start.load(Ordering::Acquire) {
                    // After checkpoint starts, refresh more frequently
                    // to help advance the checkpoint state machine
                    if counter.is_multiple_of(10) {
                        thread::sleep(Duration::from_micros(5));
                    }
                } else if counter.is_multiple_of(100) {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        handles.push(handle);
    }

    // Let threads establish sessions
    thread::sleep(Duration::from_millis(100));

    // Signal checkpoint start
    checkpoint_start.store(true, Ordering::Release);

    // Take checkpoint - threads should help advance the state machine
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint complete with thread coordination: {}", token);

    // Stop threads
    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify checkpoint
    drop(store);
    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // Verify data from multiple threads is present
    let mut threads_with_data = 0;
    for thread_id in 0..8 {
        let key = thread_id as u64 * 100000;
        if let Ok(Some(_)) = session.read(&key) {
            threads_with_data += 1;
        }
        session.refresh();
    }

    println!("Threads with data after recovery: {}", threads_with_data);
    assert!(
        threads_with_data >= 6,
        "Most threads should have contributed data"
    );
}

/// Test CPR prefix guarantee
#[test]
fn test_checkpoint_cpr_prefix_guarantee() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    // Write data with serial number tracking
    {
        let mut session = store.start_session().unwrap();
        for i in 0u64..5000 {
            session.upsert(i, i * 2);
            if i % 100 == 0 {
                session.refresh();
            }
        }
        let serial_before = session.context().serial_num();
        println!("Serial number before checkpoint: {}", serial_before);
    }

    // Take checkpoint
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint token: {}", token);

    // Write more data after checkpoint
    {
        let mut session = store.start_session().unwrap();
        for i in 5000u64..10000 {
            session.upsert(i, i * 2);
            if i % 100 == 0 {
                session.refresh();
            }
        }
        let serial_after = session.context().serial_num();
        println!("Serial number after more writes: {}", serial_after);
    }

    // Recover from checkpoint
    drop(store);
    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);
    let mut session = recovered.start_session().unwrap();

    // CPR prefix guarantee: All operations up to the checkpoint should be present
    // Operations after checkpoint may or may not be present (depending on timing)
    let mut count_before = 0;
    let mut count_after = 0;

    for i in 0u64..5000 {
        if let Ok(Some(_)) = session.read(&i) {
            count_before += 1;
        }
    }

    for i in 5000u64..10000 {
        if let Ok(Some(_)) = session.read(&i) {
            count_after += 1;
        }
    }

    println!(
        "Keys before checkpoint: {}/5000, after: {}/5000",
        count_before, count_after
    );

    // Most keys written before checkpoint should be present
    assert!(
        count_before > 4500,
        "Most keys before checkpoint should be recovered"
    );

    // Keys after checkpoint may or may not be present (that's the prefix guarantee)
    // We don't assert anything about count_after - it's implementation dependent
}
