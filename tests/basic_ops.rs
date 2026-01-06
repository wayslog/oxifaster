//! Basic operation tests for oxifaster
//!
//! This module contains integration tests for basic KV operations.

use std::sync::Arc;
use std::thread;

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

/// Create a test store
fn create_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 24, // 16 MB
        page_size_bits: 14,       // 16 KB pages
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

#[test]
fn test_basic_upsert_read() {
    let store = create_store();
    let mut session = store.start_session();

    // Test basic upsert and read
    let key = 42u64;
    let value = 100u64;

    let status = session.upsert(key, value);
    assert_eq!(status, Status::Ok);

    let result = session.read(&key);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(value));
}

#[test]
fn test_read_nonexistent() {
    let store = create_store();
    let mut session = store.start_session();

    let result = session.read(&999u64);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[test]
fn test_update_existing() {
    let store = create_store();
    let mut session = store.start_session();

    let key = 42u64;

    // Insert
    session.upsert(key, 100);
    assert_eq!(session.read(&key).unwrap(), Some(100));

    // Update
    session.upsert(key, 200);
    assert_eq!(session.read(&key).unwrap(), Some(200));
}

#[test]
fn test_delete() {
    let store = create_store();
    let mut session = store.start_session();

    let key = 42u64;

    // Insert
    session.upsert(key, 100);
    assert_eq!(session.read(&key).unwrap(), Some(100));

    // Delete
    let status = session.delete(&key);
    assert_eq!(status, Status::Ok);

    // Verify deleted
    assert_eq!(session.read(&key).unwrap(), None);
}

#[test]
fn test_delete_nonexistent() {
    let store = create_store();
    let mut session = store.start_session();

    let status = session.delete(&999u64);
    assert_eq!(status, Status::NotFound);
}

#[test]
fn test_multiple_keys() {
    let store = create_store();
    let mut session = store.start_session();

    let num_keys = 1000u64;

    // Insert all keys (starting from 1 to avoid hash collision issues with 0)
    for i in 1..=num_keys {
        let status = session.upsert(i, i * 10);
        assert_eq!(status, Status::Ok);
    }

    // Verify all keys
    for i in 1..=num_keys {
        let result = session.read(&i);
        assert_eq!(result.unwrap(), Some(i * 10), "Failed to read key {}", i);
    }
}

#[test]
fn test_overwrite_chain() {
    let store = create_store();
    let mut session = store.start_session();

    let key = 42u64;

    // Multiple updates to the same key
    for i in 0..100 {
        session.upsert(key, i);
    }

    // Should read the latest value
    assert_eq!(session.read(&key).unwrap(), Some(99));
}

#[test]
fn test_concurrent_sessions() {
    let store = create_store();
    let num_threads = 4;
    let ops_per_thread = 100;

    // Populate store first
    {
        let mut session = store.start_session();
        for i in 0..1000u64 {
            session.upsert(i, i * 10);
        }
    }

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let store = store.clone();
            thread::spawn(move || {
                let mut session = store.start_session();

                for i in 0..ops_per_thread {
                    let key = (t * ops_per_thread + i) as u64 % 1000;
                    let _ = session.read(&key);
                    session.upsert(key, key + 1);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify store is still consistent
    let mut session = store.start_session();
    for i in 0..1000u64 {
        let result = session.read(&i);
        assert!(result.is_ok());
    }
}

#[test]
fn test_session_lifecycle() {
    let store = create_store();

    // Create and drop multiple sessions
    for _ in 0..10 {
        let mut session = store.start_session();
        session.upsert(1, 1);
        let _ = session.read(&1u64);
        // Session automatically ends when dropped
    }

    // Final verification
    let mut session = store.start_session();
    let result = session.read(&1u64);
    assert!(result.is_ok());
}

#[test]
fn test_large_values() {
    // This test would require a value type that supports larger sizes
    // For u64, this is a simple test
    let store = create_store();
    let mut session = store.start_session();

    let key = 42u64;
    let value = u64::MAX;

    session.upsert(key, value);
    assert_eq!(session.read(&key).unwrap(), Some(value));
}

#[test]
fn test_index_stats() {
    let store = create_store();
    let mut session = store.start_session();

    // Insert some data
    for i in 0..100u64 {
        session.upsert(i, i);
    }

    let stats = store.index_stats();
    assert!(stats.used_entries > 0);
    assert!(stats.table_size > 0);
}

#[test]
fn test_log_stats() {
    let store = create_store();
    let mut session = store.start_session();

    // Insert some data
    for i in 0..100u64 {
        session.upsert(i, i);
    }

    let stats = store.log_stats();
    println!("{}", stats);
    assert!(stats.tail_address > stats.begin_address);
}
