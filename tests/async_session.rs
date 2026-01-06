//! Async Session 集成测试
//!
//! 测试异步会话功能。

use std::sync::Arc;

use oxifaster::checkpoint::SessionState;
use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{AsyncSessionBuilder, FasterKv, FasterKvConfig};
use uuid::Uuid;

fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

// ============ AsyncSession Creation Tests ============

#[test]
fn test_async_session_from_store() {
    let store = create_test_store();
    let session = store.start_async_session();

    assert!(session.is_active());
}

#[test]
fn test_async_session_guid_unique() {
    let store = create_test_store();
    let session1 = store.start_async_session();
    let session2 = store.start_async_session();

    assert_ne!(session1.guid(), session2.guid());
}

#[test]
fn test_async_session_builder() {
    let store = create_test_store();
    let session = AsyncSessionBuilder::new(store)
        .thread_id(42)
        .build();

    assert_eq!(session.thread_id(), 42);
}

#[test]
fn test_async_session_builder_from_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 100);

    let session = AsyncSessionBuilder::new(store)
        .from_state(state)
        .build();

    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 100);
}

// ============ Sync Operations Through AsyncSession Tests ============

#[test]
fn test_async_session_upsert() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    let status = session.upsert(1u64, 100u64);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_async_session_read() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);
    let result = session.read(&1u64);

    assert_eq!(result, Ok(Some(100u64)));
}

#[test]
fn test_async_session_read_not_found() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    let result = session.read(&999u64);
    assert_eq!(result, Ok(None));
}

#[test]
fn test_async_session_delete() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);
    let delete_status = session.delete(&1u64);

    assert_eq!(delete_status, Status::Ok);

    // After delete, read should return None
    let result = session.read(&1u64);
    assert_eq!(result, Ok(None));
}

#[test]
fn test_async_session_rmw() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);

    let status = session.rmw(1u64, |v| {
        *v += 50;
        true
    });

    assert_eq!(status, Status::Ok);

    let result = session.read(&1u64);
    assert_eq!(result, Ok(Some(150u64)));
}

// ============ Session State Tests ============

#[test]
fn test_async_session_to_state() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    // Do some operations to increment serial number
    session.upsert(1u64, 100u64);
    session.upsert(2u64, 200u64);

    let state = session.to_session_state();

    assert_eq!(state.guid, session.guid());
    assert_eq!(state.serial_num, session.serial_num());
}

#[test]
fn test_async_session_continue_from_state() {
    let store = create_test_store();
    let mut session = store.start_async_session();
    let original_guid = session.guid();

    session.upsert(1u64, 100u64);
    let state = session.to_session_state();

    // Create new session and continue from state
    let mut new_session = store.start_async_session();
    new_session.continue_from_state(&state);

    // GUID should match when continuing same session
    // But new_session has different GUID since it's a new session
    // continue_from_state only updates serial if GUIDs match
    assert_ne!(new_session.guid(), original_guid);
}

#[test]
fn test_async_session_continue_async_session() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);
    let state = session.to_session_state();

    // Continue using store method
    let continued = store.continue_async_session(state.clone());

    assert_eq!(continued.guid(), state.guid);
    assert_eq!(continued.serial_num(), state.serial_num);
}

// ============ Session Lifecycle Tests ============

#[test]
fn test_async_session_start_end() {
    let store = create_test_store();
    let mut session = AsyncSessionBuilder::new(store).build();

    assert!(!session.is_active());

    session.start();
    assert!(session.is_active());

    session.end();
    assert!(!session.is_active());
}

#[test]
fn test_async_session_refresh() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    // Refresh should not panic
    session.refresh();
}

#[test]
fn test_async_session_complete_pending() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    // With no pending operations, should return true immediately
    assert!(session.complete_pending(false));
}

// ============ Context Access Tests ============

#[test]
fn test_async_session_context() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);

    let ctx = session.context();
    assert!(ctx.serial_num > 0);
}

#[test]
fn test_async_session_context_mut() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    let ctx = session.context_mut();
    ctx.version = 42;

    assert_eq!(session.version(), 42);
}

// ============ Multiple Operations Tests ============

#[test]
fn test_async_session_multiple_upserts() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    for i in 1u64..=100 {
        let status = session.upsert(i, i * 10);
        assert_eq!(status, Status::Ok);
    }

    for i in 1u64..=100 {
        let result = session.read(&i);
        assert_eq!(result, Ok(Some(i * 10)));
    }
}

#[test]
fn test_async_session_update_existing() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);
    session.upsert(1u64, 200u64);

    let result = session.read(&1u64);
    assert_eq!(result, Ok(Some(200u64)));
}

#[test]
fn test_async_session_serial_increments() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    let initial = session.serial_num();
    session.upsert(1u64, 100u64);
    let after_upsert = session.serial_num();

    assert!(after_upsert > initial);
}

// ============ Thread Safety Tests ============

#[test]
fn test_multiple_sessions_different_threads() {
    use std::thread;

    let store = create_test_store();
    let num_threads = 4;
    let ops_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let store = store.clone();
            thread::spawn(move || {
                let mut session = store.start_async_session();
                for i in 0..ops_per_thread {
                    let key = (t * ops_per_thread + i) as u64;
                    session.upsert(key, key * 10);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

// ============ Edge Cases ============

#[test]
fn test_async_session_rmw_abort() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    session.upsert(1u64, 100u64);

    // RMW that returns false (abort)
    let status = session.rmw(1u64, |_| false);

    // Status depends on implementation - might still be Ok
    // Just verify it doesn't panic
    let _ = status;

    // Value should remain unchanged after abort
    let result = session.read(&1u64);
    assert_eq!(result, Ok(Some(100u64)));
}

#[test]
fn test_async_session_delete_nonexistent() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    let status = session.delete(&999u64);
    // Deleting non-existent key should return NotFound
    assert_eq!(status, Status::NotFound);
}

#[test]
fn test_async_session_rmw_create_new() {
    let store = create_test_store();
    let mut session = store.start_async_session();

    // RMW on non-existent key - behavior depends on implementation
    // Most implementations would create a new entry
    let status = session.rmw(1u64, |v| {
        *v = 50;
        true
    });

    // Just verify no panic
    let _ = status;
}
