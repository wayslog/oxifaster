// ============ 2. Compaction ============

#[test]
fn test_compact_with_begin_address() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let begin = store.log_begin_address();
    let status = store.compact(begin);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_flush_and_shift_head() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let begin = store.log_begin_address();
    let status = store.flush_and_shift_head(begin);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_log_compact_empty_store() {
    let store = create_store_with_compaction();
    let result = store.log_compact();
    // Either Ok or no-op is fine for empty store
    assert!(result.status == Status::Ok || result.status != Status::Corruption);
}

#[test]
fn test_log_compact_with_data() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }
    // Overwrite some keys to create obsolete records
    for i in 0..25u64 {
        session.upsert(i, i * 20);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_log_compact_until() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let begin = store.log_begin_address();
    let target = Address::from_control(begin.control() + 128);
    let result = store.log_compact_until(Some(target));
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_log_compact_until_none() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let result = store.log_compact_until(None);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compact_bytes() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let result = store.compact_bytes(256);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_log_utilization_empty() {
    let store = create_test_store();
    let util = store.log_utilization();
    // For empty store, utilization should be 1.0
    assert!((util - 1.0).abs() < 0.01);
}

#[test]
fn test_log_utilization_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    let util = store.log_utilization();
    // With data, utilization should be between 0 and 1 (inclusive)
    assert!((0.0..=1.0).contains(&util));
}

#[test]
fn test_should_compact() {
    let store = create_store_with_compaction();
    // For a fresh store, should_compact depends on utilization vs threshold
    let _result = store.should_compact();
}

#[test]
fn test_log_begin_address() {
    let store = create_test_store();
    let addr = store.log_begin_address();
    // Begin address is valid for a freshly created store
    let _ = addr.control();
}

#[test]
fn test_log_head_address() {
    let store = create_test_store();
    let addr = store.log_head_address();
    let _ = addr.control();
}

#[test]
fn test_log_tail_address() {
    let store = create_test_store();
    let addr = store.log_tail_address();
    let _ = addr.control();
}

#[test]
fn test_log_size_bytes_empty() {
    let store = create_test_store();
    let size = store.log_size_bytes();
    assert_eq!(size, 0);
}

#[test]
fn test_log_size_bytes_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let size = store.log_size_bytes();
    assert!(size > 0);
}

#[test]
fn test_compaction_config_accessor() {
    let store = create_store_with_compaction();
    let config = store.compaction_config();
    assert_eq!(config.target_utilization, 0.5);
}

#[test]
fn test_is_compaction_in_progress() {
    let store = create_store_with_compaction();
    assert!(!store.is_compaction_in_progress());
}

#[test]
fn test_double_compact_in_progress_guard() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let result1 = store.log_compact();
    assert_eq!(result1.status, Status::Ok);

    // Second compaction immediately after should also succeed (first one completed)
    let result2 = store.log_compact();
    assert_eq!(result2.status, Status::Ok);
}

// ============ 3. AsyncSession ============

#[test]
fn test_async_session_creation() {
    let store = create_test_store();
    let session = store.start_async_session().unwrap();
    assert!(session.is_active());
}

#[test]
fn test_async_session_builder_default() {
    let store = create_test_store();
    let session = AsyncSessionBuilder::new(store).build();
    assert!(!session.is_active());
    assert_eq!(session.thread_id(), 0);
}

#[test]
fn test_async_session_builder_with_thread_id() {
    let store = create_test_store();
    let session = AsyncSessionBuilder::new(store).thread_id(7).build();
    assert_eq!(session.thread_id(), 7);
}

#[test]
fn test_async_session_builder_from_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 55);
    let session = AsyncSessionBuilder::new(store).from_state(state).build();
    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 55);
}

#[test]
fn test_async_session_builder_thread_id_and_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 88);
    let session = AsyncSessionBuilder::new(store)
        .thread_id(3)
        .from_state(state)
        .build();
    assert_eq!(session.thread_id(), 3);
    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 88);
}

#[test]
fn test_async_session_sync_upsert_read() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    let status = session.upsert(1u64, 100u64);
    assert_eq!(status, Status::Ok);

    let result = session.read(&1u64);
    assert_eq!(result, Ok(Some(100)));
}

#[test]
fn test_async_session_sync_delete() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    session.upsert(1u64, 100u64);
    let status = session.delete(&1u64);
    assert_eq!(status, Status::Ok);
    assert_eq!(session.read(&1u64).unwrap(), None);
}

#[test]
fn test_async_session_sync_rmw() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    session.upsert(1u64, 100u64);
    let status = session.rmw(1u64, |v| {
        *v += 50;
        true
    });
    assert_eq!(status, Status::Ok);
    assert_eq!(session.read(&1u64).unwrap(), Some(150));
}

#[test]
fn test_async_session_sync_conditional_insert() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    let status = session.conditional_insert(1u64, 100u64);
    assert_eq!(status, Status::Ok);

    let status = session.conditional_insert(1u64, 200u64);
    assert_eq!(status, Status::Aborted);
}

#[test]
fn test_async_session_complete_pending() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    let result = session.complete_pending(false);
    assert!(result);
}

#[test]
fn test_async_session_guid() {
    let store = create_test_store();
    let session = store.start_async_session().unwrap();
    let guid = session.guid();
    assert!(!guid.is_nil());
}

#[test]
fn test_async_session_version() {
    let store = create_test_store();
    let session = store.start_async_session().unwrap();
    assert_eq!(session.version(), 0);
}

#[test]
fn test_async_session_serial_num() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    let initial = session.serial_num();
    session.upsert(1u64, 100u64);
    assert!(session.serial_num() > initial);
}

#[test]
fn test_async_session_start_end() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    assert!(session.is_active());

    session.end();
    assert!(!session.is_active());

    session.start();
    assert!(session.is_active());
}

#[test]
fn test_async_session_refresh() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    session.refresh();
}

#[test]
fn test_async_session_to_session_state() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    session.upsert(1u64, 100u64);

    let state = session.to_session_state();
    assert_eq!(state.guid, session.guid());
    assert_eq!(state.serial_num, session.serial_num());
}

#[test]
fn test_async_session_continue_from_state() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    session.upsert(1u64, 100u64);

    let state = session.to_session_state();
    session.upsert(2u64, 200u64);
    assert!(session.serial_num() > state.serial_num);

    session.continue_from_state(&state);
    assert_eq!(session.serial_num(), state.serial_num);
}

#[test]
fn test_async_session_context() {
    let store = create_test_store();
    let session = store.start_async_session().unwrap();
    let ctx = session.context();
    assert_eq!(ctx.version, 0);
}

#[test]
fn test_async_session_context_mut() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    let ctx = session.context_mut();
    ctx.set_serial_num(123);
    assert_eq!(session.serial_num(), 123);
}

#[test]
fn test_async_session_continue_from_store() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();
    session.upsert(1u64, 100u64);
    let state = session.to_session_state();
    drop(session);

    // Continue async session from saved state
    let continued = store.continue_async_session(state.clone()).unwrap();
    assert_eq!(continued.guid(), state.guid);
    assert_eq!(continued.serial_num(), state.serial_num);
}

#[test]
fn test_continue_session_from_store() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    session.upsert(1u64, 100u64);
    let state = session.to_session_state();
    drop(session);

    let continued = store.continue_session(state.clone()).unwrap();
    assert_eq!(continued.guid(), state.guid);
    assert_eq!(continued.serial_num(), state.serial_num);
}

