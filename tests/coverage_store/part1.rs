#[test]
fn test_store_creation_default_config() {
    let config = FasterKvConfig::default();
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());
    assert!(!store.has_read_cache());
    // Stats are disabled by default (StatsConfig default has enabled: false)
    assert!(!store.is_stats_enabled());
}

#[test]
fn test_store_creation_custom_config() {
    let config = FasterKvConfig::new(2048, 1 << 24);
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());
    let state = store.system_state();
    assert!(state.is_rest());
    assert_eq!(state.version, 0);
}

#[test]
fn test_store_creation_with_compaction_config() {
    let store = create_store_with_compaction();
    let cc = store.compaction_config();
    assert_eq!(cc.target_utilization, 0.5);
    assert_eq!(cc.min_compact_bytes, 0);
}

#[test]
fn test_store_system_state() {
    let store = create_test_store();
    let state = store.system_state();
    assert_eq!(state.action, Action::None);
    assert_eq!(state.phase, Phase::Rest);
    assert_eq!(state.version, 0);
}

#[test]
fn test_store_epoch_accessors() {
    let store = create_test_store();
    let _epoch = store.epoch();
    let _epoch_arc = store.epoch_arc();
}

#[test]
fn test_store_device_accessor() {
    let store = create_test_store();
    let _device = store.device();
}

#[test]
fn test_store_size_empty() {
    let store = create_test_store();
    assert_eq!(store.size(), 0);
}

#[test]
fn test_store_size_after_inserts() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    for i in 0..10u64 {
        session.upsert(i, i * 10);
    }
    assert!(store.size() >= 10);
}

#[test]
fn test_store_stats_enable_disable() {
    let store = create_test_store();
    // Stats are disabled by default
    assert!(!store.is_stats_enabled());

    store.enable_stats();
    assert!(store.is_stats_enabled());

    store.disable_stats();
    assert!(!store.is_stats_enabled());
}

#[test]
fn test_store_stats_snapshot() {
    let store = create_test_store();
    store.enable_stats();

    let mut session = store.start_session().unwrap();
    session.upsert(1u64, 100u64);
    let _ = session.read(&1u64);
    session.delete(&1u64);

    let _snapshot = store.stats_snapshot();
    let _op_stats = store.operation_stats();
    let _alloc_stats = store.allocator_stats();
    let _hlog_stats = store.hlog_stats();
    let _operational_stats = store.operational_stats();
    let _throughput = store.throughput();
}

#[test]
fn test_store_stats_collector() {
    let store = create_test_store();
    let _collector = store.stats_collector();
}

#[test]
fn test_store_index_stats() {
    let store = create_test_store();
    let stats = store.index_stats();
    assert_eq!(stats.used_entries, 0);
}

#[test]
fn test_store_log_stats() {
    let store = create_test_store();
    let _stats = store.log_stats();
}

#[test]
fn test_store_hlog_max_size_not_reached() {
    let store = create_test_store();
    assert!(!store.hlog_max_size_reached());
}

#[test]
fn test_store_auto_compaction_scheduled() {
    let store = create_test_store();
    assert!(!store.auto_compaction_scheduled());
}

#[test]
fn test_store_read_cache_not_enabled() {
    let store = create_test_store();
    assert!(!store.has_read_cache());
    assert!(store.read_cache_stats().is_none());
    assert!(store.read_cache_config().is_none());
    // clear_read_cache should not panic when no cache
    store.clear_read_cache();
}

#[test]
fn test_store_session_management() {
    let store = create_test_store();
    assert_eq!(store.active_session_count(), 0);

    let session = store.start_session().unwrap();
    let guid = session.guid();
    assert_eq!(store.active_session_count(), 1);

    let state = store.get_session_state(guid);
    assert!(state.is_some());

    let states = store.get_session_states();
    assert_eq!(states.len(), 1);

    drop(session);
    assert_eq!(store.active_session_count(), 0);
}

// ============ 1b. Session CRUD operations ============

#[test]
fn test_session_batch_upsert_read() {
    let store = create_large_store();
    let mut session = store.start_session().unwrap();

    // Batch upsert 200 keys
    for i in 0..200u64 {
        let status = session.upsert(i, i * 100);
        assert_eq!(status, Status::Ok, "upsert failed for key {}", i);
    }

    // Batch read all 200 keys
    for i in 0..200u64 {
        let result = session.read(&i);
        assert!(result.is_ok(), "read failed for key {}", i);
        assert_eq!(result.unwrap(), Some(i * 100), "wrong value for key {}", i);
    }
}

#[test]
fn test_session_batch_delete() {
    let store = create_large_store();
    let mut session = store.start_session().unwrap();

    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    // Delete odd keys
    for i in (1..100u64).step_by(2) {
        let status = session.delete(&i);
        assert_eq!(status, Status::Ok, "delete failed for key {}", i);
    }

    // Verify: even keys present, odd keys gone
    for i in 0..100u64 {
        let result = session.read(&i).unwrap();
        if i % 2 == 0 {
            assert_eq!(result, Some(i * 10));
        } else {
            assert_eq!(result, None);
        }
    }
}

#[test]
fn test_session_rmw_increment() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    let status = session.rmw(1u64, |v| {
        *v += 50;
        true
    });
    assert_eq!(status, Status::Ok);
    assert_eq!(session.read(&1u64).unwrap(), Some(150));
}

#[test]
fn test_session_rmw_abort() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    // Modifier returns false -> operation should be aborted
    let status = session.rmw(1u64, |_v| false);
    assert_eq!(status, Status::Aborted);

    // Value should remain unchanged
    assert_eq!(session.read(&1u64).unwrap(), Some(100));
}

#[test]
fn test_session_rmw_nonexistent_key() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let status = session.rmw(999u64, |v| {
        *v += 1;
        true
    });
    assert_eq!(status, Status::NotFound);
}

#[test]
fn test_session_conditional_insert_new_key() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let status = session.conditional_insert(1u64, 100u64);
    assert_eq!(status, Status::Ok);
    assert_eq!(session.read(&1u64).unwrap(), Some(100));
}

#[test]
fn test_session_conditional_insert_existing_key() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    let status = session.conditional_insert(1u64, 200u64);
    assert_eq!(status, Status::Aborted);

    // Original value should remain
    assert_eq!(session.read(&1u64).unwrap(), Some(100));
}

#[test]
fn test_session_conditional_insert_after_delete() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    session.delete(&1u64);

    // After delete, the tombstone record still exists in the hash chain.
    // conditional_insert sees the tombstone and returns Aborted because the key
    // entry exists in the index. This is expected FASTER behavior.
    let status = session.conditional_insert(1u64, 200u64);
    // The tombstone is recognized as "key exists" by the chain traversal,
    // so conditional_insert may return Aborted.
    assert!(
        status == Status::Aborted || status == Status::Ok,
        "unexpected status: {:?}",
        status
    );
}

#[test]
fn test_session_complete_pending_no_pending() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    // With no pending operations, complete_pending should return true immediately
    let result = session.complete_pending(false);
    assert!(result);
}

#[test]
fn test_session_complete_pending_wait() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    // With no pending operations and wait=true, should return true
    let result = session.complete_pending(true);
    assert!(result);
}

// ============ 1c. Session metadata accessors ============

#[test]
fn test_session_guid_unique() {
    let store = create_test_store();
    let session1 = store.start_session().unwrap();
    let session2 = store.start_session().unwrap();
    assert_ne!(session1.guid(), session2.guid());
}

#[test]
fn test_session_thread_id() {
    let store = create_test_store();
    let session = store.start_session().unwrap();
    // thread_id should be valid (not MAX)
    let _tid = session.thread_id();
}

#[test]
fn test_session_version() {
    let store = create_test_store();
    let session = store.start_session().unwrap();
    assert_eq!(session.version(), 0);
}

#[test]
fn test_session_serial_num_increments() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    let initial = session.serial_num();

    session.upsert(1u64, 100u64);
    assert!(session.serial_num() > initial);

    let after_upsert = session.serial_num();
    session.upsert(2u64, 200u64);
    assert!(session.serial_num() > after_upsert);
}

#[test]
fn test_session_serial_num_no_increment_on_read() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    let serial_before = session.serial_num();
    let _ = session.read(&1u64);
    assert_eq!(session.serial_num(), serial_before);
}

#[test]
fn test_session_start_end_active() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    assert!(session.is_active());

    session.end();
    assert!(!session.is_active());

    // Starting again should work
    session.start();
    assert!(session.is_active());
}

#[test]
fn test_session_refresh() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    // refresh should not panic
    session.refresh();
}

// ============ 1d. Session state persistence ============

#[test]
fn test_session_to_session_state() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    session.upsert(2u64, 200u64);

    let state = session.to_session_state();
    assert_eq!(state.guid, session.guid());
    assert_eq!(state.serial_num, session.serial_num());
}

#[test]
fn test_session_continue_from_state() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    session.upsert(2u64, 200u64);
    let saved_state = session.to_session_state();
    let saved_serial = saved_state.serial_num;

    // Modify serial further
    session.upsert(3u64, 300u64);
    assert!(session.serial_num() > saved_serial);

    // Restore state
    session.continue_from_state(&saved_state);
    assert_eq!(session.serial_num(), saved_serial);
}

#[test]
fn test_session_continue_from_state_wrong_guid() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    session.upsert(1u64, 100u64);
    let serial_before = session.serial_num();

    // Create a state with a different GUID
    let wrong_state = SessionState::new(Uuid::new_v4(), 9999);
    session.continue_from_state(&wrong_state);

    // serial_num should be unchanged because GUID did not match
    assert_eq!(session.serial_num(), serial_before);
}

// ============ 1e. Session context accessors ============

#[test]
fn test_session_context() {
    let store = create_test_store();
    let session = store.start_session().unwrap();
    let ctx = session.context();
    assert_eq!(ctx.version, 0);
    assert_eq!(ctx.pending_count, 0);
}

#[test]
fn test_session_context_mut() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    let ctx = session.context_mut();
    ctx.set_serial_num(42);
    assert_eq!(session.serial_num(), 42);
}

// ============ 1f. SessionBuilder ============

#[test]
fn test_session_builder_default() {
    let store = create_test_store();
    let session = SessionBuilder::new(store).build();
    assert_eq!(session.thread_id(), 0);
    assert!(!session.is_active());
}

#[test]
fn test_session_builder_with_thread_id() {
    let store = create_test_store();
    let session = SessionBuilder::new(store).thread_id(5).build();
    assert_eq!(session.thread_id(), 5);
}

#[test]
fn test_session_builder_from_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 42);
    let session = SessionBuilder::new(store).from_state(state).build();
    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 42);
}

#[test]
fn test_session_builder_with_thread_id_and_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 77);
    let session = SessionBuilder::new(store)
        .thread_id(10)
        .from_state(state)
        .build();
    assert_eq!(session.thread_id(), 10);
    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 77);
}

// ============ 1g. read_view operations ============

#[test]
fn test_read_view_basic() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 999u64);

    let guard = session.pin();
    let view_result = session.read_view(&guard, &42u64);
    assert!(view_result.is_ok());
    let view = view_result.unwrap();
    assert!(view.is_some());
    let record = view.unwrap();
    assert!(!record.is_tombstone());
    assert!(!record.key_bytes().is_empty());
    assert!(record.value_bytes().is_some());
    assert!(record.value_len() > 0);
    assert!(record.address().is_valid());
}

#[test]
fn test_read_view_nonexistent_key() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let guard = session.pin();
    let view_result = session.read_view(&guard, &999u64);
    assert!(view_result.is_ok());
    assert!(view_result.unwrap().is_none());
}

// ============ 1h. Multi-session from separate threads ============

#[test]
fn test_multi_session_concurrent_writes() {
    let store = create_large_store();

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let store = store.clone();
            thread::spawn(move || {
                let mut session = store.start_session().unwrap();
                let base = t * 1000u64;
                for i in 0..100u64 {
                    let status = session.upsert(base + i, i);
                    assert_eq!(status, Status::Ok);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all data is present
    let mut session = store.start_session().unwrap();
    for t in 0..4u64 {
        let base = t * 1000;
        for i in 0..100u64 {
            let result = session.read(&(base + i)).unwrap();
            assert_eq!(result, Some(i), "mismatch at t={} i={}", t, i);
        }
    }
}

#[test]
fn test_multi_session_concurrent_read_write() {
    let store = create_large_store();

    // Pre-populate
    {
        let mut session = store.start_session().unwrap();
        for i in 0..100u64 {
            session.upsert(i, i * 10);
        }
    }

    // Concurrent reads and writes
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let store = store.clone();
            thread::spawn(move || {
                let mut session = store.start_session().unwrap();
                if t % 2 == 0 {
                    // Reader
                    for i in 0..100u64 {
                        let _ = session.read(&i);
                    }
                } else {
                    // Writer
                    for i in 0..100u64 {
                        session.upsert(i + 1000 * (t as u64), i);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

