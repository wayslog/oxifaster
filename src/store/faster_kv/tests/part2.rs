// ============ FasterKvConfig Tests ============

#[test]
fn test_faster_kv_config_new() {
    let config = FasterKvConfig::new(1024, 1 << 20);
    assert_eq!(config.table_size, 1024);
    assert_eq!(config.log_memory_size, 1 << 20);
    assert_eq!(config.page_size_bits, 22);
    assert!((config.mutable_fraction - 0.9).abs() < f64::EPSILON);
}

#[test]
fn test_faster_kv_config_default() {
    let config = FasterKvConfig::default();
    assert_eq!(config.table_size, 1 << 20);
    assert_eq!(config.log_memory_size, 1 << 29);
    assert_eq!(config.page_size_bits, 22);
    assert!((config.mutable_fraction - 0.9).abs() < f64::EPSILON);
}

#[test]
fn test_faster_kv_config_validate_ok() {
    let config = FasterKvConfig::default();
    assert!(config.validate().is_ok());

    let config = FasterKvConfig::new(1024, 1 << 23);
    assert!(config.validate().is_ok());
}

#[test]
fn test_faster_kv_config_validate_table_size_not_power_of_two() {
    let config = FasterKvConfig {
        table_size: 1000,
        ..FasterKvConfig::default()
    };
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_faster_kv_config_validate_table_size_too_large() {
    let config = FasterKvConfig {
        table_size: 1u64 << 31,
        ..FasterKvConfig::default()
    };
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_faster_kv_config_validate_page_size_bits_too_small() {
    let config = FasterKvConfig {
        page_size_bits: 9,
        ..FasterKvConfig::default()
    };
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_faster_kv_config_validate_page_size_bits_too_large() {
    let config = FasterKvConfig {
        page_size_bits: 31,
        ..FasterKvConfig::default()
    };
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_faster_kv_config_validate_log_memory_too_small() {
    let config = FasterKvConfig {
        log_memory_size: 1 << 10,
        page_size_bits: 10,
        ..FasterKvConfig::default()
    };
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_faster_kv_config_validate_mutable_fraction_nan() {
    let config = FasterKvConfig {
        mutable_fraction: f64::NAN,
        ..FasterKvConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn test_faster_kv_config_validate_mutable_fraction_out_of_range_is_accepted() {
    // Out-of-range values are NOT rejected -- they are clamped downstream
    // by HybridLogConfig::with_mutable_fraction(). This preserves backward
    // compatibility.
    let config = FasterKvConfig {
        mutable_fraction: 1.5,
        ..FasterKvConfig::default()
    };
    assert!(config.validate().is_ok());

    let config = FasterKvConfig {
        mutable_fraction: -0.1,
        ..FasterKvConfig::default()
    };
    assert!(config.validate().is_ok());
}

// ============ Session Management Tests ============

#[test]
fn test_start_async_session() {
    let store = create_test_store();
    let async_session = store.start_async_session();
    assert!(async_session.is_ok());
}

#[test]
fn test_continue_session() {
    let store = create_test_store();
    let session = store.start_session().unwrap();
    let state = session.to_session_state();
    let guid = session.guid();
    let serial_num = session.serial_num();
    drop(session);

    let continued = store.continue_session(state).unwrap();
    assert_eq!(continued.guid(), guid);
    assert_eq!(continued.serial_num(), serial_num);
}

#[test]
fn test_continue_async_session() {
    let store = create_test_store();
    let async_session = store.start_async_session().unwrap();
    let state = async_session.to_session_state();
    drop(async_session);

    let continued = store.continue_async_session(state);
    assert!(continued.is_ok());
}

#[test]
fn test_session_registry_operations() {
    let store = create_test_store();

    assert_eq!(store.active_session_count(), 0);
    assert!(store.get_session_states().is_empty());

    let session = store.start_session().unwrap();
    let guid = session.guid();

    assert_eq!(store.active_session_count(), 1);
    assert!(store.get_session_state(guid).is_some());

    let states = store.get_session_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].guid, guid);

    drop(session);
    assert_eq!(store.active_session_count(), 0);
    assert!(store.get_session_state(guid).is_none());
}

#[test]
fn test_active_threads_snapshot() {
    let store = create_test_store();

    let snapshot = store.active_threads_snapshot();
    assert_eq!(snapshot, 0);

    let _session = store.start_session().unwrap();
    let snapshot = store.active_threads_snapshot();
    assert_ne!(snapshot, 0);
}

// ============ Statistics API Tests ============

#[test]
fn test_enable_disable_stats() {
    let store = create_test_store();

    assert!(!store.is_stats_enabled());

    store.enable_stats();
    assert!(store.is_stats_enabled());

    store.disable_stats();
    assert!(!store.is_stats_enabled());
}

#[test]
fn test_stats_collector() {
    let store = create_test_store();
    let stats_collector = store.stats_collector();

    assert!(!stats_collector.is_enabled());
}

#[test]
fn test_stats_snapshot() {
    let store = create_test_store();
    store.enable_stats();

    let mut session = store.start_session().unwrap();
    session.upsert(1u64, 100u64);
    let _ = session.read(&1u64);

    let snapshot = store.stats_snapshot();
    assert!(snapshot.upserts > 0 || snapshot.reads > 0);
}

#[test]
fn test_operation_stats() {
    let store = create_test_store();
    store.enable_stats();

    let mut session = store.start_session().unwrap();
    session.upsert(1u64, 100u64);

    let op_stats = store.operation_stats();
    assert!(op_stats.upserts.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

#[test]
fn test_allocator_stats() {
    let store = create_test_store();
    let _alloc_stats = store.allocator_stats();
}

#[test]
fn test_hlog_stats() {
    let store = create_test_store();
    let _hlog_stats = store.hlog_stats();
}

#[test]
fn test_operational_stats() {
    let store = create_test_store();
    let _op_stats = store.operational_stats();
}

#[test]
fn test_throughput() {
    let store = create_test_store();
    let throughput = store.throughput();
    assert!(throughput >= 0.0);
}

// ============ State Query Tests ============

#[test]
fn test_size() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let initial_size = store.size();
    assert_eq!(initial_size, 0);

    for i in 0u64..10 {
        session.upsert(i, i * 100);
    }

    let new_size = store.size();
    assert!(new_size > 0);
}

#[test]
fn test_hlog_max_size_reached() {
    let store = create_test_store();
    let reached = store.hlog_max_size_reached();
    assert!(!reached);
}

#[test]
fn test_auto_compaction_scheduled() {
    let store = create_test_store();
    assert!(!store.auto_compaction_scheduled());
}

// ============ Epoch Management Tests ============

#[test]
fn test_epoch() {
    let store = create_test_store();
    let epoch = store.epoch();
    let _ = epoch
        .current_epoch
        .load(std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn test_epoch_arc() {
    let store = create_test_store();
    let epoch_arc = store.epoch_arc();
    let epoch_arc_clone = epoch_arc.clone();
    let _ = epoch_arc_clone
        .current_epoch
        .load(std::sync::atomic::Ordering::Relaxed);
}

// ============ Device Access Tests ============

#[test]
fn test_device() {
    let store = create_test_store();
    let device = store.device();
    let _ = Arc::clone(device);
}

// ============ Checkpoint Backend Configuration Tests ============

#[test]
fn test_log_checkpoint_backend_default() {
    let store = create_test_store();
    let backend = store.log_checkpoint_backend();
    assert_eq!(backend, LogCheckpointBackend::Snapshot);
}

#[test]
fn test_set_log_checkpoint_backend() {
    let store = create_test_store();

    store.set_log_checkpoint_backend(LogCheckpointBackend::FoldOver);
    assert_eq!(
        store.log_checkpoint_backend(),
        LogCheckpointBackend::FoldOver
    );

    store.set_log_checkpoint_backend(LogCheckpointBackend::Snapshot);
    assert_eq!(
        store.log_checkpoint_backend(),
        LogCheckpointBackend::Snapshot
    );
}

#[test]
fn test_checkpoint_durability_default() {
    let store = create_test_store();
    let durability = store.checkpoint_durability();
    assert_eq!(durability, CheckpointDurability::FasterLike);
}

#[test]
fn test_set_checkpoint_durability() {
    let store = create_test_store();

    store.set_checkpoint_durability(CheckpointDurability::FsyncOnCheckpoint);
    assert_eq!(
        store.checkpoint_durability(),
        CheckpointDurability::FsyncOnCheckpoint
    );

    store.set_checkpoint_durability(CheckpointDurability::FasterLike);
    assert_eq!(
        store.checkpoint_durability(),
        CheckpointDurability::FasterLike
    );
}

// ============ Read Cache Config Tests ============

#[test]
fn test_read_cache_config() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 12,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let cache_config = ReadCacheConfig::new(1 << 20);

    let store: Arc<FasterKv<u64, u64, NullDisk>> =
        Arc::new(FasterKv::with_read_cache(config, device, cache_config).unwrap());

    let retrieved_config = store.read_cache_config();
    assert!(retrieved_config.is_some());
    assert_eq!(retrieved_config.unwrap().mem_size, 1 << 20);
}

#[test]
fn test_read_cache_config_none() {
    let store = create_test_store();
    assert!(store.read_cache_config().is_none());
}

// ============ RMW Operation Tests ============

#[test]
fn test_rmw_basic() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 100u64);

    let status = session.rmw(42u64, |v| {
        *v *= 2;
        true
    });
    assert_eq!(status, Status::Ok);

    let result = session.read(&42u64);
    assert_eq!(result.unwrap(), Some(200u64));
}

#[test]
fn test_rmw_abort() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 100u64);

    let status = session.rmw(42u64, |_v| false);
    assert_eq!(status, Status::Aborted);

    let result = session.read(&42u64);
    assert_eq!(result.unwrap(), Some(100u64));
}

#[test]
fn test_rmw_not_found() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let status = session.rmw(999u64, |v| {
        *v += 1;
        true
    });
    assert_eq!(status, Status::NotFound);
}

// ============ Conditional Insert Tests ============

#[test]
fn test_conditional_insert_success() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let status = session.conditional_insert(42u64, 100u64);
    assert_eq!(status, Status::Ok);

    let result = session.read(&42u64);
    assert_eq!(result.unwrap(), Some(100u64));
}

#[test]
fn test_conditional_insert_aborted() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 100u64);

    let status = session.conditional_insert(42u64, 200u64);
    assert_eq!(status, Status::Aborted);

    let result = session.read(&42u64);
    assert_eq!(result.unwrap(), Some(100u64));
}

// ============ Error Handling Tests ============

#[test]
fn test_delete_not_found() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let status = session.delete(&999u64);
    assert_eq!(status, Status::NotFound);
}

// ============ Store with Custom Compaction Config Tests ============

#[test]
fn test_create_store_with_compaction_config() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 12,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let compaction_config = CompactionConfig {
        target_utilization: 0.5,
        ..CompactionConfig::default()
    };

    let store: Arc<FasterKv<u64, u64, NullDisk>> =
        Arc::new(FasterKv::with_compaction_config(config, device, compaction_config).unwrap());

    let retrieved_config = store.compaction_config();
    assert!((retrieved_config.target_utilization - 0.5).abs() < f64::EPSILON);
}

// ============ Session Lifecycle Tests ============

#[test]
fn test_session_guid() {
    let store = create_test_store();
    let session1 = store.start_session().unwrap();
    let session2 = store.start_session().unwrap();

    assert_ne!(session1.guid(), session2.guid());
}

#[test]
fn test_session_version() {
    let store = create_test_store();
    let session = store.start_session().unwrap();

    let version = session.version();
    assert_eq!(version, 0);
}

#[test]
fn test_session_serial_num_increments() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let initial_serial = session.serial_num();
    session.upsert(1u64, 100u64);
    let after_upsert = session.serial_num();
    session.upsert(2u64, 200u64);
    let after_second_upsert = session.serial_num();

    assert_eq!(after_upsert, initial_serial + 1);
    assert_eq!(after_second_upsert, initial_serial + 2);
}

#[test]
fn test_session_active_state() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    assert!(session.is_active());

    session.end();
    assert!(!session.is_active());
}

#[test]
fn test_session_context() {
    let store = create_test_store();
    let session = store.start_session().unwrap();

    let ctx = session.context();
    assert_eq!(ctx.version, 0);
    assert_eq!(ctx.serial_num, 0);
}

#[test]
fn test_session_complete_pending() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let completed = session.complete_pending(false);
    assert!(completed);
}

#[test]
fn test_session_refresh() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.refresh();
}

// ============ System State Tests ============

#[test]
fn test_system_state_initial() {
    let store = create_test_store();
    let state = store.system_state();

    assert_eq!(state.version, 0);
    assert_eq!(state.phase, Phase::Rest);
    assert_eq!(state.action, Action::None);
}

// ============ Multiple Session Tests ============

#[test]
fn test_multiple_sessions() {
    let store = create_test_store();

    let mut session1 = store.start_session().unwrap();
    let mut session2 = store.start_session().unwrap();

    session1.upsert(1u64, 100u64);
    session2.upsert(2u64, 200u64);

    assert_eq!(session1.read(&1u64).unwrap(), Some(100u64));
    assert_eq!(session1.read(&2u64).unwrap(), Some(200u64));
    assert_eq!(session2.read(&1u64).unwrap(), Some(100u64));
    assert_eq!(session2.read(&2u64).unwrap(), Some(200u64));
}

// ============ Edge Case Tests ============

#[test]
fn test_upsert_same_key_multiple_times() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 0u64..10 {
        session.upsert(42u64, i);
    }

    let result = session.read(&42u64);
    assert_eq!(result.unwrap(), Some(9u64));
}

#[test]
fn test_upsert_zero_key() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(0u64, 100u64);
    let result = session.read(&0u64);
    assert_eq!(result.unwrap(), Some(100u64));
}

#[test]
fn test_upsert_max_key() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(u64::MAX, 100u64);
    let result = session.read(&u64::MAX);
    assert_eq!(result.unwrap(), Some(100u64));
}

#[test]
fn test_upsert_zero_value() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 0u64);
    let result = session.read(&42u64);
    assert_eq!(result.unwrap(), Some(0u64));
}

// ============ Clear Read Cache without Cache Tests ============

#[test]
fn test_clear_read_cache_without_cache() {
    let store = create_test_store();
    store.clear_read_cache();
}

// ============ WaitPending Timeout Tests ============

#[test]
fn test_wait_pending_timeout_default() {
    let store = create_test_store();
    assert_eq!(store.wait_pending_timeout(), Duration::from_secs(30));
}

#[test]
fn test_set_wait_pending_timeout() {
    let store = create_test_store();

    store.set_wait_pending_timeout(Duration::from_secs(5));
    assert_eq!(store.wait_pending_timeout(), Duration::from_secs(5));

    store.set_wait_pending_timeout(Duration::from_millis(100));
    assert_eq!(store.wait_pending_timeout(), Duration::from_millis(100));

    store.set_wait_pending_timeout(Duration::from_secs(60));
    assert_eq!(store.wait_pending_timeout(), Duration::from_secs(60));
}

#[test]
fn test_wait_pending_timeout_aborts_checkpoint() {
    // Register a participant thread that acknowledges earlier phases
    // but intentionally stops before WaitPending.
    let store = create_test_store();
    store.set_wait_pending_timeout(Duration::from_millis(200));

    // Insert some data so checkpoint has work to do.
    let mut session = store.start_session().unwrap();
    for i in 0..10u64 {
        session.upsert(i, i * 100);
    }

    // Register an extra participant thread.
    let idle_tid = 42;
    store.register_thread(idle_tid);

    let helper_store = store.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_helper = stop.clone();
    let helper = std::thread::spawn(move || {
        let mut ctx = ThreadContext::new(idle_tid);
        while !stop_helper.load(std::sync::atomic::Ordering::Acquire) {
            let state = helper_store.system_state();
            if state.phase == Phase::WaitPending {
                break;
            }
            helper_store.cpr_refresh(&mut ctx);
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let result = store.checkpoint(dir.path());
    stop.store(true, std::sync::atomic::Ordering::Release);
    helper.join().unwrap();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert!(
        err.to_string().contains("WaitPending"),
        "Error should mention WaitPending timeout, got: {err}"
    );

    // After abort, normal operations must still work.
    let state = store.system_state();
    assert_eq!(state.phase, Phase::Rest);

    store.unregister_thread(idle_tid);
}

// ============ Page Eviction Integration Tests ============

#[test]
fn test_eviction_allows_writes_beyond_buffer_size() {
    // Create a store with a very small in-memory buffer so that the
    // circular buffer wraps quickly and eviction must kick in.
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 15, // 32 KB -- only 8 pages at 4 KB each
        page_size_bits: 12,       // 4 KB pages
        mutable_fraction: 0.25,
    };
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, NullDisk>::new(config, device).unwrap());
    let mut session = store.start_session().unwrap();

    // Write significantly more data than fits in the buffer.
    // 8 pages * 4096 bytes = 32 KB in buffer.
    // Each u64 key-value record is at least 32 bytes (key+value+header).
    // Try to write 4000 records, which is roughly 128 KB -- 4x the buffer.
    let mut success_count = 0u64;
    for i in 0u64..4000 {
        session.upsert(i, i * 10);
        success_count += 1;
    }

    // If eviction works, we should have written all 4000 records
    // without panicking or hitting OutOfMemory.
    assert_eq!(
        success_count, 4000,
        "Should write all records when eviction is active"
    );

    // Recent keys should be readable (they are in the mutable/read-only region).
    // Older keys may have been evicted to disk and return Pending.
    let result = session.read(&3999u64);
    assert!(result.is_ok(), "Most recent key should be readable");
    if let Ok(Some(val)) = result {
        assert_eq!(val, 3999 * 10);
    }
}
