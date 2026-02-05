// ============ 4. State Transitions ============

#[test]
fn test_system_state_rest() {
    let state = SystemState::rest(0);
    assert_eq!(state.action, Action::None);
    assert_eq!(state.phase, Phase::Rest);
    assert_eq!(state.version, 0);
    assert!(state.is_rest());
    assert!(!state.is_action_in_progress());
    assert!(!state.is_checkpoint());
}

#[test]
fn test_system_state_rest_with_version() {
    let state = SystemState::rest(42);
    assert_eq!(state.version, 42);
    assert!(state.is_rest());
}

#[test]
fn test_system_state_new() {
    let state = SystemState::new(Action::CheckpointFull, Phase::InProgress, 5);
    assert_eq!(state.action, Action::CheckpointFull);
    assert_eq!(state.phase, Phase::InProgress);
    assert_eq!(state.version, 5);
    assert!(!state.is_rest());
    assert!(state.is_action_in_progress());
    assert!(state.is_checkpoint());
}

#[test]
fn test_system_state_is_checkpoint_variants() {
    assert!(SystemState::new(Action::CheckpointFull, Phase::Rest, 0).is_checkpoint());
    assert!(SystemState::new(Action::CheckpointIndex, Phase::Rest, 0).is_checkpoint());
    assert!(SystemState::new(Action::CheckpointHybridLog, Phase::Rest, 0).is_checkpoint());
    assert!(SystemState::new(Action::CheckpointIncremental, Phase::Rest, 0).is_checkpoint());
    assert!(!SystemState::new(Action::None, Phase::Rest, 0).is_checkpoint());
    assert!(!SystemState::new(Action::GC, Phase::Rest, 0).is_checkpoint());
    assert!(!SystemState::new(Action::GrowIndex, Phase::Rest, 0).is_checkpoint());
    assert!(!SystemState::new(Action::Recover, Phase::Rest, 0).is_checkpoint());
}

#[test]
fn test_system_state_is_incremental_checkpoint() {
    assert!(
        SystemState::new(Action::CheckpointIncremental, Phase::Rest, 0).is_incremental_checkpoint()
    );
    assert!(!SystemState::new(Action::CheckpointFull, Phase::Rest, 0).is_incremental_checkpoint());
    assert!(!SystemState::new(Action::None, Phase::Rest, 0).is_incremental_checkpoint());
}

#[test]
fn test_system_state_packing_roundtrip() {
    let states = [
        SystemState::new(Action::CheckpointFull, Phase::InProgress, 42),
        SystemState::rest(0),
        SystemState::new(Action::GC, Phase::GcInProgress, 100),
        SystemState::new(Action::GrowIndex, Phase::GrowPrepare, u32::MAX >> 1),
        SystemState::new(Action::CheckpointIncremental, Phase::WaitFlush, 7),
    ];
    for state in &states {
        let control = state.to_control();
        let unpacked = SystemState::from_control(control);
        assert_eq!(*state, unpacked);
    }
}

#[test]
fn test_system_state_default() {
    let state = SystemState::default();
    assert_eq!(state.action, Action::None);
    assert_eq!(state.phase, Phase::Rest);
    assert_eq!(state.version, 0);
}

#[test]
fn test_system_state_get_next_state_none_action() {
    let state = SystemState::new(Action::None, Phase::Rest, 0);
    let result = state.get_next_state();
    assert!(result.is_err());
}

#[test]
fn test_system_state_get_next_state_recover() {
    let state = SystemState::new(Action::Recover, Phase::Rest, 0);
    let result = state.get_next_state();
    assert!(result.is_err());
}

#[test]
fn test_system_state_full_checkpoint_transitions() {
    let mut state = SystemState::new(Action::CheckpointFull, Phase::Rest, 0);
    let expected_phases = [
        Phase::PrepIndexChkpt,
        Phase::IndexChkpt,
        Phase::Prepare,
        Phase::InProgress,
        Phase::WaitPending,
        Phase::WaitFlush,
        Phase::PersistenceCallback,
        Phase::Rest,
    ];
    for expected in &expected_phases {
        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, *expected);
    }
    assert_eq!(state.version, 1); // version incremented in Prepare -> InProgress
}

#[test]
fn test_system_state_index_checkpoint_transitions() {
    let mut state = SystemState::new(Action::CheckpointIndex, Phase::Rest, 5);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::PrepIndexChkpt);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::IndexChkpt);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::Rest);
    assert_eq!(state.version, 5); // no version change for index only
}

#[test]
fn test_system_state_hybridlog_checkpoint_transitions() {
    let mut state = SystemState::new(Action::CheckpointHybridLog, Phase::Rest, 0);
    let expected_phases = [
        Phase::Prepare,
        Phase::InProgress,
        Phase::WaitPending,
        Phase::WaitFlush,
        Phase::PersistenceCallback,
        Phase::Rest,
    ];
    for expected in &expected_phases {
        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, *expected);
    }
    assert_eq!(state.version, 1);
}

#[test]
fn test_system_state_gc_transitions() {
    let mut state = SystemState::new(Action::GC, Phase::Rest, 0);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::GcIoPending);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::GcInProgress);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::Rest);
}

#[test]
fn test_system_state_grow_index_transitions() {
    let mut state = SystemState::new(Action::GrowIndex, Phase::Rest, 0);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::GrowPrepare);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::GrowInProgress);
    state = state.get_next_state().unwrap();
    assert_eq!(state.phase, Phase::Rest);
}

#[test]
fn test_system_state_incremental_checkpoint_transitions() {
    let mut state = SystemState::new(Action::CheckpointIncremental, Phase::Rest, 0);
    let expected_phases = [
        Phase::PrepIndexChkpt,
        Phase::IndexChkpt,
        Phase::Prepare,
        Phase::InProgress,
        Phase::WaitPending,
        Phase::WaitFlush,
        Phase::PersistenceCallback,
        Phase::Rest,
    ];
    for expected in &expected_phases {
        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, *expected);
    }
    assert_eq!(state.version, 1);
}

#[test]
fn test_system_state_invalid_transition() {
    // GC action with PrepIndexChkpt phase should fail
    let state = SystemState::new(Action::GC, Phase::PrepIndexChkpt, 0);
    let result = state.get_next_state();
    assert!(result.is_err());
}

// ============ 4b. Action and Phase conversions ============

#[test]
fn test_action_from_u8() {
    assert_eq!(Action::from(0u8), Action::None);
    assert_eq!(Action::from(1u8), Action::CheckpointFull);
    assert_eq!(Action::from(2u8), Action::CheckpointIndex);
    assert_eq!(Action::from(3u8), Action::CheckpointHybridLog);
    assert_eq!(Action::from(4u8), Action::Recover);
    assert_eq!(Action::from(5u8), Action::GC);
    assert_eq!(Action::from(6u8), Action::GrowIndex);
    assert_eq!(Action::from(7u8), Action::CheckpointIncremental);
    assert_eq!(Action::from(255u8), Action::None); // unknown -> None
}

#[test]
fn test_phase_from_u8() {
    assert_eq!(Phase::from(0u8), Phase::PrepIndexChkpt);
    assert_eq!(Phase::from(1u8), Phase::IndexChkpt);
    assert_eq!(Phase::from(2u8), Phase::Prepare);
    assert_eq!(Phase::from(3u8), Phase::InProgress);
    assert_eq!(Phase::from(4u8), Phase::WaitPending);
    assert_eq!(Phase::from(5u8), Phase::WaitFlush);
    assert_eq!(Phase::from(6u8), Phase::Rest);
    assert_eq!(Phase::from(7u8), Phase::PersistenceCallback);
    assert_eq!(Phase::from(8u8), Phase::GcIoPending);
    assert_eq!(Phase::from(9u8), Phase::GcInProgress);
    assert_eq!(Phase::from(10u8), Phase::GrowPrepare);
    assert_eq!(Phase::from(11u8), Phase::GrowInProgress);
    assert_eq!(Phase::from(200u8), Phase::Invalid);
}

// ============ 4c. AtomicSystemState ============

#[test]
fn test_atomic_system_state_new() {
    let atomic = AtomicSystemState::new(SystemState::rest(10));
    let loaded = atomic.load(Ordering::SeqCst);
    assert_eq!(loaded.action, Action::None);
    assert_eq!(loaded.phase, Phase::Rest);
    assert_eq!(loaded.version, 10);
}

#[test]
fn test_atomic_system_state_default() {
    let atomic = AtomicSystemState::default();
    assert_eq!(atomic.phase(), Phase::Rest);
    assert_eq!(atomic.version(), 0);
    assert_eq!(atomic.action(), Action::None);
}

#[test]
fn test_atomic_system_state_store_load() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    let new_state = SystemState::new(Action::GC, Phase::GcInProgress, 5);
    atomic.store(new_state, Ordering::SeqCst);

    let loaded = atomic.load(Ordering::SeqCst);
    assert_eq!(loaded, new_state);
}

#[test]
fn test_atomic_system_state_compare_exchange_success() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    let expected = SystemState::rest(0);
    let desired = SystemState::new(Action::GC, Phase::GcIoPending, 0);

    let result = atomic.compare_exchange(expected, desired, Ordering::AcqRel, Ordering::Acquire);
    assert!(result.is_ok());
    assert_eq!(atomic.load(Ordering::SeqCst), desired);
}

#[test]
fn test_atomic_system_state_compare_exchange_failure() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    let wrong_expected = SystemState::rest(5);
    let desired = SystemState::new(Action::GC, Phase::GcIoPending, 0);

    let result =
        atomic.compare_exchange(wrong_expected, desired, Ordering::AcqRel, Ordering::Acquire);
    assert!(result.is_err());
}

#[test]
fn test_atomic_system_state_compare_exchange_weak() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    let expected = SystemState::rest(0);
    let desired = SystemState::new(Action::GC, Phase::GcIoPending, 0);

    // Weak CAS may spuriously fail, so retry in a loop
    let mut succeeded = false;
    for _ in 0..10 {
        if atomic
            .compare_exchange_weak(expected, desired, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            succeeded = true;
            break;
        }
    }
    assert!(succeeded);
}

#[test]
fn test_atomic_system_state_phase_version_action() {
    let state = SystemState::new(Action::CheckpointFull, Phase::InProgress, 42);
    let atomic = AtomicSystemState::new(state);
    assert_eq!(atomic.phase(), Phase::InProgress);
    assert_eq!(atomic.version(), 42);
    assert_eq!(atomic.action(), Action::CheckpointFull);
}

#[test]
fn test_atomic_system_state_try_start_action() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    let result = atomic.try_start_action(Action::CheckpointFull);
    assert!(result.is_ok());

    // Should fail because action is now in progress
    let result = atomic.try_start_action(Action::GC);
    assert!(result.is_err());
}

#[test]
fn test_atomic_system_state_try_start_action_invalid() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    // None action should fail (get_next_state returns error for Action::None)
    let result = atomic.try_start_action(Action::None);
    assert!(result.is_err());
}

#[test]
fn test_atomic_system_state_try_advance() {
    let atomic = AtomicSystemState::new(SystemState::rest(0));
    // Start an action
    let _ = atomic.try_start_action(Action::CheckpointFull);

    // Advance through the state machine
    let result = atomic.try_advance();
    assert!(result.is_ok());
}

#[test]
fn test_atomic_system_state_clone() {
    let state = SystemState::new(Action::GC, Phase::GcInProgress, 7);
    let atomic = AtomicSystemState::new(state);
    let cloned = atomic.clone();
    assert_eq!(cloned.load(Ordering::SeqCst), state);
}

// ============ 4d. ThreadContext ============

#[test]
fn test_thread_context_new() {
    let ctx = ThreadContext::new(5);
    assert_eq!(ctx.thread_id, 5);
    assert_eq!(ctx.version, 0);
    assert_eq!(ctx.serial_num, 0);
    assert_eq!(ctx.pending_count, 0);
    assert_eq!(ctx.retry_count, 0);
}

#[test]
fn test_thread_context_serial_num() {
    let mut ctx = ThreadContext::new(0);
    assert_eq!(ctx.serial_num(), 0);

    let new_serial = ctx.increment_serial();
    assert_eq!(new_serial, 1);
    assert_eq!(ctx.serial_num(), 1);

    ctx.set_serial_num(100);
    assert_eq!(ctx.serial_num(), 100);
}

#[test]
fn test_thread_context_clone() {
    let ctx = ThreadContext::new(3);
    let cloned = ctx.clone();
    assert_eq!(cloned.thread_id, 3);
    assert_eq!(cloned.version, 0);
}

// ============ Misc coverage: store with read_cache, checkpoint backend ============

#[test]
fn test_store_with_read_cache() {
    use oxifaster::cache::ReadCacheConfig;

    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let cache_config = ReadCacheConfig::new(1 << 18);
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::with_read_cache(
        config,
        NullDisk::new(),
        cache_config,
    ));

    assert!(store.has_read_cache());
    assert!(store.read_cache_stats().is_some());
    assert!(store.read_cache_config().is_some());

    let mut session = store.start_session().unwrap();
    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }
    for i in 0..50u64 {
        let result = session.read(&i).unwrap();
        assert_eq!(result, Some(i * 10));
    }

    store.clear_read_cache();
}

#[test]
fn test_store_checkpoint_backend_accessors() {
    use oxifaster::store::{CheckpointDurability, LogCheckpointBackend};

    let store = create_test_store();

    let backend = store.log_checkpoint_backend();
    assert_eq!(backend, LogCheckpointBackend::Snapshot);

    store.set_log_checkpoint_backend(LogCheckpointBackend::FoldOver);
    assert_eq!(
        store.log_checkpoint_backend(),
        LogCheckpointBackend::FoldOver
    );

    let durability = store.checkpoint_durability();
    assert_eq!(durability, CheckpointDurability::FasterLike);

    store.set_checkpoint_durability(CheckpointDurability::FsyncOnCheckpoint);
    assert_eq!(
        store.checkpoint_durability(),
        CheckpointDurability::FsyncOnCheckpoint
    );
}

#[test]
fn test_session_pin() {
    let store = create_test_store();
    let session = store.start_session().unwrap();
    let _guard = session.pin();
}

#[test]
fn test_session_upsert_overwrite_batch() {
    let store = create_large_store();
    let mut session = store.start_session().unwrap();

    // Insert 100 keys
    for i in 0..100u64 {
        session.upsert(i, i);
    }

    // Overwrite all with new values
    for i in 0..100u64 {
        session.upsert(i, i + 1000);
    }

    // Verify all have the new values
    for i in 0..100u64 {
        assert_eq!(session.read(&i).unwrap(), Some(i + 1000));
    }
}

#[test]
fn test_session_rmw_multiple() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 0u64);

    // Apply RMW 10 times
    for _ in 0..10 {
        let status = session.rmw(1u64, |v| {
            *v += 1;
            true
        });
        assert_eq!(status, Status::Ok);
    }

    assert_eq!(session.read(&1u64).unwrap(), Some(10));
}

#[test]
fn test_session_delete_nonexistent() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    // Deleting nonexistent key should return NotFound
    let status = session.delete(&999u64);
    assert_eq!(status, Status::NotFound);
}

#[test]
fn test_session_read_after_rmw_and_delete() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    session.rmw(1u64, |v| {
        *v += 50;
        true
    });
    assert_eq!(session.read(&1u64).unwrap(), Some(150));

    session.delete(&1u64);
    assert_eq!(session.read(&1u64).unwrap(), None);
}

#[test]
fn test_session_complete_pending_with_timeout() {
    use std::time::Duration;

    let store = create_test_store();
    let mut session = store.start_session().unwrap();
    let result = session.complete_pending_with_timeout(false, Duration::from_millis(100));
    assert!(result);
}

#[test]
fn test_compaction_after_overwrite() {
    let store = create_store_with_compaction();
    let mut session = store.start_session().unwrap();

    // Insert and then overwrite to create garbage
    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }
    for i in 0..50u64 {
        session.upsert(i, i * 20);
    }
    // Delete some keys to create tombstones
    for i in 0..10u64 {
        session.delete(&i);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    // Verify data integrity after compaction (remaining keys still readable)
    for i in 10..50u64 {
        assert_eq!(session.read(&i).unwrap(), Some(i * 20));
    }
}

#[test]
fn test_log_addresses_ordering() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i);
    }

    let begin = store.log_begin_address();
    let head = store.log_head_address();
    let tail = store.log_tail_address();

    // begin <= head <= tail
    assert!(begin.control() <= head.control());
    assert!(head.control() <= tail.control());
}

#[test]
fn test_session_interleaved_operations() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    // Interleave operations
    session.upsert(1u64, 100u64);
    session.upsert(2u64, 200u64);
    assert_eq!(session.read(&1u64).unwrap(), Some(100));
    session.delete(&1u64);
    assert_eq!(session.read(&1u64).unwrap(), None);

    // conditional_insert on a fresh key
    let status = session.conditional_insert(3u64, 300u64);
    assert_eq!(status, Status::Ok);
    assert_eq!(session.read(&3u64).unwrap(), Some(300));

    session.rmw(2u64, |v| {
        *v *= 2;
        true
    });
    assert_eq!(session.read(&2u64).unwrap(), Some(400));
}
