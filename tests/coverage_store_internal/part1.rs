#[test]
fn test_compaction_compact_simple() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert data
    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    // Get begin address
    let begin_addr = store.log_begin_address();

    // Compact - should succeed (no actual data to compact with NullDisk)
    let status = store.compact(begin_addr);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_compaction_flush_and_shift_head() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert data
    for i in 0..50u64 {
        session.upsert(i, i * 100);
    }

    let head_addr = store.log_head_address();
    let status = store.flush_and_shift_head(head_addr);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_compaction_is_in_progress() {
    let store = create_store_with_compaction(0.5, 0);
    assert!(!store.is_compaction_in_progress());
}

#[test]
fn test_compaction_log_compact_empty() {
    let store = create_store_with_compaction(0.5, 0);

    // Compact empty log
    let result = store.log_compact();
    // Should succeed but do nothing
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compaction_log_compact_with_data() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert records
    for i in 0..200u64 {
        session.upsert(i, i * 10);
    }

    // Update some records to create obsolete versions
    for i in 0..100u64 {
        session.upsert(i, i * 100);
    }

    // Compact
    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compaction_log_compact_until() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert records
    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    let begin = store.log_begin_address();
    let target = Address::from_control(begin.control() + 1000);

    // Compact until target
    let result = store.log_compact_until(Some(target));
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compaction_compact_bytes() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert records
    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    // Compact by bytes
    let result = store.compact_bytes(1024);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compaction_log_utilization_empty() {
    let store = create_store_with_compaction(0.5, 0);
    let util = store.log_utilization();
    // Empty store should have 100% utilization
    assert_eq!(util, 1.0);
}

#[test]
fn test_compaction_log_utilization_with_data() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert records
    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    let util = store.log_utilization();
    // Should be between 0.0 and 1.0
    assert!((0.0..=1.0).contains(&util));
}

#[test]
fn test_compaction_should_compact() {
    let store = create_store_with_compaction(0.99, 0);
    // With high target utilization, should not need to compact initially
    let _should = store.should_compact();
}

#[test]
fn test_compaction_log_address_accessors() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }

    let begin = store.log_begin_address();
    let head = store.log_head_address();
    let tail = store.log_tail_address();
    let size = store.log_size_bytes();

    // Begin should be <= head <= tail
    assert!(begin.control() <= head.control());
    assert!(head.control() <= tail.control());
    assert_eq!(size, tail.control().saturating_sub(begin.control()));
}

#[test]
fn test_compaction_with_tombstones() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert records
    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    // Delete some records (creates tombstones)
    for i in (0..100u64).step_by(2) {
        session.delete(&i);
    }

    // Compact
    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compaction_with_updates_creating_versions() {
    let store = create_store_with_compaction(0.5, 0);
    let mut session = store.start_session().unwrap();

    // Insert records
    for i in 0..50u64 {
        session.upsert(i, i);
    }

    // Update many times to create multiple versions
    for _ in 0..5 {
        for i in 0..50u64 {
            session.upsert(i, i + 1000);
        }
    }

    // Compact
    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);
    // Verify data still readable
    for i in 0..50u64 {
        let val = session.read(&i).unwrap();
        assert!(val.is_some());
    }
}

#[test]
fn test_compaction_concurrent_compact_attempts() {
    let store = create_store_with_compaction(0.5, 0);

    // Fill with data
    {
        let mut session = store.start_session().unwrap();
        for i in 0..100u64 {
            session.upsert(i, i * 10);
        }
    }

    // Try to compact from multiple threads simultaneously
    let store1 = store.clone();
    let store2 = store.clone();

    let h1 = thread::spawn(move || store1.log_compact());
    let h2 = thread::spawn(move || store2.log_compact());

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    // At least one should succeed, other might be aborted
    assert!(r1.status == Status::Ok || r2.status == Status::Ok);
}

// ============ 2. Checkpoint Tests (checkpoint.rs) ============

#[test]
fn test_checkpoint_full() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..50u64 {
            session.upsert(i, i * 10);
        }
    }

    let token = store.checkpoint(temp_dir.path()).unwrap();
    assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
        temp_dir.path(),
        token
    ));
}

#[test]
fn test_checkpoint_index_only() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..30u64 {
            session.upsert(i, i * 10);
        }
    }

    let token = store.checkpoint_index(temp_dir.path()).unwrap();
    let kind = FasterKv::<u64, u64, NullDisk>::get_checkpoint_kind(temp_dir.path(), token);
    assert!(kind == CheckpointKind::IndexOnly || kind == CheckpointKind::Full);
}

#[test]
fn test_checkpoint_hybrid_log_only() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..30u64 {
            session.upsert(i, i * 10);
        }
    }

    let token = store.checkpoint_hybrid_log(temp_dir.path()).unwrap();
    let kind = FasterKv::<u64, u64, NullDisk>::get_checkpoint_kind(temp_dir.path(), token);
    assert!(kind == CheckpointKind::LogOnly || kind == CheckpointKind::Full);
}

#[test]
fn test_checkpoint_incremental_without_base() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..30u64 {
            session.upsert(i, i * 10);
        }
    }

    // Incremental without base should fall back to full
    let token = store.checkpoint_incremental(temp_dir.path()).unwrap();
    assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
        temp_dir.path(),
        token
    ));
}

#[test]
fn test_checkpoint_full_snapshot() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..30u64 {
            session.upsert(i, i * 10);
        }
    }

    let token = store.checkpoint_full_snapshot(temp_dir.path()).unwrap();
    assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
        temp_dir.path(),
        token
    ));
}

#[test]
fn test_checkpoint_incremental_after_full_snapshot() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // First, create a full snapshot
    {
        let mut session = store.start_session().unwrap();
        for i in 0..30u64 {
            session.upsert(i, i * 10);
        }
    }
    let _base_token = store.checkpoint_full_snapshot(temp_dir.path()).unwrap();

    // Add more data
    {
        let mut session = store.start_session().unwrap();
        for i in 30..60u64 {
            session.upsert(i, i * 10);
        }
    }

    // Now incremental should work
    let incr_token = store.checkpoint_incremental(temp_dir.path()).unwrap();
    assert!(FasterKv::<u64, u64, NullDisk>::any_checkpoint_exists(
        temp_dir.path(),
        incr_token
    ));
}

#[test]
fn test_checkpoint_with_callbacks() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..20u64 {
            session.upsert(i, i * 10);
        }
    }

    use std::sync::atomic::{AtomicBool, Ordering};
    let index_called = Arc::new(AtomicBool::new(false));
    let log_called = Arc::new(AtomicBool::new(false));

    let index_called_clone = index_called.clone();
    let log_called_clone = log_called.clone();

    let index_cb: Option<oxifaster::checkpoint::IndexPersistenceCallback> =
        Some(Box::new(move |_status| {
            index_called_clone.store(true, Ordering::SeqCst);
        }));
    let log_cb: Option<oxifaster::checkpoint::HybridLogPersistenceCallback> =
        Some(Box::new(move |_status, _serial| {
            log_called_clone.store(true, Ordering::SeqCst);
        }));

    let _token = store
        .checkpoint_with_callbacks(temp_dir.path(), index_cb, log_cb)
        .unwrap();

    assert!(index_called.load(Ordering::SeqCst));
    assert!(log_called.load(Ordering::SeqCst));
}

#[test]
fn test_checkpoint_get_info() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..20u64 {
            session.upsert(i, i * 10);
        }
    }

    let token = store.checkpoint(temp_dir.path()).unwrap();
    let (index_meta, log_meta) =
        FasterKv::<u64, u64, NullDisk>::get_checkpoint_info(temp_dir.path(), token).unwrap();

    assert_eq!(index_meta.token, token);
    assert_eq!(log_meta.token, token);
}

#[test]
fn test_checkpoint_list_by_kind() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..10u64 {
            session.upsert(i, i * 10);
        }
    }

    let _token1 = store.checkpoint(temp_dir.path()).unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 10..20u64 {
            session.upsert(i, i * 10);
        }
    }

    let _token2 = store.checkpoint(temp_dir.path()).unwrap();

    let full_list = FasterKv::<u64, u64, NullDisk>::list_checkpoints_by_kind(
        temp_dir.path(),
        Some(CheckpointKind::Full),
    )
    .unwrap();
    assert_eq!(full_list.len(), 2);

    let all_list =
        FasterKv::<u64, u64, NullDisk>::list_checkpoints_by_kind(temp_dir.path(), None).unwrap();
    assert_eq!(all_list.len(), 2);
}

#[test]
fn test_checkpoint_concurrent_checkpoint_blocked() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..100u64 {
            session.upsert(i, i * 10);
        }
    }

    let store1 = store.clone();
    let store2 = store.clone();
    let path1 = temp_dir.path().to_path_buf();
    let path2 = temp_dir.path().to_path_buf();

    let h1 = thread::spawn(move || store1.checkpoint(&path1));
    let h2 = thread::spawn(move || store2.checkpoint(&path2));

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    // At least one should succeed
    assert!(r1.is_ok() || r2.is_ok());
}

#[test]
fn test_checkpoint_kind_detection() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session().unwrap();
        session.upsert(1u64, 100u64);
    }

    let token = store.checkpoint(temp_dir.path()).unwrap();
    let kind = FasterKv::<u64, u64, NullDisk>::get_checkpoint_kind(temp_dir.path(), token);
    assert_eq!(kind, CheckpointKind::Full);

    // Non-existent token
    let fake_token = Uuid::new_v4();
    let kind = FasterKv::<u64, u64, NullDisk>::get_checkpoint_kind(temp_dir.path(), fake_token);
    assert_eq!(kind, CheckpointKind::None);
}

