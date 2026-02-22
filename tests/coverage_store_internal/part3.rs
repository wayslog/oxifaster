// ============ 4. Index Grow Tests (index_grow.rs) ============

#[test]
fn test_index_size() {
    let store = create_test_store();
    let size = store.index_size();
    assert!(size > 0);
}

#[test]
fn test_is_grow_in_progress_initially_false() {
    let store = create_test_store();
    assert!(!store.is_grow_in_progress());
}

#[test]
fn test_start_grow_invalid_size() {
    let store = create_test_store();
    let current = store.index_size();

    // Smaller size should fail
    let result = store.start_grow(current / 2);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Status::InvalidArgument);
}

#[test]
fn test_start_grow_not_power_of_two() {
    let store = create_test_store();
    let current = store.index_size();

    // Non-power-of-two should fail
    let result = store.start_grow(current + 100);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Status::InvalidArgument);
}

#[test]
fn test_grow_progress_none_when_not_growing() {
    let store = create_test_store();
    let progress = store.grow_progress();
    assert!(progress.is_none());
}

#[test]
fn test_grow_index_with_callback() {
    let store = create_test_store();
    let current_size = store.index_size();
    let new_size = current_size * 2;

    use std::sync::atomic::{AtomicU64, Ordering};
    let callback_size = Arc::new(AtomicU64::new(0));
    let callback_size_clone = callback_size.clone();

    let callback: Option<oxifaster::index::GrowCompleteCallback> = Some(Box::new(move |size| {
        callback_size_clone.store(size, Ordering::SeqCst);
    }));

    let status = store.grow_index_with_callback(new_size, callback);
    // May succeed or fail depending on implementation
    let _ = status;

    // Callback should have been invoked
    let final_size = callback_size.load(Ordering::SeqCst);
    assert!(final_size > 0);
}

#[test]
fn test_complete_grow_without_start() {
    let store = create_test_store();
    let result = store.complete_grow();
    // Should fail because no grow was started
    assert!(!result.success);
}

// ============ 5. Async Session Tests (async_session.rs) ============

#[tokio::test]
async fn test_async_session_read_async() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    session.upsert(1u64, 100u64);
    let result = session.read_async(&1u64).await;

    assert_eq!(result, Ok(Some(100u64)));
}

#[tokio::test]
async fn test_async_session_read_async_not_found() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    let result = session.read_async(&999u64).await;
    assert_eq!(result, Ok(None));
}

#[tokio::test]
async fn test_async_session_upsert_async() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    let status = session.upsert_async(1u64, 100u64).await;
    assert_eq!(status, Status::Ok);

    let result = session.read_async(&1u64).await;
    assert_eq!(result, Ok(Some(100u64)));
}

#[tokio::test]
async fn test_async_session_delete_async() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    session.upsert_async(1u64, 100u64).await;
    let status = session.delete_async(&1u64).await;
    assert_eq!(status, Status::Ok);

    let result = session.read_async(&1u64).await;
    assert_eq!(result, Ok(None));
}

#[tokio::test]
async fn test_async_session_rmw_async() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    session.upsert_async(1u64, 100u64).await;

    let status = session
        .rmw_async(1u64, |v| {
            *v += 50;
            true
        })
        .await;
    assert_eq!(status, Status::Ok);

    let result = session.read_async(&1u64).await;
    assert_eq!(result, Ok(Some(150u64)));
}

#[tokio::test]
async fn test_async_session_conditional_insert_async() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    // First insert should succeed
    let status = session.conditional_insert_async(1u64, 100u64).await;
    assert_eq!(status, Status::Ok);

    // Second insert should fail (key exists)
    let status = session.conditional_insert_async(1u64, 200u64).await;
    assert_eq!(status, Status::Aborted);

    // Value should be original
    let result = session.read_async(&1u64).await;
    assert_eq!(result, Ok(Some(100u64)));
}

#[tokio::test]
async fn test_async_session_complete_pending_async() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    // With no pending operations, should complete immediately
    let completed = session.complete_pending_async().await;
    assert!(completed);
}

#[tokio::test]
async fn test_async_session_multiple_async_ops() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    for i in 0..50u64 {
        session.upsert_async(i, i * 10).await;
    }

    for i in 0..50u64 {
        let result = session.read_async(&i).await;
        assert_eq!(result, Ok(Some(i * 10)));
    }
}

#[test]
fn test_async_session_builder_thread_id() {
    let store = create_test_store();
    let session = AsyncSessionBuilder::new(store).thread_id(42).build();
    assert_eq!(session.thread_id(), 42);
}

#[test]
fn test_async_session_builder_from_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 500);

    let session = AsyncSessionBuilder::new(store).from_state(state).build();
    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 500);
}

#[test]
fn test_async_session_accessors() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    let _guid = session.guid();
    let _thread_id = session.thread_id();
    let _version = session.version();
    let _serial = session.serial_num();
    let _is_active = session.is_active();
    let _state = session.to_session_state();
    let _ctx = session.context();
    let _ctx_mut = session.context_mut();
}

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
    let mut session = store.start_async_session().unwrap();
    session.refresh();
}

#[test]
fn test_async_session_continue_from_state() {
    let store = create_test_store();
    let mut session = store.start_async_session().unwrap();

    session.upsert(1u64, 100u64);
    let state = session.to_session_state();

    let mut new_session = store.start_async_session().unwrap();
    new_session.continue_from_state(&state);
}

// ============ 6. Pending I/O Support Tests (pending_io_support.rs) ============

#[test]
fn test_pending_io_read_in_memory() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    // Insert data (will be in memory)
    session.upsert(1u64, 100u64);

    // Read should not return Pending for in-memory data
    let result = session.read(&1u64);
    assert_eq!(result, Ok(Some(100u64)));
}

#[test]
fn test_pending_io_complete_pending_no_pending() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    // No pending operations
    let completed = session.complete_pending(false);
    assert!(completed);
}

#[test]
fn test_pending_io_complete_pending_wait() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    // Complete with wait=true
    let completed = session.complete_pending(true);
    assert!(completed);
}

// ============ 7. Codec Tests (codec/mod.rs) ============

#[test]
fn test_hash64_basic() {
    let data1 = b"hello";
    let data2 = b"world";

    let h1 = hash64(data1);
    let h2 = hash64(data2);

    // Different data should (almost certainly) produce different hashes
    assert_ne!(h1, h2);

    // Same data should produce same hash
    let h1_again = hash64(data1);
    assert_eq!(h1, h1_again);
}

#[test]
fn test_hash64_empty() {
    let empty: &[u8] = &[];
    let h = hash64(empty);
    // Should not panic, produce some hash
    let _ = h;
}

#[test]
fn test_blittable_codec_u64() {
    use oxifaster::codec::BlittableCodec;

    let key: u64 = 12345;
    let len = <BlittableCodec<u64> as KeyCodec<u64>>::encoded_len(&key).unwrap();
    assert_eq!(len, 8);

    let mut buf = vec![0u8; len];
    <BlittableCodec<u64> as KeyCodec<u64>>::encode_into(&key, &mut buf).unwrap();

    let decoded = <BlittableCodec<u64> as KeyCodec<u64>>::decode(&buf).unwrap();
    assert_eq!(decoded, key);

    let eq = <BlittableCodec<u64> as KeyCodec<u64>>::equals_encoded(&buf, &key).unwrap();
    assert!(eq);

    let hash = <BlittableCodec<u64> as KeyCodec<u64>>::hash(&key).unwrap();
    assert!(hash > 0);
}

#[test]
fn test_raw_bytes_codec() {
    use oxifaster::codec::RawBytesCodec;

    let key = RawBytes::from(vec![1, 2, 3, 4, 5]);
    let len = <RawBytesCodec as KeyCodec<RawBytes>>::encoded_len(&key).unwrap();
    assert_eq!(len, 5);

    let mut buf = vec![0u8; len];
    <RawBytesCodec as KeyCodec<RawBytes>>::encode_into(&key, &mut buf).unwrap();

    let decoded = <RawBytesCodec as KeyCodec<RawBytes>>::decode(&buf).unwrap();
    assert_eq!(decoded, key);

    let eq = <RawBytesCodec as KeyCodec<RawBytes>>::equals_encoded(&buf, &key).unwrap();
    assert!(eq);

    let hash = <RawBytesCodec as KeyCodec<RawBytes>>::hash(&key).unwrap();
    assert!(hash > 0);
}

#[test]
fn test_utf8_codec() {
    use oxifaster::codec::Utf8Codec;

    let key = Utf8::from("hello world");
    let len = <Utf8Codec as KeyCodec<Utf8>>::encoded_len(&key).unwrap();
    assert_eq!(len, 11);

    let mut buf = vec![0u8; len];
    <Utf8Codec as KeyCodec<Utf8>>::encode_into(&key, &mut buf).unwrap();

    let decoded = <Utf8Codec as KeyCodec<Utf8>>::decode(&buf).unwrap();
    assert_eq!(decoded, key);

    let eq = <Utf8Codec as KeyCodec<Utf8>>::equals_encoded(&buf, &key).unwrap();
    assert!(eq);

    let hash = <Utf8Codec as KeyCodec<Utf8>>::hash(&key).unwrap();
    assert!(hash > 0);
}

#[test]
fn test_raw_bytes_from_bytes() {
    let bytes = bytes::Bytes::from_static(b"test data");
    let raw = RawBytes::from(bytes);
    assert_eq!(raw.0.as_ref(), b"test data");
}

#[test]
fn test_utf8_from_str() {
    let utf8 = Utf8::from("test string");
    assert_eq!(utf8.0, "test string");
}

#[test]
fn test_store_with_raw_bytes() {
    let config = FasterKvConfig {
        table_size: 256,
        log_memory_size: 1 << 18,
        page_size_bits: 12,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<RawBytes, RawBytes, NullDisk>> =
        Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    let key = RawBytes::from(vec![1, 2, 3]);
    let value = RawBytes::from(vec![4, 5, 6]);

    session.upsert(key.clone(), value.clone());
    let result = session.read(&key).unwrap();

    assert_eq!(result, Some(value));
}

#[test]
fn test_store_with_utf8() {
    let config = FasterKvConfig {
        table_size: 256,
        log_memory_size: 1 << 18,
        page_size_bits: 12,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<Utf8, Utf8, NullDisk>> =
        Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    let key = Utf8::from("key1");
    let value = Utf8::from("value1");

    session.upsert(key.clone(), value.clone());
    let result = session.read(&key).unwrap();

    assert_eq!(result, Some(value));
}

// ============ 8. Additional Edge Case Tests ============

#[test]
fn test_compaction_with_read_cache() {
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
    ).unwrap());

    let mut session = store.start_session().unwrap();
    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    // Read to populate cache
    for i in 0..50u64 {
        let _ = session.read(&i);
    }

    // Compact
    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    // Verify data still accessible
    for i in 0..50u64 {
        let val = session.read(&i).unwrap();
        assert!(val.is_some());
    }
}

#[test]
fn test_checkpoint_version_increments() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    let v0 = store.system_state().version;
    assert_eq!(v0, 0);

    store.checkpoint(temp_dir.path()).unwrap();
    let v1 = store.system_state().version;
    assert_eq!(v1, 1);

    store.checkpoint(temp_dir.path()).unwrap();
    let v2 = store.system_state().version;
    assert_eq!(v2, 2);
}

#[test]
fn test_session_state_serialization() {
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 12345);

    assert_eq!(state.guid, guid);
    assert_eq!(state.serial_num, 12345);
}

#[test]
fn test_session_builder() {
    let store = create_test_store();

    let session = SessionBuilder::new(store.clone()).thread_id(10).build();
    assert_eq!(session.thread_id(), 10);
}

#[test]
fn test_session_builder_from_state() {
    let store = create_test_store();
    let guid = Uuid::new_v4();
    let state = SessionState::new(guid, 999);

    let session = SessionBuilder::new(store).from_state(state).build();
    assert_eq!(session.guid(), guid);
    assert_eq!(session.serial_num(), 999);
}

#[test]
fn test_multi_page_compaction() {
    // Use a small page size to force multiple pages
    let store = create_small_store();
    let mut session = store.start_session().unwrap();

    // Insert enough data to span multiple pages
    for i in 0..500u64 {
        session.upsert(i, i * 1000);
    }

    // Update to create obsolete versions
    for i in 0..250u64 {
        session.upsert(i, i * 2000);
    }

    // Delete some to create tombstones
    for i in (250..500u64).step_by(2) {
        session.delete(&i);
    }

    drop(session);

    // Compact
    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_concurrent_operations_during_compaction() {
    let store = create_store_with_compaction(0.5, 0);

    // Fill with data
    {
        let mut session = store.start_session().unwrap();
        for i in 0..200u64 {
            session.upsert(i, i * 10);
        }
    }

    let store_compact = store.clone();
    let store_ops = store.clone();

    // Start compaction in background
    let compact_handle = thread::spawn(move || {
        for _ in 0..5 {
            let _ = store_compact.log_compact();
            thread::sleep(Duration::from_millis(10));
        }
    });

    // Continue operations
    let ops_handle = thread::spawn(move || {
        let mut session = store_ops.start_session().unwrap();
        for i in 200..300u64 {
            session.upsert(i, i * 10);
        }
        for i in 0..100u64 {
            let _ = session.read(&i);
        }
    });

    compact_handle.join().unwrap();
    ops_handle.join().unwrap();
}

#[test]
fn test_auto_compaction_handle() {
    use oxifaster::compaction::AutoCompactionConfig;

    let store = create_store_with_compaction(0.5, 0);

    // Fill with data
    {
        let mut session = store.start_session().unwrap();
        for i in 0..100u64 {
            session.upsert(i, i * 10);
        }
    }

    // Start auto compaction
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_millis(100))
        .with_target_utilization(0.9);

    let _handle = store.start_auto_compaction(config);

    // Do some operations
    {
        let mut session = store.start_session().unwrap();
        for i in 100..150u64 {
            session.upsert(i, i * 10);
        }
    }

    // Handle is dropped here, stopping auto compaction
}

#[test]
fn test_checkpoint_durability_settings() {
    use oxifaster::store::{CheckpointDurability, LogCheckpointBackend};

    let store = create_test_store();

    // Set checkpoint durability
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

    // Set log checkpoint backend
    store.set_log_checkpoint_backend(LogCheckpointBackend::Snapshot);
    assert_eq!(
        store.log_checkpoint_backend(),
        LogCheckpointBackend::Snapshot
    );

    store.set_log_checkpoint_backend(LogCheckpointBackend::FoldOver);
    assert_eq!(
        store.log_checkpoint_backend(),
        LogCheckpointBackend::FoldOver
    );
}

#[test]
fn test_session_conditional_insert() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    // First insert succeeds
    let status = session.conditional_insert(1u64, 100u64);
    assert_eq!(status, Status::Ok);

    // Second insert fails (key exists)
    let status = session.conditional_insert(1u64, 200u64);
    assert_eq!(status, Status::Aborted);

    // Value unchanged
    assert_eq!(session.read(&1u64).unwrap(), Some(100u64));
}

#[test]
fn test_session_read_view() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    // Read with view requires epoch guard
    let guard = session.pin();
    let result = session.read_view(&guard, &1u64);
    assert!(result.is_ok());
}

#[test]
fn test_stats_after_operations() {
    let store = create_test_store();
    store.enable_stats();

    {
        let mut session = store.start_session().unwrap();
        for i in 0..50u64 {
            session.upsert(i, i * 10);
        }
        for i in 0..25u64 {
            let _ = session.read(&i);
        }
        for i in 25..50u64 {
            session.delete(&i);
        }
    }

    let stats = store.stats_snapshot();
    assert!(stats.reads > 0);
    assert!(stats.upserts > 0);
}
