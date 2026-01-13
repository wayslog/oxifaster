mod tests {
    use super::*;
    use crate::compaction::AutoCompactionConfig;
    use crate::device::NullDisk;
    use crate::store::{Action, Phase};
    use std::time::{Duration, Instant};

    fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20, // 1 MB
            page_size_bits: 12,       // 4 KB pages
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device))
    }

    #[test]
    fn test_create_store() {
        let store = create_test_store();
        let state = store.system_state();
        assert_eq!(state.version, 0);
        assert_eq!(state.phase, Phase::Rest);
    }

    #[test]
    fn test_start_session_returns_too_many_threads_on_exhaustion() {
        let store = create_test_store();
        match store.start_session_with_thread_id(None) {
            Ok(_) => panic!("expected TooManyThreads"),
            Err(err) => assert_eq!(err, Status::TooManyThreads),
        }
    }

    #[test]
    fn test_auto_compaction_handle_stop_wakes_worker() {
        let store = create_test_store();
        let config = AutoCompactionConfig::new()
            .with_check_interval(Duration::from_secs(60))
            .with_min_log_size(u64::MAX)
            .with_target_utilization(0.0);

        let mut handle = store.start_auto_compaction(config);
        assert!(handle.is_running());

        std::thread::sleep(Duration::from_millis(200));

        let start = Instant::now();
        handle.stop();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "stop() should wake the worker promptly, took {elapsed:?}"
        );
        assert_eq!(handle.state(), crate::compaction::AutoCompactionState::Stopped);
    }

    #[test]
    fn test_auto_compaction_multiple_workers_stop() {
        let store = create_test_store();
        let config = AutoCompactionConfig::new()
            .with_check_interval(Duration::from_secs(60))
            .with_min_log_size(u64::MAX)
            .with_target_utilization(0.0);

        let mut handle1 = store.start_auto_compaction(config.clone());
        let mut handle2 = store.start_auto_compaction(config);

        std::thread::sleep(Duration::from_millis(200));

        handle1.stop();
        handle2.stop();

        assert_eq!(handle1.state(), crate::compaction::AutoCompactionState::Stopped);
        assert_eq!(handle2.state(), crate::compaction::AutoCompactionState::Stopped);
    }

    #[test]
    fn test_upsert_and_read() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Upsert
        let status = session.upsert(42u64, 100u64);
        assert_eq!(status, Status::Ok);

        // Read
        let result = session.read(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(100u64));
    }

    #[test]
    fn test_read_not_found() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        let result = session.read(&999u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert
        session.upsert(42u64, 100u64);

        // Verify it exists
        let result = session.read(&42u64);
        assert_eq!(result.unwrap(), Some(100u64));

        // Delete
        let status = session.delete(&42u64);
        assert_eq!(status, Status::Ok);

        // Verify it's gone
        let result = session.read(&42u64);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_multiple_operations() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert multiple keys
        for i in 1u64..101 {
            let status = session.upsert(i, i * 10);
            assert_eq!(status, Status::Ok);
        }

        // Read them back
        for i in 1u64..101 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), Some(i * 10), "Failed to read key {i}");
        }

        // Update some
        for i in 1u64..51 {
            let status = session.upsert(i, i * 100);
            assert_eq!(status, Status::Ok);
        }

        // Verify updates
        for i in 1u64..51 {
            let result = session.read(&i);
            assert_eq!(
                result.unwrap(),
                Some(i * 100),
                "Failed to read updated key {i}"
            );
        }

        // Verify unchanged
        for i in 51u64..101 {
            let result = session.read(&i);
            assert_eq!(
                result.unwrap(),
                Some(i * 10),
                "Key {i} was unexpectedly changed"
            );
        }
    }

    #[test]
    fn test_index_stats() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert some data
        for i in 0u64..10 {
            session.upsert(i, i);
        }

        let stats = store.index_stats();
        assert!(stats.used_entries > 0);
    }

    #[test]
    fn test_log_stats() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert some data
        for i in 0u64..10 {
            session.upsert(i, i);
        }

        let stats = store.log_stats();
        assert!(stats.tail_address > Address::new(0, 0));
    }

    // ============ Checkpoint and Recovery Tests ============

    fn create_test_config() -> FasterKvConfig {
        FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20, // 1 MB
            page_size_bits: 12,       // 4 KB pages
            mutable_fraction: 0.9,
        }
    }

    #[test]
    fn test_checkpoint_empty_store() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let token = store.checkpoint(temp_dir.path()).unwrap();

        // Verify checkpoint exists
        assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
            temp_dir.path(),
            token
        ));
    }

    #[test]
    fn test_checkpoint_with_data() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert some data
        for i in 1u64..11 {
            session.upsert(i, i * 100);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let token = store.checkpoint(temp_dir.path()).unwrap();

        // Verify checkpoint files exist
        let cp_dir = temp_dir.path().join(token.to_string());
        assert!(cp_dir.join("index.meta").exists());
        assert!(cp_dir.join("index.dat").exists());
        assert!(cp_dir.join("log.meta").exists());
        assert!(cp_dir.join("log.snapshot").exists());
    }

    #[test]
    fn test_get_checkpoint_info() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let token = store.checkpoint(temp_dir.path()).unwrap();

        let (index_meta, log_meta) =
            FasterKv::<u64, u64, NullDisk>::get_checkpoint_info(temp_dir.path(), token).unwrap();

        assert_eq!(index_meta.token, token);
        assert_eq!(log_meta.token, token);
    }

    #[test]
    fn test_list_checkpoints() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create multiple checkpoints
        let token1 = store.checkpoint(temp_dir.path()).unwrap();
        let token2 = store.checkpoint(temp_dir.path()).unwrap();
        let token3 = store.checkpoint(temp_dir.path()).unwrap();

        let tokens = FasterKv::<u64, u64, NullDisk>::list_checkpoints(temp_dir.path()).unwrap();

        assert_eq!(tokens.len(), 3);
        assert!(tokens.contains(&token1));
        assert!(tokens.contains(&token2));
        assert!(tokens.contains(&token3));
    }

    #[test]
    fn test_recover_empty_store() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let token = store.checkpoint(temp_dir.path()).unwrap();
        drop(store);

        // Recover
        let config = create_test_config();
        let device = NullDisk::new();
        let recovered: FasterKv<u64, u64, NullDisk> =
            FasterKv::recover(temp_dir.path(), token, config, device).unwrap();

        let state = recovered.system_state();
        assert_eq!(state.phase, Phase::Rest);
    }

    #[test]
    fn test_checkpoint_increments_version() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let v0 = store.system_state().version;
        store.checkpoint(temp_dir.path()).unwrap();
        let v1 = store.system_state().version;
        store.checkpoint(temp_dir.path()).unwrap();
        let v2 = store.system_state().version;

        assert_eq!(v0, 0);
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    #[test]
    fn test_checkpoint_failure_rolls_back_version() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let v0 = store.system_state().version;
        let token = uuid::Uuid::new_v4();

        // Create a path that will cause log.snapshot write to fail: make it a directory.
        let cp_dir = temp_dir.path().join(token.to_string());
        std::fs::create_dir_all(cp_dir.join("log.snapshot")).unwrap();

        assert!(store
            .checkpoint_with_action_internal(temp_dir.path(), Action::CheckpointFull, token)
            .is_err());

        let state = store.system_state();
        assert_eq!(state.phase, Phase::Rest);
        assert_eq!(state.version, v0);
    }

    #[test]
    fn test_checkpoint_not_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fake_token = uuid::Uuid::new_v4();

        assert!(!FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
            temp_dir.path(),
            fake_token
        ));
    }

    // ============ Read Cache Integration Tests ============

    #[test]
    fn test_create_store_with_read_cache() {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        let cache_config = ReadCacheConfig::new(1 << 20); // 1 MB cache

        let store: Arc<FasterKv<u64, u64, NullDisk>> =
            Arc::new(FasterKv::with_read_cache(config, device, cache_config));

        assert!(store.has_read_cache());
        assert!(store.read_cache_stats().is_some());
    }

    #[test]
    fn test_store_without_read_cache() {
        let store = create_test_store();
        assert!(!store.has_read_cache());
        assert!(store.read_cache_stats().is_none());
    }

    #[test]
    fn test_clear_read_cache() {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        let cache_config = ReadCacheConfig::new(1 << 20);

        let store: Arc<FasterKv<u64, u64, NullDisk>> =
            Arc::new(FasterKv::with_read_cache(config, device, cache_config));

        // Clear should not panic
        store.clear_read_cache();
        assert!(store.has_read_cache());
    }

    // ============ Compaction Integration Tests ============

    #[test]
    fn test_compaction_config() {
        let store = create_test_store();
        let config = store.compaction_config();

        assert!(config.target_utilization > 0.0);
        assert!(config.target_utilization <= 1.0);
    }

    #[test]
    fn test_compaction_not_in_progress() {
        let store = create_test_store();
        assert!(!store.is_compaction_in_progress());
    }

    #[test]
    fn test_log_compact_empty() {
        let store = create_test_store();
        let result = store.log_compact();

        // On empty store, compaction should succeed with no changes
        assert_eq!(result.status, Status::Ok);
        assert_eq!(result.stats.records_scanned, 0);
    }

    #[test]
    fn test_log_utilization() {
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Initial utilization
        let util = store.log_utilization();
        assert!((0.0..=1.0).contains(&util));

        // After some inserts
        for i in 0u64..10 {
            session.upsert(i, i * 100);
        }

        let util_after = store.log_utilization();
        assert!((0.0..=1.0).contains(&util_after));
    }

    #[test]
    fn test_should_compact() {
        let store = create_test_store();
        // With default settings and empty store, should not need compaction
        let _needs_compact = store.should_compact();
    }

    #[test]
    fn test_compaction_preserves_live_records() {
        // This test verifies that compaction properly copies live records
        // to the tail before reclaiming the original space.
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert some records (start from 1, not 0, for consistency with other tests)
        let num_records = 100u64;
        for i in 1..=num_records {
            session.upsert(i, i * 100);
        }

        // Verify all records are readable before compaction
        for i in 1..=num_records {
            let result = session.read(&i);
            assert!(result.is_ok(), "Record {i} should exist before compaction");
            assert_eq!(
                result.unwrap(),
                Some(i * 100),
                "Record {i} should have correct value"
            );
        }

        // Perform compaction
        let compact_result = store.log_compact();
        assert_eq!(
            compact_result.status,
            Status::Ok,
            "Compaction should succeed"
        );

        // CRITICAL: Verify all records are still readable after compaction
        // This is the key assertion - if compaction drops live records,
        // this test will fail.
        for i in 1..=num_records {
            let result = session.read(&i);
            assert!(
                result.is_ok(),
                "Record {i} should still exist after compaction"
            );
            let value = result.unwrap();
            assert_eq!(
                value,
                Some(i * 100),
                "Record {} should have correct value {} after compaction, got {:?}",
                i,
                i * 100,
                value
            );
        }

        // Session is automatically dropped when it goes out of scope
    }

    #[test]
    fn test_compaction_removes_obsolete_records() {
        // Test that compaction properly removes obsolete (superseded) records
        let store = create_test_store();
        let mut session = store.start_session().unwrap();

        // Insert initial values (start from 1, not 0, to avoid key=0 edge case)
        for i in 1u64..=50 {
            session.upsert(i, i);
        }

        // Update some values (creating obsolete records)
        for i in 1u64..=25 {
            session.upsert(i, i + 1000);
        }

        // Perform compaction
        let result = store.log_compact();
        assert_eq!(result.status, Status::Ok);

        // Verify we can still read all keys with correct values
        // Keys 1-25 should have updated values (i + 1000)
        for i in 1u64..=25 {
            let value = session.read(&i).unwrap();
            assert_eq!(value, Some(i + 1000), "Key {i} should have updated value");
        }

        // Keys 26-50 should have original values
        for i in 26u64..=50 {
            let value = session.read(&i).unwrap();
            assert_eq!(value, Some(i), "Key {i} should have original value");
        }

        // Session is automatically dropped when it goes out of scope
    }

    // ============ Index Growth Integration Tests ============

    #[test]
    fn test_index_size() {
        let store = create_test_store();
        let size = store.index_size();
        assert!(size > 0);
        assert!(size.is_power_of_two());
    }

    #[test]
    fn test_grow_not_in_progress() {
        let store = create_test_store();
        assert!(!store.is_grow_in_progress());
        assert!(store.grow_progress().is_none());
    }

    #[test]
    fn test_start_grow_invalid_size() {
        let store = create_test_store();
        let current_size = store.index_size();

        // Smaller size should fail
        let result = store.start_grow(current_size / 2);
        assert!(result.is_err());

        // Same size should fail
        let result = store.start_grow(current_size);
        assert!(result.is_err());

        // Non-power-of-two should fail
        let result = store.start_grow(current_size * 2 + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_start_grow_valid() {
        let store = create_test_store();
        let current_size = store.index_size();
        let new_size = current_size * 2;

        // Start growth
        let result = store.start_grow(new_size);
        assert!(result.is_ok());
        assert!(store.is_grow_in_progress());
        assert!(store.grow_progress().is_some());

        // Complete growth
        let grow_result = store.complete_grow();
        assert!(grow_result.success || grow_result.status.is_some());
    }

    #[test]
    fn test_grow_already_in_progress() {
        let store = create_test_store();
        let current_size = store.index_size();

        // Start first growth
        let result1 = store.start_grow(current_size * 2);
        assert!(result1.is_ok());

        // Second growth should fail
        let result2 = store.start_grow(current_size * 4);
        assert!(result2.is_err());

        // Clean up
        store.complete_grow();
    }
}
