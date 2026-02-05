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
        assert!(
            op_stats
                .upserts
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );
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
        let _ = epoch.current_epoch.load(std::sync::atomic::Ordering::Relaxed);
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
            Arc::new(FasterKv::with_read_cache(config, device, cache_config));

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
            Arc::new(FasterKv::with_compaction_config(config, device, compaction_config));

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
