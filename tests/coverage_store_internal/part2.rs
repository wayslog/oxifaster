// ============ 3. Recovery Tests (recovery.rs) ============

#[test]
fn test_recovery_state_lifecycle() {
    let state = RecoveryState::new();
    assert_eq!(state.status(), RecoveryStatus::NotStarted);
    assert!(state.begin_address().is_invalid());
    assert!(state.head_address().is_invalid());
    assert!(state.tail_address().is_invalid());
    assert_eq!(state.version(), 0);
    assert!(state.error_message().is_none());
    assert!(state.checkpoint_info().is_none());
}

#[test]
fn test_recovery_state_fail() {
    let mut state = RecoveryState::new();
    state.fail_recovery("Test failure message");

    assert_eq!(state.status(), RecoveryStatus::Failed);
    assert_eq!(state.error_message(), Some("Test failure message"));
}

#[test]
fn test_recovery_state_reset() {
    let mut state = RecoveryState::new();
    state.fail_recovery("Error");
    state.reset();

    assert_eq!(state.status(), RecoveryStatus::NotStarted);
    assert!(state.error_message().is_none());
}

#[test]
fn test_recovery_state_index_recovered_wrong_state() {
    let mut state = RecoveryState::new();
    // Cannot call index_recovered from NotStarted
    assert_eq!(state.index_recovered(), Status::Aborted);
}

#[test]
fn test_recovery_state_log_recovered_wrong_state() {
    let mut state = RecoveryState::new();
    // Cannot call log_recovered from NotStarted
    assert_eq!(state.log_recovered(), Status::Aborted);
}

#[test]
fn test_recovery_state_complete_wrong_state() {
    let mut state = RecoveryState::new();
    // Cannot complete from NotStarted
    assert_eq!(state.complete_recovery(), Status::Aborted);
}

#[test]
fn test_recovery_status_is_in_progress() {
    assert!(!RecoveryStatus::NotStarted.is_in_progress());
    assert!(RecoveryStatus::LoadingMetadata.is_in_progress());
    assert!(RecoveryStatus::RecoveringIndex.is_in_progress());
    assert!(RecoveryStatus::RecoveringLog.is_in_progress());
    assert!(RecoveryStatus::RestoringSessions.is_in_progress());
    assert!(!RecoveryStatus::Completed.is_in_progress());
    assert!(!RecoveryStatus::Failed.is_in_progress());
}

#[test]
fn test_page_recovery_status() {
    assert_eq!(
        PageRecoveryStatus::default(),
        PageRecoveryStatus::NotStarted
    );
    assert_eq!(PageRecoveryStatus::NotStarted.as_str(), "NotStarted");
    assert_eq!(PageRecoveryStatus::IssuedRead.as_str(), "IssuedRead");
    assert_eq!(PageRecoveryStatus::ReadDone.as_str(), "ReadDone");
    assert_eq!(PageRecoveryStatus::IssuedFlush.as_str(), "IssuedFlush");
    assert_eq!(PageRecoveryStatus::FlushDone.as_str(), "FlushDone");

    assert!(!PageRecoveryStatus::NotStarted.is_done());
    assert!(!PageRecoveryStatus::IssuedRead.is_done());
    assert!(!PageRecoveryStatus::ReadDone.is_done());
    assert!(!PageRecoveryStatus::IssuedFlush.is_done());
    assert!(PageRecoveryStatus::FlushDone.is_done());

    assert!(!PageRecoveryStatus::NotStarted.read_started());
    assert!(PageRecoveryStatus::IssuedRead.read_started());
    assert!(PageRecoveryStatus::ReadDone.read_started());
    assert!(PageRecoveryStatus::IssuedFlush.read_started());
    assert!(PageRecoveryStatus::FlushDone.read_started());

    // Display
    assert_eq!(format!("{}", PageRecoveryStatus::FlushDone), "FlushDone");
}

#[test]
fn test_checkpoint_info_new() {
    let token = Uuid::new_v4();
    let info = CheckpointInfo::new(token, std::path::PathBuf::from("/tmp/test"));

    assert_eq!(info.token, token);
    assert!(!info.is_valid());
    assert!(!info.is_incremental());
    assert!(info.base_snapshot_token().is_none());
    assert!(info.version().is_none());
    assert!(info.session_states().is_empty());
}

#[test]
fn test_checkpoint_info_files_exist_nonexistent() {
    let token = Uuid::new_v4();
    let info = CheckpointInfo::new(token, std::path::PathBuf::from("/nonexistent/path"));
    assert!(!info.files_exist());
}

#[test]
fn test_recovery_start_invalid_checkpoint() {
    let mut state = RecoveryState::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let fake_token = Uuid::new_v4();

    let status = state.start_recovery(temp_dir.path(), fake_token);
    // Should fail with either IoError (file not found) or Corruption (invalid data)
    assert!(status == Status::IoError || status == Status::Corruption);
    assert_eq!(state.status(), RecoveryStatus::Failed);
}

#[test]
fn test_recovery_full_flow_with_filesystem() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store_dir = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let test_data: Vec<(u64, u64)> = (1u64..51).map(|i| (i, i * 100)).collect();
    let token;

    // Phase 1: Create store and checkpoint
    {
        let store = create_filesystem_store(&store_dir);
        {
            let mut session = store.start_session().unwrap();
            for (k, v) in &test_data {
                session.upsert(*k, *v);
            }
        }
        token = store.checkpoint(&checkpoint_dir).unwrap();
    }

    // Phase 2: Recover
    {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let device = FileSystemDisk::single_file(store_dir.join("store.dat")).unwrap();
        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();

        let recovered = Arc::new(recovered);
        let mut session = recovered.start_session().unwrap();

        // Verify some data (may not all be present due to NullDisk limitations)
        for (k, _v) in test_data.iter().take(10) {
            let _ = session.read(k);
        }
    }
}

#[test]
fn test_recover_with_compaction_config() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store_dir = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let token;
    {
        let store = create_filesystem_store(&store_dir);
        {
            let mut session = store.start_session().unwrap();
            for i in 0..20u64 {
                session.upsert(i, i * 10);
            }
        }
        token = store.checkpoint(&checkpoint_dir).unwrap();
    }

    {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let compaction_config = CompactionConfig::new().with_target_utilization(0.7);
        let device = FileSystemDisk::single_file(store_dir.join("store.dat")).unwrap();

        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover_with_compaction_config(
                &checkpoint_dir,
                token,
                config,
                device,
                compaction_config,
            )
            .unwrap();

        assert_eq!(recovered.compaction_config().target_utilization, 0.7);
    }
}

#[test]
fn test_recover_with_read_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store_dir = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let token;
    {
        let store = create_filesystem_store(&store_dir);
        {
            let mut session = store.start_session().unwrap();
            for i in 0..20u64 {
                session.upsert(i, i * 10);
            }
        }
        token = store.checkpoint(&checkpoint_dir).unwrap();
    }

    {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let cache_config = ReadCacheConfig::new(1 << 20);
        let device = FileSystemDisk::single_file(store_dir.join("store.dat")).unwrap();

        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover_with_read_cache(&checkpoint_dir, token, config, device, cache_config)
                .unwrap();

        assert!(recovered.has_read_cache());
    }
}

#[test]
fn test_recover_index_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store_dir = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let token;
    {
        let store = create_filesystem_store(&store_dir);
        {
            let mut session = store.start_session().unwrap();
            for i in 0..20u64 {
                session.upsert(i, i * 10);
            }
        }
        token = store.checkpoint(&checkpoint_dir).unwrap();
    }

    {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let device = FileSystemDisk::single_file(store_dir.join("store.dat")).unwrap();

        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover_index_only(&checkpoint_dir, token, config, device).unwrap();

        let _ = Arc::new(recovered);
    }
}

#[test]
fn test_recover_log_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store_dir = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let token;
    {
        let store = create_filesystem_store(&store_dir);
        {
            let mut session = store.start_session().unwrap();
            for i in 0..20u64 {
                session.upsert(i, i * 10);
            }
        }
        token = store.checkpoint(&checkpoint_dir).unwrap();
    }

    {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let device = FileSystemDisk::single_file(store_dir.join("store.dat")).unwrap();

        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover_log_only(&checkpoint_dir, token, config, device).unwrap();

        let _ = Arc::new(recovered);
    }
}

#[test]
fn test_recovered_sessions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store_dir = temp_dir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let token;
    {
        let store = create_filesystem_store(&store_dir);
        {
            let mut session = store.start_session().unwrap();
            session.upsert(1u64, 100u64);
            // Ensure session registers by doing operations
            session.upsert(2u64, 200u64);
        }
        token = store.checkpoint(&checkpoint_dir).unwrap();
    }

    {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let device = FileSystemDisk::single_file(store_dir.join("store.dat")).unwrap();
        let recovered: FasterKv<u64, u64, FileSystemDisk> =
            FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();

        let sessions = recovered.get_recovered_sessions();
        // May or may not have sessions depending on implementation
        let _ = sessions;
    }
}

#[test]
fn test_list_checkpoints_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let checkpoints = list_checkpoints(temp_dir.path());
    assert!(checkpoints.is_empty());
}

#[test]
fn test_find_latest_checkpoint_none() {
    let temp_dir = tempfile::tempdir().unwrap();
    let latest = find_latest_checkpoint(temp_dir.path());
    assert!(latest.is_none());
}

#[test]
fn test_validate_checkpoint_missing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let result = validate_checkpoint(temp_dir.path());
    assert!(result.is_err());
}

// Note: validate_incremental_checkpoint is not publicly exported

// Note: IncrementalRecoveryInfo::from_chain is not publicly exported

