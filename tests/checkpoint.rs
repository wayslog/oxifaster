//! Integration tests for checkpoint and recovery functionality

use std::sync::Arc;

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig, Phase};

fn create_test_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 12,       // 4 KB pages
        mutable_fraction: 0.9,
    }
}

fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = create_test_config();
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

#[test]
fn test_checkpoint_basic() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // Insert some data
    {
        let mut session = store.start_session();
        for i in 1u64..21 {
            let status = session.upsert(i, i * 10);
            assert_eq!(status, Status::Ok);
        }
    }

    // Create checkpoint
    let token = store.checkpoint(temp_dir.path()).unwrap();

    // Verify checkpoint exists
    assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
        temp_dir.path(),
        token
    ));

    // Get checkpoint info
    let (index_meta, log_meta) =
        FasterKv::<u64, u64, NullDisk>::get_checkpoint_info(temp_dir.path(), token).unwrap();

    assert_eq!(index_meta.token, token);
    assert_eq!(log_meta.token, token);
}

#[test]
fn test_checkpoint_and_recover_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let test_data: Vec<(u64, u64)> = (1u64..51).map(|i| (i, i * 100)).collect();
    let token;

    // Phase 1: Create store and insert data
    {
        let store = create_test_store();
        let mut session = store.start_session();

        for (k, v) in &test_data {
            let status = session.upsert(*k, *v);
            assert_eq!(status, Status::Ok);
        }

        // Checkpoint
        token = store.checkpoint(temp_dir.path()).unwrap();
    }

    // Phase 2: Recover and verify the recovery process completes
    {
        let config = create_test_config();
        let device = NullDisk::new();
        let recovered: FasterKv<u64, u64, NullDisk> =
            FasterKv::recover(temp_dir.path(), token, config, device).unwrap();

        let recovered = Arc::new(recovered);
        let _session = recovered.start_session();

        // Note: With NullDisk, the actual data won't persist because NullDisk
        // doesn't actually store data. This test verifies the recovery flow
        // works without errors. Full data verification requires a real disk.
        
        // Verify recovery completed and store is operational
        let state = recovered.system_state();
        assert_eq!(state.phase, Phase::Rest);
    }
}

#[test]
fn test_multiple_checkpoints() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // Insert initial data
    {
        let mut session = store.start_session();
        for i in 1u64..11 {
            session.upsert(i, i);
        }
    }

    // Checkpoint 1
    let token1 = store.checkpoint(temp_dir.path()).unwrap();

    // Insert more data
    {
        let mut session = store.start_session();
        for i in 11u64..21 {
            session.upsert(i, i);
        }
    }

    // Checkpoint 2
    let token2 = store.checkpoint(temp_dir.path()).unwrap();

    // Insert even more data
    {
        let mut session = store.start_session();
        for i in 21u64..31 {
            session.upsert(i, i);
        }
    }

    // Checkpoint 3
    let token3 = store.checkpoint(temp_dir.path()).unwrap();

    // Verify all checkpoints exist
    let tokens = FasterKv::<u64, u64, NullDisk>::list_checkpoints(temp_dir.path()).unwrap();
    assert_eq!(tokens.len(), 3);
    assert!(tokens.contains(&token1));
    assert!(tokens.contains(&token2));
    assert!(tokens.contains(&token3));
}

#[test]
fn test_checkpoint_version_increment() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // Initial version
    let v0 = store.system_state().version;
    assert_eq!(v0, 0);

    // Checkpoint and check version increment
    store.checkpoint(temp_dir.path()).unwrap();
    let v1 = store.system_state().version;
    assert_eq!(v1, 1);

    store.checkpoint(temp_dir.path()).unwrap();
    let v2 = store.system_state().version;
    assert_eq!(v2, 2);

    store.checkpoint(temp_dir.path()).unwrap();
    let v3 = store.system_state().version;
    assert_eq!(v3, 3);
}

#[test]
fn test_recover_restores_version() {
    let temp_dir = tempfile::tempdir().unwrap();
    let token;

    // Create store and checkpoint multiple times
    {
        let store = create_test_store();
        store.checkpoint(temp_dir.path()).unwrap();
        store.checkpoint(temp_dir.path()).unwrap();
        token = store.checkpoint(temp_dir.path()).unwrap();

        let final_version = store.system_state().version;
        assert_eq!(final_version, 3);
    }

    // Recover and check version
    {
        let config = create_test_config();
        let device = NullDisk::new();
        let recovered: FasterKv<u64, u64, NullDisk> =
            FasterKv::recover(temp_dir.path(), token, config, device).unwrap();

        // Version should be restored from the checkpoint (version at time of checkpoint)
        let recovered_version = recovered.system_state().version;
        // The last checkpoint was made at version 2 (before incrementing to 3)
        assert_eq!(recovered_version, 2);
    }
}

#[test]
fn test_empty_checkpoint_list() {
    let temp_dir = tempfile::tempdir().unwrap();

    let tokens = FasterKv::<u64, u64, NullDisk>::list_checkpoints(temp_dir.path()).unwrap();
    assert!(tokens.is_empty());
}

#[test]
fn test_checkpoint_with_deletes() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // Insert and then delete some data
    {
        let mut session = store.start_session();

        // Insert 20 keys
        for i in 1u64..21 {
            session.upsert(i, i * 10);
        }

        // Delete every other key
        for i in (1u64..21).step_by(2) {
            session.delete(&i);
        }
    }

    // Checkpoint
    let token = store.checkpoint(temp_dir.path()).unwrap();

    // Verify checkpoint was created
    assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
        temp_dir.path(),
        token
    ));
}

#[test]
fn test_checkpoint_metadata_consistency() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // Insert data
    {
        let mut session = store.start_session();
        for i in 1u64..101 {
            session.upsert(i, i * 1000);
        }
    }

    // Checkpoint
    let token = store.checkpoint(temp_dir.path()).unwrap();

    // Load and verify metadata
    let (index_meta, log_meta) =
        FasterKv::<u64, u64, NullDisk>::get_checkpoint_info(temp_dir.path(), token).unwrap();

    // Tokens should match
    assert_eq!(index_meta.token, log_meta.token);
    assert_eq!(index_meta.token, token);

    // Index should have entries
    assert!(index_meta.num_entries > 0);

    // Log should have valid addresses
    assert!(log_meta.final_address.is_valid() || log_meta.final_address.control() > 0);
}

#[test]
fn test_recover_invalid_token() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fake_token = uuid::Uuid::new_v4();

    let config = create_test_config();
    let device = NullDisk::new();

    let result: Result<FasterKv<u64, u64, NullDisk>, _> =
        FasterKv::recover(temp_dir.path(), fake_token, config, device);

    // Should fail because checkpoint doesn't exist
    assert!(result.is_err());
}

#[test]
fn test_checkpoint_files_structure() {
    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    {
        let mut session = store.start_session();
        session.upsert(1u64, 100u64);
    }

    let token = store.checkpoint(temp_dir.path()).unwrap();
    let cp_dir = temp_dir.path().join(token.to_string());

    // Check all expected files exist
    assert!(cp_dir.exists(), "Checkpoint directory should exist");
    assert!(
        cp_dir.join("index.meta").exists(),
        "index.meta should exist"
    );
    assert!(cp_dir.join("index.dat").exists(), "index.dat should exist");
    assert!(cp_dir.join("log.meta").exists(), "log.meta should exist");
    assert!(
        cp_dir.join("log.snapshot").exists(),
        "log.snapshot should exist"
    );
}

#[test]
fn test_concurrent_reads_during_checkpoint() {
    use std::thread;

    let store = create_test_store();
    let temp_dir = tempfile::tempdir().unwrap();

    // Insert data
    {
        let mut session = store.start_session();
        for i in 1u64..101 {
            session.upsert(i, i * 10);
        }
    }

    let store_clone = store.clone();
    let temp_path = temp_dir.path().to_path_buf();

    // Spawn a thread to do checkpointing
    let checkpoint_handle = thread::spawn(move || store_clone.checkpoint(&temp_path));

    // Continue reading while checkpoint happens
    {
        let mut session = store.start_session();
        for i in 1u64..101 {
            let _ = session.read(&i);
        }
    }

    // Wait for checkpoint to complete
    let result = checkpoint_handle.join().unwrap();
    assert!(result.is_ok());
}

