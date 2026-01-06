//! Integration tests for incremental checkpoint functionality
//!
//! Tests the complete flow of:
//! - Full snapshot creation
//! - Incremental checkpoint creation
//! - Recovery from incremental checkpoints

use oxifaster::checkpoint::{
    delta_log_path, delta_metadata_path, CheckpointType, DeltaLogMetadata,
    IncrementalCheckpointChain,
};
use oxifaster::delta_log::{
    DeltaLog, DeltaLogConfig, DeltaLogEntry, DeltaLogEntryType, DeltaLogIterator,
};
use oxifaster::device::NullDisk;
use oxifaster::store::FasterKv;
use oxifaster::store::FasterKvConfig;
use std::sync::Arc;
use tempfile::tempdir;

/// Create a test store
fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 12,       // 4 KB pages
        mutable_fraction: 0.9,
    };
    Arc::new(FasterKv::new(config, NullDisk::new()))
}

// ============ Delta Log Unit Tests ============

#[test]
fn test_delta_log_entry_types() {
    // Test entry type conversion
    assert_eq!(DeltaLogEntryType::Delta as i32, 0);
    assert_eq!(DeltaLogEntryType::CheckpointMetadata as i32, 1);

    // Test from_i32
    assert_eq!(
        DeltaLogEntryType::from_i32(0),
        Some(DeltaLogEntryType::Delta)
    );
    assert_eq!(
        DeltaLogEntryType::from_i32(1),
        Some(DeltaLogEntryType::CheckpointMetadata)
    );
    assert_eq!(DeltaLogEntryType::from_i32(99), None);
}

#[test]
fn test_delta_log_entry_serialization() {
    // Create a delta entry
    let payload = b"test delta record data".to_vec();
    let entry = DeltaLogEntry::delta(payload.clone());

    assert_eq!(entry.entry_type(), Some(DeltaLogEntryType::Delta));
    assert!(entry.verify());

    // Serialize and deserialize
    let bytes = entry.to_bytes();
    let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();

    assert_eq!(restored.payload, payload);
    assert_eq!(restored.entry_type(), Some(DeltaLogEntryType::Delta));
    assert!(restored.verify());
}

#[test]
fn test_delta_log_checkpoint_metadata_entry() {
    // Create a checkpoint metadata entry
    let metadata = r#"{"token": "abc", "version": 5}"#.as_bytes().to_vec();
    let entry = DeltaLogEntry::checkpoint_metadata(metadata.clone());

    assert_eq!(
        entry.entry_type(),
        Some(DeltaLogEntryType::CheckpointMetadata)
    );
    assert!(entry.verify());

    // Serialize and deserialize
    let bytes = entry.to_bytes();
    let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();

    assert_eq!(restored.payload, metadata);
    assert_eq!(
        restored.entry_type(),
        Some(DeltaLogEntryType::CheckpointMetadata)
    );
}

#[test]
fn test_delta_log_write_and_read() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12); // 4KB pages
    let delta_log = DeltaLog::new(device.clone(), config, 0);

    // Initialize for writes
    delta_log.init_for_writes();
    assert!(delta_log.is_initialized_for_writes());

    // Write some entries
    let entry1 = DeltaLogEntry::delta(b"first record".to_vec());
    let entry2 = DeltaLogEntry::delta(b"second record".to_vec());

    let addr1 = delta_log.write_entry(&entry1).unwrap();
    let addr2 = delta_log.write_entry(&entry2).unwrap();

    assert_eq!(addr1, 0);
    assert!(addr2 > addr1);
    assert!(delta_log.tail_address() > 0);
}

#[test]
fn test_delta_log_config() {
    let config = DeltaLogConfig::new(20);

    assert_eq!(config.page_size_bits, 20);
    assert_eq!(config.page_size(), 1 << 20); // 1MB
    assert_eq!(config.page_size_mask(), (1 << 20) - 1);

    // Test alignment
    let aligned = config.align(1000);
    assert_eq!(aligned % config.sector_size as i64, 0);
}

// ============ Checkpoint Type Tests ============

#[test]
fn test_checkpoint_type_methods() {
    // Test uses_snapshot_file
    assert!(CheckpointType::Snapshot.uses_snapshot_file());
    assert!(CheckpointType::Full.uses_snapshot_file());
    assert!(CheckpointType::IncrementalSnapshot.uses_snapshot_file());
    assert!(!CheckpointType::FoldOver.uses_snapshot_file());
    assert!(!CheckpointType::IndexOnly.uses_snapshot_file());

    // Test is_incremental
    assert!(CheckpointType::IncrementalSnapshot.is_incremental());
    assert!(!CheckpointType::Snapshot.is_incremental());
    assert!(!CheckpointType::FoldOver.is_incremental());
    assert!(!CheckpointType::Full.is_incremental());

    // Test from_u8
    assert_eq!(CheckpointType::from_u8(0), Some(CheckpointType::FoldOver));
    assert_eq!(CheckpointType::from_u8(1), Some(CheckpointType::Snapshot));
    assert_eq!(
        CheckpointType::from_u8(5),
        Some(CheckpointType::IncrementalSnapshot)
    );
    assert_eq!(CheckpointType::from_u8(99), None);
}

// ============ Metadata Serialization Tests ============

#[test]
fn test_delta_log_metadata_serialization() {
    let token = uuid::Uuid::new_v4();
    let base_token = uuid::Uuid::new_v4();

    let mut metadata = DeltaLogMetadata::new(token, base_token, 5);
    metadata.delta_tail_address = 4096;
    metadata.prev_snapshot_final_address = 1000;
    metadata.current_final_address = 2000;
    metadata.num_entries = 50;

    // Serialize
    let json = metadata.serialize_json().unwrap();
    assert!(!json.is_empty());

    // Deserialize
    let restored = DeltaLogMetadata::deserialize_json(&json).unwrap();
    assert_eq!(metadata.token, restored.token);
    assert_eq!(metadata.base_snapshot_token, restored.base_snapshot_token);
    assert_eq!(metadata.version, restored.version);
    assert_eq!(metadata.delta_tail_address, restored.delta_tail_address);
    assert_eq!(metadata.num_entries, restored.num_entries);
}

#[test]
fn test_incremental_checkpoint_chain() {
    let base_token = uuid::Uuid::new_v4();
    let mut chain = IncrementalCheckpointChain::new(base_token);

    assert_eq!(chain.num_incrementals(), 0);
    assert!(chain.is_complete);
    assert_eq!(chain.latest_token().unwrap(), base_token);

    // Add incrementals
    let incr1 = uuid::Uuid::new_v4();
    chain.add_incremental(incr1, 50);
    assert_eq!(chain.num_incrementals(), 1);
    assert_eq!(chain.total_delta_entries, 50);
    assert_eq!(chain.latest_token().unwrap(), incr1);

    let incr2 = uuid::Uuid::new_v4();
    chain.add_incremental(incr2, 30);
    assert_eq!(chain.num_incrementals(), 2);
    assert_eq!(chain.total_delta_entries, 80);
    assert_eq!(chain.latest_token().unwrap(), incr2);
}

#[test]
fn test_delta_log_metadata_file_io() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("delta.meta");

    let token = uuid::Uuid::new_v4();
    let base_token = uuid::Uuid::new_v4();

    let mut metadata = DeltaLogMetadata::new(token, base_token, 3);
    metadata.delta_tail_address = 8192;
    metadata.num_entries = 100;

    // Write to file
    metadata.write_to_file(&file_path).unwrap();
    assert!(file_path.exists());

    // Read from file
    let restored = DeltaLogMetadata::read_from_file(&file_path).unwrap();
    assert_eq!(metadata.token, restored.token);
    assert_eq!(metadata.get_token().unwrap(), token);
    assert_eq!(metadata.get_base_token().unwrap(), base_token);
}

// ============ Path Helper Tests ============

#[test]
fn test_delta_log_paths() {
    let checkpoint_dir = std::path::Path::new("/tmp/checkpoint/abc123");

    assert_eq!(
        delta_log_path(checkpoint_dir, 0),
        std::path::Path::new("/tmp/checkpoint/abc123/delta.0.log")
    );
    assert_eq!(
        delta_log_path(checkpoint_dir, 3),
        std::path::Path::new("/tmp/checkpoint/abc123/delta.3.log")
    );
    assert_eq!(
        delta_metadata_path(checkpoint_dir),
        std::path::Path::new("/tmp/checkpoint/abc123/delta.meta")
    );
}

// ============ Store Integration Tests ============

#[test]
fn test_store_full_checkpoint_enables_incremental() {
    let store = create_test_store();
    let temp_dir = tempdir().unwrap();

    // Insert some data using a session
    {
        let mut session = store.start_session();
        for i in 0..100u64 {
            let _ = session.upsert(i, i * 10);
        }
        session.refresh();
    }

    // Create full checkpoint
    let token = store.checkpoint(temp_dir.path()).unwrap();
    assert!(!token.is_nil());

    // Verify checkpoint directory was created
    let cp_dir = temp_dir.path().join(token.to_string());
    assert!(cp_dir.exists());
    assert!(cp_dir.join("index.meta").exists());
    assert!(cp_dir.join("log.meta").exists());
}

#[test]
fn test_store_checkpoint_types() {
    let store = create_test_store();
    let temp_dir = tempdir().unwrap();

    // Test full checkpoint
    let full_token = store.checkpoint_full_snapshot(temp_dir.path()).unwrap();
    assert!(!full_token.is_nil());

    // Test index-only checkpoint
    let index_token = store.checkpoint_index(temp_dir.path()).unwrap();
    assert!(!index_token.is_nil());
    assert_ne!(full_token, index_token);

    // Test hybrid log-only checkpoint
    let log_token = store.checkpoint_hybrid_log(temp_dir.path()).unwrap();
    assert!(!log_token.is_nil());
    assert_ne!(full_token, log_token);
    assert_ne!(index_token, log_token);
}

// ============ Iterator Tests ============

#[test]
fn test_delta_log_iterator_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    let result = iter.get_next().unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[test]
fn test_delta_log_checksum_verification() {
    use oxifaster::delta_log::{verify_checksum, DeltaLogHeader};

    let payload = b"test payload for checksum".to_vec();
    let mut header = DeltaLogHeader::new(payload.len() as i32, DeltaLogEntryType::Delta);

    // Compute checksum
    oxifaster::delta_log::compute_entry_checksum(&mut header, &payload);
    assert_ne!(header.checksum, 0);

    // Verify checksum
    assert!(verify_checksum(&header, &payload));

    // Corrupt checksum and verify failure
    header.checksum ^= 1;
    assert!(!verify_checksum(&header, &payload));
}

// ============ Performance Baseline Tests ============

#[test]
fn test_delta_log_bulk_write_performance() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(16); // 64KB pages
    let delta_log = DeltaLog::new(device, config, 0);

    delta_log.init_for_writes();

    let start = std::time::Instant::now();
    let num_entries = 1000;

    for i in 0..num_entries {
        let payload = format!("delta record {i}").into_bytes();
        let entry = DeltaLogEntry::delta(payload);
        delta_log.write_entry(&entry).unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Wrote {} delta entries in {:?} ({:.0} entries/sec)",
        num_entries,
        elapsed,
        num_entries as f64 / elapsed.as_secs_f64()
    );

    // Performance baseline: should complete in reasonable time
    assert!(elapsed.as_secs() < 5);
    assert!(delta_log.tail_address() > 0);
}

// ============ Stress Tests ============

#[test]
fn test_multiple_checkpoint_cycles() {
    let store = create_test_store();
    let temp_dir = tempdir().unwrap();

    // Run multiple checkpoint cycles
    for cycle in 0..5 {
        // Insert some data
        {
            let mut session = store.start_session();
            for i in 0..50u64 {
                let key = cycle * 100 + i;
                let _ = session.upsert(key, key * 10);
            }
            session.refresh();
        }

        // Create checkpoint
        let token = store.checkpoint(temp_dir.path()).unwrap();
        assert!(!token.is_nil());

        // Verify checkpoint files
        let cp_dir = temp_dir.path().join(token.to_string());
        assert!(cp_dir.exists());
    }
}

#[test]
fn test_store_incremental_checkpoint_persists_index_files() {
    let store = create_test_store();
    let temp_dir = tempdir().unwrap();

    // 先做一次完整快照，作为增量检查点的 base。
    {
        let mut session = store.start_session();
        for i in 0..100u64 {
            let _ = session.upsert(i, i);
        }
        session.refresh();
    }
    let base_token = store.checkpoint_full_snapshot(temp_dir.path()).unwrap();
    assert!(!base_token.is_nil());

    // 写入一些变更，确保后续增量检查点会产生 delta。
    {
        let mut session = store.start_session();
        for i in 50..150u64 {
            let _ = session.upsert(i, i * 10);
        }
        session.refresh();
    }

    let incr_token = store.checkpoint_incremental(temp_dir.path()).unwrap();
    assert!(!incr_token.is_nil());
    assert_ne!(base_token, incr_token);

    let cp_dir = temp_dir.path().join(incr_token.to_string());
    assert!(cp_dir.exists());

    // 增量检查点也必须包含索引文件，否则校验/恢复会失败。
    assert!(cp_dir.join("index.meta").exists());
    assert!(cp_dir.join("index.dat").exists());
    assert!(cp_dir.join("log.meta").exists());

    // delta log 与其元数据应存在（即便没有 delta 也会创建文件）。
    assert!(delta_log_path(&cp_dir, 0).exists());
    assert!(delta_metadata_path(&cp_dir).exists());
}
