use super::*;

use super::store_index::StoreIndex;
use super::types::IndexType;
use crate::address::Address;
use crate::device::NullDisk;
use crate::f2::HotToColdMigrationStrategy;
use crate::index::ColdIndexConfig;
use crate::index::KeyHash;
use crate::status::Status;
use std::sync::Arc;

use bytemuck::{Pod, Zeroable};

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default, Pod, Zeroable)]
struct TestKey(u64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Default, Pod, Zeroable)]
struct TestValue(u64);

#[test]
fn test_create_f2() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device);
    assert!(f2.is_ok());
}

#[test]
fn test_session_lifecycle() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start session
    let session = f2.start_session();
    assert!(session.is_ok());

    // Stop session
    f2.stop_session();
}

#[test]
fn test_checkpoint() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start checkpoint
    let token = f2.checkpoint(false);
    assert!(token.is_ok());

    // Reset for next test
    f2.checkpoint.reset();
}

#[test]
fn test_store_type() {
    assert_ne!(StoreType::Hot, StoreType::Cold);
}

#[test]
fn test_operation_stages() {
    assert_ne!(
        ReadOperationStage::HotLogRead,
        ReadOperationStage::ColdLogRead
    );
    assert_ne!(RmwOperationStage::HotLogRmw, RmwOperationStage::ColdLogRead);
}

#[test]
fn test_f2_size() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Initial size should be 0
    assert_eq!(f2.size(), 0);
    assert_eq!(f2.hot_store_size(), 0);
    assert_eq!(f2.cold_store_size(), 0);
}

#[test]
fn test_f2_stats() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let hot_stats = f2.hot_store_stats();
    let cold_stats = f2.cold_store_stats();

    assert_eq!(hot_stats.size, 0);
    assert_eq!(cold_stats.size, 0);
}

#[test]
fn test_f2_read_write() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start session
    let _session = f2.start_session().unwrap();

    // Read non-existent key
    let result = f2.read(&TestKey(1));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Upsert
    let result = f2.upsert(TestKey(1), TestValue(100));
    assert!(result.is_ok());

    // Read should hit (hot)
    let result = f2.read(&TestKey(1)).unwrap();
    assert_eq!(result, Some(TestValue(100)));

    // Delete
    let result = f2.delete(&TestKey(1));
    assert!(result.is_ok());

    // Read should miss (tombstone)
    let result = f2.read(&TestKey(1)).unwrap();
    assert!(result.is_none());

    // Stop session
    f2.stop_session();
}

#[test]
fn test_f2_rmw_updates_value() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    f2.upsert(TestKey(7), TestValue(10)).unwrap();
    f2.rmw(TestKey(7), |v| v.0 += 5).unwrap();
    assert_eq!(f2.read(&TestKey(7)).unwrap(), Some(TestValue(15)));

    f2.stop_session();
}

#[test]
fn test_f2_hot_compaction_migrates_live_records_to_cold() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    f2.upsert(TestKey(1), TestValue(100)).unwrap();
    f2.upsert(TestKey(2), TestValue(200)).unwrap();
    f2.delete(&TestKey(2)).unwrap(); // tombstone

    let hot_tail = f2.hot_store.tail_address();
    let result = f2.compact_hot_log(hot_tail).unwrap();
    assert_eq!(result.status, Status::Ok);
    assert!(result.new_begin_address.control() > 0);

    // 迁移后仍然可读：key1 命中 cold；key2 tombstone 覆盖 cold 的旧值
    assert_eq!(f2.read(&TestKey(1)).unwrap(), Some(TestValue(100)));
    assert!(f2.read(&TestKey(2)).unwrap().is_none());

    f2.stop_session();
}

#[test]
fn test_f2_hot_compaction_access_frequency_keeps_hot() {
    let mut config = F2Config::default();
    config.compaction =
        config
            .compaction
            .with_hot_to_cold_migration(HotToColdMigrationStrategy::AccessFrequency {
                min_hot_accesses: 3,
                decay_shift: 0,
            });

    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<u64, u64, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    f2.upsert(1u64, 10u64).unwrap();
    f2.upsert(2u64, 20u64).unwrap();

    for _ in 0..5 {
        assert_eq!(f2.read(&1u64).unwrap(), Some(10u64));
    }

    let hot_tail = f2.hot_store.tail_address();
    let result = f2.compact_hot_log(hot_tail).unwrap();
    assert_eq!(result.status, Status::Ok);

    let hash1 = KeyHash::new(crate::codec::hash64(bytemuck::bytes_of(&1u64)));
    let hash2 = KeyHash::new(crate::codec::hash64(bytemuck::bytes_of(&2u64)));

    // key1 stays in hot (hot index hits, cold index misses).
    assert!(f2.hot_store.hash_index.find_entry(hash1).found());
    assert!(!f2.cold_store.hash_index.find_entry(hash1).found());

    // key2 migrates to cold (hot index cleared, cold index hits).
    assert!(!f2.hot_store.hash_index.find_entry(hash2).found());
    assert!(f2.cold_store.hash_index.find_entry(hash2).found());
    assert_eq!(f2.read(&2u64).unwrap(), Some(20u64));

    f2.stop_session();
}

#[test]
fn test_f2_compaction_check() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Should not need compaction with empty stores
    assert!(f2.should_compact_hot_log().is_none());
    assert!(f2.should_compact_cold_log().is_none());
}

#[test]
fn test_store_type_debug() {
    let debug_str = format!("{:?}", StoreType::Hot);
    assert!(debug_str.contains("Hot"));

    let debug_str = format!("{:?}", StoreType::Cold);
    assert!(debug_str.contains("Cold"));
}

#[test]
fn test_store_type_clone_copy() {
    let store_type = StoreType::Hot;
    let cloned = store_type;
    let copied = store_type;
    assert_eq!(store_type, cloned);
    assert_eq!(store_type, copied);
}

#[test]
fn test_read_operation_stage_debug() {
    let debug_str = format!("{:?}", ReadOperationStage::HotLogRead);
    assert!(debug_str.contains("HotLogRead"));

    let debug_str = format!("{:?}", ReadOperationStage::ColdLogRead);
    assert!(debug_str.contains("ColdLogRead"));
}

#[test]
fn test_read_operation_stage_clone_copy() {
    let stage = ReadOperationStage::HotLogRead;
    let cloned = stage;
    let copied = stage;
    assert_eq!(stage, cloned);
    assert_eq!(stage, copied);
}

#[test]
fn test_rmw_operation_stage_debug() {
    let debug_str = format!("{:?}", RmwOperationStage::HotLogRmw);
    assert!(debug_str.contains("HotLogRmw"));

    let debug_str = format!("{:?}", RmwOperationStage::ColdLogRead);
    assert!(debug_str.contains("ColdLogRead"));

    let debug_str = format!("{:?}", RmwOperationStage::HotLogConditionalInsert);
    assert!(debug_str.contains("HotLogConditionalInsert"));
}

#[test]
fn test_rmw_operation_stage_clone_copy() {
    let stage = RmwOperationStage::HotLogRmw;
    let cloned = stage;
    let copied = stage;
    assert_eq!(stage, cloned);
    assert_eq!(stage, copied);
}

#[test]
fn test_rmw_operation_stage_all_values() {
    assert_ne!(RmwOperationStage::HotLogRmw, RmwOperationStage::ColdLogRead);
    assert_ne!(
        RmwOperationStage::ColdLogRead,
        RmwOperationStage::HotLogConditionalInsert
    );
    assert_ne!(
        RmwOperationStage::HotLogRmw,
        RmwOperationStage::HotLogConditionalInsert
    );
}

#[test]
fn test_index_type_debug() {
    let debug_str = format!("{:?}", IndexType::MemoryIndex);
    assert!(debug_str.contains("MemoryIndex"));

    let debug_str = format!("{:?}", IndexType::ColdIndex);
    assert!(debug_str.contains("ColdIndex"));
}

#[test]
fn test_index_type_default() {
    assert_eq!(IndexType::default(), IndexType::MemoryIndex);
}

#[test]
fn test_index_type_clone_copy() {
    let idx_type = IndexType::ColdIndex;
    let cloned = idx_type;
    let copied = idx_type;
    assert_eq!(idx_type, cloned);
    assert_eq!(idx_type, copied);
}

#[test]
fn test_store_index_new_memory() {
    let index = StoreIndex::new_memory(1024);
    assert!(index.is_memory());
    assert!(!index.is_cold());
}

#[test]
fn test_store_index_as_memory() {
    let index = StoreIndex::new_memory(1024);
    let mem_idx = index.as_memory();
    assert!(mem_idx.is_some());
}

#[test]
fn test_store_index_memory_as_cold_returns_none() {
    let index = StoreIndex::new_memory(1024);
    let cold_idx = index.as_cold();
    assert!(cold_idx.is_none());
}

#[test]
fn test_store_index_find_entry() {
    let index = StoreIndex::new_memory(1024);
    let hash = KeyHash::new(12345);
    let result = index.find_entry(hash);
    // Should not find anything in empty index
    assert!(result.entry.is_unused());
}

#[test]
fn test_store_index_find_or_create_entry() {
    let index = StoreIndex::new_memory(1024);
    let hash = KeyHash::new(12345);
    let _result = index.find_or_create_entry(hash);
    // Entry should be created
}

#[test]
fn test_store_index_garbage_collect() {
    let index = StoreIndex::new_memory(1024);
    // Should not panic on empty index
    index.garbage_collect(Address::INVALID);
}

#[test]
fn test_f2_checkpoint_state() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Checkpoint phase should be Rest initially
    assert_eq!(
        f2.checkpoint.phase.load(Ordering::Relaxed),
        F2CheckpointPhase::Rest
    );
}

#[test]
fn test_f2_checkpoint_version() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Version should start at 0
    assert_eq!(f2.checkpoint.version(), 0);
}

#[test]
fn test_f2_refresh() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start session
    let _session = f2.start_session().unwrap();

    // Refresh should not panic
    f2.refresh();

    // Stop session
    f2.stop_session();
}

#[test]
fn test_f2_complete_pending() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start session
    let _session = f2.start_session().unwrap();

    // Complete pending should not panic
    f2.complete_pending(true);

    // Stop session
    f2.stop_session();
}

#[test]
fn test_f2_multiple_upserts() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    // Upsert multiple keys
    for i in 0..10 {
        let result = f2.upsert(TestKey(i), TestValue(i * 100));
        assert!(result.is_ok());
    }

    f2.stop_session();
}

#[test]
fn test_f2_delete_nonexistent() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    // Delete non-existent key should still succeed
    let result = f2.delete(&TestKey(999));
    assert!(result.is_ok());

    f2.stop_session();
}

#[test]
fn test_f2_config() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let config_ref = f2.config();
    assert!(config_ref.hot_store.log_mem_size > 0);
    assert!(config_ref.cold_store.log_mem_size > 0);
}

#[test]
fn test_f2_checkpoint_dir() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Initially no checkpoint dir
    assert!(f2.checkpoint_dir().is_none());

    // Set checkpoint dir
    f2.set_checkpoint_dir("/tmp/test_checkpoint");

    // Verify it's set
    assert!(f2.checkpoint_dir().is_some());
    assert!(f2
        .checkpoint_dir()
        .unwrap()
        .to_string_lossy()
        .contains("test_checkpoint"));
}

#[test]
fn test_f2_compaction_config() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let compaction_config = f2.compaction_config();
    assert!(compaction_config.trigger_percentage > 0.0);
}

#[test]
fn test_f2_num_active_sessions() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Initially no active sessions
    assert_eq!(f2.num_active_sessions(), 0);

    // Start session
    let _session = f2.start_session().unwrap();
    assert_eq!(f2.num_active_sessions(), 1);

    // Stop session
    f2.stop_session();
    assert_eq!(f2.num_active_sessions(), 0);
}

#[test]
fn test_f2_is_compaction_scheduled() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Initially compaction is not scheduled
    assert!(!f2.is_compaction_scheduled());
}

#[test]
fn test_f2_continue_session() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let session_id = uuid::Uuid::new_v4();
    let result = f2.continue_session(session_id);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    f2.stop_session();
}

#[test]
fn test_f2_complete_pending_compactions() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Should not hang or panic on empty store
    f2.complete_pending_compactions();
}

#[test]
fn test_f2_rmw() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    // RMW on non-existent key creates new entry
    let result = f2.rmw(TestKey(1), |v| {
        v.0 = 100;
    });
    assert!(result.is_ok());

    f2.stop_session();
}

#[test]
fn test_store_index_cold_creation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ColdIndexConfig::new(1024, 1024 * 1024, 0.5).with_root_path(temp_dir.path());
    let result = StoreIndex::new_cold(config);
    assert!(result.is_ok());

    let index = result.unwrap();
    assert!(index.is_cold());
    assert!(!index.is_memory());
}

#[test]
fn test_store_index_as_cold_mut() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ColdIndexConfig::new(1024, 1024 * 1024, 0.5).with_root_path(temp_dir.path());
    let mut index = StoreIndex::new_cold(config).unwrap();

    let cold_idx = index.as_cold_mut();
    assert!(cold_idx.is_some());
}

#[test]
fn test_f2_cold_store_can_use_cold_index_when_configured() {
    let temp_dir = tempfile::tempdir().unwrap();

    let mut config = F2Config::default();
    config.cold_store = config.cold_store.with_cold_index_config(
        ColdIndexConfig::new(1024, 1024 * 1024, 0.5).with_root_path(temp_dir.path()),
    );

    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<u64, u64, NullDisk>::new(config, hot_device, cold_device).unwrap();

    assert!(f2.cold_store.hash_index.is_cold());
    assert!(!f2.cold_store.hash_index.is_memory());
}

#[test]
fn test_store_stats_debug() {
    let stats = StoreStats {
        size: 1000,
        begin_address: Address::new(0, 0),
        tail_address: Address::new(100, 0),
        safe_read_only_address: Address::new(50, 0),
    };

    let debug_str = format!("{stats:?}");
    assert!(debug_str.contains("StoreStats"));
    assert!(debug_str.contains("size"));
    assert!(debug_str.contains("1000"));
}

#[test]
fn test_store_stats_clone() {
    let stats = StoreStats {
        size: 2000,
        begin_address: Address::new(10, 0),
        tail_address: Address::new(200, 0),
        safe_read_only_address: Address::new(100, 0),
    };

    let cloned = stats.clone();
    assert_eq!(stats.size, cloned.size);
    assert_eq!(stats.begin_address, cloned.begin_address);
    assert_eq!(stats.tail_address, cloned.tail_address);
    assert_eq!(stats.safe_read_only_address, cloned.safe_read_only_address);
}

#[test]
fn test_f2_save_checkpoint() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    let result = f2.save_checkpoint(temp_dir.path(), token);
    assert!(result.is_ok());
}

#[test]
fn test_f2_recover_not_found() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    // Should fail because checkpoint doesn't exist
    let result = f2.recover(temp_dir.path(), token);
    assert!(result.is_err());
}

#[test]
fn test_f2_checkpoint_during_checkpoint() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start first checkpoint
    let token1 = f2.checkpoint(false);
    assert!(token1.is_ok());

    // Second checkpoint should fail (already in progress)
    let token2 = f2.checkpoint(false);
    assert!(token2.is_err());

    // Reset for cleanup
    f2.checkpoint.reset();
}

#[test]
fn test_f2_start_session_during_checkpoint() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start checkpoint (changes phase from Rest)
    let _token = f2.checkpoint(false);

    // Try to start session during checkpoint - should fail
    let session_result = f2.start_session();
    assert!(session_result.is_err());

    f2.checkpoint.reset();
}

#[test]
fn test_f2_continue_session_during_checkpoint() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Start checkpoint
    let _token = f2.checkpoint(false);

    // Try to continue session during checkpoint - should fail
    let session_id = uuid::Uuid::new_v4();
    let result = f2.continue_session(session_id);
    assert!(result.is_err());

    f2.checkpoint.reset();
}

#[test]
fn test_store_index_find_entry_cold() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ColdIndexConfig::new(1024, 1024 * 1024, 0.5).with_root_path(temp_dir.path());
    let index = StoreIndex::new_cold(config).unwrap();

    let hash = KeyHash::new(12345);
    let result = index.find_entry(hash);
    // Should not find anything in empty index
    assert!(result.entry.is_unused());
}

#[test]
fn test_store_index_find_or_create_entry_cold() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ColdIndexConfig::new(1024, 1024 * 1024, 0.5).with_root_path(temp_dir.path());
    let index = StoreIndex::new_cold(config).unwrap();

    let hash = KeyHash::new(12345);
    let _result = index.find_or_create_entry(hash);
    // Entry should be created (or found)
}

#[test]
fn test_store_index_garbage_collect_cold() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ColdIndexConfig::new(1024, 1024 * 1024, 0.5).with_root_path(temp_dir.path());
    let index = StoreIndex::new_cold(config).unwrap();

    // Should not panic on cold index
    index.garbage_collect(Address::new(100, 0));
}

#[test]
fn test_f2_synchronous_checkpoint_flow_resets_phase() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();

    // 1. 发起 Checkpoint
    let token = f2.checkpoint(false).unwrap();
    assert_eq!(
        f2.checkpoint.phase.load(Ordering::Acquire),
        F2CheckpointPhase::HotStoreCheckpoint
    );

    // 2. 同步保存 Checkpoint
    f2.save_checkpoint(temp_dir.path(), token).unwrap();

    // 3. 验证状态已重置为 REST
    assert_eq!(
        f2.checkpoint.phase.load(Ordering::Acquire),
        F2CheckpointPhase::Rest
    );
    assert!(f2
        .checkpoint
        .hot_store_status
        .load(Ordering::Acquire)
        .is_done());
    assert!(f2
        .checkpoint
        .cold_store_status
        .load(Ordering::Acquire)
        .is_done());

    // 4. 验证可以开启新 session (这在之前会失败)
    let session = f2.start_session();
    assert!(session.is_ok());
}

#[test]
fn test_f2_multi_session_independent_epoch() {
    use std::sync::Arc;

    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = Arc::new(
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap(),
    );

    let f2_a = Arc::clone(&f2);
    let f2_b = Arc::clone(&f2);

    // Use a barrier so both threads start their sessions concurrently
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let handle_a = std::thread::spawn(move || {
        let _session = f2_a.start_session().unwrap();
        barrier_a.wait();

        // Thread A writes keys 0..50
        for i in 0u64..50 {
            f2_a.upsert(TestKey(i), TestValue(i * 10)).unwrap();
        }

        f2_a.stop_session();
    });

    let handle_b = std::thread::spawn(move || {
        let _session = f2_b.start_session().unwrap();
        barrier_b.wait();

        // Thread B writes keys 50..100
        for i in 50u64..100 {
            f2_b.upsert(TestKey(i), TestValue(i * 10)).unwrap();
        }

        f2_b.stop_session();
    });

    handle_a.join().unwrap();
    handle_b.join().unwrap();

    // Verify all writes from both threads are visible
    let _session = f2.start_session().unwrap();
    for i in 0u64..100 {
        let val = f2.read(&TestKey(i)).unwrap();
        assert_eq!(val, Some(TestValue(i * 10)), "key {i} should be readable");
    }
    f2.stop_session();
}

#[test]
fn test_mutable_fraction_wired_to_hlog() {
    use crate::f2::config::HotStoreConfig;

    // Use a custom mutable_fraction for hot store (0.5); cold store defaults to 0.0
    let config = F2Config::default().with_hot_store(
        HotStoreConfig::new()
            .with_mutable_fraction(0.5)
            .with_log_mem_size(16 * 1024 * 1024), // 16 MB
    );

    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    // Verify hot store: mutable_pages should be ~50% of memory_pages
    let hot_hlog_config = f2.hot_store.hlog().config();
    let expected_hot_mutable = (hot_hlog_config.memory_pages as f64 * 0.5).round() as u32;
    assert_eq!(
        hot_hlog_config.mutable_pages, expected_hot_mutable,
        "hot store mutable_pages should reflect mutable_fraction=0.5"
    );

    // Verify cold store: mutable_pages should be 0 (fraction=0.0)
    let cold_hlog_config = f2.cold_store.hlog().config();
    assert_eq!(
        cold_hlog_config.mutable_pages, 0,
        "cold store mutable_pages should be 0 for mutable_fraction=0.0"
    );
}

#[test]
fn test_f2_conditional_upsert_succeeds_with_matching_address() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    // Insert initial value
    f2.upsert(TestKey(42), TestValue(100)).unwrap();

    // Snapshot the current address
    let expected_addr = f2.get_hot_entry_address(&TestKey(42));
    assert!(
        expected_addr.is_valid(),
        "address should be valid after upsert"
    );

    // Conditional upsert with matching address should succeed
    let result = f2.conditional_upsert_into_hot(TestKey(42), TestValue(200), expected_addr);
    assert!(
        result.is_ok(),
        "conditional upsert should succeed with matching address"
    );

    // Verify the new value is readable
    let val = f2.read(&TestKey(42)).unwrap();
    assert_eq!(val, Some(TestValue(200)));

    f2.stop_session();
}

#[test]
fn test_f2_conditional_upsert_aborts_on_stale_address() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    // Insert initial value and snapshot the address
    f2.upsert(TestKey(42), TestValue(100)).unwrap();
    let stale_addr = f2.get_hot_entry_address(&TestKey(42));

    // Update the key again -- this changes the hash-index entry address
    f2.upsert(TestKey(42), TestValue(150)).unwrap();

    // The address should have changed
    let current_addr = f2.get_hot_entry_address(&TestKey(42));
    assert_ne!(
        stale_addr, current_addr,
        "address should change after second upsert"
    );

    // Conditional upsert with stale address should abort
    let result = f2.conditional_upsert_into_hot(TestKey(42), TestValue(200), stale_addr);
    assert_eq!(
        result,
        Err(Status::Aborted),
        "should abort on stale address"
    );

    // Value should remain at 150 (the second upsert)
    let val = f2.read(&TestKey(42)).unwrap();
    assert_eq!(val, Some(TestValue(150)));

    f2.stop_session();
}

#[test]
fn test_f2_checkpoint_roundtrip() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config.clone(), hot_device, cold_device).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();

    // Write 100 key-value pairs
    let _session = f2.start_session().unwrap();
    for i in 0u64..100 {
        f2.upsert(TestKey(i), TestValue(i * 100)).unwrap();
    }
    f2.stop_session();

    // Checkpoint: initiate and then save to disk
    let token = f2.checkpoint(false).unwrap();
    f2.save_checkpoint(temp_dir.path(), token).unwrap();

    // Drop the original store
    drop(f2);

    // Create a new F2Kv instance and recover
    let hot_device2 = NullDisk::new();
    let cold_device2 = NullDisk::new();
    let mut f2_recovered =
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device2, cold_device2).unwrap();

    let version = f2_recovered.recover(temp_dir.path(), token).unwrap();
    // checkpoint() calls initialize() which bumps version from 0 to 1
    assert_eq!(version, 1, "first checkpoint version should be 1");

    // Verify all 100 key-value pairs are readable
    let _session = f2_recovered.start_session().unwrap();
    for i in 0u64..100 {
        let result = f2_recovered.read(&TestKey(i)).unwrap();
        assert_eq!(
            result,
            Some(TestValue(i * 100)),
            "key {i} should have value {} after recovery",
            i * 100
        );
    }
    f2_recovered.stop_session();
}

#[test]
fn test_f2_rmw_concurrent_correctness() {
    use crate::f2::config::HotStoreConfig;

    // Use mutable_fraction=0.0 so ALL RMW operations go through the slow
    // (read-modify-conditional_upsert) path, exercising the retry loop.
    let config = F2Config::default().with_hot_store(
        HotStoreConfig::new()
            .with_mutable_fraction(0.0)
            .with_log_mem_size(64 * 1024 * 1024),
    );
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = Arc::new(
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap(),
    );

    // Start a session to insert the initial value.
    let _session = f2.start_session().unwrap();
    f2.upsert(TestKey(1), TestValue(0)).unwrap();
    f2.stop_session();

    let num_threads = 4;
    let increments_per_thread = 100;
    let barrier = Arc::new(std::sync::Barrier::new(num_threads));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let f2_clone = Arc::clone(&f2);
        let barrier_clone = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let _session = f2_clone.start_session().unwrap();
            barrier_clone.wait();
            for _ in 0..increments_per_thread {
                f2_clone
                    .rmw(TestKey(1), |v: &mut TestValue| {
                        v.0 += 1;
                    })
                    .unwrap();
            }
            f2_clone.stop_session();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify: the final value should be exactly num_threads * increments_per_thread.
    let _session = f2.start_session().unwrap();
    let final_value = f2.read(&TestKey(1)).unwrap();
    f2.stop_session();

    assert_eq!(
        final_value,
        Some(TestValue((num_threads * increments_per_thread) as u64)),
        "concurrent RMW should not lose updates"
    );
}

#[test]
fn test_f2_concurrent_crud() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = Arc::new(
        F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap(),
    );

    let num_threads = 4;
    let keys_per_thread = 250;
    let barrier = Arc::new(std::sync::Barrier::new(num_threads));
    let mut handles = vec![];

    for t in 0..num_threads {
        let f2_clone = Arc::clone(&f2);
        let barrier_clone = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            f2_clone.start_session().unwrap();
            barrier_clone.wait();
            let base = t * keys_per_thread;
            for i in 0..keys_per_thread {
                let key = TestKey((base + i) as u64);
                let value = TestValue((base + i) as u64 * 10);
                f2_clone.upsert(key, value).unwrap();
            }
            f2_clone.stop_session();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all writes from a single thread
    f2.start_session().unwrap();
    for t in 0..num_threads {
        let base = t * keys_per_thread;
        for i in 0..keys_per_thread {
            let key = TestKey((base + i) as u64);
            let expected = TestValue((base + i) as u64 * 10);
            let v = f2.read(&key).unwrap();
            assert_eq!(v, Some(expected), "Key {} missing", base + i);
        }
    }
    f2.stop_session();
}
