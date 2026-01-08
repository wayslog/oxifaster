use super::*;

use super::store_index::StoreIndex;
use super::types::IndexType;
use crate::address::Address;
use crate::device::NullDisk;
use crate::f2::HotToColdMigrationStrategy;
use crate::index::ColdIndexConfig;
use crate::index::KeyHash;
use crate::status::Status;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct TestKey(u64);

impl Key for TestKey {
    fn size(&self) -> u32 {
        std::mem::size_of::<Self>() as u32
    }

    fn get_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
struct TestValue(u64);

impl Value for TestValue {
    fn size(&self) -> u32 {
        std::mem::size_of::<Self>() as u32
    }
}

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

    let hash1 = KeyHash::new(1u64.get_hash());
    let hash2 = KeyHash::new(2u64.get_hash());

    // key1 保留在 hot（hot index 仍可命中）
    assert!(f2.hot_store.hash_index.find_entry(hash1).found());
    assert!(!f2.cold_store.hash_index.find_entry(hash1).found());

    // key2 迁移到 cold（hot index 清空，cold index 命中）
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
fn test_f2_rejects_non_pod_types() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();

    // 非 POD 类型（包含堆指针，需要 drop）目前不支持。
    let err = match F2Kv::<String, String, NullDisk>::new(config, hot_device, cold_device) {
        Ok(_) => panic!("预期创建失败，但实际成功"),
        Err(e) => e,
    };
    assert!(err.contains("POD"));
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
