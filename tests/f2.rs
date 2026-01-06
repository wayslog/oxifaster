//! F2 Architecture 集成测试
//!
//! 测试 F2 热冷分离架构的各项功能。

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use oxifaster::cache::ReadCacheConfig;
use oxifaster::f2::{
    ColdStoreConfig, F2CheckpointPhase, F2CheckpointState, F2CompactionConfig, F2Config,
    HotStoreConfig, ReadOperationStage, RmwOperationStage, StoreCheckpointStatus, StoreType,
};

// ============ HotStoreConfig Tests ============

#[test]
fn test_hot_store_config_default() {
    let config = HotStoreConfig::default();

    assert_eq!(config.index_size, 1 << 20);
    assert_eq!(config.log_mem_size, 512 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.6);
    assert!(config.read_cache.is_some());
}

#[test]
fn test_hot_store_config_builder() {
    let config = HotStoreConfig::new()
        .with_index_size(1 << 16)
        .with_log_mem_size(128 * 1024 * 1024)
        .with_mutable_fraction(0.8)
        .with_log_path(PathBuf::from("custom_hot.log"))
        .with_read_cache(None);

    assert_eq!(config.index_size, 1 << 16);
    assert_eq!(config.log_mem_size, 128 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.8);
    assert_eq!(config.log_path, PathBuf::from("custom_hot.log"));
    assert!(config.read_cache.is_none());
}

#[test]
fn test_hot_store_config_mutable_fraction_clamp() {
    let config1 = HotStoreConfig::new().with_mutable_fraction(1.5);
    assert_eq!(config1.mutable_fraction, 1.0);

    let config2 = HotStoreConfig::new().with_mutable_fraction(-0.5);
    assert_eq!(config2.mutable_fraction, 0.0);
}

#[test]
fn test_hot_store_config_with_read_cache() {
    let cache_config = ReadCacheConfig::new(32 * 1024 * 1024);
    let config = HotStoreConfig::new().with_read_cache(Some(cache_config));

    assert!(config.read_cache.is_some());
    assert_eq!(config.read_cache.unwrap().mem_size, 32 * 1024 * 1024);
}

// ============ ColdStoreConfig Tests ============

#[test]
fn test_cold_store_config_default() {
    let config = ColdStoreConfig::default();

    assert_eq!(config.index_size, 1 << 22);
    assert_eq!(config.log_mem_size, 256 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.0); // Cold store is read-only in memory
}

#[test]
fn test_cold_store_config_builder() {
    let config = ColdStoreConfig::new()
        .with_index_size(1 << 20)
        .with_log_mem_size(64 * 1024 * 1024)
        .with_log_path(PathBuf::from("custom_cold.log"));

    assert_eq!(config.index_size, 1 << 20);
    assert_eq!(config.log_mem_size, 64 * 1024 * 1024);
    assert_eq!(config.log_path, PathBuf::from("custom_cold.log"));
}

// ============ F2CompactionConfig Tests ============

#[test]
fn test_compaction_config_default() {
    let config = F2CompactionConfig::default();

    assert!(config.hot_store_enabled);
    assert!(config.cold_store_enabled);
    assert_eq!(config.trigger_percentage, 0.9);
    assert_eq!(config.compact_percentage, 0.2);
    assert_eq!(config.num_threads, 1);
}

#[test]
fn test_compaction_config_builder() {
    let config = F2CompactionConfig::new()
        .with_hot_store_enabled(false)
        .with_cold_store_enabled(true)
        .with_hot_log_size_budget(2 << 30)
        .with_cold_log_size_budget(20 << 30)
        .with_trigger_percentage(0.8)
        .with_check_interval(Duration::from_secs(10));

    assert!(!config.hot_store_enabled);
    assert!(config.cold_store_enabled);
    assert_eq!(config.hot_log_size_budget, 2 << 30);
    assert_eq!(config.cold_log_size_budget, 20 << 30);
    assert_eq!(config.trigger_percentage, 0.8);
    assert_eq!(config.check_interval, Duration::from_secs(10));
}

#[test]
fn test_compaction_config_trigger_percentage_clamp() {
    let config1 = F2CompactionConfig::new().with_trigger_percentage(1.5);
    assert_eq!(config1.trigger_percentage, 1.0);

    let config2 = F2CompactionConfig::new().with_trigger_percentage(-0.5);
    assert_eq!(config2.trigger_percentage, 0.0);
}

// ============ F2Config Tests ============

#[test]
fn test_f2_config_default() {
    let config = F2Config::default();

    assert_eq!(config.hot_store.index_size, 1 << 20);
    assert_eq!(config.cold_store.index_size, 1 << 22);
    assert!(config.compaction.hot_store_enabled);
}

#[test]
fn test_f2_config_builder() {
    let config = F2Config::new()
        .with_hot_store(HotStoreConfig::new().with_index_size(1 << 18))
        .with_cold_store(ColdStoreConfig::new().with_index_size(1 << 20))
        .with_compaction(F2CompactionConfig::new().with_hot_store_enabled(false));

    assert_eq!(config.hot_store.index_size, 1 << 18);
    assert_eq!(config.cold_store.index_size, 1 << 20);
    assert!(!config.compaction.hot_store_enabled);
}

#[test]
fn test_f2_config_validate_ok() {
    let config = F2Config::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_f2_config_validate_hot_budget_too_small() {
    let mut config = F2Config::default();
    config.compaction.hot_log_size_budget = 1 << 20; // 1 MB - too small
    assert!(config.validate().is_err());
}

#[test]
fn test_f2_config_validate_cold_budget_too_small() {
    let mut config = F2Config::default();
    config.compaction.cold_log_size_budget = 1 << 20; // 1 MB - too small
    assert!(config.validate().is_err());
}

#[test]
fn test_f2_config_validate_small_budget_with_disabled_compaction() {
    let mut config = F2Config::default();
    config.compaction.hot_store_enabled = false;
    config.compaction.hot_log_size_budget = 1 << 20; // OK when disabled

    // Should pass for hot store (disabled)
    // But cold store is still enabled with default budget, so should pass
    assert!(config.validate().is_ok());
}

// ============ StoreType Tests ============

#[test]
fn test_store_type_values() {
    assert_eq!(StoreType::Hot, StoreType::Hot);
    assert_eq!(StoreType::Cold, StoreType::Cold);
    assert_ne!(StoreType::Hot, StoreType::Cold);
}

#[test]
fn test_store_type_debug() {
    let hot = StoreType::Hot;
    let cold = StoreType::Cold;

    assert_eq!(format!("{:?}", hot), "Hot");
    assert_eq!(format!("{:?}", cold), "Cold");
}

// ============ ReadOperationStage Tests ============

#[test]
fn test_read_operation_stage_values() {
    assert_eq!(ReadOperationStage::HotLogRead, ReadOperationStage::HotLogRead);
    assert_eq!(ReadOperationStage::ColdLogRead, ReadOperationStage::ColdLogRead);
    assert_ne!(ReadOperationStage::HotLogRead, ReadOperationStage::ColdLogRead);
}

#[test]
fn test_read_operation_stage_debug() {
    assert_eq!(format!("{:?}", ReadOperationStage::HotLogRead), "HotLogRead");
    assert_eq!(format!("{:?}", ReadOperationStage::ColdLogRead), "ColdLogRead");
}

// ============ RmwOperationStage Tests ============

#[test]
fn test_rmw_operation_stage_values() {
    assert_eq!(RmwOperationStage::HotLogRmw, RmwOperationStage::HotLogRmw);
    assert_eq!(RmwOperationStage::ColdLogRead, RmwOperationStage::ColdLogRead);
    assert_eq!(
        RmwOperationStage::HotLogConditionalInsert,
        RmwOperationStage::HotLogConditionalInsert
    );
}

#[test]
fn test_rmw_operation_stage_debug() {
    assert_eq!(format!("{:?}", RmwOperationStage::HotLogRmw), "HotLogRmw");
    assert_eq!(format!("{:?}", RmwOperationStage::ColdLogRead), "ColdLogRead");
    assert_eq!(
        format!("{:?}", RmwOperationStage::HotLogConditionalInsert),
        "HotLogConditionalInsert"
    );
}

// ============ F2CheckpointPhase Tests ============

#[test]
fn test_checkpoint_phase_from_u8() {
    assert_eq!(F2CheckpointPhase::from_u8(0), Some(F2CheckpointPhase::Rest));
    assert_eq!(
        F2CheckpointPhase::from_u8(1),
        Some(F2CheckpointPhase::HotStoreCheckpoint)
    );
    assert_eq!(
        F2CheckpointPhase::from_u8(2),
        Some(F2CheckpointPhase::ColdStoreCheckpoint)
    );
    assert_eq!(F2CheckpointPhase::from_u8(3), Some(F2CheckpointPhase::Recover));
    assert_eq!(F2CheckpointPhase::from_u8(4), None);
    assert_eq!(F2CheckpointPhase::from_u8(255), None);
}

#[test]
fn test_checkpoint_phase_default() {
    let phase = F2CheckpointPhase::default();
    assert_eq!(phase, F2CheckpointPhase::Rest);
}

// ============ StoreCheckpointStatus Tests ============

#[test]
fn test_checkpoint_status_from_u8() {
    assert_eq!(StoreCheckpointStatus::from_u8(0), Some(StoreCheckpointStatus::Idle));
    assert_eq!(StoreCheckpointStatus::from_u8(1), Some(StoreCheckpointStatus::Requested));
    assert_eq!(StoreCheckpointStatus::from_u8(2), Some(StoreCheckpointStatus::Active));
    assert_eq!(StoreCheckpointStatus::from_u8(3), Some(StoreCheckpointStatus::Finished));
    assert_eq!(StoreCheckpointStatus::from_u8(4), Some(StoreCheckpointStatus::Failed));
    assert_eq!(StoreCheckpointStatus::from_u8(5), None);
}

#[test]
fn test_checkpoint_status_is_done() {
    assert!(!StoreCheckpointStatus::Idle.is_done());
    assert!(!StoreCheckpointStatus::Requested.is_done());
    assert!(!StoreCheckpointStatus::Active.is_done());
    assert!(StoreCheckpointStatus::Finished.is_done());
    assert!(StoreCheckpointStatus::Failed.is_done());
}

#[test]
fn test_checkpoint_status_default() {
    let status = StoreCheckpointStatus::default();
    assert_eq!(status, StoreCheckpointStatus::Idle);
}

// ============ F2CheckpointState Tests ============

#[test]
fn test_checkpoint_state_new() {
    let state = F2CheckpointState::new();

    assert!(!state.is_in_progress());
    assert_eq!(state.version(), 0);
    assert_eq!(state.token(), uuid::Uuid::nil());
}

#[test]
fn test_checkpoint_state_initialize() {
    let mut state = F2CheckpointState::new();
    let token = uuid::Uuid::new_v4();

    state.initialize(token, 4);

    assert_eq!(state.token(), token);
    assert_eq!(state.version(), 1); // Version incremented
}

#[test]
fn test_checkpoint_state_version_increments() {
    let mut state = F2CheckpointState::new();

    assert_eq!(state.version(), 0);

    state.initialize(uuid::Uuid::new_v4(), 1);
    assert_eq!(state.version(), 1);

    state.reset();
    state.initialize(uuid::Uuid::new_v4(), 1);
    assert_eq!(state.version(), 2);
}

#[test]
fn test_checkpoint_state_set_version() {
    let state = F2CheckpointState::new();

    state.set_version(42);
    assert_eq!(state.version(), 42);
}

#[test]
fn test_checkpoint_state_reset() {
    let mut state = F2CheckpointState::new();
    let token = uuid::Uuid::new_v4();

    state.initialize(token, 4);
    state.phase.store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);

    state.reset();

    assert!(!state.is_in_progress());
    assert_eq!(state.token(), uuid::Uuid::nil());
}

#[test]
fn test_checkpoint_state_is_in_progress() {
    let mut state = F2CheckpointState::new();

    assert!(!state.is_in_progress());

    state.phase.store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
    assert!(state.is_in_progress());

    state.phase.store(F2CheckpointPhase::ColdStoreCheckpoint, Ordering::Release);
    assert!(state.is_in_progress());

    state.phase.store(F2CheckpointPhase::Rest, Ordering::Release);
    assert!(!state.is_in_progress());
}

#[test]
fn test_checkpoint_state_persistent_serial_nums() {
    let state = F2CheckpointState::new();

    state.set_persistent_serial_num(0, 100);
    state.set_persistent_serial_num(5, 500);
    state.set_persistent_serial_num(95, 9500);

    assert_eq!(state.get_persistent_serial_num(0), 100);
    assert_eq!(state.get_persistent_serial_num(5), 500);
    assert_eq!(state.get_persistent_serial_num(95), 9500);
}

#[test]
fn test_checkpoint_state_persistent_serial_num_out_of_range() {
    let state = F2CheckpointState::new();

    state.set_persistent_serial_num(100, 1000); // Out of range (max is 95)
    assert_eq!(state.get_persistent_serial_num(100), 0);
}

#[test]
fn test_checkpoint_state_decrement_pending() {
    let mut state = F2CheckpointState::new();
    state.initialize(uuid::Uuid::new_v4(), 3);

    // Decrement hot store pending
    let remaining = state.decrement_pending_hot_store();
    assert_eq!(remaining, 3); // Returns old value

    let remaining = state.decrement_pending_hot_store();
    assert_eq!(remaining, 2);

    // Decrement callback pending
    let remaining = state.decrement_pending_callback();
    assert_eq!(remaining, 3);
}

// ============ Atomic Wrappers Tests ============
// Note: AtomicCheckpointPhase and AtomicCheckpointStatus are tested through
// F2CheckpointState which uses them internally. Direct tests are not possible
// as the state module is private.

#[test]
fn test_checkpoint_state_phase_through_state() {
    let mut state = F2CheckpointState::new();

    // Initial phase is Rest
    assert!(!state.is_in_progress());

    // Change phase through state's phase field
    state.phase.store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
    assert!(state.is_in_progress());
    assert_eq!(state.phase.load(Ordering::Acquire), F2CheckpointPhase::HotStoreCheckpoint);

    state.phase.store(F2CheckpointPhase::Rest, Ordering::Release);
    assert!(!state.is_in_progress());
}

#[test]
fn test_checkpoint_state_status_through_state() {
    let mut state = F2CheckpointState::new();
    state.initialize(uuid::Uuid::new_v4(), 1);

    // Test hot store status
    state.hot_store_status.store(StoreCheckpointStatus::Active, Ordering::Release);
    assert_eq!(state.hot_store_status.load(Ordering::Acquire), StoreCheckpointStatus::Active);

    state.hot_store_status.store(StoreCheckpointStatus::Finished, Ordering::Release);
    assert!(state.hot_store_status.load(Ordering::Acquire).is_done());

    // Test cold store status
    state.cold_store_status.store(StoreCheckpointStatus::Failed, Ordering::Release);
    assert!(state.cold_store_status.load(Ordering::Acquire).is_done());
}

// ============ Integration Scenarios ============

#[test]
fn test_typical_checkpoint_flow() {
    let mut state = F2CheckpointState::new();

    // Start checkpoint
    let token = uuid::Uuid::new_v4();
    state.initialize(token, 2);

    // Hot store checkpoint begins
    state.phase.store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
    state.hot_store_status.store(StoreCheckpointStatus::Active, Ordering::Release);

    assert!(state.is_in_progress());

    // Hot store checkpoint completes
    state.hot_store_status.store(StoreCheckpointStatus::Finished, Ordering::Release);

    // Cold store checkpoint begins
    state.phase.store(F2CheckpointPhase::ColdStoreCheckpoint, Ordering::Release);
    state.cold_store_status.store(StoreCheckpointStatus::Active, Ordering::Release);

    // Cold store checkpoint completes
    state.cold_store_status.store(StoreCheckpointStatus::Finished, Ordering::Release);

    // All done
    state.phase.store(F2CheckpointPhase::Rest, Ordering::Release);
    assert!(!state.is_in_progress());
    assert!(state.hot_store_status.load(Ordering::Acquire).is_done());
    assert!(state.cold_store_status.load(Ordering::Acquire).is_done());
}

#[test]
fn test_checkpoint_failure_handling() {
    let state = F2CheckpointState::new();

    state.phase.store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
    state.hot_store_status.store(StoreCheckpointStatus::Active, Ordering::Release);

    // Simulate failure
    state.hot_store_status.store(StoreCheckpointStatus::Failed, Ordering::Release);

    assert!(state.hot_store_status.load(Ordering::Acquire).is_done());
    assert_eq!(
        state.hot_store_status.load(Ordering::Acquire),
        StoreCheckpointStatus::Failed
    );
}
