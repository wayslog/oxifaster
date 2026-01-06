//! Index Growth 集成测试
//!
//! 测试哈希索引动态扩容的各项功能。

use std::sync::Arc;
use std::thread;

use oxifaster::device::NullDisk;
use oxifaster::index::{
    calculate_num_chunks, get_chunk_bounds, GrowConfig, GrowResult, GrowState, MemHashIndex,
    MemHashIndexConfig, HASH_TABLE_CHUNK_SIZE,
};
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

// ============ Helper Functions ============

fn create_index(size: u64) -> MemHashIndex {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(size);
    index.initialize(&config);
    index
}

fn create_index_with_grow_config(size: u64, grow_config: GrowConfig) -> MemHashIndex {
    let mut index = MemHashIndex::with_grow_config(grow_config);
    let config = MemHashIndexConfig::new(size);
    index.initialize(&config);
    index
}

// ============ GrowConfig Tests ============

#[test]
fn test_grow_config_default() {
    let config = GrowConfig::default();

    assert_eq!(config.min_load_factor, 0.5);
    assert_eq!(config.max_load_factor, 0.9);
    assert_eq!(config.growth_factor, 2);
    assert!(!config.auto_grow);
}

#[test]
fn test_grow_config_builder() {
    let config = GrowConfig::new()
        .with_min_load_factor(0.4)
        .with_max_load_factor(0.8)
        .with_growth_factor(4)
        .with_auto_grow(true);

    assert_eq!(config.min_load_factor, 0.4);
    assert_eq!(config.max_load_factor, 0.8);
    assert_eq!(config.growth_factor, 4);
    assert!(config.auto_grow);
}

#[test]
fn test_grow_config_clamp_load_factor() {
    let config1 = GrowConfig::new().with_max_load_factor(1.5);
    assert_eq!(config1.max_load_factor, 1.0);

    let config2 = GrowConfig::new().with_max_load_factor(-0.5);
    assert_eq!(config2.max_load_factor, 0.0);

    let config3 = GrowConfig::new().with_min_load_factor(2.0);
    assert_eq!(config3.min_load_factor, 1.0);
}

#[test]
fn test_grow_config_min_growth_factor() {
    let config = GrowConfig::new().with_growth_factor(1);
    assert_eq!(config.growth_factor, 2); // Should be clamped to 2
}

#[test]
fn test_grow_config_should_grow() {
    let config = GrowConfig::new()
        .with_max_load_factor(0.8)
        .with_auto_grow(true);

    assert!(!config.should_grow(0.5));
    assert!(!config.should_grow(0.79));
    assert!(config.should_grow(0.8));
    assert!(config.should_grow(0.9));
}

#[test]
fn test_grow_config_should_grow_disabled() {
    let config = GrowConfig::new()
        .with_max_load_factor(0.8)
        .with_auto_grow(false);

    // Even with high load factor, should not grow when auto_grow is false
    assert!(!config.should_grow(0.95));
}

// ============ GrowState Tests ============

#[test]
fn test_grow_state_new() {
    let state = GrowState::new();

    assert!(!state.is_in_progress());
    assert_eq!(state.num_chunks(), 0);
}

#[test]
fn test_grow_state_initialize() {
    let mut state = GrowState::new();
    state.initialize(0, 10);

    assert!(state.is_in_progress());
    assert_eq!(state.old_version(), 0);
    assert_eq!(state.new_version(), 1);
    assert_eq!(state.num_chunks(), 10);
    assert_eq!(state.remaining_chunks(), 10);
}

#[test]
fn test_grow_state_initialize_version_flip() {
    let mut state = GrowState::new();

    state.initialize(0, 5);
    assert_eq!(state.old_version(), 0);
    assert_eq!(state.new_version(), 1);

    state.reset();

    state.initialize(1, 5);
    assert_eq!(state.old_version(), 1);
    assert_eq!(state.new_version(), 0);
}

#[test]
fn test_grow_state_get_next_chunk() {
    let mut state = GrowState::new();
    state.initialize(0, 3);

    assert_eq!(state.get_next_chunk(), Some(0));
    assert_eq!(state.get_next_chunk(), Some(1));
    assert_eq!(state.get_next_chunk(), Some(2));
    assert_eq!(state.get_next_chunk(), None);
    assert_eq!(state.get_next_chunk(), None); // Should continue returning None
}

#[test]
fn test_grow_state_complete_chunk() {
    let mut state = GrowState::new();
    state.initialize(0, 3);

    assert_eq!(state.remaining_chunks(), 3);

    assert!(!state.complete_chunk()); // Not last
    assert_eq!(state.remaining_chunks(), 2);

    assert!(!state.complete_chunk()); // Not last
    assert_eq!(state.remaining_chunks(), 1);

    assert!(state.complete_chunk()); // Last chunk!
    assert_eq!(state.remaining_chunks(), 0);
}

#[test]
fn test_grow_state_progress() {
    let mut state = GrowState::new();
    state.initialize(0, 10);

    let (completed, total) = state.progress();
    assert_eq!(completed, 0);
    assert_eq!(total, 10);

    state.complete_chunk();
    state.complete_chunk();
    state.complete_chunk();

    let (completed, total) = state.progress();
    assert_eq!(completed, 3);
    assert_eq!(total, 10);
}

#[test]
fn test_grow_state_reset() {
    let mut state = GrowState::new();
    state.initialize(0, 5);
    state.get_next_chunk();
    state.complete_chunk();

    state.reset();

    assert!(!state.is_in_progress());
    assert_eq!(state.num_chunks(), 0);
    assert_eq!(state.remaining_chunks(), 0);
}

#[test]
fn test_grow_state_record_chunk_result() {
    let mut state = GrowState::new();
    state.initialize(0, 2);

    state.record_chunk_result(100, 5, 2);
    state.record_chunk_result(150, 3, 1);

    assert_eq!(state.get_entries_migrated(), 250);
    assert_eq!(state.get_overflow_buckets_skipped(), 8);
    assert_eq!(state.get_rehash_failures(), 3);
}

// ============ GrowResult Tests ============

#[test]
fn test_grow_result_success() {
    let result = GrowResult::success(1024, 2048, 800, 50);

    assert!(result.success);
    assert_eq!(result.old_size, 1024);
    assert_eq!(result.new_size, 2048);
    assert_eq!(result.entries_migrated, 800);
    assert_eq!(result.duration_ms, 50);
    assert!(result.status.is_none());
    assert_eq!(result.overflow_buckets_skipped, 0);
}

#[test]
fn test_grow_result_failure() {
    let result = GrowResult::failure(Status::Aborted);

    assert!(!result.success);
    assert_eq!(result.status, Some(Status::Aborted));
}

#[test]
fn test_grow_result_growth_ratio() {
    let result = GrowResult::success(100, 200, 50, 10);
    assert_eq!(result.growth_ratio(), 2.0);

    let result2 = GrowResult::success(100, 400, 50, 10);
    assert_eq!(result2.growth_ratio(), 4.0);
}

#[test]
fn test_grow_result_growth_ratio_zero_old_size() {
    let result = GrowResult::success(0, 100, 50, 10);
    assert_eq!(result.growth_ratio(), 0.0);
}

#[test]
fn test_grow_result_with_overflow() {
    let result = GrowResult::success_with_overflow(1024, 2048, 750, 60, 50);

    assert!(!result.success); // Not successful if overflow
    assert!(result.has_overflow_warning());
    assert_eq!(result.overflow_buckets_skipped, 50);
}

#[test]
fn test_grow_result_with_data_loss_tracking() {
    let result = GrowResult::with_data_loss_tracking(1024, 2048, 700, 70, 30, 20);

    assert!(!result.success);
    assert!(result.has_overflow_warning());
    assert!(result.has_rehash_failures());
    assert!(result.has_data_loss_warning());
    assert_eq!(result.overflow_buckets_skipped, 30);
    assert_eq!(result.rehash_failures, 20);
}

#[test]
fn test_grow_result_no_data_loss() {
    let result = GrowResult::with_data_loss_tracking(1024, 2048, 800, 50, 0, 0);

    assert!(result.success);
    assert!(!result.has_overflow_warning());
    assert!(!result.has_rehash_failures());
    assert!(!result.has_data_loss_warning());
}

// ============ Utility Function Tests ============

#[test]
fn test_calculate_num_chunks() {
    assert_eq!(calculate_num_chunks(0), 0);
    assert_eq!(calculate_num_chunks(1), 1);
    assert_eq!(calculate_num_chunks(HASH_TABLE_CHUNK_SIZE), 1);
    assert_eq!(calculate_num_chunks(HASH_TABLE_CHUNK_SIZE + 1), 2);
    assert_eq!(calculate_num_chunks(HASH_TABLE_CHUNK_SIZE * 2), 2);
    assert_eq!(calculate_num_chunks(HASH_TABLE_CHUNK_SIZE * 2 + 1), 3);
}

#[test]
fn test_get_chunk_bounds() {
    let total = 50000u64;

    let (start0, end0) = get_chunk_bounds(0, total);
    assert_eq!(start0, 0);
    assert_eq!(end0, HASH_TABLE_CHUNK_SIZE);

    let (start1, end1) = get_chunk_bounds(1, total);
    assert_eq!(start1, HASH_TABLE_CHUNK_SIZE);
    assert_eq!(end1, HASH_TABLE_CHUNK_SIZE * 2);
}

#[test]
fn test_get_chunk_bounds_last_chunk() {
    let total = 50000u64;
    let last_chunk = calculate_num_chunks(total) - 1;
    let (_, end) = get_chunk_bounds(last_chunk, total);
    assert_eq!(end, total);
}

#[test]
fn test_get_chunk_bounds_exact_multiple() {
    let total = HASH_TABLE_CHUNK_SIZE * 3;
    let num_chunks = calculate_num_chunks(total);
    assert_eq!(num_chunks, 3);

    let (_, end) = get_chunk_bounds(2, total);
    assert_eq!(end, total);
}

// ============ MemHashIndex Tests ============

#[test]
fn test_index_creation() {
    let index = create_index(1024);

    assert_eq!(index.size(), 1024);
    assert_eq!(index.version(), 0);
    assert!(!index.is_grow_in_progress());
}

#[test]
fn test_index_with_grow_config() {
    let config = GrowConfig::new()
        .with_auto_grow(true)
        .with_max_load_factor(0.7);
    let index = create_index_with_grow_config(1024, config);

    assert!(index.grow_config().auto_grow);
    assert_eq!(index.grow_config().max_load_factor, 0.7);
}

#[test]
fn test_index_size_must_be_power_of_two() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1000); // Not power of 2
    let status = index.initialize(&config);
    assert_eq!(status, Status::Corruption);
}

#[test]
fn test_index_set_grow_config() {
    let mut index = create_index(1024);

    let new_config = GrowConfig::new().with_growth_factor(4);
    index.set_grow_config(new_config);

    assert_eq!(index.grow_config().growth_factor, 4);
}

#[test]
fn test_index_start_grow() {
    let mut index = create_index(1024);

    let result = index.start_grow();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2048); // 1024 * 2

    assert!(index.is_grow_in_progress());
}

#[test]
fn test_index_start_grow_already_in_progress() {
    let mut index = create_index(1024);

    // First grow should succeed
    assert!(index.start_grow().is_ok());

    // Second grow should fail
    let result = index.start_grow();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Status::Aborted);
}

#[test]
fn test_index_grow_state_access() {
    let index = create_index(1024);

    let state = index.grow_state();
    assert!(!state.is_in_progress());
}

// ============ FasterKv Integration Tests ============

#[test]
fn test_store_index_stats() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));

    // Insert some data
    {
        let mut session = store.start_session();
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }
    }

    let stats = store.index_stats();
    assert!(stats.used_entries > 0);
    assert!(stats.load_factor > 0.0);
    assert!(stats.load_factor < 1.0);
}

// ============ Concurrent Tests ============

#[test]
fn test_grow_state_concurrent_chunk_get() {
    let mut state = GrowState::new();
    state.initialize(0, 100);

    let state = Arc::new(state);
    let num_threads = 4;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let state = state.clone();
            thread::spawn(move || {
                let mut chunks = Vec::new();
                while let Some(chunk) = state.get_next_chunk() {
                    chunks.push(chunk);
                }
                chunks
            })
        })
        .collect();

    let all_chunks: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    // All chunks should be claimed exactly once
    assert_eq!(all_chunks.len(), 100);

    // Check all chunks are unique
    let mut sorted = all_chunks.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 100);
}

// ============ Edge Cases ============

#[test]
fn test_grow_state_zero_chunks() {
    let mut state = GrowState::new();
    state.initialize(0, 0);

    assert!(state.is_in_progress());
    assert_eq!(state.num_chunks(), 0);
    assert_eq!(state.get_next_chunk(), None);
}

#[test]
fn test_grow_state_single_chunk() {
    let mut state = GrowState::new();
    state.initialize(0, 1);

    assert_eq!(state.get_next_chunk(), Some(0));
    assert_eq!(state.get_next_chunk(), None);

    assert!(state.complete_chunk()); // Single chunk is also last
}

#[test]
fn test_get_chunk_bounds_small_table() {
    let total = 100u64; // Smaller than chunk size
    let (start, end) = get_chunk_bounds(0, total);

    assert_eq!(start, 0);
    assert_eq!(end, total);
}

#[test]
fn test_hash_table_chunk_size_value() {
    // Verify the chunk size constant
    assert_eq!(HASH_TABLE_CHUNK_SIZE, 16384);
}
