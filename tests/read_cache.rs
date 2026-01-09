//! Read Cache 集成测试
//!
//! 测试 Read Cache 的各项功能，包括配置、缓存操作、淘汰机制等。

use std::sync::Arc;
use std::thread;

use oxifaster::cache::{ReadCache, ReadCacheConfig, ReadCacheStats};
use oxifaster::device::NullDisk;
use oxifaster::record::{Key, Value};
use oxifaster::store::{FasterKv, FasterKvConfig};

// ============ Test Types ============

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

// ============ Helper Functions ============

fn create_store_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 14,       // 16 KB pages
        mutable_fraction: 0.9,
    }
}

fn create_cache_config() -> ReadCacheConfig {
    ReadCacheConfig::new(1024 * 1024) // 1 MB
        .with_mutable_fraction(0.9)
        .with_pre_allocate(false)
}

fn create_store_with_cache() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = create_store_config();
    let device = NullDisk::new();
    let cache_config = create_cache_config();
    Arc::new(FasterKv::with_read_cache(config, device, cache_config))
}

// ============ ReadCacheConfig Tests ============

#[test]
fn test_cache_config_default() {
    let config = ReadCacheConfig::default();

    assert_eq!(config.mem_size, 256 * 1024 * 1024); // 256 MB
    assert_eq!(config.mutable_fraction, 0.9);
    assert!(!config.pre_allocate);
    assert!(config.copy_to_tail);
}

#[test]
fn test_cache_config_builder() {
    let config = ReadCacheConfig::new(64 * 1024 * 1024)
        .with_mutable_fraction(0.8)
        .with_pre_allocate(true)
        .with_copy_to_tail(false);

    assert_eq!(config.mem_size, 64 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.8);
    assert!(config.pre_allocate);
    assert!(!config.copy_to_tail);
}

#[test]
fn test_cache_config_region_sizes() {
    let config = ReadCacheConfig::new(100 * 1024 * 1024).with_mutable_fraction(0.9);

    let mutable = config.mutable_size();
    let read_only = config.read_only_size();

    // 90% mutable
    assert!((89 * 1024 * 1024..=91 * 1024 * 1024).contains(&mutable));
    // 10% read-only
    assert!((9 * 1024 * 1024..=11 * 1024 * 1024).contains(&read_only));
}

#[test]
fn test_cache_config_clamp_fraction() {
    // Fraction should be clamped to [0.0, 1.0]
    let config1 = ReadCacheConfig::new(1024).with_mutable_fraction(1.5);
    assert_eq!(config1.mutable_fraction, 1.0);

    let config2 = ReadCacheConfig::new(1024).with_mutable_fraction(-0.5);
    assert_eq!(config2.mutable_fraction, 0.0);
}

// ============ ReadCache Unit Tests ============

#[test]
fn test_read_cache_create() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    assert_eq!(cache.tail_address().control(), 64);
    assert_eq!(cache.stats().read_calls(), 0);
}

#[test]
fn test_read_cache_insert_and_read() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    let key = TestKey(42);
    let value = TestValue(100);
    let prev_addr = oxifaster::Address::new(1, 500);

    // Insert
    let result = cache.try_insert(&key, &value, prev_addr, false);
    assert!(result.is_ok());

    let cache_addr = result.unwrap();
    assert!(cache_addr.in_readcache());

    // Read
    let read_result = cache.read(cache_addr, &key);
    assert!(read_result.is_some());

    let (read_value, rc_info) = read_result.unwrap();
    assert_eq!(read_value, value);
    assert_eq!(rc_info.get_previous_address(), prev_addr);
}

#[test]
fn test_read_cache_miss_wrong_key() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    let key = TestKey(42);
    let wrong_key = TestKey(999);
    let value = TestValue(100);

    let cache_addr = cache
        .try_insert(&key, &value, oxifaster::Address::INVALID, false)
        .unwrap();

    // Read with wrong key should miss
    let result = cache.read(cache_addr, &wrong_key);
    assert!(result.is_none());
}

#[test]
fn test_read_cache_skip() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    let key = TestKey(42);
    let value = TestValue(100);
    let prev_addr = oxifaster::Address::new(1, 500);

    let cache_addr = cache.try_insert(&key, &value, prev_addr, false).unwrap();

    // Skip should return the previous address
    let skipped = cache.skip(cache_addr);
    assert_eq!(skipped, prev_addr);

    // Skip on non-cache address should return same address
    let hlog_addr = oxifaster::Address::new(5, 100);
    assert_eq!(cache.skip(hlog_addr), hlog_addr);
}

#[test]
fn test_read_cache_invalidate() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    let key = TestKey(42);
    let value = TestValue(100);

    let cache_addr = cache
        .try_insert(&key, &value, oxifaster::Address::INVALID, false)
        .unwrap();

    // Read should succeed before invalidation
    assert!(cache.read(cache_addr, &key).is_some());

    // Invalidate the entry
    cache.invalidate(cache_addr, &key);

    // Read should fail after invalidation
    assert!(cache.read(cache_addr, &key).is_none());
}

#[test]
fn test_read_cache_clear() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    // Insert some data
    for i in 0..10 {
        let key = TestKey(i);
        let value = TestValue(i * 100);
        cache
            .try_insert(&key, &value, oxifaster::Address::INVALID, false)
            .unwrap();
    }

    // Verify stats before clear
    assert!(cache.stats().insert_calls() > 0);

    // Clear the cache
    cache.clear();

    // Stats should be reset
    assert_eq!(cache.stats().insert_calls(), 0);
    assert_eq!(cache.tail_address().control(), 64);
}

// ============ ReadCacheStats Tests ============

#[test]
fn test_read_cache_stats_recording() {
    let stats = ReadCacheStats::new();

    stats.record_read();
    stats.record_read();
    stats.record_hit();
    stats.record_miss();

    assert_eq!(stats.read_calls(), 2);
    assert_eq!(stats.read_hits(), 1);
    assert_eq!(stats.read_misses(), 1);
    assert_eq!(stats.hit_rate(), 0.5);
}

#[test]
fn test_read_cache_stats_insert() {
    let stats = ReadCacheStats::new();

    stats.record_insert();
    stats.record_insert();
    stats.record_insert_success();

    assert_eq!(stats.insert_calls(), 2);
    assert_eq!(stats.insert_rate(), 0.5);
}

#[test]
fn test_read_cache_stats_eviction() {
    let stats = ReadCacheStats::new();

    stats.record_eviction(100, 20);

    assert_eq!(stats.evicted_records(), 100);
    assert_eq!(stats.eviction_invalid_rate(), 0.2);
}

#[test]
fn test_read_cache_stats_reset() {
    let stats = ReadCacheStats::new();

    stats.record_read();
    stats.record_hit();
    stats.record_insert();

    stats.reset();

    assert_eq!(stats.read_calls(), 0);
    assert_eq!(stats.read_hits(), 0);
    assert_eq!(stats.insert_calls(), 0);
}

#[test]
fn test_read_cache_stats_summary() {
    let stats = ReadCacheStats::new();

    stats.record_read();
    stats.record_hit();
    stats.record_insert();
    stats.record_insert_success();

    let summary = stats.summary();

    assert_eq!(summary.read_calls, 1);
    assert_eq!(summary.read_hits, 1);
    assert_eq!(summary.hit_rate, 1.0);
    assert_eq!(summary.insert_calls, 1);
    assert_eq!(summary.insert_rate, 1.0);
}

// ============ FasterKv Integration Tests ============

#[test]
fn test_fasterkv_with_read_cache() {
    let store = create_store_with_cache();

    // Verify cache is enabled
    assert!(store.read_cache_stats().is_some());
    assert!(store.read_cache_config().is_some());
}

#[test]
fn test_fasterkv_cache_operations() {
    let store = create_store_with_cache();

    // Insert data
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }
    }

    // Read data (will populate cache)
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            let result = session.read(&i);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(i * 10));
        }
    }

    // Verify cache is configured (stats may not be recorded if all reads hit memory)
    assert!(store.read_cache_config().is_some());
}

#[test]
fn test_fasterkv_cache_invalidation_on_update() {
    let store = create_store_with_cache();
    let key = 42u64;

    // Insert initial value
    {
        let mut session = store.start_session().unwrap();
        session.upsert(key, 100);
    }

    // Read to populate cache
    {
        let mut session = store.start_session().unwrap();
        let result = session.read(&key);
        assert_eq!(result.unwrap(), Some(100));
    }

    // Update value
    {
        let mut session = store.start_session().unwrap();
        session.upsert(key, 200);
    }

    // Read should return updated value
    {
        let mut session = store.start_session().unwrap();
        let result = session.read(&key);
        assert_eq!(result.unwrap(), Some(200));
    }
}

#[test]
fn test_fasterkv_cache_invalidation_on_delete() {
    let store = create_store_with_cache();
    let key = 42u64;

    // Insert value
    {
        let mut session = store.start_session().unwrap();
        session.upsert(key, 100);
    }

    // Read to populate cache
    {
        let mut session = store.start_session().unwrap();
        let result = session.read(&key);
        assert_eq!(result.unwrap(), Some(100));
    }

    // Delete
    {
        let mut session = store.start_session().unwrap();
        session.delete(&key);
    }

    // Read should return None
    {
        let mut session = store.start_session().unwrap();
        let result = session.read(&key);
        assert_eq!(result.unwrap(), None);
    }
}

#[test]
fn test_fasterkv_clear_read_cache() {
    let store = create_store_with_cache();

    // Insert and read data
    {
        let mut session = store.start_session().unwrap();
        session.upsert(1u64, 100);
        let _ = session.read(&1u64);
    }

    // Clear cache
    store.clear_read_cache();

    // Stats should be reset
    if let Some(stats) = store.read_cache_stats() {
        assert_eq!(stats.read_calls(), 0);
    }
}

#[test]
fn test_fasterkv_without_read_cache() {
    let config = create_store_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));

    // Verify no cache
    assert!(store.read_cache_stats().is_none());
    assert!(store.read_cache_config().is_none());

    // Operations should still work
    {
        let mut session = store.start_session().unwrap();
        session.upsert(1u64, 100);
        let result = session.read(&1u64);
        assert_eq!(result.unwrap(), Some(100));
    }
}

// ============ Concurrent Access Tests ============

#[test]
fn test_concurrent_cache_access() {
    let store = create_store_with_cache();

    // Pre-populate data
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }
    }

    // Concurrent reads
    let num_threads = 4;
    let ops_per_thread = 50;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let store = store.clone();
            thread::spawn(move || {
                let mut session = store.start_session().unwrap();
                for i in 0..ops_per_thread {
                    let key = (i % 100) + 1;
                    let result = session.read(&key);
                    assert!(result.is_ok());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify store is still consistent
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            let result = session.read(&i);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(i * 10));
        }
    }
}

#[test]
fn test_concurrent_cache_insert_and_read() {
    let store = create_store_with_cache();

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let store = store.clone();
            thread::spawn(move || {
                let mut session = store.start_session().unwrap();
                for i in 0..25 {
                    let key = (t * 25 + i + 1) as u64;
                    session.upsert(key, key * 100);
                    let _ = session.read(&key);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all data
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            let result = session.read(&i);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(i * 100));
        }
    }
}

// ============ Edge Cases ============

#[test]
fn test_cache_with_zero_size() {
    let config = ReadCacheConfig::new(0);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    // Should handle gracefully
    assert_eq!(cache.config().mem_size, 0);
}

#[test]
fn test_cache_with_large_mutable_fraction() {
    let config = ReadCacheConfig::new(1024 * 1024).with_mutable_fraction(1.0);

    assert_eq!(config.mutable_fraction, 1.0);
    assert_eq!(config.read_only_size(), 0);
    assert_eq!(config.mutable_size(), 1024 * 1024);
}

#[test]
fn test_cache_with_zero_mutable_fraction() {
    let config = ReadCacheConfig::new(1024 * 1024).with_mutable_fraction(0.0);

    assert_eq!(config.mutable_fraction, 0.0);
    assert_eq!(config.mutable_size(), 0);
    assert_eq!(config.read_only_size(), 1024 * 1024);
}

#[test]
fn test_cache_multiple_inserts_same_key() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<TestKey, TestValue>::new(config);

    let key = TestKey(42);

    // Insert multiple times with different values
    for i in 0..10 {
        let value = TestValue(i * 100);
        cache
            .try_insert(&key, &value, oxifaster::Address::INVALID, false)
            .unwrap();
    }

    // Stats should reflect all inserts
    assert_eq!(cache.stats().insert_calls(), 10);
}
