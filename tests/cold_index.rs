//! Cold Index 集成测试
//!
//! 测试磁盘冷索引的配置和基本操作。

use std::path::PathBuf;

use oxifaster::index::{
    ColdIndexConfig, HashBucketEntry, KeyHash, DEFAULT_NUM_BUCKETS_PER_CHUNK, ENTRIES_PER_BUCKET,
};
use oxifaster::status::Status;
use oxifaster::Address;

// ============ ColdIndexConfig Tests ============

#[test]
fn test_cold_index_config_default() {
    let config = ColdIndexConfig::default();

    assert_eq!(config.table_size, 1 << 16);
    assert_eq!(config.in_mem_size, 64 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.5);
    assert_eq!(config.root_path, PathBuf::from("cold-index"));
}

#[test]
fn test_cold_index_config_new() {
    let config = ColdIndexConfig::new(1 << 14, 32 * 1024 * 1024, 0.6);

    assert_eq!(config.table_size, 1 << 14);
    assert_eq!(config.in_mem_size, 32 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.6);
}

#[test]
fn test_cold_index_config_with_root_path() {
    let config = ColdIndexConfig::default().with_root_path("/data/cold_index");

    assert_eq!(config.root_path, PathBuf::from("/data/cold_index"));
}

#[test]
fn test_cold_index_config_mutable_fraction_clamp() {
    let config1 = ColdIndexConfig::new(1 << 14, 32 * 1024 * 1024, 1.5);
    assert_eq!(config1.mutable_fraction, 1.0);

    let config2 = ColdIndexConfig::new(1 << 14, 32 * 1024 * 1024, -0.5);
    assert_eq!(config2.mutable_fraction, 0.0);
}

#[test]
fn test_cold_index_config_validate_ok() {
    let config = ColdIndexConfig::new(1 << 14, 32 * 1024 * 1024, 0.5);
    assert!(config.validate().is_ok());
}

#[test]
fn test_cold_index_config_validate_not_power_of_two() {
    let config = ColdIndexConfig::new(1000, 32 * 1024 * 1024, 0.5);
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_cold_index_config_validate_too_large() {
    let config = ColdIndexConfig::new(1u64 << 32, 32 * 1024 * 1024, 0.5);
    assert_eq!(config.validate(), Err(Status::InvalidArgument));
}

#[test]
fn test_cold_index_config_validate_power_of_two_sizes() {
    for shift in 10..24 {
        let config = ColdIndexConfig::new(1u64 << shift, 32 * 1024 * 1024, 0.5);
        assert!(config.validate().is_ok(), "Failed for size 2^{shift}");
    }
}

// ============ Chunk Structure Constants Tests ============

#[test]
fn test_buckets_per_chunk_constant() {
    assert_eq!(DEFAULT_NUM_BUCKETS_PER_CHUNK, 4);
}

#[test]
fn test_entries_per_bucket_constant() {
    assert_eq!(ENTRIES_PER_BUCKET, 8);
}

#[test]
fn test_total_entries_per_chunk() {
    let total = DEFAULT_NUM_BUCKETS_PER_CHUNK * ENTRIES_PER_BUCKET;
    assert_eq!(total, 32);
}

// ============ KeyHash Tests ============

#[test]
fn test_key_hash_new() {
    let hash = 0x123456789ABCDEF0u64;
    let key_hash = KeyHash::new(hash);

    // KeyHash should preserve the hash value
    assert_eq!(key_hash.hash(), hash);
}

#[test]
fn test_key_hash_tag() {
    let hash = 0x123456789ABCDEF0u64;
    let key_hash = KeyHash::new(hash);

    // Tag is derived from hash
    let tag = key_hash.tag();
    assert!(tag <= 0x3FFF); // 14-bit max
}

#[test]
fn test_key_hash_table_index() {
    let hash = 0x123456789ABCDEF0u64;
    let key_hash = KeyHash::new(hash);

    // Index should be within table size
    let table_size = 1024u64;
    let index = key_hash.hash_table_index(table_size);
    assert!(index < table_size as usize);
}

#[test]
fn test_key_hash_different_values() {
    let hash1 = KeyHash::new(0x1111111111111111);
    let hash2 = KeyHash::new(0x2222222222222222);

    // Different hashes should likely have different tags
    // (not guaranteed but highly probable)
    assert_ne!(hash1.hash(), hash2.hash());
}

#[test]
fn test_key_hash_table_index_varies_with_table_size() {
    let hash = 0xFFFFFFFFFFFFFFFFu64;
    let key_hash = KeyHash::new(hash);

    let index_small = key_hash.hash_table_index(256);
    let index_large = key_hash.hash_table_index(65536);

    assert!(index_small < 256);
    assert!(index_large < 65536);
}

// ============ HashBucketEntry Tests ============

#[test]
fn test_hash_bucket_entry_invalid() {
    let entry = HashBucketEntry::INVALID;

    assert!(entry.is_unused());
}

#[test]
fn test_hash_bucket_entry_new() {
    let address = Address::new(5, 1000);
    let entry = HashBucketEntry::new(address);

    assert!(!entry.is_unused());
    assert_eq!(entry.address(), address);
}

#[test]
fn test_hash_bucket_entry_invalid_address() {
    let entry = HashBucketEntry::new(Address::INVALID);

    // Entry with invalid address might still be "used" depending on implementation
    // but the address should be INVALID
    assert!(entry.address().is_invalid());
}

#[test]
fn test_hash_bucket_entry_various_addresses() {
    let addresses = [
        Address::new(0, 0),
        Address::new(1, 100),
        Address::new(10, 5000),
        Address::new(100, 65535),
    ];

    for addr in &addresses {
        let entry = HashBucketEntry::new(*addr);
        assert_eq!(entry.address(), *addr);
    }
}

// ============ Config Builder Pattern Tests ============

#[test]
fn test_config_chained_building() {
    let config =
        ColdIndexConfig::new(1 << 16, 128 * 1024 * 1024, 0.7).with_root_path("/custom/path");

    assert_eq!(config.table_size, 1 << 16);
    assert_eq!(config.in_mem_size, 128 * 1024 * 1024);
    assert_eq!(config.mutable_fraction, 0.7);
    assert_eq!(config.root_path, PathBuf::from("/custom/path"));
}

// ============ Edge Cases ============

#[test]
fn test_cold_index_config_min_table_size() {
    // Minimum valid power of two
    let config = ColdIndexConfig::new(1, 1024, 0.5);
    assert!(config.validate().is_ok());
}

#[test]
fn test_cold_index_config_max_valid_table_size() {
    // Maximum valid size (just under i32::MAX)
    let config = ColdIndexConfig::new(1 << 30, 1024, 0.5);
    assert!(config.validate().is_ok());
}

#[test]
fn test_key_hash_zero() {
    let key_hash = KeyHash::new(0);

    assert_eq!(key_hash.hash(), 0);
    assert_eq!(key_hash.hash_table_index(1024), 0);
}

#[test]
fn test_key_hash_max_value() {
    let key_hash = KeyHash::new(u64::MAX);

    let index = key_hash.hash_table_index(1024);
    assert!(index < 1024);
}

#[test]
fn test_hash_bucket_entry_with_valid_address() {
    let entry = HashBucketEntry::new(Address::new(1, 100));
    assert!(!entry.is_unused());
    assert_eq!(entry.address(), Address::new(1, 100));
}

// ============ Path Handling Tests ============

#[test]
fn test_config_relative_path() {
    let config = ColdIndexConfig::default().with_root_path("./data");
    assert_eq!(config.root_path, PathBuf::from("./data"));
}

#[test]
fn test_config_absolute_path() {
    let config = ColdIndexConfig::default().with_root_path("/absolute/path");
    assert_eq!(config.root_path, PathBuf::from("/absolute/path"));
}

#[test]
fn test_config_path_with_subdirs() {
    let config = ColdIndexConfig::default().with_root_path("data/cold/index/v1");
    assert_eq!(config.root_path, PathBuf::from("data/cold/index/v1"));
}
