//! Hash index for FASTER
//!
//! This module provides the hash index implementation used by FasterKV
//! to quickly locate records in the hybrid log.
//!
//! # Overview
//!
//! The hash index maps key hashes to record addresses in the hybrid log.
//! It uses a cache-line aligned hash bucket design for optimal memory access
//! patterns and supports both in-memory and on-disk (cold) indexes.
//!
//! # Index Types
//!
//! - [`MemHashIndex`]: In-memory hash index for hot data (primary index)
//! - [`ColdIndex`]: On-disk hash index for cold data in F2 architecture
//!
//! # Hash Bucket Design
//!
//! Each hash bucket is 64 bytes (one cache line) and contains 7 entries
//! plus an overflow pointer. Entries use a 14-bit tag for fast collision
//! detection before full key comparison.
//!
//! ```text
//! +------------------+
//! | Entry 0 (8B)     |  <- 48-bit address + 14-bit tag + flags
//! | Entry 1 (8B)     |
//! | Entry 2 (8B)     |
//! | Entry 3 (8B)     |
//! | Entry 4 (8B)     |
//! | Entry 5 (8B)     |
//! | Entry 6 (8B)     |
//! | Overflow ptr (8B)|  <- Points to overflow bucket chain
//! +------------------+
//! ```
//!
//! # Dynamic Growth
//!
//! The index supports dynamic growth via the [`GrowState`] mechanism.
//! Growth doubles the hash table size and rehashes entries incrementally
//! across multiple threads.
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::index::{MemHashIndex, MemHashIndexConfig, KeyHash};
//!
//! let mut index = MemHashIndex::new();
//! index.initialize(&MemHashIndexConfig::new(1 << 20)); // 1M buckets
//!
//! let hash = KeyHash::new(my_key.get_hash());
//! let result = index.find_or_create_entry(hash);
//! ```

mod cold_index;
mod grow;
mod hash_bucket;
mod hash_table;
mod mem_index;

#[cfg(feature = "index-profile")]
pub mod profile;

pub use cold_index::{
    ColdIndex, ColdIndexConfig, ColdIndexFindResult, ColdIndexStats, DefaultHashIndexChunk,
    GcStateColdIndex, HashIndexChunk, HashIndexChunkKey, HashIndexChunkPos, HashIndexOp,
    IndexOperationType, DEFAULT_NUM_BUCKETS_PER_CHUNK, ENTRIES_PER_BUCKET,
};
pub use grow::{
    calculate_num_chunks, get_chunk_bounds, GrowCompleteCallback, GrowConfig, GrowResult,
    GrowState, HASH_TABLE_CHUNK_SIZE,
};
pub use hash_bucket::{
    AtomicHashBucketEntry, ColdHashBucket, HashBucket, HashBucketEntry, HashBucketOverflowEntry,
    IndexHashBucketEntry,
};
pub use hash_table::InternalHashTable;
pub use mem_index::{FindResult, IndexStats, MemHashIndex, MemHashIndexConfig};

/// Key hash type for index operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KeyHash {
    hash: u64,
}

impl KeyHash {
    /// Number of bits used for the tag
    pub const TAG_BITS: u32 = 14;

    /// Create a new key hash
    #[inline]
    pub const fn new(hash: u64) -> Self {
        Self { hash }
    }

    /// Get the full hash value
    #[inline]
    pub const fn hash(&self) -> u64 {
        self.hash
    }

    /// Get the hash table index for a given table size
    #[inline]
    pub const fn hash_table_index(&self, size: u64) -> usize {
        (self.hash as usize) & ((size as usize) - 1)
    }

    /// Get the tag portion of the hash (14 bits)
    #[inline]
    pub const fn tag(&self) -> u16 {
        ((self.hash >> 48) & ((1 << Self::TAG_BITS) - 1)) as u16
    }
}

impl From<u64> for KeyHash {
    #[inline]
    fn from(hash: u64) -> Self {
        Self::new(hash)
    }
}

/// Configuration for the hash index
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Size of the hash table (must be power of 2)
    pub table_size: u64,
}

impl IndexConfig {
    /// Create a new index configuration
    pub fn new(table_size: u64) -> Self {
        Self { table_size }
    }
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            table_size: 1 << 20, // 1M buckets by default
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_hash_new() {
        let hash = KeyHash::new(12345);
        assert_eq!(hash.hash(), 12345);
    }

    #[test]
    fn test_key_hash_from_u64() {
        let hash: KeyHash = 67890u64.into();
        assert_eq!(hash.hash(), 67890);
    }

    #[test]
    fn test_key_hash_default() {
        let hash = KeyHash::default();
        assert_eq!(hash.hash(), 0);
    }

    #[test]
    fn test_key_hash_clone_copy() {
        let hash = KeyHash::new(42);
        let cloned = hash;
        let copied = hash;
        assert_eq!(hash, cloned);
        assert_eq!(hash, copied);
    }

    #[test]
    fn test_key_hash_debug() {
        let hash = KeyHash::new(123);
        let debug_str = format!("{hash:?}");
        assert!(debug_str.contains("KeyHash"));
        assert!(debug_str.contains("123"));
    }

    #[test]
    fn test_key_hash_table_index() {
        let hash = KeyHash::new(0x123456789ABCDEF0);
        // Test with different table sizes (must be power of 2)
        let idx_16 = hash.hash_table_index(16);
        assert!(idx_16 < 16);

        let idx_1024 = hash.hash_table_index(1024);
        assert!(idx_1024 < 1024);

        let idx_1m = hash.hash_table_index(1 << 20);
        assert!(idx_1m < (1 << 20));
    }

    #[test]
    fn test_key_hash_table_index_deterministic() {
        let hash = KeyHash::new(12345);
        let idx1 = hash.hash_table_index(1024);
        let idx2 = hash.hash_table_index(1024);
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn test_key_hash_tag() {
        let hash = KeyHash::new(0xFFFF_0000_0000_0000);
        let tag = hash.tag();
        // Tag should be 14 bits from bits 48-61
        assert!(tag < (1 << KeyHash::TAG_BITS));
    }

    #[test]
    fn test_key_hash_tag_range() {
        // Test various hash values to ensure tag is always within 14 bits
        for i in 0u64..100 {
            let hash = KeyHash::new(i.wrapping_mul(0x1234_5678_9ABC_DEF0));
            let tag = hash.tag();
            assert!(tag < (1 << KeyHash::TAG_BITS));
        }
    }

    #[test]
    fn test_key_hash_eq() {
        let hash1 = KeyHash::new(100);
        let hash2 = KeyHash::new(100);
        let hash3 = KeyHash::new(200);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_key_hash_tag_bits_constant() {
        assert_eq!(KeyHash::TAG_BITS, 14);
    }

    #[test]
    fn test_index_config_new() {
        let config = IndexConfig::new(1024);
        assert_eq!(config.table_size, 1024);
    }

    #[test]
    fn test_index_config_default() {
        let config = IndexConfig::default();
        assert_eq!(config.table_size, 1 << 20);
    }

    #[test]
    fn test_index_config_debug() {
        let config = IndexConfig::new(2048);
        let debug_str = format!("{config:?}");
        assert!(debug_str.contains("IndexConfig"));
        assert!(debug_str.contains("table_size"));
        assert!(debug_str.contains("2048"));
    }

    #[test]
    fn test_index_config_clone() {
        let config = IndexConfig::new(4096);
        let cloned = config.clone();
        assert_eq!(config.table_size, cloned.table_size);
    }
}
