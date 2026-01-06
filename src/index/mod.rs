//! Hash index for FASTER
//!
//! This module provides the hash index implementation used by FasterKV
//! to quickly locate records in the hybrid log.

mod cold_index;
mod grow;
mod hash_bucket;
mod hash_table;
mod mem_index;

pub use cold_index::{
    ColdIndex, ColdIndexConfig, ColdIndexFindResult, ColdIndexStats, DefaultHashIndexChunk,
    GcStateColdIndex, HashIndexChunk, HashIndexChunkKey, HashIndexChunkPos, HashIndexOp,
    IndexOperationType, DEFAULT_NUM_BUCKETS_PER_CHUNK, ENTRIES_PER_BUCKET,
};
pub use grow::{
    calculate_num_chunks, get_chunk_bounds, GrowConfig, GrowResult, GrowState,
    HASH_TABLE_CHUNK_SIZE,
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
