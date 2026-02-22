//! In-memory hash index implementation for FASTER
//!
//! Note: This file is the `mem_index` module entrypoint and only contains type definitions and
//! module wiring. The implementation lives in `src/index/mem_index/*.rs` to keep this file small.

use std::sync::atomic::{AtomicBool, AtomicU8};

use crate::index::grow::{GrowConfig, GrowState};
use crate::index::{AtomicHashBucketEntry, IndexHashBucketEntry, InternalHashTable};
use crate::status::Status;
use crate::utility::is_power_of_two;

use self::overflow::OverflowBucketPool;

/// Result of a find operation
#[derive(Debug)]
pub struct FindResult {
    /// The entry found (or INVALID if not found)
    pub entry: IndexHashBucketEntry,
    /// Pointer to the atomic entry in the bucket
    pub atomic_entry: Option<*const AtomicHashBucketEntry>,
}

impl FindResult {
    /// Create a not-found result
    pub fn not_found() -> Self {
        Self {
            entry: IndexHashBucketEntry::INVALID,
            atomic_entry: None,
        }
    }

    /// Check if an entry was found
    pub fn found(&self) -> bool {
        self.atomic_entry.is_some() && !self.entry.is_unused()
    }
}

/// Configuration for the memory hash index
#[derive(Debug, Clone)]
pub struct MemHashIndexConfig {
    /// Size of the hash table (must be power of 2)
    pub table_size: u64,
}

impl MemHashIndexConfig {
    /// Create a new configuration.
    ///
    /// Returns `Err(Status::InvalidArgument)` if `table_size` is not a power of 2
    /// or is >= 2^31.
    pub fn new(table_size: u64) -> Result<Self, Status> {
        if !is_power_of_two(table_size) {
            return Err(Status::InvalidArgument);
        }
        if table_size >= (1u64 << 31) {
            return Err(Status::InvalidArgument);
        }
        Ok(Self { table_size })
    }
}

impl Default for MemHashIndexConfig {
    fn default() -> Self {
        Self {
            table_size: 1 << 20, // 1M buckets
        }
    }
}

/// In-memory hash index
///
/// Provides fast lookup from key hash to record address in the hybrid log.
/// Uses a two-version scheme to support concurrent index growth.
pub struct MemHashIndex {
    /// Hash tables (two versions for growth)
    tables: [InternalHashTable; 2],
    /// Overflow bucket pool (one per table version, switched together with the table).
    overflow_pools: [OverflowBucketPool; 2],
    /// Current version (0 or 1)
    version: AtomicU8,
    /// Grow state for tracking growth operations
    grow_state: GrowState,
    /// Grow configuration
    grow_config: GrowConfig,
    /// Whether a grow operation is in progress
    grow_in_progress: AtomicBool,
}

impl Default for MemHashIndex {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: MemHashIndex uses atomic operations for all concurrent access
unsafe impl Send for MemHashIndex {}
unsafe impl Sync for MemHashIndex {}

/// Statistics about the hash index
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Total table size
    pub table_size: u64,
    /// Total entry slots
    pub total_entries: u64,
    /// Number of used entries
    pub used_entries: u64,
    /// Number of buckets with at least one entry
    pub buckets_with_entries: u64,
    /// Load factor (used/total)
    pub load_factor: f64,
}

impl std::fmt::Display for IndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Index Statistics:")?;
        writeln!(f, "  Table size: {}", self.table_size)?;
        writeln!(f, "  Total entries: {}", self.total_entries)?;
        writeln!(f, "  Used entries: {}", self.used_entries)?;
        writeln!(f, "  Buckets with entries: {}", self.buckets_with_entries)?;
        writeln!(f, "  Load factor: {:.2}%", self.load_factor * 100.0)
    }
}

mod checkpoint;
mod grow;
mod ops;
mod overflow;

#[cfg(test)]
mod tests;
