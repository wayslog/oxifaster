//! Index growth state and management
//!
//! This module provides the state tracking for dynamic hash index resizing.
//! Based on C++ FASTER's grow_state.h implementation.

use std::sync::atomic::{AtomicU64, Ordering};

/// Callback type for grow completion
pub type GrowCompleteCallback = Box<dyn FnOnce(u64) + Send>;

/// Size of hash table chunks for parallel growth
pub const HASH_TABLE_CHUNK_SIZE: u64 = 16384;

/// Configuration for index growth
#[derive(Debug, Clone)]
pub struct GrowConfig {
    /// Minimum load factor before triggering growth
    pub min_load_factor: f64,
    /// Maximum load factor to trigger growth
    pub max_load_factor: f64,
    /// Growth factor (new_size = old_size * growth_factor)
    pub growth_factor: u64,
    /// Whether to allow automatic growth
    pub auto_grow: bool,
}

impl Default for GrowConfig {
    fn default() -> Self {
        Self {
            min_load_factor: 0.5,
            max_load_factor: 0.9,
            growth_factor: 2,
            auto_grow: false,
        }
    }
}

impl GrowConfig {
    /// Create a new grow configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum load factor
    pub fn with_min_load_factor(mut self, factor: f64) -> Self {
        self.min_load_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Set the maximum load factor
    pub fn with_max_load_factor(mut self, factor: f64) -> Self {
        self.max_load_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Set the growth factor
    pub fn with_growth_factor(mut self, factor: u64) -> Self {
        self.growth_factor = factor.max(2);
        self
    }

    /// Enable or disable automatic growth
    pub fn with_auto_grow(mut self, auto: bool) -> Self {
        self.auto_grow = auto;
        self
    }

    /// Check if growth should be triggered
    pub fn should_grow(&self, load_factor: f64) -> bool {
        self.auto_grow && load_factor >= self.max_load_factor
    }
}

/// State for an active index growth operation
pub struct GrowState {
    /// Old hash table version (0 or 1)
    old_version: u8,
    /// New hash table version (0 or 1)
    new_version: u8,
    /// Total number of chunks to process
    num_chunks: u64,
    /// Number of pending chunks
    num_pending_chunks: AtomicU64,
    /// Next chunk to process
    next_chunk: AtomicU64,
    /// Whether growth is in progress
    in_progress: bool,
    /// Accumulated overflow buckets skipped during migration
    overflow_buckets_skipped: AtomicU64,
    /// Accumulated rehash failures during migration
    rehash_failures: AtomicU64,
    /// Total entries migrated
    entries_migrated: AtomicU64,
}

impl GrowState {
    /// Create a new grow state
    pub fn new() -> Self {
        Self {
            old_version: u8::MAX,
            new_version: u8::MAX,
            num_chunks: 0,
            num_pending_chunks: AtomicU64::new(0),
            next_chunk: AtomicU64::new(0),
            in_progress: false,
            overflow_buckets_skipped: AtomicU64::new(0),
            rehash_failures: AtomicU64::new(0),
            entries_migrated: AtomicU64::new(0),
        }
    }

    /// Initialize the grow state for a new growth operation
    ///
    /// # Arguments
    /// * `current_version` - Current hash table version (0 or 1)
    /// * `num_chunks` - Number of chunks to process
    pub fn initialize(&mut self, current_version: u8, num_chunks: u64) {
        assert!(current_version == 0 || current_version == 1);
        self.old_version = current_version;
        self.new_version = 1 - current_version;
        self.num_chunks = num_chunks;
        self.num_pending_chunks.store(num_chunks, Ordering::Release);
        self.next_chunk.store(0, Ordering::Release);
        self.in_progress = true;
        // Reset data loss tracking
        self.overflow_buckets_skipped.store(0, Ordering::Release);
        self.rehash_failures.store(0, Ordering::Release);
        self.entries_migrated.store(0, Ordering::Release);
    }

    /// Get the old version
    pub fn old_version(&self) -> u8 {
        self.old_version
    }

    /// Get the new version
    pub fn new_version(&self) -> u8 {
        self.new_version
    }

    /// Get the total number of chunks
    pub fn num_chunks(&self) -> u64 {
        self.num_chunks
    }

    /// Check if growth is in progress
    pub fn is_in_progress(&self) -> bool {
        self.in_progress
    }

    /// Get the next chunk to process
    ///
    /// Returns `Some(chunk_index)` if there are more chunks, `None` if done
    pub fn get_next_chunk(&self) -> Option<u64> {
        let chunk = self.next_chunk.fetch_add(1, Ordering::AcqRel);
        if chunk < self.num_chunks {
            Some(chunk)
        } else {
            None
        }
    }

    /// Mark a chunk as completed
    ///
    /// Returns `true` if this was the last chunk
    pub fn complete_chunk(&self) -> bool {
        let remaining = self.num_pending_chunks.fetch_sub(1, Ordering::AcqRel);
        remaining == 1
    }

    /// Get the number of remaining chunks
    pub fn remaining_chunks(&self) -> u64 {
        self.num_pending_chunks.load(Ordering::Acquire)
    }

    /// Reset the grow state
    pub fn reset(&mut self) {
        self.old_version = u8::MAX;
        self.new_version = u8::MAX;
        self.num_chunks = 0;
        self.num_pending_chunks.store(0, Ordering::Release);
        self.next_chunk.store(0, Ordering::Release);
        self.in_progress = false;
        self.overflow_buckets_skipped.store(0, Ordering::Release);
        self.rehash_failures.store(0, Ordering::Release);
        self.entries_migrated.store(0, Ordering::Release);
    }

    /// Get grow progress as (completed_chunks, total_chunks)
    pub fn progress(&self) -> (u64, u64) {
        let remaining = self.num_pending_chunks.load(Ordering::Acquire);
        let completed = self.num_chunks.saturating_sub(remaining);
        (completed, self.num_chunks)
    }

    /// Record migration results from a chunk
    ///
    /// This should be called after each chunk migration to accumulate
    /// statistics for the final result.
    pub fn record_chunk_result(
        &self,
        entries_migrated: u64,
        overflow_buckets_skipped: u64,
        rehash_failures: u64,
    ) {
        self.entries_migrated
            .fetch_add(entries_migrated, Ordering::AcqRel);
        self.overflow_buckets_skipped
            .fetch_add(overflow_buckets_skipped, Ordering::AcqRel);
        self.rehash_failures
            .fetch_add(rehash_failures, Ordering::AcqRel);
    }

    /// Get accumulated overflow buckets skipped
    pub fn get_overflow_buckets_skipped(&self) -> u64 {
        self.overflow_buckets_skipped.load(Ordering::Acquire)
    }

    /// Get accumulated rehash failures
    pub fn get_rehash_failures(&self) -> u64 {
        self.rehash_failures.load(Ordering::Acquire)
    }

    /// Get total entries migrated
    pub fn get_entries_migrated(&self) -> u64 {
        self.entries_migrated.load(Ordering::Acquire)
    }

    /// Complete the grow operation and return the result
    pub fn complete(mut self) -> GrowResult {
        let remaining = self.remaining_chunks();
        let overflow = self.get_overflow_buckets_skipped();
        let failures = self.get_rehash_failures();
        let migrated = self.get_entries_migrated();
        self.reset();

        if remaining == 0 {
            GrowResult::with_data_loss_tracking(0, 0, migrated, 0, overflow, failures)
        } else {
            GrowResult::failure(crate::status::Status::Aborted)
        }
    }
}

impl Default for GrowState {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a grow operation
#[derive(Debug, Clone)]
pub struct GrowResult {
    /// Whether the operation was successful
    pub success: bool,
    /// New size of the hash table
    pub new_size: u64,
    /// Old size of the hash table
    pub old_size: u64,
    /// Number of entries migrated
    pub entries_migrated: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Status code if failed
    pub status: Option<crate::status::Status>,
    /// Number of overflow buckets encountered that could not be migrated
    /// (data loss warning if > 0)
    pub overflow_buckets_skipped: u64,
    /// Number of entries that failed to rehash (key could not be read)
    /// These entries were skipped and may be lost
    pub rehash_failures: u64,
}

impl GrowResult {
    /// Create a successful result
    pub fn success(old_size: u64, new_size: u64, entries_migrated: u64, duration_ms: u64) -> Self {
        Self {
            success: true,
            new_size,
            old_size,
            entries_migrated,
            duration_ms,
            status: None,
            overflow_buckets_skipped: 0,
            rehash_failures: 0,
        }
    }

    /// Create a successful result with overflow bucket tracking
    pub fn success_with_overflow(
        old_size: u64,
        new_size: u64,
        entries_migrated: u64,
        duration_ms: u64,
        overflow_buckets_skipped: u64,
    ) -> Self {
        Self {
            success: overflow_buckets_skipped == 0, // Not fully successful if we skipped overflow entries
            new_size,
            old_size,
            entries_migrated,
            duration_ms,
            status: if overflow_buckets_skipped > 0 {
                Some(crate::status::Status::OverflowBucketsSkipped)
            } else {
                None
            },
            overflow_buckets_skipped,
            rehash_failures: 0,
        }
    }

    /// Create a result with both overflow and rehash failure tracking
    ///
    /// Success is false if either overflow buckets were skipped or rehash failures occurred,
    /// as both indicate potential data loss.
    pub fn with_data_loss_tracking(
        old_size: u64,
        new_size: u64,
        entries_migrated: u64,
        duration_ms: u64,
        overflow_buckets_skipped: u64,
        rehash_failures: u64,
    ) -> Self {
        let has_data_loss = overflow_buckets_skipped > 0 || rehash_failures > 0;
        Self {
            success: !has_data_loss,
            new_size,
            old_size,
            entries_migrated,
            duration_ms,
            status: if has_data_loss {
                Some(crate::status::Status::OverflowBucketsSkipped)
            } else {
                None
            },
            overflow_buckets_skipped,
            rehash_failures,
        }
    }

    /// Create a failure result
    pub fn failure(status: crate::status::Status) -> Self {
        Self {
            success: false,
            new_size: 0,
            old_size: 0,
            entries_migrated: 0,
            duration_ms: 0,
            status: Some(status),
            overflow_buckets_skipped: 0,
            rehash_failures: 0,
        }
    }

    /// Check if any overflow buckets were skipped (potential data loss)
    pub fn has_overflow_warning(&self) -> bool {
        self.overflow_buckets_skipped > 0
    }

    /// Check if any rehash failures occurred (potential data loss)
    pub fn has_rehash_failures(&self) -> bool {
        self.rehash_failures > 0
    }

    /// Check if the grow had any issues (overflow or rehash failures)
    pub fn has_data_loss_warning(&self) -> bool {
        self.overflow_buckets_skipped > 0 || self.rehash_failures > 0
    }

    /// Calculate the growth ratio
    pub fn growth_ratio(&self) -> f64 {
        if self.old_size == 0 {
            return 0.0;
        }
        self.new_size as f64 / self.old_size as f64
    }
}

/// Calculate the number of chunks for a given table size
pub fn calculate_num_chunks(table_size: u64) -> u64 {
    (table_size + HASH_TABLE_CHUNK_SIZE - 1) / HASH_TABLE_CHUNK_SIZE
}

/// Calculate chunk boundaries
///
/// # Arguments
/// * `chunk_index` - Index of the chunk
/// * `total_buckets` - Total number of buckets in the hash table
///
/// # Returns
/// `(start_bucket, end_bucket)` - Start and end bucket indices for the chunk
pub fn get_chunk_bounds(chunk_index: u64, total_buckets: u64) -> (u64, u64) {
    let start = chunk_index * HASH_TABLE_CHUNK_SIZE;
    let end = std::cmp::min(start + HASH_TABLE_CHUNK_SIZE, total_buckets);
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grow_config_default() {
        let config = GrowConfig::default();
        assert_eq!(config.growth_factor, 2);
        assert!(!config.auto_grow);
    }

    #[test]
    fn test_grow_config_builder() {
        let config = GrowConfig::new()
            .with_max_load_factor(0.8)
            .with_growth_factor(4)
            .with_auto_grow(true);

        assert_eq!(config.max_load_factor, 0.8);
        assert_eq!(config.growth_factor, 4);
        assert!(config.auto_grow);
    }

    #[test]
    fn test_should_grow() {
        let config = GrowConfig::new()
            .with_max_load_factor(0.8)
            .with_auto_grow(true);

        assert!(!config.should_grow(0.5));
        assert!(config.should_grow(0.9));
    }

    #[test]
    fn test_grow_state_initialize() {
        let mut state = GrowState::new();
        assert!(!state.is_in_progress());

        state.initialize(0, 100);
        assert!(state.is_in_progress());
        assert_eq!(state.old_version(), 0);
        assert_eq!(state.new_version(), 1);
        assert_eq!(state.num_chunks(), 100);
    }

    #[test]
    fn test_grow_state_chunks() {
        let mut state = GrowState::new();
        state.initialize(1, 3);

        // Get all chunks
        assert_eq!(state.get_next_chunk(), Some(0));
        assert_eq!(state.get_next_chunk(), Some(1));
        assert_eq!(state.get_next_chunk(), Some(2));
        assert_eq!(state.get_next_chunk(), None);
    }

    #[test]
    fn test_grow_state_complete() {
        let mut state = GrowState::new();
        state.initialize(0, 3);

        // Complete chunks
        assert_eq!(state.remaining_chunks(), 3);

        assert!(!state.complete_chunk());
        assert_eq!(state.remaining_chunks(), 2);

        assert!(!state.complete_chunk());
        assert_eq!(state.remaining_chunks(), 1);

        assert!(state.complete_chunk()); // Last chunk
        assert_eq!(state.remaining_chunks(), 0);
    }

    #[test]
    fn test_calculate_num_chunks() {
        assert_eq!(calculate_num_chunks(0), 0);
        assert_eq!(calculate_num_chunks(1), 1);
        assert_eq!(calculate_num_chunks(HASH_TABLE_CHUNK_SIZE), 1);
        assert_eq!(calculate_num_chunks(HASH_TABLE_CHUNK_SIZE + 1), 2);
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

        // Last partial chunk
        let last_chunk = calculate_num_chunks(total) - 1;
        let (_start_last, end_last) = get_chunk_bounds(last_chunk, total);
        assert_eq!(end_last, total);
    }

    #[test]
    fn test_grow_result() {
        let result = GrowResult::success(100, 200, 50, 10);

        assert!(result.success);
        assert_eq!(result.growth_ratio(), 2.0);
    }

    #[test]
    fn test_grow_result_failure() {
        let result = GrowResult::failure(crate::status::Status::Aborted);

        assert!(!result.success);
        assert!(result.status.is_some());
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
}
