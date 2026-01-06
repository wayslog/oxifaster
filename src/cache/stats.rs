//! Read cache statistics
//!
//! Based on C++ FASTER's read_cache.h statistics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for read cache operations
pub struct ReadCacheStats {
    /// Number of read calls
    read_calls: AtomicU64,
    /// Number of successful reads (cache hits)
    read_hits: AtomicU64,
    /// Number of read misses
    read_misses: AtomicU64,
    /// Number of copy-to-tail operations
    copy_to_tail_calls: AtomicU64,
    /// Number of successful copy-to-tail operations
    copy_to_tail_success: AtomicU64,
    /// Number of insert attempts
    insert_calls: AtomicU64,
    /// Number of successful inserts
    insert_success: AtomicU64,
    /// Number of evicted records
    evicted_records: AtomicU64,
    /// Number of invalid records found during eviction
    evicted_invalid: AtomicU64,
}

impl ReadCacheStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            read_calls: AtomicU64::new(0),
            read_hits: AtomicU64::new(0),
            read_misses: AtomicU64::new(0),
            copy_to_tail_calls: AtomicU64::new(0),
            copy_to_tail_success: AtomicU64::new(0),
            insert_calls: AtomicU64::new(0),
            insert_success: AtomicU64::new(0),
            evicted_records: AtomicU64::new(0),
            evicted_invalid: AtomicU64::new(0),
        }
    }

    /// Record a read call
    pub fn record_read(&self) {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache hit
    pub fn record_hit(&self) {
        self.read_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_miss(&self) {
        self.read_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a copy-to-tail operation
    pub fn record_copy_to_tail(&self) {
        self.copy_to_tail_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful copy-to-tail operation
    pub fn record_copy_to_tail_success(&self) {
        self.copy_to_tail_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an insert attempt
    pub fn record_insert(&self) {
        self.insert_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful insert
    pub fn record_insert_success(&self) {
        self.insert_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record evicted records
    pub fn record_eviction(&self, count: u64, invalid_count: u64) {
        self.evicted_records.fetch_add(count, Ordering::Relaxed);
        self.evicted_invalid
            .fetch_add(invalid_count, Ordering::Relaxed);
    }

    /// Get the number of read calls
    pub fn read_calls(&self) -> u64 {
        self.read_calls.load(Ordering::Relaxed)
    }

    /// Get the number of cache hits
    pub fn read_hits(&self) -> u64 {
        self.read_hits.load(Ordering::Relaxed)
    }

    /// Get the number of cache misses
    pub fn read_misses(&self) -> u64 {
        self.read_misses.load(Ordering::Relaxed)
    }

    /// Get the hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let calls = self.read_calls.load(Ordering::Relaxed);
        if calls == 0 {
            return 0.0;
        }
        self.read_hits.load(Ordering::Relaxed) as f64 / calls as f64
    }

    /// Get the number of copy-to-tail calls
    pub fn copy_to_tail_calls(&self) -> u64 {
        self.copy_to_tail_calls.load(Ordering::Relaxed)
    }

    /// Get the copy-to-tail success rate
    pub fn copy_to_tail_rate(&self) -> f64 {
        let calls = self.copy_to_tail_calls.load(Ordering::Relaxed);
        if calls == 0 {
            return 0.0;
        }
        self.copy_to_tail_success.load(Ordering::Relaxed) as f64 / calls as f64
    }

    /// Get the number of insert calls
    pub fn insert_calls(&self) -> u64 {
        self.insert_calls.load(Ordering::Relaxed)
    }

    /// Get the insert success rate
    pub fn insert_rate(&self) -> f64 {
        let calls = self.insert_calls.load(Ordering::Relaxed);
        if calls == 0 {
            return 0.0;
        }
        self.insert_success.load(Ordering::Relaxed) as f64 / calls as f64
    }

    /// Get the number of evicted records
    pub fn evicted_records(&self) -> u64 {
        self.evicted_records.load(Ordering::Relaxed)
    }

    /// Get the invalid record rate during eviction
    pub fn eviction_invalid_rate(&self) -> f64 {
        let evicted = self.evicted_records.load(Ordering::Relaxed);
        if evicted == 0 {
            return 0.0;
        }
        self.evicted_invalid.load(Ordering::Relaxed) as f64 / evicted as f64
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.read_calls.store(0, Ordering::Relaxed);
        self.read_hits.store(0, Ordering::Relaxed);
        self.read_misses.store(0, Ordering::Relaxed);
        self.copy_to_tail_calls.store(0, Ordering::Relaxed);
        self.copy_to_tail_success.store(0, Ordering::Relaxed);
        self.insert_calls.store(0, Ordering::Relaxed);
        self.insert_success.store(0, Ordering::Relaxed);
        self.evicted_records.store(0, Ordering::Relaxed);
        self.evicted_invalid.store(0, Ordering::Relaxed);
    }

    /// Get a summary of all statistics
    pub fn summary(&self) -> ReadCacheStatsSummary {
        ReadCacheStatsSummary {
            read_calls: self.read_calls(),
            read_hits: self.read_hits(),
            read_misses: self.read_misses(),
            hit_rate: self.hit_rate(),
            copy_to_tail_calls: self.copy_to_tail_calls(),
            copy_to_tail_rate: self.copy_to_tail_rate(),
            insert_calls: self.insert_calls(),
            insert_rate: self.insert_rate(),
            evicted_records: self.evicted_records(),
            eviction_invalid_rate: self.eviction_invalid_rate(),
        }
    }
}

impl Default for ReadCacheStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of read cache statistics
#[derive(Debug, Clone)]
pub struct ReadCacheStatsSummary {
    /// Number of read calls
    pub read_calls: u64,
    /// Number of cache hits
    pub read_hits: u64,
    /// Number of cache misses
    pub read_misses: u64,
    /// Cache hit rate
    pub hit_rate: f64,
    /// Number of copy-to-tail calls
    pub copy_to_tail_calls: u64,
    /// Copy-to-tail success rate
    pub copy_to_tail_rate: f64,
    /// Number of insert calls
    pub insert_calls: u64,
    /// Insert success rate
    pub insert_rate: f64,
    /// Number of evicted records
    pub evicted_records: u64,
    /// Invalid record rate during eviction
    pub eviction_invalid_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats() {
        let stats = ReadCacheStats::new();
        assert_eq!(stats.read_calls(), 0);
        assert_eq!(stats.read_hits(), 0);
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_record_operations() {
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
    fn test_insert_stats() {
        let stats = ReadCacheStats::new();

        stats.record_insert();
        stats.record_insert();
        stats.record_insert_success();

        assert_eq!(stats.insert_calls(), 2);
        assert_eq!(stats.insert_rate(), 0.5);
    }

    #[test]
    fn test_reset() {
        let stats = ReadCacheStats::new();

        stats.record_read();
        stats.record_hit();

        stats.reset();

        assert_eq!(stats.read_calls(), 0);
        assert_eq!(stats.read_hits(), 0);
    }
}
