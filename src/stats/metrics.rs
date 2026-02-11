//! Statistics metrics definitions
//!
//! Defines various statistics structures for different FASTER components.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Statistics for store operations
#[derive(Debug, Default)]
pub struct OperationStats {
    /// Read operation count
    pub reads: AtomicU64,
    /// Read hit count
    pub read_hits: AtomicU64,
    /// Read miss count
    pub read_misses: AtomicU64,
    /// Upsert operation count
    pub upserts: AtomicU64,
    /// RMW operation count
    pub rmws: AtomicU64,
    /// Delete operation count
    pub deletes: AtomicU64,
    /// Conditional insert operation count
    pub conditional_inserts: AtomicU64,
    /// Conditional insert exists count (key already existed)
    pub conditional_insert_exists: AtomicU64,
    /// Pending operations
    pub pending: AtomicU64,
    /// Retry count
    pub retries: AtomicU64,
    /// Total latency in nanoseconds
    pub total_latency_ns: AtomicU64,
    /// Operation count for latency calculation
    pub latency_count: AtomicU64,
}

impl OperationStats {
    /// Create new operation stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read operation
    pub fn record_read(&self, hit: bool) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.read_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.read_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an upsert operation
    pub fn record_upsert(&self) {
        self.upserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an RMW operation
    pub fn record_rmw(&self) {
        self.rmws.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delete operation
    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a pending operation
    pub fn record_pending(&self) {
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry
    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a conditional insert operation
    pub fn record_conditional_insert(&self, inserted: bool) {
        self.conditional_inserts.fetch_add(1, Ordering::Relaxed);
        if !inserted {
            self.conditional_insert_exists
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record operation latency
    pub fn record_latency(&self, duration: Duration) {
        self.total_latency_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total operations
    pub fn total_operations(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
            + self.upserts.load(Ordering::Relaxed)
            + self.rmws.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
            + self.conditional_inserts.load(Ordering::Relaxed)
    }

    /// Get average latency
    pub fn average_latency(&self) -> Duration {
        let count = self.latency_count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let total_ns = self.total_latency_ns.load(Ordering::Relaxed);
        Duration::from_nanos(total_ns / count)
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let reads = self.reads.load(Ordering::Relaxed);
        if reads == 0 {
            return 0.0;
        }
        self.read_hits.load(Ordering::Relaxed) as f64 / reads as f64
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.read_hits.store(0, Ordering::Relaxed);
        self.read_misses.store(0, Ordering::Relaxed);
        self.upserts.store(0, Ordering::Relaxed);
        self.rmws.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.conditional_inserts.store(0, Ordering::Relaxed);
        self.conditional_insert_exists.store(0, Ordering::Relaxed);
        self.pending.store(0, Ordering::Relaxed);
        self.retries.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.latency_count.store(0, Ordering::Relaxed);
    }
}

/// Coarse per-phase latency breakdown for hot store operations.
///
/// This is intended for profiling runs. It is disabled by default to avoid adding `Instant::now()`
/// calls to hot paths.
#[allow(missing_docs)]
#[derive(Debug, Default)]
pub struct PhaseStats {
    pub upsert_ops: AtomicU64,
    pub upsert_hash_ns: AtomicU64,
    pub upsert_index_ns: AtomicU64,
    pub upsert_layout_ns: AtomicU64,
    pub upsert_alloc_ns: AtomicU64,
    pub upsert_init_ns: AtomicU64,
    pub upsert_index_update_ns: AtomicU64,

    pub read_ops: AtomicU64,
    pub read_hash_ns: AtomicU64,
    pub read_index_ns: AtomicU64,
    pub read_chain_ns: AtomicU64,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Default)]
pub struct PhaseStatsSnapshot {
    pub upsert_ops: u64,
    pub upsert_hash_ns: u64,
    pub upsert_index_ns: u64,
    pub upsert_layout_ns: u64,
    pub upsert_alloc_ns: u64,
    pub upsert_init_ns: u64,
    pub upsert_index_update_ns: u64,

    pub read_ops: u64,
    pub read_hash_ns: u64,
    pub read_index_ns: u64,
    pub read_chain_ns: u64,
}

#[allow(missing_docs)]
impl PhaseStats {
    pub fn reset(&self) {
        self.upsert_ops.store(0, Ordering::Relaxed);
        self.upsert_hash_ns.store(0, Ordering::Relaxed);
        self.upsert_index_ns.store(0, Ordering::Relaxed);
        self.upsert_layout_ns.store(0, Ordering::Relaxed);
        self.upsert_alloc_ns.store(0, Ordering::Relaxed);
        self.upsert_init_ns.store(0, Ordering::Relaxed);
        self.upsert_index_update_ns.store(0, Ordering::Relaxed);

        self.read_ops.store(0, Ordering::Relaxed);
        self.read_hash_ns.store(0, Ordering::Relaxed);
        self.read_index_ns.store(0, Ordering::Relaxed);
        self.read_chain_ns.store(0, Ordering::Relaxed);
    }

    #[inline]
    pub fn snapshot(&self) -> PhaseStatsSnapshot {
        PhaseStatsSnapshot {
            upsert_ops: self.upsert_ops.load(Ordering::Relaxed),
            upsert_hash_ns: self.upsert_hash_ns.load(Ordering::Relaxed),
            upsert_index_ns: self.upsert_index_ns.load(Ordering::Relaxed),
            upsert_layout_ns: self.upsert_layout_ns.load(Ordering::Relaxed),
            upsert_alloc_ns: self.upsert_alloc_ns.load(Ordering::Relaxed),
            upsert_init_ns: self.upsert_init_ns.load(Ordering::Relaxed),
            upsert_index_update_ns: self.upsert_index_update_ns.load(Ordering::Relaxed),
            read_ops: self.read_ops.load(Ordering::Relaxed),
            read_hash_ns: self.read_hash_ns.load(Ordering::Relaxed),
            read_index_ns: self.read_index_ns.load(Ordering::Relaxed),
            read_chain_ns: self.read_chain_ns.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn record_upsert_op(&self) {
        self.upsert_ops.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_read_op(&self) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn add_ns(counter: &AtomicU64, d: Duration) {
        counter.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_upsert_hash(&self, d: Duration) {
        Self::add_ns(&self.upsert_hash_ns, d);
    }

    #[inline]
    pub fn record_upsert_index(&self, d: Duration) {
        Self::add_ns(&self.upsert_index_ns, d);
    }

    #[inline]
    pub fn record_upsert_layout(&self, d: Duration) {
        Self::add_ns(&self.upsert_layout_ns, d);
    }

    #[inline]
    pub fn record_upsert_alloc(&self, d: Duration) {
        Self::add_ns(&self.upsert_alloc_ns, d);
    }

    #[inline]
    pub fn record_upsert_init(&self, d: Duration) {
        Self::add_ns(&self.upsert_init_ns, d);
    }

    #[inline]
    pub fn record_upsert_index_update(&self, d: Duration) {
        Self::add_ns(&self.upsert_index_update_ns, d);
    }

    #[inline]
    pub fn record_read_hash(&self, d: Duration) {
        Self::add_ns(&self.read_hash_ns, d);
    }

    #[inline]
    pub fn record_read_index(&self, d: Duration) {
        Self::add_ns(&self.read_index_ns, d);
    }

    #[inline]
    pub fn record_read_chain(&self, d: Duration) {
        Self::add_ns(&self.read_chain_ns, d);
    }
}

/// Statistics for the hash index
#[derive(Debug, Default)]
pub struct HashIndexStats {
    /// Number of buckets
    pub num_buckets: AtomicU64,
    /// Number of entries
    pub num_entries: AtomicU64,
    /// Number of overflow buckets
    pub overflow_buckets: AtomicU64,
    /// Find operations
    pub finds: AtomicU64,
    /// Insert operations
    pub inserts: AtomicU64,
    /// Update operations
    pub updates: AtomicU64,
    /// Delete operations
    pub deletes: AtomicU64,
    /// Collisions
    pub collisions: AtomicU64,
}

impl HashIndexStats {
    /// Create new hash index stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate load factor
    pub fn load_factor(&self) -> f64 {
        let buckets = self.num_buckets.load(Ordering::Relaxed);
        if buckets == 0 {
            return 0.0;
        }
        self.num_entries.load(Ordering::Relaxed) as f64 / buckets as f64
    }

    /// Calculate overflow ratio
    pub fn overflow_ratio(&self) -> f64 {
        let buckets = self.num_buckets.load(Ordering::Relaxed);
        if buckets == 0 {
            return 0.0;
        }
        self.overflow_buckets.load(Ordering::Relaxed) as f64 / buckets as f64
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.num_buckets.store(0, Ordering::Relaxed);
        self.num_entries.store(0, Ordering::Relaxed);
        self.overflow_buckets.store(0, Ordering::Relaxed);
        self.finds.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.updates.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.collisions.store(0, Ordering::Relaxed);
    }
}

/// Statistics for the hybrid log
#[derive(Debug, Default)]
pub struct HybridLogStats {
    /// Total bytes allocated
    pub bytes_allocated: AtomicU64,
    /// Total records written
    pub records_written: AtomicU64,
    /// Pages flushed to disk
    pub pages_flushed: AtomicU64,
    /// Bytes flushed to disk
    pub bytes_flushed: AtomicU64,
    /// Pages read from disk
    pub pages_read: AtomicU64,
    /// Bytes read from disk
    pub bytes_read: AtomicU64,
    /// Head shifts
    pub head_shifts: AtomicU64,
    /// Read-only shifts
    pub read_only_shifts: AtomicU64,
}

impl HybridLogStats {
    /// Create new hybrid log stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an allocation
    pub fn record_allocation(&self, size: u64) {
        self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
        self.records_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a page flush
    pub fn record_flush(&self, bytes: u64) {
        self.pages_flushed.fetch_add(1, Ordering::Relaxed);
        self.bytes_flushed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a page read
    pub fn record_read(&self, bytes: u64) {
        self.pages_read.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.bytes_allocated.store(0, Ordering::Relaxed);
        self.records_written.store(0, Ordering::Relaxed);
        self.pages_flushed.store(0, Ordering::Relaxed);
        self.bytes_flushed.store(0, Ordering::Relaxed);
        self.pages_read.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.head_shifts.store(0, Ordering::Relaxed);
        self.read_only_shifts.store(0, Ordering::Relaxed);
    }
}

/// Statistics for the memory allocator
#[derive(Debug, Default)]
pub struct AllocatorStats {
    /// Total memory allocated
    pub total_allocated: AtomicU64,
    /// Current memory in use
    pub current_in_use: AtomicU64,
    /// Peak memory usage
    pub peak_usage: AtomicU64,
    /// Allocation count
    pub allocation_count: AtomicU64,
    /// Free count
    pub free_count: AtomicU64,
    /// Failed allocations
    pub failed_allocations: AtomicU64,
}

impl AllocatorStats {
    /// Create new allocator stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an allocation
    pub fn record_allocation(&self, size: u64) {
        self.total_allocated.fetch_add(size, Ordering::Relaxed);
        let current = self.current_in_use.fetch_add(size, Ordering::Relaxed) + size;
        self.allocation_count.fetch_add(1, Ordering::Relaxed);

        // Update peak if necessary
        loop {
            let peak = self.peak_usage.load(Ordering::Relaxed);
            if current <= peak {
                break;
            }
            if self
                .peak_usage
                .compare_exchange(peak, current, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Record a free
    pub fn record_free(&self, size: u64) {
        self.current_in_use.fetch_sub(size, Ordering::Relaxed);
        self.free_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed allocation
    pub fn record_failed(&self) {
        self.failed_allocations.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.total_allocated.store(0, Ordering::Relaxed);
        self.current_in_use.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
        self.allocation_count.store(0, Ordering::Relaxed);
        self.free_count.store(0, Ordering::Relaxed);
        self.failed_allocations.store(0, Ordering::Relaxed);
    }
}

/// Statistics for a session
#[derive(Debug, Default)]
pub struct SessionStats {
    /// Session start time
    start_time: Option<Instant>,
    /// Operations in this session
    pub operations: AtomicU64,
    /// Pending operations
    pub pending_operations: AtomicU64,
    /// Completed operations
    pub completed_operations: AtomicU64,
}

impl SessionStats {
    /// Create new session stats
    pub fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Get session duration
    pub fn duration(&self) -> Duration {
        self.start_time.map(|t| t.elapsed()).unwrap_or_default()
    }

    /// Record an operation
    pub fn record_operation(&self) {
        self.operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a pending operation
    pub fn record_pending(&self) {
        self.pending_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed operation
    pub fn record_completed(&self) {
        self.completed_operations.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get operations per second
    pub fn ops_per_second(&self) -> f64 {
        let duration = self.duration();
        if duration.is_zero() {
            return 0.0;
        }
        self.operations.load(Ordering::Relaxed) as f64 / duration.as_secs_f64()
    }
}

/// Aggregate statistics for the store
#[derive(Debug, Default)]
pub struct StoreStats {
    /// Operation statistics
    pub operations: OperationStats,
    /// Per-phase profiling counters (disabled by default, enabled via `StatsCollector`).
    pub phases: PhaseStats,
    /// Hash index statistics
    pub hash_index: HashIndexStats,
    /// Hybrid log statistics
    pub hybrid_log: HybridLogStats,
    /// Allocator statistics
    pub allocator: AllocatorStats,
    /// Operational statistics
    pub operational: OperationalStats,
}

impl StoreStats {
    /// Create new store stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.operations.reset();
        self.phases.reset();
        self.hash_index.reset();
        self.hybrid_log.reset();
        self.allocator.reset();
        self.operational.reset();
    }
}

/// Statistics for operational workflows (checkpoint, compaction, growth, recovery, I/O).
#[derive(Debug, Default)]
pub struct OperationalStats {
    /// Checkpoints started
    pub checkpoints_started: AtomicU64,
    /// Checkpoints completed successfully
    pub checkpoints_completed: AtomicU64,
    /// Checkpoints failed
    pub checkpoints_failed: AtomicU64,
    /// Compactions started
    pub compactions_started: AtomicU64,
    /// Compactions completed successfully
    pub compactions_completed: AtomicU64,
    /// Compactions failed
    pub compactions_failed: AtomicU64,
    /// Index growth operations started
    pub index_grows_started: AtomicU64,
    /// Index growth operations completed successfully
    pub index_grows_completed: AtomicU64,
    /// Index growth operations failed
    pub index_grows_failed: AtomicU64,
    /// Recovery attempts started
    pub recoveries_started: AtomicU64,
    /// Recovery attempts failed
    pub recoveries_failed: AtomicU64,
    /// Pending I/O submissions
    pub pending_io_submitted: AtomicU64,
    /// Pending I/O completions
    pub pending_io_completed: AtomicU64,
    /// Pending I/O failures
    pub pending_io_failed: AtomicU64,
}

impl OperationalStats {
    /// Create new operational stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the start of a checkpoint.
    pub fn record_checkpoint_started(&self) {
        self.checkpoints_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful checkpoint completion.
    pub fn record_checkpoint_completed(&self) {
        self.checkpoints_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed checkpoint.
    pub fn record_checkpoint_failed(&self) {
        self.checkpoints_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the start of a compaction.
    pub fn record_compaction_started(&self) {
        self.compactions_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful compaction completion.
    pub fn record_compaction_completed(&self) {
        self.compactions_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed compaction.
    pub fn record_compaction_failed(&self) {
        self.compactions_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the start of an index growth.
    pub fn record_index_grow_started(&self) {
        self.index_grows_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful index growth completion.
    pub fn record_index_grow_completed(&self) {
        self.index_grows_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed index growth.
    pub fn record_index_grow_failed(&self) {
        self.index_grows_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the start of a recovery attempt.
    pub fn record_recovery_started(&self) {
        self.recoveries_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed recovery attempt.
    pub fn record_recovery_failed(&self) {
        self.recoveries_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a pending I/O submission.
    pub fn record_pending_io_submitted(&self) {
        self.pending_io_submitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a pending I/O completion.
    pub fn record_pending_io_completed(&self) {
        self.pending_io_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed pending I/O.
    pub fn record_pending_io_failed(&self) {
        self.pending_io_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.checkpoints_started.store(0, Ordering::Relaxed);
        self.checkpoints_completed.store(0, Ordering::Relaxed);
        self.checkpoints_failed.store(0, Ordering::Relaxed);
        self.compactions_started.store(0, Ordering::Relaxed);
        self.compactions_completed.store(0, Ordering::Relaxed);
        self.compactions_failed.store(0, Ordering::Relaxed);
        self.index_grows_started.store(0, Ordering::Relaxed);
        self.index_grows_completed.store(0, Ordering::Relaxed);
        self.index_grows_failed.store(0, Ordering::Relaxed);
        self.recoveries_started.store(0, Ordering::Relaxed);
        self.recoveries_failed.store(0, Ordering::Relaxed);
        self.pending_io_submitted.store(0, Ordering::Relaxed);
        self.pending_io_completed.store(0, Ordering::Relaxed);
        self.pending_io_failed.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_stats() {
        let stats = OperationStats::new();

        stats.record_read(true);
        stats.record_read(false);
        stats.record_upsert();
        stats.record_rmw();
        stats.record_delete();

        assert_eq!(stats.reads.load(Ordering::Relaxed), 2);
        assert_eq!(stats.read_hits.load(Ordering::Relaxed), 1);
        assert_eq!(stats.upserts.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_operations(), 5);
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_hash_index_stats() {
        let stats = HashIndexStats::new();

        stats.num_buckets.store(1000, Ordering::Relaxed);
        stats.num_entries.store(500, Ordering::Relaxed);
        stats.overflow_buckets.store(50, Ordering::Relaxed);

        assert_eq!(stats.load_factor(), 0.5);
        assert_eq!(stats.overflow_ratio(), 0.05);
    }

    #[test]
    fn test_hybrid_log_stats() {
        let stats = HybridLogStats::new();

        stats.record_allocation(100);
        stats.record_flush(4096);
        stats.record_read(4096);

        assert_eq!(stats.records_written.load(Ordering::Relaxed), 1);
        assert_eq!(stats.pages_flushed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.pages_read.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_allocator_stats() {
        let stats = AllocatorStats::new();

        stats.record_allocation(1000);
        stats.record_allocation(2000);

        assert_eq!(stats.total_allocated.load(Ordering::Relaxed), 3000);
        assert_eq!(stats.current_in_use.load(Ordering::Relaxed), 3000);
        assert_eq!(stats.peak_usage.load(Ordering::Relaxed), 3000);

        stats.record_free(1000);
        assert_eq!(stats.current_in_use.load(Ordering::Relaxed), 2000);
        assert_eq!(stats.peak_usage.load(Ordering::Relaxed), 3000); // Peak unchanged
    }

    #[test]
    fn test_session_stats() {
        let stats = SessionStats::new();

        stats.record_operation();
        stats.record_operation();
        stats.record_pending();
        stats.record_completed();

        assert_eq!(stats.operations.load(Ordering::Relaxed), 2);
        assert_eq!(stats.completed_operations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_store_stats() {
        let stats = StoreStats::new();

        stats.operations.record_read(true);
        stats.hash_index.num_entries.store(100, Ordering::Relaxed);

        assert_eq!(stats.operations.reads.load(Ordering::Relaxed), 1);
        assert_eq!(stats.hash_index.num_entries.load(Ordering::Relaxed), 100);

        stats.reset();

        assert_eq!(stats.operations.reads.load(Ordering::Relaxed), 0);
        assert_eq!(stats.hash_index.num_entries.load(Ordering::Relaxed), 0);
    }
}
