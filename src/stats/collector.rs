//! Statistics collector
//!
//! Provides centralized statistics collection and management.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::stats::metrics::StoreStats;

/// Configuration for statistics collection
#[derive(Debug, Clone)]
pub struct StatsConfig {
    /// Whether statistics collection is enabled
    pub enabled: bool,
    /// Collection interval for periodic stats
    pub collection_interval: Duration,
    /// Whether to track latency histograms
    pub track_latency_histogram: bool,
    /// Number of latency percentile buckets
    pub latency_buckets: usize,
    /// Whether to track per-operation stats
    pub track_per_operation: bool,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(1),
            track_latency_histogram: false,
            latency_buckets: 100,
            track_per_operation: true,
        }
    }
}

impl StatsConfig {
    /// Create a new configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable statistics collection
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the collection interval
    pub fn with_collection_interval(mut self, interval: Duration) -> Self {
        self.collection_interval = interval;
        self
    }

    /// Enable or disable latency histogram tracking
    pub fn with_latency_histogram(mut self, enabled: bool) -> Self {
        self.track_latency_histogram = enabled;
        self
    }
}

/// Statistics collector
///
/// Centralized collector for all FASTER statistics.
pub struct StatsCollector {
    /// Configuration
    config: StatsConfig,
    /// Whether collection is currently enabled
    enabled: AtomicBool,
    /// Store statistics
    pub store_stats: StoreStats,
    /// Start time for throughput calculation
    start_time: Instant,
}

impl StatsCollector {
    /// Create a new statistics collector
    pub fn new(config: StatsConfig) -> Self {
        let enabled = config.enabled;
        Self {
            config,
            enabled: AtomicBool::new(enabled),
            store_stats: StoreStats::new(),
            start_time: Instant::now(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(StatsConfig::default())
    }

    /// Get the configuration
    pub fn config(&self) -> &StatsConfig {
        &self.config
    }

    /// Check if collection is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Enable statistics collection
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    /// Disable statistics collection
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    /// Get elapsed time since collection started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get overall throughput (operations per second)
    pub fn throughput(&self) -> f64 {
        let elapsed = self.elapsed();
        if elapsed.is_zero() {
            return 0.0;
        }
        let total_ops = self.store_stats.operations.total_operations();
        total_ops as f64 / elapsed.as_secs_f64()
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        self.store_stats.reset();
        self.start_time = Instant::now();
    }

    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            elapsed: self.elapsed(),
            total_operations: self.store_stats.operations.total_operations(),
            reads: self.store_stats.operations.reads.load(Ordering::Relaxed),
            read_hits: self
                .store_stats
                .operations
                .read_hits
                .load(Ordering::Relaxed),
            upserts: self.store_stats.operations.upserts.load(Ordering::Relaxed),
            rmws: self.store_stats.operations.rmws.load(Ordering::Relaxed),
            deletes: self.store_stats.operations.deletes.load(Ordering::Relaxed),
            pending: self.store_stats.operations.pending.load(Ordering::Relaxed),
            hit_rate: self.store_stats.operations.hit_rate(),
            throughput: self.throughput(),
            avg_latency: self.store_stats.operations.average_latency(),
            index_entries: self
                .store_stats
                .hash_index
                .num_entries
                .load(Ordering::Relaxed),
            index_load_factor: self.store_stats.hash_index.load_factor(),
            bytes_allocated: self
                .store_stats
                .hybrid_log
                .bytes_allocated
                .load(Ordering::Relaxed),
            pages_flushed: self
                .store_stats
                .hybrid_log
                .pages_flushed
                .load(Ordering::Relaxed),
            memory_in_use: self
                .store_stats
                .allocator
                .current_in_use
                .load(Ordering::Relaxed),
            peak_memory: self
                .store_stats
                .allocator
                .peak_usage
                .load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of statistics at a point in time
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    /// Elapsed time since collection started
    pub elapsed: Duration,
    /// Total operations
    pub total_operations: u64,
    /// Read operations
    pub reads: u64,
    /// Read hits
    pub read_hits: u64,
    /// Upsert operations
    pub upserts: u64,
    /// RMW operations
    pub rmws: u64,
    /// Delete operations
    pub deletes: u64,
    /// Pending operations
    pub pending: u64,
    /// Hit rate
    pub hit_rate: f64,
    /// Throughput (ops/sec)
    pub throughput: f64,
    /// Average latency
    pub avg_latency: Duration,
    /// Number of index entries
    pub index_entries: u64,
    /// Index load factor
    pub index_load_factor: f64,
    /// Bytes allocated
    pub bytes_allocated: u64,
    /// Pages flushed
    pub pages_flushed: u64,
    /// Memory currently in use
    pub memory_in_use: u64,
    /// Peak memory usage
    pub peak_memory: u64,
}

impl StatsSnapshot {
    /// Format bytes as human-readable string
    pub fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = StatsConfig::default();
        assert!(config.enabled);
        assert!(config.track_per_operation);
    }

    #[test]
    fn test_config_builder() {
        let config = StatsConfig::new()
            .with_enabled(false)
            .with_collection_interval(Duration::from_secs(5))
            .with_latency_histogram(true);

        assert!(!config.enabled);
        assert_eq!(config.collection_interval, Duration::from_secs(5));
        assert!(config.track_latency_histogram);
    }

    #[test]
    fn test_collector_creation() {
        let collector = StatsCollector::with_defaults();
        assert!(collector.is_enabled());
    }

    #[test]
    fn test_collector_enable_disable() {
        let collector = StatsCollector::with_defaults();

        collector.disable();
        assert!(!collector.is_enabled());

        collector.enable();
        assert!(collector.is_enabled());
    }

    #[test]
    fn test_collector_snapshot() {
        let collector = StatsCollector::with_defaults();

        collector.store_stats.operations.record_read(true);
        collector.store_stats.operations.record_upsert();

        let snapshot = collector.snapshot();

        assert_eq!(snapshot.reads, 1);
        assert_eq!(snapshot.upserts, 1);
        assert_eq!(snapshot.total_operations, 2);
    }

    #[test]
    fn test_collector_reset() {
        let mut collector = StatsCollector::with_defaults();

        collector.store_stats.operations.record_read(true);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.reads, 1);

        collector.reset();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.reads, 0);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(StatsSnapshot::format_bytes(100), "100 B");
        assert_eq!(StatsSnapshot::format_bytes(1024), "1.00 KB");
        assert_eq!(StatsSnapshot::format_bytes(1048576), "1.00 MB");
        assert_eq!(StatsSnapshot::format_bytes(1073741824), "1.00 GB");
    }
}
