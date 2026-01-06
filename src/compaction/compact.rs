//! Compaction implementation for FASTER
//!
//! Based on C++ FASTER's compaction algorithms.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::address::Address;
use crate::compaction::contexts::CompactionContext;
use crate::scan::ScanRange;
use crate::status::Status;

/// Configuration for compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Target utilization ratio (0.0 to 1.0)
    /// Compaction is triggered when actual utilization falls below this
    pub target_utilization: f64,
    /// Minimum bytes to compact in one operation
    pub min_compact_bytes: u64,
    /// Maximum bytes to compact in one operation  
    pub max_compact_bytes: u64,
    /// Number of threads for concurrent compaction
    pub num_threads: usize,
    /// Whether to compact tombstones
    pub compact_tombstones: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_utilization: 0.5,
            min_compact_bytes: 1 << 20, // 1 MB
            max_compact_bytes: 1 << 30, // 1 GB
            num_threads: 1,
            compact_tombstones: true,
        }
    }
}

impl CompactionConfig {
    /// Create a new compaction configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the target utilization ratio
    pub fn with_target_utilization(mut self, ratio: f64) -> Self {
        self.target_utilization = ratio.clamp(0.0, 1.0);
        self
    }

    /// Set the minimum bytes to compact
    pub fn with_min_compact_bytes(mut self, bytes: u64) -> Self {
        self.min_compact_bytes = bytes;
        self
    }

    /// Set the maximum bytes to compact
    pub fn with_max_compact_bytes(mut self, bytes: u64) -> Self {
        self.max_compact_bytes = bytes;
        self
    }

    /// Set the number of threads for concurrent compaction
    pub fn with_num_threads(mut self, num: usize) -> Self {
        self.num_threads = num.max(1);
        self
    }
}

/// Result of a compaction operation
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Status of the compaction
    pub status: Status,
    /// New begin address after compaction
    pub new_begin_address: Address,
    /// Statistics from the compaction
    pub stats: CompactionStats,
}

impl CompactionResult {
    /// Create a successful compaction result
    pub fn success(new_begin_address: Address, stats: CompactionStats) -> Self {
        Self {
            status: Status::Ok,
            new_begin_address,
            stats,
        }
    }

    /// Create a failed compaction result
    pub fn failure(status: Status) -> Self {
        Self {
            status,
            new_begin_address: Address::INVALID,
            stats: CompactionStats::default(),
        }
    }
}

/// Statistics from a compaction operation
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Total records scanned
    pub records_scanned: u64,
    /// Records that were compacted (moved to new location)
    pub records_compacted: u64,
    /// Records that were skipped (obsolete/superseded)
    pub records_skipped: u64,
    /// Tombstone records found
    pub tombstones_found: u64,
    /// Bytes scanned
    pub bytes_scanned: u64,
    /// Bytes that were compacted (copied to new location)
    pub bytes_compacted: u64,
    /// Bytes reclaimed
    pub bytes_reclaimed: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl CompactionStats {
    /// Calculate the compaction ratio (reclaimed / scanned)
    pub fn compaction_ratio(&self) -> f64 {
        if self.bytes_scanned == 0 {
            return 0.0;
        }
        self.bytes_reclaimed as f64 / self.bytes_scanned as f64
    }

    /// Calculate the live record ratio
    pub fn live_ratio(&self) -> f64 {
        if self.records_scanned == 0 {
            return 0.0;
        }
        self.records_compacted as f64 / self.records_scanned as f64
    }
}

/// Compactor for performing log compaction
pub struct Compactor {
    /// Configuration
    config: CompactionConfig,
    /// Compaction in progress flag
    in_progress: AtomicU64,
}

impl Compactor {
    /// Create a new compactor with default configuration
    pub fn new() -> Self {
        Self::with_config(CompactionConfig::default())
    }

    /// Create a new compactor with the given configuration
    pub fn with_config(config: CompactionConfig) -> Self {
        Self {
            config,
            in_progress: AtomicU64::new(0),
        }
    }

    /// Get the compaction configuration
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    /// Check if compaction is currently in progress
    pub fn is_in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Acquire) != 0
    }

    /// Try to start a compaction operation
    /// Returns Ok(()) if compaction can start, Err if already in progress
    pub fn try_start(&self) -> Result<(), Status> {
        match self
            .in_progress
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(_) => Err(Status::Aborted),
        }
    }

    /// Complete a compaction operation
    pub fn complete(&self) {
        self.in_progress.store(0, Ordering::Release);
    }

    /// Calculate the scan range for compaction
    ///
    /// # Arguments
    /// * `begin_address` - Current begin address of the log
    /// * `safe_head_address` - Safe head address (oldest address in memory)
    /// * `target_address` - Optional target address to compact up to
    ///
    /// # Returns
    /// The scan range for compaction, or None if no compaction needed
    pub fn calculate_scan_range(
        &self,
        begin_address: Address,
        safe_head_address: Address,
        target_address: Option<Address>,
    ) -> Option<ScanRange> {
        // Determine the end of the scan range
        let scan_end = match target_address {
            Some(target) if target < safe_head_address => target,
            _ => safe_head_address,
        };

        // Check if there's anything to compact
        if begin_address >= scan_end {
            return None;
        }

        // Check minimum size
        let range_size = scan_end.control().saturating_sub(begin_address.control());
        if range_size < self.config.min_compact_bytes {
            return None;
        }

        // Limit to maximum size
        let effective_end = if range_size > self.config.max_compact_bytes {
            Address::from_control(begin_address.control() + self.config.max_compact_bytes)
        } else {
            scan_end
        };

        Some(ScanRange::new(begin_address, effective_end))
    }

    /// Create a compaction context for the given range
    pub fn create_context(&self, range: ScanRange) -> CompactionContext {
        CompactionContext::new(range.begin, range.end)
    }

    /// Check if a record should be compacted (is still live)
    ///
    /// A record is considered live if:
    /// 1. Its address matches the address in the hash index entry
    /// 2. It's not a tombstone (unless compact_tombstones is true)
    ///
    /// # Arguments
    /// * `record_address` - Address of the record
    /// * `index_address` - Address stored in the hash index for this key
    /// * `is_tombstone` - Whether the record is a tombstone
    ///
    /// # Returns
    /// true if the record should be compacted (moved), false if it should be skipped
    pub fn should_compact_record(
        &self,
        record_address: Address,
        index_address: Address,
        is_tombstone: bool,
    ) -> bool {
        // Skip if record is not the latest version
        if record_address != index_address {
            return false;
        }

        // Skip tombstones unless configured to compact them
        if is_tombstone && !self.config.compact_tombstones {
            return false;
        }

        true
    }
}

impl Default for Compactor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.target_utilization, 0.5);
        assert_eq!(config.num_threads, 1);
    }

    #[test]
    fn test_compaction_config_builder() {
        let config = CompactionConfig::new()
            .with_target_utilization(0.7)
            .with_min_compact_bytes(1 << 22)
            .with_num_threads(4);

        assert_eq!(config.target_utilization, 0.7);
        assert_eq!(config.min_compact_bytes, 1 << 22);
        assert_eq!(config.num_threads, 4);
    }

    #[test]
    fn test_compaction_stats() {
        let stats = CompactionStats {
            records_scanned: 100,
            records_compacted: 60,
            records_skipped: 40,
            tombstones_found: 10,
            bytes_scanned: 10000,
            bytes_compacted: 6000,
            bytes_reclaimed: 4000,
            duration_ms: 100,
        };

        assert_eq!(stats.live_ratio(), 0.6);
        assert_eq!(stats.compaction_ratio(), 0.4);
    }

    #[test]
    fn test_compactor_try_start() {
        let compactor = Compactor::new();

        // First start should succeed
        assert!(compactor.try_start().is_ok());
        assert!(compactor.is_in_progress());

        // Second start should fail
        assert!(compactor.try_start().is_err());

        // After complete, should be able to start again
        compactor.complete();
        assert!(!compactor.is_in_progress());
        assert!(compactor.try_start().is_ok());
    }

    #[test]
    fn test_calculate_scan_range() {
        let compactor = Compactor::with_config(CompactionConfig::new().with_min_compact_bytes(0));

        let begin = Address::new(0, 0);
        let head = Address::new(10, 0);

        let range = compactor.calculate_scan_range(begin, head, None);
        assert!(range.is_some());

        let range = range.unwrap();
        assert_eq!(range.begin, begin);
        assert_eq!(range.end, head);
    }

    #[test]
    fn test_calculate_scan_range_with_target() {
        let compactor = Compactor::with_config(CompactionConfig::new().with_min_compact_bytes(0));

        let begin = Address::new(0, 0);
        let head = Address::new(10, 0);
        let target = Address::new(5, 0);

        let range = compactor.calculate_scan_range(begin, head, Some(target));
        assert!(range.is_some());

        let range = range.unwrap();
        assert_eq!(range.begin, begin);
        assert_eq!(range.end, target);
    }

    #[test]
    fn test_should_compact_record() {
        let compactor = Compactor::new();
        let addr1 = Address::new(1, 100);
        let addr2 = Address::new(2, 200);

        // Record is latest version, not tombstone
        assert!(compactor.should_compact_record(addr1, addr1, false));

        // Record is latest version, is tombstone (should still compact by default)
        assert!(compactor.should_compact_record(addr1, addr1, true));

        // Record is not latest version
        assert!(!compactor.should_compact_record(addr1, addr2, false));
    }

    #[test]
    fn test_should_compact_skip_tombstones() {
        // Modify config to not compact tombstones
        let mut config = CompactionConfig::default();
        config.compact_tombstones = false;
        let compactor = Compactor::with_config(config);

        let addr = Address::new(1, 100);

        // Tombstone should be skipped
        assert!(!compactor.should_compact_record(addr, addr, true));

        // Non-tombstone should still be compacted
        assert!(compactor.should_compact_record(addr, addr, false));
    }
}
