//! Automatic log compaction for FASTER
//!
//! This module provides automatic background compaction functionality.
//! A background worker thread periodically checks if compaction is needed
//! and triggers compaction when the log exceeds configured thresholds.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::address::Address;
use crate::compaction::{CompactionResult, CompactionStats};

/// Configuration for automatic compaction
#[derive(Debug, Clone)]
pub struct AutoCompactionConfig {
    /// Interval between compaction checks
    pub check_interval: Duration,
    /// Minimum log size before considering compaction
    pub min_log_size: u64,
    /// Target utilization ratio (0.0 to 1.0)
    /// Compaction is triggered when actual utilization falls below this
    pub target_utilization: f64,
    /// Maximum compaction operations per hour (0 = unlimited)
    pub max_compactions_per_hour: u32,
    /// Whether to enable adaptive scheduling based on workload
    pub adaptive_scheduling: bool,
    /// Quiet period after a compaction before checking again
    pub quiet_period: Duration,
}

impl Default for AutoCompactionConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            min_log_size: 64 * 1024 * 1024, // 64 MB
            target_utilization: 0.5,
            max_compactions_per_hour: 0, // unlimited
            adaptive_scheduling: true,
            quiet_period: Duration::from_secs(5),
        }
    }
}

impl AutoCompactionConfig {
    /// Create a new auto compaction configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the check interval
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Set the minimum log size
    pub fn with_min_log_size(mut self, size: u64) -> Self {
        self.min_log_size = size;
        self
    }

    /// Set the target utilization ratio
    pub fn with_target_utilization(mut self, ratio: f64) -> Self {
        self.target_utilization = ratio.clamp(0.0, 1.0);
        self
    }

    /// Set the maximum compactions per hour
    pub fn with_max_compactions_per_hour(mut self, max: u32) -> Self {
        self.max_compactions_per_hour = max;
        self
    }

    /// Set the quiet period
    pub fn with_quiet_period(mut self, period: Duration) -> Self {
        self.quiet_period = period;
        self
    }
}

/// Callback interface for auto compaction
///
/// Implementations of this trait provide the necessary operations
/// for the auto compaction worker to interact with the store.
pub trait AutoCompactionTarget: Send + Sync + 'static {
    /// Check if compaction should be triggered
    ///
    /// Returns (should_compact, current_utilization, log_size)
    fn should_compact(&self) -> (bool, f64, u64);

    /// Get the current begin address
    fn get_begin_address(&self) -> Address;

    /// Get the safe head address (oldest address in memory)
    fn get_safe_head_address(&self) -> Address;

    /// Perform compaction up to the specified address
    fn compact_until(&self, until_address: Address) -> CompactionResult;
}

/// Statistics for auto compaction
#[derive(Debug, Default)]
pub struct AutoCompactionStats {
    /// Total compactions performed
    pub total_compactions: AtomicU64,
    /// Successful compactions
    pub successful_compactions: AtomicU64,
    /// Failed compactions
    pub failed_compactions: AtomicU64,
    /// Total bytes reclaimed
    pub total_bytes_reclaimed: AtomicU64,
    /// Total records compacted
    pub total_records_compacted: AtomicU64,
    /// Last compaction timestamp (Unix epoch seconds)
    pub last_compaction_time: AtomicU64,
    /// Last compaction duration in milliseconds
    pub last_compaction_duration_ms: AtomicU64,
}

impl AutoCompactionStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful compaction
    pub fn record_success(&self, stats: &CompactionStats) {
        self.total_compactions.fetch_add(1, Ordering::Relaxed);
        self.successful_compactions.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_reclaimed
            .fetch_add(stats.bytes_reclaimed, Ordering::Relaxed);
        self.total_records_compacted
            .fetch_add(stats.records_compacted, Ordering::Relaxed);
        self.last_compaction_duration_ms
            .store(stats.duration_ms, Ordering::Relaxed);
        self.last_compaction_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    /// Record a failed compaction
    pub fn record_failure(&self) {
        self.total_compactions.fetch_add(1, Ordering::Relaxed);
        self.failed_compactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.total_compactions.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        self.successful_compactions.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.total_compactions.store(0, Ordering::Relaxed);
        self.successful_compactions.store(0, Ordering::Relaxed);
        self.failed_compactions.store(0, Ordering::Relaxed);
        self.total_bytes_reclaimed.store(0, Ordering::Relaxed);
        self.total_records_compacted.store(0, Ordering::Relaxed);
        self.last_compaction_time.store(0, Ordering::Relaxed);
        self.last_compaction_duration_ms.store(0, Ordering::Relaxed);
    }
}

/// State of the auto compaction worker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoCompactionState {
    /// Worker is stopped
    Stopped,
    /// Worker is running and idle
    Idle,
    /// Worker is currently compacting
    Compacting,
    /// Worker is paused
    Paused,
}

/// Auto compaction worker
///
/// Runs a background thread that periodically checks if compaction
/// is needed and triggers compaction when appropriate.
pub struct AutoCompactionWorker<T: AutoCompactionTarget> {
    /// Configuration
    config: AutoCompactionConfig,
    /// Weak reference to the target store
    target: Weak<T>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Paused flag
    paused: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<AutoCompactionStats>,
    /// Worker thread handle
    handle: Option<JoinHandle<()>>,
    /// Current state
    state: Arc<std::sync::atomic::AtomicU8>,
}

impl<T: AutoCompactionTarget> AutoCompactionWorker<T> {
    /// Create a new auto compaction worker
    pub fn new(target: Weak<T>, config: AutoCompactionConfig) -> Self {
        Self {
            config,
            target,
            running: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(AutoCompactionStats::new()),
            handle: None,
            state: Arc::new(std::sync::atomic::AtomicU8::new(
                AutoCompactionState::Stopped as u8,
            )),
        }
    }

    /// Get the current state
    pub fn state(&self) -> AutoCompactionState {
        match self.state.load(Ordering::Acquire) {
            0 => AutoCompactionState::Stopped,
            1 => AutoCompactionState::Idle,
            2 => AutoCompactionState::Compacting,
            3 => AutoCompactionState::Paused,
            _ => AutoCompactionState::Stopped,
        }
    }

    /// Check if the worker is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Check if the worker is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Get the statistics
    pub fn stats(&self) -> &Arc<AutoCompactionStats> {
        &self.stats
    }

    /// Get the configuration
    pub fn config(&self) -> &AutoCompactionConfig {
        &self.config
    }

    /// Start the auto compaction worker
    ///
    /// Returns true if the worker was started, false if already running.
    pub fn start(&mut self) -> bool {
        if self.running.swap(true, Ordering::AcqRel) {
            return false;
        }

        let running = Arc::clone(&self.running);
        let paused = Arc::clone(&self.paused);
        let stats = Arc::clone(&self.stats);
        let state = Arc::clone(&self.state);
        let target = self.target.clone();
        let config = self.config.clone();

        let handle = thread::spawn(move || {
            Self::worker_loop(running, paused, stats, state, target, config);
        });

        self.handle = Some(handle);
        true
    }

    /// Stop the auto compaction worker
    ///
    /// Signals the worker to stop and waits for it to finish.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Release);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        self.state
            .store(AutoCompactionState::Stopped as u8, Ordering::Release);
    }

    /// Pause the auto compaction worker
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
        self.state
            .store(AutoCompactionState::Paused as u8, Ordering::Release);
    }

    /// Resume the auto compaction worker
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        if self.running.load(Ordering::Acquire) {
            self.state
                .store(AutoCompactionState::Idle as u8, Ordering::Release);
        }
    }

    /// Worker loop that runs in the background thread
    fn worker_loop(
        running: Arc<AtomicBool>,
        paused: Arc<AtomicBool>,
        stats: Arc<AutoCompactionStats>,
        state: Arc<std::sync::atomic::AtomicU8>,
        target: Weak<T>,
        config: AutoCompactionConfig,
    ) {
        let mut last_compaction = Instant::now();
        let mut compactions_this_hour = 0u32;
        let mut hour_start = Instant::now();

        state.store(AutoCompactionState::Idle as u8, Ordering::Release);

        while running.load(Ordering::Acquire) {
            // Check if paused
            if paused.load(Ordering::Acquire) {
                thread::sleep(config.check_interval);
                continue;
            }

            // Check rate limiting
            if hour_start.elapsed() >= Duration::from_secs(3600) {
                hour_start = Instant::now();
                compactions_this_hour = 0;
            }

            if config.max_compactions_per_hour > 0
                && compactions_this_hour >= config.max_compactions_per_hour
            {
                thread::sleep(config.check_interval);
                continue;
            }

            // Check quiet period
            if last_compaction.elapsed() < config.quiet_period {
                thread::sleep(config.check_interval);
                continue;
            }

            // Try to get the target
            let store = match target.upgrade() {
                Some(s) => s,
                None => {
                    // Target has been dropped, clear running flag and stop the worker
                    // This ensures is_running() returns false and start() can restart
                    running.store(false, Ordering::Release);
                    break;
                }
            };

            // Check if compaction is needed
            let (should_compact, _utilization, log_size) = store.should_compact();

            if !should_compact || log_size < config.min_log_size {
                thread::sleep(config.check_interval);
                continue;
            }

            // Perform compaction
            state.store(AutoCompactionState::Compacting as u8, Ordering::Release);

            let begin_address = store.get_begin_address();
            let safe_head = store.get_safe_head_address();

            // Calculate target address (compact up to safe head)
            let result = store.compact_until(safe_head);

            if result.status.is_ok() && result.new_begin_address > begin_address {
                stats.record_success(&result.stats);
                compactions_this_hour += 1;
            } else {
                stats.record_failure();
            }

            last_compaction = Instant::now();
            state.store(AutoCompactionState::Idle as u8, Ordering::Release);

            // Sleep before next check
            thread::sleep(config.check_interval);
        }

        state.store(AutoCompactionState::Stopped as u8, Ordering::Release);
    }
}

impl<T: AutoCompactionTarget> Drop for AutoCompactionWorker<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::Status;
    use std::sync::atomic::AtomicU64;

    /// Mock target for testing
    struct MockTarget {
        should_compact_result: AtomicBool,
        utilization: std::sync::atomic::AtomicU64,
        log_size: AtomicU64,
        begin_address: Address,
        safe_head: Address,
        compact_count: AtomicU64,
    }

    impl MockTarget {
        fn new() -> Self {
            Self {
                should_compact_result: AtomicBool::new(false),
                utilization: AtomicU64::new(50), // 0.50
                log_size: AtomicU64::new(100 * 1024 * 1024),
                begin_address: Address::new(0, 0),
                safe_head: Address::new(10, 0),
                compact_count: AtomicU64::new(0),
            }
        }

        fn set_should_compact(&self, value: bool) {
            self.should_compact_result.store(value, Ordering::Release);
        }

        fn get_compact_count(&self) -> u64 {
            self.compact_count.load(Ordering::Acquire)
        }
    }

    impl AutoCompactionTarget for MockTarget {
        fn should_compact(&self) -> (bool, f64, u64) {
            let should = self.should_compact_result.load(Ordering::Acquire);
            let util = self.utilization.load(Ordering::Acquire) as f64 / 100.0;
            let size = self.log_size.load(Ordering::Acquire);
            (should, util, size)
        }

        fn get_begin_address(&self) -> Address {
            self.begin_address
        }

        fn get_safe_head_address(&self) -> Address {
            self.safe_head
        }

        fn compact_until(&self, _until_address: Address) -> CompactionResult {
            self.compact_count.fetch_add(1, Ordering::Relaxed);
            CompactionResult::success(
                Address::new(5, 0),
                CompactionStats {
                    records_scanned: 100,
                    records_compacted: 50,
                    records_skipped: 50,
                    tombstones_found: 0,
                    bytes_scanned: 10000,
                    bytes_compacted: 5000,
                    bytes_reclaimed: 5000,
                    duration_ms: 10,
                },
            )
        }
    }

    #[test]
    fn test_auto_compaction_config() {
        let config = AutoCompactionConfig::new()
            .with_check_interval(Duration::from_secs(10))
            .with_min_log_size(32 * 1024 * 1024)
            .with_target_utilization(0.6)
            .with_max_compactions_per_hour(10);

        assert_eq!(config.check_interval, Duration::from_secs(10));
        assert_eq!(config.min_log_size, 32 * 1024 * 1024);
        assert_eq!(config.target_utilization, 0.6);
        assert_eq!(config.max_compactions_per_hour, 10);
    }

    #[test]
    fn test_auto_compaction_stats() {
        let stats = AutoCompactionStats::new();

        let compaction_stats = CompactionStats {
            records_scanned: 100,
            records_compacted: 50,
            bytes_reclaimed: 5000,
            ..Default::default()
        };

        stats.record_success(&compaction_stats);
        assert_eq!(stats.successful_compactions.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_bytes_reclaimed.load(Ordering::Relaxed), 5000);

        stats.record_failure();
        assert_eq!(stats.total_compactions.load(Ordering::Relaxed), 2);
        assert_eq!(stats.failed_compactions.load(Ordering::Relaxed), 1);
        assert_eq!(stats.success_rate(), 0.5);
    }

    #[test]
    fn test_auto_compaction_worker_start_stop() {
        let target = Arc::new(MockTarget::new());
        let config = AutoCompactionConfig::new()
            .with_check_interval(Duration::from_millis(50))
            .with_quiet_period(Duration::from_millis(10));

        let mut worker = AutoCompactionWorker::new(Arc::downgrade(&target), config);

        assert!(!worker.is_running());
        assert_eq!(worker.state(), AutoCompactionState::Stopped);

        assert!(worker.start());
        assert!(worker.is_running());

        // Second start should return false
        assert!(!worker.start());

        // Let the worker run briefly
        thread::sleep(Duration::from_millis(100));

        worker.stop();
        assert!(!worker.is_running());
        assert_eq!(worker.state(), AutoCompactionState::Stopped);
    }

    #[test]
    fn test_auto_compaction_worker_pause_resume() {
        let target = Arc::new(MockTarget::new());
        let config = AutoCompactionConfig::new()
            .with_check_interval(Duration::from_millis(50));

        let mut worker = AutoCompactionWorker::new(Arc::downgrade(&target), config);
        worker.start();

        thread::sleep(Duration::from_millis(50));

        worker.pause();
        assert!(worker.is_paused());

        worker.resume();
        assert!(!worker.is_paused());

        worker.stop();
    }

    #[test]
    fn test_auto_compaction_worker_triggers_compaction() {
        let target = Arc::new(MockTarget::new());
        target.set_should_compact(true);

        let config = AutoCompactionConfig::new()
            .with_check_interval(Duration::from_millis(50))
            .with_quiet_period(Duration::from_millis(10))
            .with_min_log_size(0); // Always compact

        let mut worker = AutoCompactionWorker::new(Arc::downgrade(&target), config);
        worker.start();

        // Wait for at least one compaction
        thread::sleep(Duration::from_millis(200));

        worker.stop();

        // Should have performed at least one compaction
        assert!(target.get_compact_count() >= 1);
        assert!(worker.stats().successful_compactions.load(Ordering::Relaxed) >= 1);
    }
}
