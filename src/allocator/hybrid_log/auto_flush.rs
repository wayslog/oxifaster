use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::device::StorageDevice;

use super::PersistentMemoryMalloc;

/// Configuration for automatic background flushing.
#[derive(Debug, Clone)]
pub struct AutoFlushConfig {
    /// Enable automatic background flushing.
    pub enabled: bool,
    /// Interval between flush checks in milliseconds.
    pub check_interval_ms: u64,
    /// Minimum lag (in read-only pages) before a background flush is triggered.
    pub min_readonly_pages: u32,
    /// Trigger auto-flush when log usage reaches this percentage of max in-memory capacity.
    pub memory_pressure_threshold_percent: u8,
}

impl Default for AutoFlushConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval_ms: 100,
            min_readonly_pages: 4,
            memory_pressure_threshold_percent: 85,
        }
    }
}

impl AutoFlushConfig {
    /// Create a new auto-flush configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable automatic flushing.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the check interval in milliseconds.
    ///
    /// Values below 10ms are clamped to 10ms.
    pub fn with_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.check_interval_ms = interval_ms.max(10);
        self
    }

    /// Set the minimum number of lagging read-only pages before flushing.
    ///
    /// Values below 1 are clamped to 1.
    pub fn with_min_readonly_pages(mut self, pages: u32) -> Self {
        self.min_readonly_pages = pages.max(1);
        self
    }

    /// Set the memory pressure threshold percentage.
    ///
    /// Values are clamped to [1, 100].
    pub fn with_memory_pressure_threshold_percent(mut self, percent: u8) -> Self {
        self.memory_pressure_threshold_percent = percent.clamp(1, 100);
        self
    }

    /// Return whether auto-flush is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Snapshot of automatic flush metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutoFlushMetrics {
    /// Number of successful automatic flush runs.
    pub runs_total: u64,
    /// Total bytes advanced by automatic flushes.
    pub bytes_total: u64,
    /// Number of failed automatic flush attempts.
    pub failures_total: u64,
}

#[derive(Default)]
struct AutoFlushMetricsAtomic {
    runs_total: AtomicU64,
    bytes_total: AtomicU64,
    failures_total: AtomicU64,
}

impl AutoFlushMetricsAtomic {
    fn snapshot(&self) -> AutoFlushMetrics {
        AutoFlushMetrics {
            runs_total: self.runs_total.load(Ordering::Relaxed),
            bytes_total: self.bytes_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
        }
    }

    fn record_success(&self, bytes_flushed: u64) {
        self.runs_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes_flushed, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures_total.fetch_add(1, Ordering::Relaxed);
    }
}

struct AutoFlushWorker {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

#[derive(Clone, Copy)]
struct WorkerHlogPtr<D: StorageDevice>(*const PersistentMemoryMalloc<D>);

// SAFETY: The pointer always targets the owning `PersistentMemoryMalloc`.
// The owner guarantees the auto-flush thread is stopped and joined in `Drop`
// before freeing memory, so the pointed object outlives the worker thread.
unsafe impl<D: StorageDevice> Send for WorkerHlogPtr<D> {}

impl AutoFlushWorker {
    fn start<D: StorageDevice>(
        hlog_ptr: *const PersistentMemoryMalloc<D>,
        config: AutoFlushConfig,
        metrics: Arc<AutoFlushMetricsAtomic>,
    ) -> Self {
        let hlog_ptr = WorkerHlogPtr(hlog_ptr);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::clone(&stop);
        let handle = thread::spawn(move || worker_loop(hlog_ptr, config, stop_signal, metrics));

        Self { stop, handle }
    }

    fn stop(self) {
        self.stop.store(true, Ordering::Release);
        self.handle.thread().unpark();
        let _ = self.handle.join();
    }
}

pub(crate) struct AutoFlushController<D: StorageDevice> {
    config: AutoFlushConfig,
    metrics: Arc<AutoFlushMetricsAtomic>,
    worker: Option<AutoFlushWorker>,
    _marker: PhantomData<fn(D)>,
}

impl<D: StorageDevice> AutoFlushController<D> {
    pub(crate) fn new() -> Self {
        Self {
            config: AutoFlushConfig::default(),
            metrics: Arc::new(AutoFlushMetricsAtomic::default()),
            worker: None,
            _marker: PhantomData,
        }
    }

    pub(crate) fn start(
        &mut self,
        hlog_ptr: *const PersistentMemoryMalloc<D>,
        config: AutoFlushConfig,
    ) -> bool {
        self.config = config.clone();
        if !config.enabled || self.worker.is_some() {
            return false;
        }

        self.worker = Some(AutoFlushWorker::start(
            hlog_ptr,
            config,
            Arc::clone(&self.metrics),
        ));
        true
    }

    pub(crate) fn restart(
        &mut self,
        hlog_ptr: *const PersistentMemoryMalloc<D>,
        config: AutoFlushConfig,
    ) {
        if let Some(worker) = self.worker.take() {
            worker.stop();
        }
        self.config = config.clone();
        if config.enabled {
            self.worker = Some(AutoFlushWorker::start(
                hlog_ptr,
                config,
                Arc::clone(&self.metrics),
            ));
        }
    }

    pub(crate) fn stop(&mut self) -> bool {
        if let Some(worker) = self.worker.take() {
            worker.stop();
            return true;
        }
        false
    }

    pub(crate) fn is_running(&self) -> bool {
        self.worker.is_some()
    }

    pub(crate) fn config(&self) -> AutoFlushConfig {
        self.config.clone()
    }

    pub(crate) fn metrics(&self) -> AutoFlushMetrics {
        self.metrics.snapshot()
    }
}

impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    /// Start the automatic flush worker.
    ///
    /// Returns `true` when a new worker was started. Returns `false` if
    /// `config.enabled == false` or a worker is already running.
    pub fn start_auto_flush(&self, config: AutoFlushConfig) -> bool {
        let mut auto_flush = self.auto_flush.lock();
        auto_flush.start(self as *const Self, config)
    }

    /// Apply auto-flush configuration.
    ///
    /// If a worker is already running, it will be restarted with the new
    /// configuration. If `config.enabled` is false, any running worker is
    /// stopped.
    pub fn set_auto_flush_config(&self, config: AutoFlushConfig) {
        let mut auto_flush = self.auto_flush.lock();
        auto_flush.restart(self as *const Self, config);
    }

    /// Stop the automatic flush worker.
    ///
    /// Returns `true` when a running worker was stopped.
    pub fn stop_auto_flush(&self) -> bool {
        let mut auto_flush = self.auto_flush.lock();
        auto_flush.stop()
    }

    /// Returns whether the automatic flush worker is running.
    pub fn is_auto_flush_running(&self) -> bool {
        self.auto_flush.lock().is_running()
    }

    /// Get the currently applied automatic flush configuration.
    pub fn auto_flush_config(&self) -> AutoFlushConfig {
        self.auto_flush.lock().config()
    }

    /// Snapshot automatic flush metrics.
    pub fn auto_flush_metrics(&self) -> AutoFlushMetrics {
        self.auto_flush.lock().metrics()
    }
}

fn worker_loop<D: StorageDevice>(
    hlog_ptr: WorkerHlogPtr<D>,
    config: AutoFlushConfig,
    stop: Arc<AtomicBool>,
    metrics: Arc<AutoFlushMetricsAtomic>,
) {
    let interval = Duration::from_millis(config.check_interval_ms.max(10));
    let min_readonly_pages = u64::from(config.min_readonly_pages.max(1));
    let pressure_threshold = u64::from(config.memory_pressure_threshold_percent.clamp(1, 100));

    while !stop.load(Ordering::Acquire) {
        // SAFETY: `hlog_ptr` points to the owning `PersistentMemoryMalloc`.
        // The owner guarantees worker shutdown in `Drop`, so the pointer stays
        // valid for the whole worker lifetime.
        let hlog = unsafe { &*hlog_ptr.0 };
        let read_only = hlog.get_read_only_address();
        let safe_read_only = hlog.get_safe_read_only_address();
        let page_size = hlog.page_size() as u64;
        let readonly_lag_pages = if page_size == 0 {
            0
        } else {
            read_only.control().saturating_sub(safe_read_only.control()) / page_size
        };
        let begin = hlog.get_begin_address();
        let tail = hlog.get_tail_address();
        let max_size = hlog.max_size_bytes();
        let usage = tail.control().saturating_sub(begin.control());
        let under_pressure = max_size > 0
            && usage.saturating_mul(100) >= max_size.saturating_mul(pressure_threshold);

        if readonly_lag_pages >= min_readonly_pages || under_pressure {
            let flushed_before = hlog.get_flushed_until_address();
            if read_only > flushed_before {
                match hlog.flush_until(read_only) {
                    Ok(()) => {
                        let flushed_after = hlog.get_flushed_until_address();
                        let bytes_flushed = flushed_after
                            .control()
                            .saturating_sub(flushed_before.control());
                        metrics.record_success(bytes_flushed);
                    }
                    Err(error) => {
                        metrics.record_failure();
                        if tracing::enabled!(tracing::Level::WARN) {
                            tracing::warn!(error = %error, "auto flush failed");
                        }
                    }
                }
            }
        }

        thread::park_timeout(interval);
    }
}
