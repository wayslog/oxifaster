# Phase 4: Implement Automatic Background Flush Worker

**Priority**: Enhancement/Optimization
**Estimated Effort**: 2-3 days
**Status**: Ready for implementation

## Overview

Currently, pages must be explicitly flushed using `flush_until()` or during checkpoint. This phase implements an automatic background worker that monitors memory pressure and proactively flushes pages, making the system more autonomous and production-ready.

---

## Task 4.1: Design Background Flush Architecture

**Estimated Time**: 1-2 hours
**Difficulty**: Low

### Design Goals

1. **Automatic Operation**: No manual intervention required
2. **Memory Awareness**: Respond to memory pressure
3. **Performance**: Minimal impact on foreground operations
4. **Configurable**: Allow tuning for different workloads
5. **Observable**: Provide metrics and logs

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  HybridLog (Main Thread)                │
│  - Allocates pages                                      │
│  - Advances tail_address                                │
│  - Tracks safe_read_only_address                        │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │ Monitors via atomics
                   │
┌──────────────────▼──────────────────────────────────────┐
│              Background Flush Worker                     │
│                                                          │
│  Loop:                                                   │
│   1. Check memory usage                                 │
│   2. Check tail vs flushed delta                        │
│   3. If threshold exceeded:                             │
│      - Calculate flush target                           │
│      - Call flush_until(target)                         │
│      - Update metrics                                   │
│   4. Sleep (configurable interval)                      │
│                                                          │
│  Shutdown:                                               │
│   - Flush all remaining pages                           │
│   - Join thread cleanly                                 │
└─────────────────────────────────────────────────────────┘
```

### Configuration

```rust
pub struct AutoFlushConfig {
    /// Enable automatic background flushing
    pub enabled: bool,

    /// Flush when unflushed data exceeds this many bytes
    pub flush_threshold_bytes: u64,

    /// Flush when unflushed pages exceed this count
    pub flush_threshold_pages: u32,

    /// Check interval in milliseconds
    pub check_interval_ms: u64,

    /// Target to flush: either threshold or absolute address
    pub flush_target: FlushTarget,
}

pub enum FlushTarget {
    /// Flush to read-only address
    ToReadOnly,
    /// Flush to (tail - threshold)
    BehindTail { lag_bytes: u64 },
    /// Flush all pages older than N milliseconds
    OlderThan { age_ms: u64 },
}
```

---

## Task 4.2: Implement Background Flush Worker

**File**: `src/allocator/hybrid_log/auto_flush.rs` (NEW)
**Estimated Time**: 6-8 hours
**Difficulty**: Medium-High

### Implementation

```rust
// TODO: Create src/allocator/hybrid_log/auto_flush.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::address::Address;
use crate::device::StorageDevice;

use super::PersistentMemoryMalloc;

/// Configuration for automatic background flushing.
#[derive(Debug, Clone)]
pub struct AutoFlushConfig {
    /// Enable automatic flushing
    pub enabled: bool,

    /// Flush when unflushed data exceeds this many bytes (default: 64 MB)
    pub flush_threshold_bytes: u64,

    /// Check interval in milliseconds (default: 100ms)
    pub check_interval_ms: u64,

    /// Lag behind tail address when flushing (default: 16 MB)
    pub lag_bytes: u64,
}

impl Default for AutoFlushConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            flush_threshold_bytes: 64 * 1024 * 1024, // 64 MB
            check_interval_ms: 100,
            lag_bytes: 16 * 1024 * 1024, // 16 MB
        }
    }
}

/// Handle to the background flush worker.
///
/// Dropping this handle will stop the worker and wait for it to finish.
pub struct AutoFlushHandle {
    thread: Option<JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
}

impl AutoFlushHandle {
    /// Stop the background flush worker.
    pub fn stop(mut self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for AutoFlushHandle {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    /// Start automatic background flushing.
    ///
    /// Returns a handle that will stop the worker when dropped.
    pub fn start_auto_flush(&self, config: AutoFlushConfig) -> AutoFlushHandle {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = Arc::clone(&stop_flag);

        // Clone necessary fields for the worker thread
        let hlog = self.clone_for_worker();
        let check_interval = Duration::from_millis(config.check_interval_ms);

        let thread = thread::spawn(move || {
            flush_worker_loop(hlog, config, stop_flag_clone, check_interval)
        });

        AutoFlushHandle {
            thread: Some(thread),
            stop_flag,
        }
    }

    /// Clone the HybridLog for use in the worker thread.
    ///
    /// This is a shallow clone that shares the device and pages.
    fn clone_for_worker(&self) -> Self {
        // TODO: Implement proper shallow clone
        // This may require adding Clone trait or using Arc
        // For now, placeholder:
        unimplemented!("Need to add Arc wrapper for HybridLog")
    }
}

fn flush_worker_loop<D: StorageDevice>(
    hlog: PersistentMemoryMalloc<D>,
    config: AutoFlushConfig,
    stop_flag: Arc<AtomicBool>,
    check_interval: Duration,
) {
    if tracing::enabled!(tracing::Level::INFO) {
        tracing::info!("auto flush worker started");
    }

    let mut last_flush = Instant::now();
    let mut total_flushes = 0u64;
    let mut total_bytes_flushed = 0u64;

    while !stop_flag.load(Ordering::Relaxed) {
        thread::sleep(check_interval);

        // Check if flush is needed
        let tail = hlog.get_tail_address();
        let flushed = hlog.get_flushed_until_address();
        let unflushed_bytes = tail.control().saturating_sub(flushed.control());

        if unflushed_bytes >= config.flush_threshold_bytes {
            // Calculate flush target (lag behind tail)
            let target_offset = tail.control().saturating_sub(config.lag_bytes);
            let target = Address::from_control(target_offset);

            if target > flushed {
                let flush_start = Instant::now();

                if let Err(e) = hlog.flush_until(target) {
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(
                            error = %e,
                            target = %target,
                            "auto flush failed"
                        );
                    }
                } else {
                    let flush_duration = flush_start.elapsed();
                    let bytes_flushed = target.control().saturating_sub(flushed.control());

                    total_flushes += 1;
                    total_bytes_flushed += bytes_flushed;

                    if tracing::enabled!(tracing::Level::DEBUG) {
                        tracing::debug!(
                            target = %target,
                            bytes = bytes_flushed,
                            duration_ms = flush_duration.as_millis(),
                            "auto flush completed"
                        );
                    }

                    last_flush = Instant::now();
                }
            }
        }
    }

    // Final flush on shutdown
    if let Err(e) = hlog.flush_to_disk() {
        if tracing::enabled!(tracing::Level::WARN) {
            tracing::warn!(error = %e, "final flush on shutdown failed");
        }
    }

    if tracing::enabled!(tracing::Level::INFO) {
        tracing::info!(
            total_flushes,
            total_bytes_flushed,
            "auto flush worker stopped"
        );
    }
}
```

### Integration Points

```rust
// TODO: Update src/allocator/hybrid_log.rs

// Add this field to PersistentMemoryMalloc
pub struct PersistentMemoryMalloc<D: StorageDevice> {
    // ... existing fields ...

    /// Auto flush worker handle
    auto_flush_handle: Option<AutoFlushHandle>,
}

// Add method to start auto flush
impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    pub fn new_with_auto_flush(
        config: HybridLogConfig,
        device: Arc<D>,
        auto_flush_config: AutoFlushConfig,
    ) -> io::Result<Self> {
        let mut hlog = Self::new(config, device)?;

        if auto_flush_config.enabled {
            let handle = hlog.start_auto_flush(auto_flush_config);
            hlog.auto_flush_handle = Some(handle);
        }

        Ok(hlog)
    }
}
```

---

## Task 4.3: Add Memory Pressure Detection

**Estimated Time**: 3-4 hours
**Difficulty**: Medium

### Purpose

Make flush decisions based on actual memory usage, not just page count.

```rust
// TODO: Add to src/allocator/hybrid_log/auto_flush.rs

/// Memory pressure monitor
struct MemoryPressureMonitor {
    /// Total memory allocated for pages
    total_memory_bytes: u64,

    /// Target memory usage (e.g., 80% of total)
    target_usage_fraction: f64,
}

impl MemoryPressureMonitor {
    fn new(total_memory_bytes: u64, target_fraction: f64) -> Self {
        Self {
            total_memory_bytes,
            target_usage_fraction: target_fraction,
        }
    }

    /// Check if memory pressure is high
    fn is_high_pressure(&self, pages_in_memory: u32, page_size: usize) -> bool {
        let current_usage = pages_in_memory as u64 * page_size as u64;
        let threshold = (self.total_memory_bytes as f64 * self.target_usage_fraction) as u64;
        current_usage >= threshold
    }

    /// Get aggressive flush target based on pressure
    fn get_flush_target(
        &self,
        tail: Address,
        flushed: Address,
        pages_in_memory: u32,
        page_size: usize,
    ) -> Address {
        let current_usage = pages_in_memory as u64 * page_size as u64;
        let target_usage = (self.total_memory_bytes as f64 * self.target_usage_fraction) as u64;

        if current_usage <= target_usage {
            return flushed; // No flush needed
        }

        // Calculate how much to flush
        let excess = current_usage - target_usage;
        let flush_to = tail.control().saturating_sub(excess);

        Address::from_control(flush_to).max(flushed)
    }
}
```

### Integration

```rust
// Update flush_worker_loop to use memory pressure

fn flush_worker_loop<D: StorageDevice>(
    hlog: PersistentMemoryMalloc<D>,
    config: AutoFlushConfig,
    stop_flag: Arc<AtomicBool>,
    check_interval: Duration,
) {
    let memory_monitor = MemoryPressureMonitor::new(
        config.total_memory_bytes,
        0.8, // Flush when 80% full
    );

    while !stop_flag.load(Ordering::Relaxed) {
        thread::sleep(check_interval);

        let tail = hlog.get_tail_address();
        let flushed = hlog.get_flushed_until_address();
        let pages_in_memory = hlog.pages_in_memory_count();

        // Check memory pressure
        if memory_monitor.is_high_pressure(pages_in_memory, hlog.page_size()) {
            let target = memory_monitor.get_flush_target(
                tail,
                flushed,
                pages_in_memory,
                hlog.page_size(),
            );

            if target > flushed {
                let _ = hlog.flush_until(target);
            }
        }
    }
}
```

---

## Task 4.4: Add Auto Flush Metrics

**File**: `src/stats/metrics.rs`
**Estimated Time**: 2-3 hours
**Difficulty**: Low

### Metrics to Track

```rust
// TODO: Add to src/stats/metrics.rs

/// Auto flush statistics
#[derive(Debug, Default, Clone)]
pub struct AutoFlushMetrics {
    /// Total number of auto flushes performed
    pub total_flushes: u64,

    /// Total bytes flushed by auto flush
    pub total_bytes_flushed: u64,

    /// Total time spent in auto flush (microseconds)
    pub total_flush_time_us: u64,

    /// Number of flush failures
    pub flush_failures: u64,

    /// Last flush timestamp (milliseconds since epoch)
    pub last_flush_time_ms: u64,

    /// Last flush size in bytes
    pub last_flush_bytes: u64,

    /// Last flush duration in microseconds
    pub last_flush_duration_us: u64,

    /// Average flush size
    pub avg_flush_bytes: u64,

    /// Average flush duration
    pub avg_flush_duration_us: u64,
}

impl AutoFlushMetrics {
    pub fn record_flush(&mut self, bytes: u64, duration_us: u64) {
        self.total_flushes += 1;
        self.total_bytes_flushed += bytes;
        self.total_flush_time_us += duration_us;
        self.last_flush_bytes = bytes;
        self.last_flush_duration_us = duration_us;
        self.last_flush_time_ms = current_time_ms();

        // Update averages
        if self.total_flushes > 0 {
            self.avg_flush_bytes = self.total_bytes_flushed / self.total_flushes;
            self.avg_flush_duration_us = self.total_flush_time_us / self.total_flushes;
        }
    }

    pub fn record_failure(&mut self) {
        self.flush_failures += 1;
    }
}
```

---

## Task 4.5: Add Tests

**File**: `tests/auto_flush.rs` (NEW)
**Estimated Time**: 4-5 hours
**Difficulty**: Medium

```rust
// TODO: Create tests/auto_flush.rs

#[test]
fn test_auto_flush_triggers() {
    // 1. Create HybridLog with auto flush enabled
    // 2. Write data to exceed threshold
    // 3. Wait for auto flush
    // 4. Verify pages were flushed
}

#[test]
fn test_auto_flush_stops_cleanly() {
    // 1. Start auto flush
    // 2. Drop handle
    // 3. Verify thread stopped
    // 4. Verify final flush occurred
}

#[test]
fn test_auto_flush_respects_lag() {
    // 1. Configure specific lag
    // 2. Trigger flush
    // 3. Verify flush target respects lag setting
}

#[test]
fn test_auto_flush_memory_pressure() {
    // 1. Fill memory to threshold
    // 2. Verify aggressive flushing occurs
    // 3. Verify memory usage drops
}

#[test]
fn test_auto_flush_metrics() {
    // 1. Perform multiple flushes
    // 2. Check metrics are accurate
    // 3. Verify averages calculated correctly
}

#[test]
fn test_auto_flush_disabled() {
    // 1. Create with auto flush disabled
    // 2. Fill memory
    // 3. Verify no automatic flushing occurs
}
```

---

## Task 4.6: Update FasterKv Integration

**File**: `src/store/faster_kv.rs`
**Estimated Time**: 2-3 hours
**Difficulty**: Low

### Integration with FasterKv

```rust
// TODO: Update src/store/faster_kv.rs

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Create a new FasterKv with automatic background flushing.
    pub fn with_auto_flush(
        config: FasterKvConfig,
        device: D,
        auto_flush_config: AutoFlushConfig,
    ) -> Self {
        // Create HybridLog with auto flush enabled
        let hlog = PersistentMemoryMalloc::new_with_auto_flush(
            config.to_hybrid_log_config(),
            Arc::new(device),
            auto_flush_config,
        ).expect("Failed to create hybrid log");

        // ... rest of FasterKv initialization ...
    }

    /// Get auto flush metrics
    pub fn auto_flush_metrics(&self) -> Option<AutoFlushMetrics> {
        unsafe { (*self.hlog.get()).get_auto_flush_metrics() }
    }
}
```

---

## Success Criteria

Phase 4 is complete when:

 Background flush worker implemented
 Memory pressure detection working
 Metrics collection in place
 Auto flush can be enabled/disabled
 Clean shutdown implemented
 All tests passing
 Integration with FasterKv complete
 Documentation updated

---

## Estimated Breakdown

| Task | Time | Difficulty |
|------|------|-----------|
| 4.1 Design | 1-2h | Low |
| 4.2 Worker implementation | 6-8h | Medium-High |
| 4.3 Memory pressure | 3-4h | Medium |
| 4.4 Metrics | 2-3h | Low |
| 4.5 Tests | 4-5h | Medium |
| 4.6 Integration | 2-3h | Low |
| **Total** | **18-25h** | **~2-3 days** |

---

## Notes for Implementer

### Architecture Considerations

1. **Thread Safety**: HybridLog must be shared safely between main thread and worker
   - Consider wrapping in `Arc<Mutex<>>` or using atomics only
   - Current implementation uses raw pointers, may need refactoring

2. **Performance**: Background flush should not block foreground operations
   - Use separate tokio runtime or blocking I/O
   - Consider rate limiting

3. **Shutdown**: Clean shutdown is critical
   - Final flush before thread stops
   - No data loss on drop

### Challenges

- **Ownership**: HybridLog currently uses `UnsafeCell`, may need Arc wrapper
- **Blocking**: Worker thread does blocking I/O, separate from main runtime
- **Metrics**: Need atomic counters or mutex for thread-safe updates

### Testing Strategy

- Test with mock device to control timing
- Use small memory limits to trigger flushes quickly
- Verify no deadlocks or race conditions
- Test shutdown in various states

---

## Optional Enhancements

- **Adaptive intervals**: Adjust check frequency based on write rate
- **Predictive flushing**: Flush before threshold based on write patterns
- **Multi-threaded flush**: Parallel page writes
- **Flush scheduling**: Avoid flushing during peak write times

---

## References

- HybridLog implementation: `src/allocator/hybrid_log.rs`
- Existing flush logic: `src/allocator/hybrid_log/flush.rs`
- FasterLog auto flush: `src/log/faster_log.rs` (for inspiration)
- Compaction auto worker: `src/compaction/auto_compact.rs` (similar pattern)
