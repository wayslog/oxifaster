//! Concurrent Compaction for FASTER
//!
//! This module provides multi-threaded log compaction capabilities.
//! Based on C++ FASTER's ConcurrentCompactionThreadsContext implementation.
//!
//! # Architecture
//!
//! The concurrent compaction system uses a page-based work distribution model:
//! - The log is divided into pages
//! - Each thread claims pages atomically to process
//! - Results are aggregated at the end
//!
//! # Usage
//!
//! ```ignore
//! let config = ConcurrentCompactionConfig::new(4); // 4 threads
//! let compactor = ConcurrentCompactor::new(config);
//!
//! let range = ScanRange::new(begin_addr, end_addr);
//! let result = compactor.compact_range(range, &store);
//! ```

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::address::Address;
use crate::compaction::{CompactionConfig, CompactionResult, CompactionStats};
use crate::scan::ScanRange;
use crate::status::Status;

/// Configuration for concurrent compaction
#[derive(Debug, Clone)]
pub struct ConcurrentCompactionConfig {
    /// Number of threads to use
    pub num_threads: usize,
    /// Pages per chunk (work unit size)
    pub pages_per_chunk: u64,
    /// Page size in bytes
    pub page_size: u64,
    /// Whether compacting to another store (F2 hot-cold transfer)
    pub to_other_store: bool,
    /// Whether to compact tombstones
    pub compact_tombstones: bool,
}

impl ConcurrentCompactionConfig {
    /// Create a new concurrent compaction configuration
    pub fn new(num_threads: usize) -> Self {
        Self {
            num_threads: num_threads.max(1),
            pages_per_chunk: 16,
            page_size: 1 << 22, // 4 MB default page size
            to_other_store: false,
            compact_tombstones: true,
        }
    }

    /// Set the number of threads
    pub fn with_num_threads(mut self, num: usize) -> Self {
        self.num_threads = num.max(1);
        self
    }

    /// Set pages per chunk (work unit)
    pub fn with_pages_per_chunk(mut self, pages: u64) -> Self {
        self.pages_per_chunk = pages.max(1);
        self
    }

    /// Set the page size
    pub fn with_page_size(mut self, size: u64) -> Self {
        self.page_size = size;
        self
    }

    /// Set whether compacting to another store
    pub fn with_to_other_store(mut self, value: bool) -> Self {
        self.to_other_store = value;
        self
    }

    /// Create from base CompactionConfig
    pub fn from_compaction_config(config: &CompactionConfig) -> Self {
        Self {
            num_threads: config.num_threads,
            pages_per_chunk: 16,
            page_size: 1 << 22,
            to_other_store: false,
            compact_tombstones: config.compact_tombstones,
        }
    }
}

impl Default for ConcurrentCompactionConfig {
    fn default() -> Self {
        Self::new(4)
    }
}

/// Thread context for concurrent compaction
///
/// This is shared among all compaction threads and provides
/// synchronization primitives for work distribution.
pub struct ConcurrentCompactionContext {
    /// Current page being processed (atomic counter for page distribution)
    current_page: AtomicU64,
    /// End page (exclusive)
    end_page: u64,
    /// Begin address
    begin_address: Address,
    /// End address
    end_address: Address,
    /// Page size
    page_size: u64,
    /// Pages per thread chunk
    pages_per_chunk: u64,
    /// Thread finished flags
    thread_finished: Vec<AtomicBool>,
    /// Number of active threads
    active_threads: AtomicU16,
    /// Whether more pages are available
    pages_available: AtomicBool,
    /// Whether compacting to another store
    to_other_store: bool,
    /// Per-thread statistics
    thread_stats: Vec<parking_lot::Mutex<CompactionStats>>,
}

impl ConcurrentCompactionContext {
    /// Create a new concurrent compaction context
    pub fn new(
        range: ScanRange,
        num_threads: usize,
        page_size: u64,
        pages_per_chunk: u64,
        to_other_store: bool,
    ) -> Self {
        let begin = range.begin.control();
        let end = range.end.control();

        // Calculate page range
        let start_page = begin / page_size;
        let end_page = (end + page_size - 1) / page_size;

        // Create thread finished flags
        let thread_finished: Vec<AtomicBool> =
            (0..num_threads).map(|_| AtomicBool::new(false)).collect();

        // Create per-thread statistics
        let thread_stats: Vec<parking_lot::Mutex<CompactionStats>> = (0..num_threads)
            .map(|_| parking_lot::Mutex::new(CompactionStats::default()))
            .collect();

        Self {
            current_page: AtomicU64::new(start_page),
            end_page,
            begin_address: range.begin,
            end_address: range.end,
            page_size,
            pages_per_chunk,
            thread_finished,
            active_threads: AtomicU16::new(0),
            pages_available: AtomicBool::new(true),
            to_other_store,
            thread_stats,
        }
    }

    /// Get the next chunk of pages to process
    ///
    /// Returns None if no more pages are available
    pub fn get_next_chunk(&self) -> Option<PageChunk> {
        if !self.pages_available.load(Ordering::Acquire) {
            return None;
        }

        // Atomically claim a chunk of pages
        let start_page = self
            .current_page
            .fetch_add(self.pages_per_chunk, Ordering::AcqRel);

        if start_page >= self.end_page {
            self.pages_available.store(false, Ordering::Release);
            return None;
        }

        let end_page = (start_page + self.pages_per_chunk).min(self.end_page);

        // Convert page numbers back to addresses
        let chunk_begin = Address::from_control(start_page * self.page_size);
        let chunk_end = Address::from_control(end_page * self.page_size);

        // Clamp to actual range
        let actual_begin = if chunk_begin.control() < self.begin_address.control() {
            self.begin_address
        } else {
            chunk_begin
        };

        let actual_end = if chunk_end.control() > self.end_address.control() {
            self.end_address
        } else {
            chunk_end
        };

        if actual_begin >= actual_end {
            return None;
        }

        Some(PageChunk {
            begin: actual_begin,
            end: actual_end,
            page_index: start_page,
        })
    }

    /// Register a thread as active
    pub fn register_thread(&self) -> u16 {
        self.active_threads.fetch_add(1, Ordering::AcqRel)
    }

    /// Mark a thread as finished
    pub fn mark_thread_finished(&self, thread_id: usize) {
        if thread_id < self.thread_finished.len() {
            self.thread_finished[thread_id].store(true, Ordering::Release);
        }
        self.active_threads.fetch_sub(1, Ordering::AcqRel);
    }

    /// Check if all threads are finished
    pub fn all_threads_finished(&self) -> bool {
        self.active_threads.load(Ordering::Acquire) == 0
    }

    /// Check if any pages are still available
    pub fn has_pages_available(&self) -> bool {
        self.pages_available.load(Ordering::Acquire)
    }

    /// Update statistics for a thread
    pub fn update_stats(&self, thread_id: usize, stats: CompactionStats) {
        if thread_id < self.thread_stats.len() {
            let mut guard = self.thread_stats[thread_id].lock();
            guard.records_scanned += stats.records_scanned;
            guard.records_compacted += stats.records_compacted;
            guard.records_skipped += stats.records_skipped;
            guard.tombstones_found += stats.tombstones_found;
            guard.bytes_scanned += stats.bytes_scanned;
            guard.bytes_compacted += stats.bytes_compacted;
            guard.bytes_reclaimed += stats.bytes_reclaimed;
        }
    }

    /// Aggregate statistics from all threads
    pub fn aggregate_stats(&self) -> CompactionStats {
        let mut total = CompactionStats::default();

        for stats_mutex in &self.thread_stats {
            let stats = stats_mutex.lock();
            total.records_scanned += stats.records_scanned;
            total.records_compacted += stats.records_compacted;
            total.records_skipped += stats.records_skipped;
            total.tombstones_found += stats.tombstones_found;
            total.bytes_scanned += stats.bytes_scanned;
            total.bytes_compacted += stats.bytes_compacted;
            total.bytes_reclaimed += stats.bytes_reclaimed;
        }

        total
    }

    /// Get the number of threads
    pub fn num_threads(&self) -> usize {
        self.thread_finished.len()
    }

    /// Check if compacting to another store
    pub fn to_other_store(&self) -> bool {
        self.to_other_store
    }
}

/// A chunk of pages to be processed by a single thread
#[derive(Debug, Clone)]
pub struct PageChunk {
    /// Begin address
    pub begin: Address,
    /// End address
    pub end: Address,
    /// Page index (for tracking)
    pub page_index: u64,
}

impl PageChunk {
    /// Get the size of this chunk in bytes
    pub fn size(&self) -> u64 {
        self.end.control().saturating_sub(self.begin.control())
    }

    /// Convert to a scan range
    pub fn to_scan_range(&self) -> ScanRange {
        ScanRange::new(self.begin, self.end)
    }
}

/// Concurrent log page iterator for multi-threaded access
///
/// This iterator distributes pages among threads atomically.
pub struct ConcurrentLogPageIterator {
    /// Context containing shared state
    context: Arc<ConcurrentCompactionContext>,
}

impl ConcurrentLogPageIterator {
    /// Create a new concurrent log page iterator
    pub fn new(context: Arc<ConcurrentCompactionContext>) -> Self {
        Self { context }
    }

    /// Get the next page chunk for the calling thread
    pub fn next(&self) -> Option<PageChunk> {
        self.context.get_next_chunk()
    }

    /// Check if more pages are available
    pub fn has_more(&self) -> bool {
        self.context.has_pages_available()
    }
}

/// Result of concurrent compaction
#[derive(Debug)]
pub struct ConcurrentCompactionResult {
    /// Overall status
    pub status: Status,
    /// New begin address after compaction
    pub new_begin_address: Address,
    /// Aggregated statistics
    pub stats: CompactionStats,
    /// Number of threads used
    pub threads_used: usize,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl ConcurrentCompactionResult {
    /// Convert to regular CompactionResult
    pub fn to_compaction_result(mut self) -> CompactionResult {
        self.stats.duration_ms = self.duration_ms;
        CompactionResult {
            status: self.status,
            new_begin_address: self.new_begin_address,
            stats: self.stats,
        }
    }
}

/// Thread-safe handle for compaction workers
pub struct CompactionWorkerHandle {
    /// Thread ID
    pub id: usize,
    /// Thread join handle
    pub handle: Option<thread::JoinHandle<CompactionStats>>,
}

/// Concurrent compactor for multi-threaded log compaction
pub struct ConcurrentCompactor {
    /// Configuration
    config: ConcurrentCompactionConfig,
    /// Compaction in progress flag
    in_progress: AtomicBool,
}

impl ConcurrentCompactor {
    /// Create a new concurrent compactor
    pub fn new(config: ConcurrentCompactionConfig) -> Self {
        Self {
            config,
            in_progress: AtomicBool::new(false),
        }
    }

    /// Create with default configuration
    pub fn with_threads(num_threads: usize) -> Self {
        Self::new(ConcurrentCompactionConfig::new(num_threads))
    }

    /// Get the configuration
    pub fn config(&self) -> &ConcurrentCompactionConfig {
        &self.config
    }

    /// Check if compaction is in progress
    pub fn is_in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Acquire)
    }

    /// Try to start a compaction
    pub fn try_start(&self) -> Result<(), Status> {
        match self.in_progress.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(Status::Aborted),
        }
    }

    /// Complete a compaction
    pub fn complete(&self) {
        self.in_progress.store(false, Ordering::Release);
    }

    /// Compact a range using multiple threads
    ///
    /// This is a simplified implementation that demonstrates the concurrent
    /// compaction architecture. A full implementation would integrate with
    /// the actual log storage and hash index.
    pub fn compact_range<F>(
        &self,
        range: ScanRange,
        process_chunk: F,
    ) -> ConcurrentCompactionResult
    where
        F: Fn(PageChunk) -> CompactionStats + Send + Sync + Clone + 'static,
    {
        let start_time = Instant::now();

        // Create shared context
        let context = Arc::new(ConcurrentCompactionContext::new(
            range.clone(),
            self.config.num_threads,
            self.config.page_size,
            self.config.pages_per_chunk,
            self.config.to_other_store,
        ));

        // Spawn worker threads
        let mut handles = Vec::with_capacity(self.config.num_threads);

        for thread_id in 0..self.config.num_threads {
            let ctx = Arc::clone(&context);
            let processor = process_chunk.clone();

            let handle = thread::spawn(move || {
                // Register this thread
                ctx.register_thread();

                let mut local_stats = CompactionStats::default();

                // Process chunks until none are left
                while let Some(chunk) = ctx.get_next_chunk() {
                    let chunk_stats = processor(chunk);

                    // Accumulate local statistics
                    local_stats.records_scanned += chunk_stats.records_scanned;
                    local_stats.records_compacted += chunk_stats.records_compacted;
                    local_stats.records_skipped += chunk_stats.records_skipped;
                    local_stats.tombstones_found += chunk_stats.tombstones_found;
                    local_stats.bytes_scanned += chunk_stats.bytes_scanned;
                    local_stats.bytes_compacted += chunk_stats.bytes_compacted;
                    local_stats.bytes_reclaimed += chunk_stats.bytes_reclaimed;
                }

                // Mark this thread as finished
                ctx.mark_thread_finished(thread_id);

                local_stats
            });

            handles.push(CompactionWorkerHandle {
                id: thread_id,
                handle: Some(handle),
            });
        }

        // Wait for all threads to complete and collect results
        let mut total_stats = CompactionStats::default();

        for mut worker in handles {
            if let Some(handle) = worker.handle.take() {
                match handle.join() {
                    Ok(stats) => {
                        total_stats.records_scanned += stats.records_scanned;
                        total_stats.records_compacted += stats.records_compacted;
                        total_stats.records_skipped += stats.records_skipped;
                        total_stats.tombstones_found += stats.tombstones_found;
                        total_stats.bytes_scanned += stats.bytes_scanned;
                        total_stats.bytes_compacted += stats.bytes_compacted;
                        total_stats.bytes_reclaimed += stats.bytes_reclaimed;
                    }
                    Err(_) => {
                        // Thread panicked - log error but continue
                    }
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        ConcurrentCompactionResult {
            status: Status::Ok,
            new_begin_address: range.end,
            stats: total_stats,
            threads_used: self.config.num_threads,
            duration_ms,
        }
    }

    /// Compact using a simple callback (for integration with existing code)
    pub fn compact_simple(
        &self,
        range: ScanRange,
    ) -> ConcurrentCompactionResult {
        // Simple processor that just counts bytes
        let page_size = self.config.page_size;
        self.compact_range(range, move |chunk| {
            let size = chunk.size();
            CompactionStats {
                records_scanned: size / 64, // Estimate: 64 bytes per record
                records_compacted: 0,
                records_skipped: 0,
                tombstones_found: 0,
                bytes_scanned: size,
                bytes_compacted: 0,
                bytes_reclaimed: size,
                duration_ms: 0,
            }
        })
    }
}

impl Default for ConcurrentCompactor {
    fn default() -> Self {
        Self::new(ConcurrentCompactionConfig::default())
    }
}

/// State for tracking concurrent compaction progress
#[derive(Debug)]
pub struct ConcurrentCompactionState {
    /// Whether compaction is active
    pub active: AtomicBool,
    /// Number of chunks processed
    pub chunks_processed: AtomicU64,
    /// Total chunks to process
    pub total_chunks: AtomicU64,
    /// Bytes processed
    pub bytes_processed: AtomicU64,
    /// Records processed
    pub records_processed: AtomicU64,
}

impl ConcurrentCompactionState {
    /// Create new state
    pub fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            chunks_processed: AtomicU64::new(0),
            total_chunks: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            records_processed: AtomicU64::new(0),
        }
    }

    /// Reset state for a new compaction
    pub fn reset(&self, total_chunks: u64) {
        self.active.store(true, Ordering::Release);
        self.chunks_processed.store(0, Ordering::Release);
        self.total_chunks.store(total_chunks, Ordering::Release);
        self.bytes_processed.store(0, Ordering::Release);
        self.records_processed.store(0, Ordering::Release);
    }

    /// Mark compaction as complete
    pub fn complete(&self) {
        self.active.store(false, Ordering::Release);
    }

    /// Record progress
    pub fn record_progress(&self, bytes: u64, records: u64) {
        self.chunks_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
        self.records_processed.fetch_add(records, Ordering::Relaxed);
    }

    /// Get progress percentage
    pub fn progress_percent(&self) -> f64 {
        let processed = self.chunks_processed.load(Ordering::Relaxed);
        let total = self.total_chunks.load(Ordering::Relaxed);
        if total == 0 {
            return 100.0;
        }
        (processed as f64 / total as f64) * 100.0
    }
}

impl Default for ConcurrentCompactionState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_config() {
        let config = ConcurrentCompactionConfig::new(4);
        assert_eq!(config.num_threads, 4);

        let config = ConcurrentCompactionConfig::new(0);
        assert_eq!(config.num_threads, 1); // Minimum is 1
    }

    #[test]
    fn test_concurrent_config_builder() {
        let config = ConcurrentCompactionConfig::new(2)
            .with_pages_per_chunk(32)
            .with_page_size(1 << 20)
            .with_to_other_store(true);

        assert_eq!(config.num_threads, 2);
        assert_eq!(config.pages_per_chunk, 32);
        assert_eq!(config.page_size, 1 << 20);
        assert!(config.to_other_store);
    }

    #[test]
    fn test_page_chunk() {
        let chunk = PageChunk {
            begin: Address::new(0, 0),
            end: Address::new(0, 1000),
            page_index: 0,
        };

        assert_eq!(chunk.size(), 1000);
    }

    #[test]
    fn test_concurrent_context_page_distribution() {
        let range = ScanRange::new(
            Address::from_control(0),
            Address::from_control(1 << 24), // 16 MB
        );

        let context = ConcurrentCompactionContext::new(
            range,
            4,      // 4 threads
            1 << 22, // 4 MB page size
            1,      // 1 page per chunk
            false,
        );

        // Should have 4 chunks (16 MB / 4 MB)
        let mut chunks = Vec::new();
        while let Some(chunk) = context.get_next_chunk() {
            chunks.push(chunk);
        }

        assert_eq!(chunks.len(), 4);
    }

    #[test]
    fn test_concurrent_compactor_basic() {
        let compactor = ConcurrentCompactor::with_threads(2);

        // Try start should succeed
        assert!(compactor.try_start().is_ok());
        assert!(compactor.is_in_progress());

        // Second try should fail
        assert!(compactor.try_start().is_err());

        // After complete, should be able to start again
        compactor.complete();
        assert!(!compactor.is_in_progress());
    }

    #[test]
    fn test_concurrent_compaction() {
        let compactor = ConcurrentCompactor::with_threads(2);

        let range = ScanRange::new(
            Address::from_control(0),
            Address::from_control(1 << 20), // 1 MB
        );

        let result = compactor.compact_simple(range);

        assert_eq!(result.status, Status::Ok);
        assert!(result.stats.bytes_scanned > 0);
        assert!(result.threads_used == 2);
    }

    #[test]
    fn test_concurrent_compaction_with_processor() {
        let compactor = ConcurrentCompactor::with_threads(4);

        let range = ScanRange::new(
            Address::from_control(0),
            Address::from_control(1 << 22), // 4 MB
        );

        let result = compactor.compact_range(range, |chunk| {
            // Simulate processing
            CompactionStats {
                records_scanned: 100,
                records_compacted: 50,
                records_skipped: 50,
                tombstones_found: 10,
                bytes_scanned: chunk.size(),
                bytes_compacted: chunk.size() / 2,
                bytes_reclaimed: chunk.size() / 2,
                duration_ms: 0,
            }
        });

        assert_eq!(result.status, Status::Ok);
        assert!(result.stats.records_scanned > 0);
    }

    #[test]
    fn test_compaction_state() {
        let state = ConcurrentCompactionState::new();
        assert!(!state.active.load(Ordering::Relaxed));

        state.reset(100);
        assert!(state.active.load(Ordering::Relaxed));
        assert_eq!(state.progress_percent(), 0.0);

        state.record_progress(1000, 10);
        state.record_progress(1000, 10);
        assert_eq!(state.progress_percent(), 2.0);

        state.complete();
        assert!(!state.active.load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_iterator() {
        let range = ScanRange::new(
            Address::from_control(0),
            Address::from_control(1 << 24),
        );

        let context = Arc::new(ConcurrentCompactionContext::new(
            range,
            2,
            1 << 22,
            2,
            false,
        ));

        let iter = ConcurrentLogPageIterator::new(context);

        let mut count = 0;
        while let Some(_chunk) = iter.next() {
            count += 1;
        }

        assert!(count > 0);
        assert!(!iter.has_more());
    }
}
