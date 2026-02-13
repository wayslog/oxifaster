//! Hybrid log allocator (PersistentMemoryMalloc) for FASTER
//!
//! This module provides the core log allocator that manages a circular buffer of pages,
//! with hot pages in memory and cold pages on disk.

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::address::{Address, AtomicAddress, AtomicPageOffset};

/// Callback type for truncation notification.
///
/// Called before the begin address is shifted, receiving the new begin address.
/// This allows callers to perform cleanup or notification before truncation occurs.
pub type TruncateCallback = Box<dyn FnOnce(Address) + Send>;

/// Callback type for shift completion notification.
///
/// Called after the begin address has been shifted, receiving the new begin address.
/// This allows callers to perform post-shift operations or logging.
pub type ShiftCompleteCallback = Box<dyn FnOnce(Address) + Send>;
use crate::allocator::page_allocator::PageInfo;
use crate::constants::PAGE_SIZE;
use crate::device::StorageDevice;
use crate::status::Status;
use crate::utility::{AlignedBuffer, is_power_of_two};
use flush_worker::{FlushManager, FlushShared};

/// Configuration for the hybrid log allocator
#[derive(Debug, Clone)]
pub struct HybridLogConfig {
    /// Page size in bytes (must be power of 2)
    pub page_size: usize,
    /// Number of pages in the in-memory buffer
    pub memory_pages: u32,
    /// Size of the mutable region in pages
    pub mutable_pages: u32,
    /// Segment size for disk storage
    pub segment_size: u64,
}

impl HybridLogConfig {
    /// Create a new configuration
    pub fn new(memory_size: u64, page_size_bits: u32) -> Self {
        let page_size = 1 << page_size_bits;
        let memory_pages = (memory_size / page_size as u64) as u32;

        Self {
            page_size,
            memory_pages,
            mutable_pages: memory_pages / 4,
            segment_size: 1 << 30, // 1 GB segments
        }
    }
}

impl Default for HybridLogConfig {
    fn default() -> Self {
        Self {
            page_size: PAGE_SIZE,
            memory_pages: 64,
            mutable_pages: 16,
            segment_size: 1 << 30,
        }
    }
}

/// Configuration for automatic background flushing
#[derive(Debug, Clone)]
pub struct AutoFlushConfig {
    /// Enable automatic background flushing
    pub enabled: bool,
    /// Interval between flush checks in milliseconds
    pub check_interval_ms: u64,
    /// Minimum number of pages in read-only region before triggering flush
    pub min_readonly_pages: u32,
}

impl Default for AutoFlushConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval_ms: 100,
            min_readonly_pages: 4,
        }
    }
}

impl AutoFlushConfig {
    /// Create a new auto-flush configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable automatic flushing
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the check interval in milliseconds
    pub fn with_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.check_interval_ms = interval_ms.max(10); // Minimum 10ms
        self
    }

    /// Set the minimum number of read-only pages before flushing
    pub fn with_min_readonly_pages(mut self, pages: u32) -> Self {
        self.min_readonly_pages = pages.max(1);
        self
    }

    /// Check if auto-flush is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Page array - manages pages in memory
struct PageArray {
    /// Page buffers
    buffers: Vec<Option<AlignedBuffer>>,
    /// Page info
    info: Arc<Vec<PageInfo>>,
    /// Number of buffer pages
    buffer_size: u32,
}

impl PageArray {
    fn new(buffer_size: u32) -> Self {
        let mut buffers = Vec::with_capacity(buffer_size as usize);
        for _ in 0..buffer_size {
            buffers.push(None);
        }

        Self {
            buffers,
            info: Arc::new(vec![PageInfo::new(); buffer_size as usize]),
            buffer_size,
        }
    }

    /// Get the buffer index for a page
    #[inline]
    fn buffer_index(&self, page: u32) -> usize {
        (page % self.buffer_size) as usize
    }

    /// Allocate a page buffer
    fn allocate_page(&mut self, page: u32, page_size: usize) -> bool {
        let idx = self.buffer_index(page);
        if self.buffers[idx].is_some() {
            return true;
        }

        match AlignedBuffer::zeroed(page_size, page_size) {
            Some(buf) => {
                self.buffers[idx] = Some(buf);
                true
            }
            None => false,
        }
    }

    /// Get a page buffer
    fn get_page(&self, page: u32) -> Option<&[u8]> {
        let idx = self.buffer_index(page);
        self.buffers[idx].as_ref().map(|b| b.as_slice())
    }

    /// Get a mutable page buffer
    fn get_page_mut(&mut self, page: u32) -> Option<&mut [u8]> {
        let idx = self.buffer_index(page);
        self.buffers[idx].as_mut().map(|b| b.as_mut_slice())
    }

    /// Get page info
    fn get_info(&self, page: u32) -> &PageInfo {
        let idx = self.buffer_index(page);
        &self.info[idx]
    }

    fn info_arc(&self) -> Arc<Vec<PageInfo>> {
        Arc::clone(&self.info)
    }

    /// Clear a page
    fn clear_page(&mut self, page: u32) {
        let idx = self.buffer_index(page);
        if let Some(ref mut buf) = self.buffers[idx] {
            buf.as_mut_slice().fill(0);
        }
        self.info[idx].reset();
    }
}

/// Hybrid log allocator - PersistentMemoryMalloc
///
/// Manages a circular buffer of pages with the following regions:
/// - Mutable region: Most recent pages, can be updated in-place
/// - Read-only region: Older pages in memory, cannot be updated
/// - On-disk region: Cold pages flushed to disk
pub struct PersistentMemoryMalloc<D: StorageDevice> {
    /// Configuration
    config: HybridLogConfig,
    /// Storage device
    device: Arc<D>,
    /// Page array
    pages: PageArray,
    /// Current tail (page + offset)
    tail_page_offset: AtomicPageOffset,
    /// Read-only address boundary
    read_only_address: Arc<AtomicAddress>,
    /// Safe read-only address (flushed read-only boundary)
    safe_read_only_address: Arc<AtomicAddress>,
    /// Head address (beginning of log)
    head_address: AtomicAddress,
    /// Safe head address (can be reclaimed)
    safe_head_address: AtomicAddress,
    /// Flushed until address (pages written; durability requires explicit device flush)
    flushed_until_address: Arc<AtomicAddress>,
    /// Begin address (first valid address)
    begin_address: AtomicAddress,
    /// Buffer size (number of pages in memory)
    buffer_size: u32,
    /// Number of pending flushes
    pending_flushes: Arc<AtomicU64>,
    /// Shared flush state
    flush_shared: Arc<FlushShared<D>>,
    /// Flush worker
    flush_manager: FlushManager<D>,
}

impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    /// Create a new hybrid log allocator
    pub fn new(config: HybridLogConfig, device: Arc<D>) -> Self {
        let buffer_size = config.memory_pages;
        let page_size = config.page_size;

        assert!(is_power_of_two(page_size as u64));
        assert!(buffer_size > 0);

        let mut pages = PageArray::new(buffer_size);

        // Pre-allocate all pages
        for i in 0..buffer_size {
            pages.allocate_page(i, page_size);
        }

        let read_only_address = Arc::new(AtomicAddress::new(Address::new(0, 0)));
        let safe_read_only_address = Arc::new(AtomicAddress::new(Address::new(0, 0)));
        let flushed_until_address = Arc::new(AtomicAddress::new(Address::new(0, 0)));
        let pending_flushes = Arc::new(AtomicU64::new(0));
        let flush_shared = Arc::new(FlushShared::new(
            device.clone(),
            pages.info_arc(),
            buffer_size,
            Arc::clone(&read_only_address),
            Arc::clone(&safe_read_only_address),
            Arc::clone(&flushed_until_address),
            Arc::clone(&pending_flushes),
        ));
        let flush_manager = FlushManager::new(Arc::clone(&flush_shared));

        Self {
            config,
            device,
            pages,
            tail_page_offset: AtomicPageOffset::from_address(Address::new(0, 0)),
            read_only_address,
            safe_read_only_address,
            head_address: AtomicAddress::new(Address::new(0, 0)),
            safe_head_address: AtomicAddress::new(Address::new(0, 0)),
            flushed_until_address,
            begin_address: AtomicAddress::new(Address::new(0, 0)),
            buffer_size,
            pending_flushes,
            flush_shared,
            flush_manager,
        }
    }

    /// Get the page size
    #[inline]
    pub fn page_size(&self) -> usize {
        self.config.page_size
    }

    /// Get the buffer size
    #[inline]
    pub fn buffer_size(&self) -> u32 {
        self.buffer_size
    }

    /// Get the maximum configured size in bytes.
    ///
    /// This is the total in-memory buffer capacity (page_size * memory_pages).
    /// This corresponds to C++ FASTER's GetMaxSize() method.
    #[inline]
    pub fn max_size_bytes(&self) -> u64 {
        self.config.page_size as u64 * self.config.memory_pages as u64
    }

    /// Get the current tail address
    #[inline]
    pub fn get_tail_address(&self) -> Address {
        self.tail_page_offset.load(Ordering::Acquire).to_address()
    }

    /// Get the read-only address boundary
    #[inline]
    pub fn get_read_only_address(&self) -> Address {
        self.read_only_address.load(Ordering::Acquire)
    }

    /// Get the safe read-only address
    #[inline]
    pub fn get_safe_read_only_address(&self) -> Address {
        self.safe_read_only_address.load(Ordering::Acquire)
    }

    /// Get the head address
    #[inline]
    pub fn get_head_address(&self) -> Address {
        self.head_address.load(Ordering::Acquire)
    }

    /// Get the begin address
    #[inline]
    pub fn get_begin_address(&self) -> Address {
        self.begin_address.load(Ordering::Acquire)
    }

    /// Get the flushed until address
    #[inline]
    pub fn get_flushed_until_address(&self) -> Address {
        self.flushed_until_address.load(Ordering::Acquire)
    }

    /// Reserve space in the log
    ///
    /// Returns the address where the record should be written.
    /// If the page overflows, triggers a new page allocation.
    pub fn allocate(&self, num_slots: u32) -> Result<Address, Status> {
        debug_assert!(num_slots <= Address::MAX_OFFSET);

        loop {
            // Get current position
            let page_offset = self.tail_page_offset.reserve(num_slots);
            let page = page_offset.page();
            let offset = page_offset.offset();

            // Check if offset exceeds Address::MAX_OFFSET (which is smaller than u32::MAX)
            // If so, we need to move to a new page before checking page size
            if offset > Address::MAX_OFFSET as u64 {
                // Offset exceeds valid range - need to move to new page
                let (_advanced, won_cas) = self.tail_page_offset.new_page(page);
                if won_cas {
                    self.on_page_full(page)?;
                }
                continue;
            }

            // Check if we fit in the current page
            let new_offset = offset + num_slots as u64;

            if new_offset <= self.config.page_size as u64 {
                // Safe to cast: we've verified offset <= Address::MAX_OFFSET above
                // Also verify offset doesn't exceed u32 range (defensive check)
                if offset > u32::MAX as u64 {
                    // This should never happen since MAX_OFFSET < u32::MAX,
                    // but we check for safety
                    return Err(Status::Corruption);
                }

                // Calculate address - safe cast since offset <= Address::MAX_OFFSET
                let address = Address::new(page, offset as u32);
                // Mark the page dirty only once per page. Subsequent allocations within the same
                // page do not need to touch the atomic flush state, which keeps the hot allocation
                // path lightweight.
                if offset == 0 {
                    self.mark_page_dirty(page);
                }
                return Ok(address);
            }

            // Need to move to new page
            let (advanced, won_cas) = self.tail_page_offset.new_page(page);

            if !advanced {
                continue;
            }

            if won_cas {
                // We won - need to handle page transition
                self.on_page_full(page)?;
            }

            // Retry allocation
        }
    }

    /// Get a pointer to a record at the given address
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `address` refers to a live record that is still resident in memory (i.e.
    ///   `self.get_head_address() <= address < self.get_tail_address()`).
    /// - `address.offset() < self.page_size()`.
    /// - The returned pointer is only used to read bytes that belong to the record at `address`.
    /// - No mutable aliasing occurs while the returned pointer is in use.
    #[inline]
    pub unsafe fn get(&self, address: Address) -> Option<NonNull<u8>> {
        if address < self.get_head_address() || address >= self.get_tail_address() {
            return None;
        }

        let offset = address.offset() as usize;
        if offset >= self.config.page_size {
            return None;
        }

        let page = address.page();

        // Check if the page is in memory
        if let Some(buf) = self.pages.get_page(page) {
            let ptr = unsafe { buf.as_ptr().add(offset) as *mut u8 };
            NonNull::new(ptr)
        } else {
            None
        }
    }

    /// Get a mutable pointer to a record at the given address
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `address` refers to a live record that is still resident in memory (i.e.
    ///   `self.get_head_address() <= address < self.get_tail_address()`).
    /// - `address` is in the mutable region (i.e. `self.is_mutable(address)`).
    /// - `address.offset() < self.page_size()`.
    /// - The returned pointer is only used to write bytes that belong to the record at `address`.
    /// - No other thread concurrently reads/writes the same bytes without synchronization.
    #[inline]
    pub unsafe fn get_mut(&mut self, address: Address) -> Option<NonNull<u8>> {
        if address < self.get_head_address()
            || address >= self.get_tail_address()
            || !self.is_mutable(address)
        {
            return None;
        }

        let offset = address.offset() as usize;
        if offset >= self.config.page_size {
            return None;
        }

        let page = address.page();

        // Check if the page is in memory
        if let Some(buf) = self.pages.get_page_mut(page) {
            let ptr = unsafe { buf.as_mut_ptr().add(offset) };
            NonNull::new(ptr)
        } else {
            None
        }
    }

    /// Check if an address is in the mutable region
    #[inline]
    pub fn is_mutable(&self, address: Address) -> bool {
        address >= self.get_read_only_address()
    }

    /// Check if an address is in the safe read-only region
    #[inline]
    pub fn is_safe_read_only(&self, address: Address) -> bool {
        address < self.get_safe_read_only_address() && address >= self.get_head_address()
    }

    /// Check if an address is on disk
    #[inline]
    pub fn is_on_disk(&self, address: Address) -> bool {
        address < self.get_head_address()
    }

    /// Handle a full page
    fn on_page_full(&self, page: u32) -> Result<(), Status> {
        // Update read-only boundary if needed
        let new_read_only = Address::new(page.saturating_sub(self.config.mutable_pages), 0);

        loop {
            let current = self.read_only_address.load(Ordering::Acquire);
            if new_read_only <= current {
                break;
            }

            match self.read_only_address.compare_exchange(
                current,
                new_read_only,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.schedule_read_only_flushes(current, new_read_only);
                    break;
                }
                Err(_) => continue,
            }
        }

        Ok(())
    }

    /// Shift the read-only address
    pub fn shift_read_only_address(&self, new_address: Address) {
        loop {
            let current = self.read_only_address.load(Ordering::Acquire);
            if new_address <= current {
                return;
            }

            if self
                .read_only_address
                .compare_exchange(current, new_address, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.schedule_read_only_flushes(current, new_address);
                return;
            }
        }
    }

    /// Shift the head address
    pub fn shift_head_address(&self, new_address: Address) {
        Self::try_advance_address(&self.head_address, new_address);
    }

    /// Shift the begin address
    pub fn shift_begin_address(&self, new_address: Address) {
        Self::try_advance_address(&self.begin_address, new_address);
    }

    /// Shift the begin address with optional callbacks for truncation and completion.
    ///
    /// This method provides hooks for callers to be notified before and after
    /// the begin address is shifted. This corresponds to C++ FASTER's
    /// ShiftBeginAddress with callbacks.
    ///
    /// # Arguments
    /// * `new_address` - The new begin address to shift to
    /// * `truncate_callback` - Optional callback invoked before truncation with the new address
    /// * `complete_callback` - Optional callback invoked after shift completes with the final address
    ///
    /// # Returns
    /// The new begin address after the shift operation
    ///
    /// # Example
    /// ```ignore
    /// let truncate_cb = Some(Box::new(|addr| {
    ///     println!("About to truncate to: {}", addr);
    /// }) as TruncateCallback);
    /// let complete_cb = Some(Box::new(|addr| {
    ///     println!("Truncation complete at: {}", addr);
    /// }) as ShiftCompleteCallback);
    /// let new_addr = allocator.shift_begin_address_with_callbacks(
    ///     target_address,
    ///     truncate_cb,
    ///     complete_cb,
    /// );
    /// ```
    pub fn shift_begin_address_with_callbacks(
        &self,
        new_address: Address,
        truncate_callback: Option<TruncateCallback>,
        complete_callback: Option<ShiftCompleteCallback>,
    ) -> Address {
        // Invoke truncate callback before shifting
        if let Some(callback) = truncate_callback {
            callback(new_address);
        }

        // Perform the actual shift
        Self::try_advance_address(&self.begin_address, new_address);

        // Get the actual new begin address (may be different if concurrent shift occurred)
        let final_address = self.begin_address.load(Ordering::Acquire);

        // Invoke completion callback
        if let Some(callback) = complete_callback {
            callback(final_address);
        }

        final_address
    }

    /// Helper: atomically advance an address if the new value is greater.
    fn try_advance_address(target: &AtomicAddress, new_address: Address) {
        loop {
            let current = target.load(Ordering::Acquire);
            if new_address <= current {
                return;
            }

            if target
                .compare_exchange(current, new_address, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Get page info for a page
    pub fn get_page_info(&self, page: u32) -> &PageInfo {
        self.pages.get_info(page)
    }

    fn mark_page_dirty(&self, page: u32) {
        self.flush_shared.mark_page_dirty(page);
    }

    fn schedule_read_only_flushes(&self, old_read_only: Address, new_read_only: Address) {
        if new_read_only <= old_read_only {
            return;
        }

        let start_page = old_read_only.page();
        let end_page = new_read_only.page();
        for page in start_page..end_page {
            self.schedule_flush_page(page);
        }

        self.flush_shared.advance_safe_read_only(new_read_only);
    }

    fn schedule_flush_page(&self, page: u32) {
        if !self.flush_shared.try_mark_flushing(page) {
            return;
        }
        let Some(page_data) = self.pages.get_page(page) else {
            self.flush_shared.mark_page_dirty_after_error(page);
            return;
        };

        let mut data = vec![0u8; page_data.len()];
        data.copy_from_slice(page_data);

        self.flush_shared.increment_pending();
        if !self.flush_manager.submit_page(page, data) {
            self.flush_shared.mark_page_dirty_after_error(page);
            self.flush_shared.decrement_pending();
        }
    }

    /// Initialize the log from a given address (for recovery)
    pub fn initialize_from_address(&self, begin_address: Address, head_address: Address) {
        self.begin_address.store(begin_address, Ordering::Release);
        self.head_address.store(head_address, Ordering::Release);
        self.safe_head_address
            .store(head_address, Ordering::Release);
        self.read_only_address
            .store(head_address, Ordering::Release);
        self.safe_read_only_address
            .store(head_address, Ordering::Release);
        self.flushed_until_address
            .store(head_address, Ordering::Release);
        self.tail_page_offset
            .store_address(head_address, Ordering::Release);
    }

    /// Get log statistics
    pub fn get_stats(&self) -> LogStats {
        let tail = self.get_tail_address();
        let read_only = self.get_read_only_address();
        let head = self.get_head_address();
        let begin = self.get_begin_address();

        LogStats {
            tail_address: tail,
            read_only_address: read_only,
            head_address: head,
            begin_address: begin,
            mutable_bytes: tail - read_only,
            read_only_bytes: read_only - head,
            on_disk_bytes: head - begin,
        }
    }

    /// Get a reference to the device.
    pub fn device(&self) -> &Arc<D> {
        &self.device
    }
}

mod checkpoint;
mod delta;
mod flush;
mod flush_worker;

// SAFETY: `PersistentMemoryMalloc` is safe to send/share between threads because:
// - All concurrent state is stored in atomics (`*_address`, `tail_page_offset`, `pending_flushes`).
// - The in-memory page ring (`pages`) is pre-allocated during construction and only mutated through
//   `&mut self` methods; shared `&self` access only reads from those buffers.
// - `D: StorageDevice` is `Send + Sync` and is held behind an `Arc`.
// - Background flush coordination uses channels and atomic page metadata.
unsafe impl<D: StorageDevice> Send for PersistentMemoryMalloc<D> {}
unsafe impl<D: StorageDevice> Sync for PersistentMemoryMalloc<D> {}

/// Statistics about the log
#[derive(Debug, Clone)]
pub struct LogStats {
    /// Tail address
    pub tail_address: Address,
    /// Read-only boundary
    pub read_only_address: Address,
    /// Head address
    pub head_address: Address,
    /// Begin address
    pub begin_address: Address,
    /// Bytes in mutable region
    pub mutable_bytes: u64,
    /// Bytes in read-only region
    pub read_only_bytes: u64,
    /// Bytes on disk
    pub on_disk_bytes: u64,
}

impl std::fmt::Display for LogStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Log Statistics:")?;
        writeln!(f, "  Tail: {}", self.tail_address)?;
        writeln!(f, "  Read-only: {}", self.read_only_address)?;
        writeln!(f, "  Head: {}", self.head_address)?;
        writeln!(f, "  Begin: {}", self.begin_address)?;
        writeln!(f, "  Mutable bytes: {}", self.mutable_bytes)?;
        writeln!(f, "  Read-only bytes: {}", self.read_only_bytes)?;
        writeln!(f, "  On-disk bytes: {}", self.on_disk_bytes)
    }
}

#[cfg(test)]
mod tests;
