//! Hybrid log allocator (PersistentMemoryMalloc) for FASTER
//!
//! This module provides the core log allocator that manages a circular buffer of pages,
//! with hot pages in memory and cold pages on disk.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::address::{Address, AtomicAddress, AtomicPageOffset};
use crate::allocator::page_allocator::PageInfo;
use crate::constants::PAGE_SIZE;
use crate::device::StorageDevice;
use crate::status::Status;
use crate::utility::{is_power_of_two, AlignedBuffer};

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

/// Page array - manages pages in memory
struct PageArray {
    /// Page buffers
    buffers: Vec<Option<AlignedBuffer>>,
    /// Page info
    info: Vec<PageInfo>,
    /// Number of buffer pages
    buffer_size: u32,
}

impl PageArray {
    fn new(buffer_size: u32) -> Self {
        let mut buffers = Vec::with_capacity(buffer_size as usize);
        let mut info = Vec::with_capacity(buffer_size as usize);

        for _ in 0..buffer_size {
            buffers.push(None);
            info.push(PageInfo::new());
        }

        Self {
            buffers,
            info,
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
    read_only_address: AtomicAddress,
    /// Safe read-only address (flushed)
    safe_read_only_address: AtomicAddress,
    /// Head address (beginning of log)
    head_address: AtomicAddress,
    /// Safe head address (can be reclaimed)
    safe_head_address: AtomicAddress,
    /// Flushed until address
    flushed_until_address: AtomicAddress,
    /// Begin address (first valid address)
    begin_address: AtomicAddress,
    /// Buffer size (number of pages in memory)
    buffer_size: u32,
    /// Number of pending flushes
    pending_flushes: AtomicU64,
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

        Self {
            config,
            device,
            pages,
            tail_page_offset: AtomicPageOffset::from_address(Address::new(0, 0)),
            read_only_address: AtomicAddress::new(Address::new(0, 0)),
            safe_read_only_address: AtomicAddress::new(Address::new(0, 0)),
            head_address: AtomicAddress::new(Address::new(0, 0)),
            safe_head_address: AtomicAddress::new(Address::new(0, 0)),
            flushed_until_address: AtomicAddress::new(Address::new(0, 0)),
            begin_address: AtomicAddress::new(Address::new(0, 0)),
            buffer_size,
            pending_flushes: AtomicU64::new(0),
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
            let ptr = buf.as_ptr().add(offset) as *mut u8;
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
            let ptr = buf.as_mut_ptr().add(offset);
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
        address >= self.get_safe_read_only_address() && address < self.get_read_only_address()
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
                    // Trigger flush for pages that became read-only
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
                return;
            }
        }
    }

    /// Shift the head address
    pub fn shift_head_address(&self, new_address: Address) {
        loop {
            let current = self.head_address.load(Ordering::Acquire);
            if new_address <= current {
                return;
            }

            if self
                .head_address
                .compare_exchange(current, new_address, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Shift the begin address
    pub fn shift_begin_address(&self, new_address: Address) {
        loop {
            let current = self.begin_address.load(Ordering::Acquire);
            if new_address <= current {
                return;
            }

            if self
                .begin_address
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

// SAFETY: `PersistentMemoryMalloc` is safe to send/share between threads because:
// - All concurrent state is stored in atomics (`*_address`, `tail_page_offset`, `pending_flushes`).
// - The in-memory page ring (`pages`) is pre-allocated during construction and only mutated through
//   `&mut self` methods; shared `&self` access only reads from those buffers.
// - `D: StorageDevice` is `Send + Sync` and is held behind an `Arc`.
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
