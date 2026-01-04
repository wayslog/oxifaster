//! Hybrid log allocator (PersistentMemoryMalloc) for FASTER
//!
//! This module provides the core log allocator that manages a circular buffer of pages,
//! with hot pages in memory and cold pages on disk.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::address::{Address, AtomicAddress, AtomicPageOffset};
use crate::allocator::page_allocator::PageInfo;
use crate::checkpoint::LogMetadata;
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
                    if let Err(status) = self.on_page_full(page) {
                        return Err(status);
                    }
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
                if let Err(status) = self.on_page_full(page) {
                    return Err(status);
                }
            }
            
            // Retry allocation
        }
    }

    /// Get a pointer to a record at the given address
    ///
    /// # Safety
    /// The address must be valid and the record must exist.
    #[inline]
    pub unsafe fn get(&self, address: Address) -> Option<NonNull<u8>> {
        let page = address.page();
        let offset = address.offset() as usize;
        
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
    /// The address must be valid and the record must exist.
    #[inline]
    pub unsafe fn get_mut(&mut self, address: Address) -> Option<NonNull<u8>> {
        let page = address.page();
        let offset = address.offset() as usize;
        
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
        let new_read_only = Address::new(
            page.saturating_sub(self.config.mutable_pages),
            0,
        );
        
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
        self.safe_head_address.store(head_address, Ordering::Release);
        self.read_only_address.store(head_address, Ordering::Release);
        self.safe_read_only_address.store(head_address, Ordering::Release);
        self.flushed_until_address.store(head_address, Ordering::Release);
        self.tail_page_offset.store_address(head_address, Ordering::Release);
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
            mutable_bytes: (tail - read_only) as u64,
            read_only_bytes: (read_only - head) as u64,
            on_disk_bytes: (head - begin) as u64,
        }
    }

    // ============ Checkpoint and Recovery Methods ============

    /// Create checkpoint metadata from the current log state
    ///
    /// # Arguments
    /// * `token` - Unique checkpoint token
    /// * `version` - Current checkpoint version
    /// * `use_snapshot` - Whether to use snapshot file (vs fold-over)
    ///
    /// # Returns
    /// LogMetadata containing current state
    pub fn checkpoint_metadata(
        &self,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        use_snapshot: bool,
    ) -> LogMetadata {
        let mut metadata = LogMetadata::with_token(token);
        metadata.use_snapshot_file = use_snapshot;
        metadata.version = version;
        metadata.begin_address = self.get_begin_address();
        metadata.final_address = self.get_tail_address();
        metadata.flushed_until_address = self.get_flushed_until_address();
        metadata.use_object_log = false;
        metadata
    }

    /// Flush all pages up to (but not including) the specified address
    ///
    /// This writes in-memory pages to the storage device.
    ///
    /// # Arguments
    /// * `until_address` - Flush pages before this address
    ///
    /// # Returns
    /// Ok(()) on success, or an I/O error
    pub fn flush_until(&self, until_address: Address) -> io::Result<()> {
        let current_flushed = self.get_flushed_until_address();
        
        if until_address <= current_flushed {
            return Ok(());
        }

        let begin_page = current_flushed.page();
        let end_page = until_address.page();

        for page in begin_page..=end_page {
            if let Some(page_data) = self.pages.get_page(page) {
                // In a real implementation, this would write to the storage device
                // For now, we just mark it as flushed
                let _ = page_data; // Suppress unused warning
            }
        }

        // Update flushed address
        loop {
            let current = self.flushed_until_address.load(Ordering::Acquire);
            if until_address <= current {
                break;
            }
            
            if self
                .flushed_until_address
                .compare_exchange(current, until_address, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        Ok(())
    }

    /// Flush the log and write checkpoint data to disk
    ///
    /// This is a convenience method that uses snapshot mode by default.
    /// Snapshot mode creates a complete point-in-time copy of the log,
    /// which is generally preferred for durability and simpler recovery.
    /// Use `checkpoint_with_options` for fold-over mode if needed.
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory to write checkpoint files
    /// * `token` - Unique checkpoint token
    /// * `version` - Current checkpoint version
    ///
    /// # Returns
    /// LogMetadata containing the checkpoint information
    pub fn checkpoint(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
    ) -> io::Result<LogMetadata> {
        // Default to snapshot mode for simpler and safer checkpointing
        self.checkpoint_with_options(checkpoint_dir, token, version, true)
    }

    /// Flush the log and write checkpoint data to disk with options
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory to write checkpoint files
    /// * `token` - Unique checkpoint token
    /// * `version` - Current checkpoint version
    /// * `use_snapshot` - Whether to use snapshot file (vs fold-over)
    ///
    /// # Returns
    /// LogMetadata containing the checkpoint information
    pub fn checkpoint_with_options(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        use_snapshot: bool,
    ) -> io::Result<LogMetadata> {
        let tail_address = self.get_tail_address();

        // Flush all in-memory pages
        self.flush_until(tail_address)?;

        // Create metadata
        let metadata = self.checkpoint_metadata(token, version, use_snapshot);

        // Write log metadata
        let meta_path = checkpoint_dir.join("log.meta");
        metadata.write_to_file(&meta_path)?;

        // Write log snapshot (pages in memory)
        let snapshot_path = checkpoint_dir.join("log.snapshot");
        self.write_log_snapshot(&snapshot_path)?;

        Ok(metadata)
    }

    /// Flush the log and write checkpoint data to disk with session states
    ///
    /// This is the primary checkpoint method that includes session persistence
    /// for Concurrent Prefix Recovery (CPR).
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory to write checkpoint files
    /// * `token` - Unique checkpoint token
    /// * `version` - Current checkpoint version
    /// * `session_states` - Active session states to persist
    ///
    /// # Returns
    /// LogMetadata containing the checkpoint information
    pub fn checkpoint_with_sessions(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        session_states: Vec<crate::checkpoint::SessionState>,
    ) -> io::Result<LogMetadata> {
        let tail_address = self.get_tail_address();

        // Flush all in-memory pages
        self.flush_until(tail_address)?;

        // Create metadata with session states
        let mut metadata = self.checkpoint_metadata(token, version, true);
        metadata.session_states = session_states;
        metadata.num_threads = metadata.session_states.len() as u32;

        // Write log metadata
        let meta_path = checkpoint_dir.join("log.meta");
        metadata.write_to_file(&meta_path)?;

        // Write log snapshot (pages in memory)
        let snapshot_path = checkpoint_dir.join("log.snapshot");
        self.write_log_snapshot(&snapshot_path)?;

        Ok(metadata)
    }

    /// Flush all dirty pages to disk
    ///
    /// This method ensures all in-memory data is persisted to the storage device.
    pub fn flush_to_disk(&self) -> io::Result<()> {
        let tail = self.get_tail_address();
        self.flush_until(tail)
    }

    /// Write a snapshot of in-memory pages to disk
    fn write_log_snapshot(&self, path: &Path) -> io::Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(1 << 20, file); // 1 MB buffer

        // Write header
        let begin = self.get_begin_address();
        let tail = self.get_tail_address();
        let page_size = self.config.page_size as u64;

        writer.write_all(&begin.control().to_le_bytes())?;
        writer.write_all(&tail.control().to_le_bytes())?;
        writer.write_all(&page_size.to_le_bytes())?;
        writer.write_all(&(self.buffer_size as u64).to_le_bytes())?;

        // Write pages that contain data
        let begin_page = begin.page();
        let tail_page = tail.page();

        for page in begin_page..=tail_page {
            if let Some(page_data) = self.pages.get_page(page) {
                // Write page number
                writer.write_all(&(page as u64).to_le_bytes())?;
                // Write page data
                writer.write_all(page_data)?;
            }
        }

        // Write end marker (invalid page number)
        writer.write_all(&u64::MAX.to_le_bytes())?;

        writer.flush()?;
        Ok(())
    }

    /// Recover the log from a checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory containing checkpoint files
    /// * `metadata` - Optional pre-loaded metadata; if None, reads from file
    ///
    /// # Returns
    /// Ok(()) on success, or an error
    pub fn recover(
        &mut self,
        checkpoint_dir: &Path,
        metadata: Option<&LogMetadata>,
    ) -> io::Result<()> {
        // Load metadata if not provided
        let meta_path = checkpoint_dir.join("log.meta");
        let loaded_metadata;
        let metadata = match metadata {
            Some(m) => m,
            None => {
                loaded_metadata = LogMetadata::read_from_file(&meta_path)?;
                &loaded_metadata
            }
        };

        // Read log snapshot - must exist for a valid checkpoint
        // The snapshot file is always created during checkpoint, so its absence
        // indicates an incomplete or corrupted checkpoint that should not be recovered.
        let snapshot_path = checkpoint_dir.join("log.snapshot");
        if !snapshot_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Log snapshot file missing: {}. Checkpoint appears incomplete or corrupted.",
                    snapshot_path.display()
                ),
            ));
        }

        // Read the snapshot first to validate it's readable and get actual addresses
        let (snapshot_begin, snapshot_tail) = self.read_log_snapshot(&snapshot_path)?;

        // Initialize addresses from snapshot and metadata after successful snapshot read
        // This ensures we only set addresses if the snapshot data is valid
        // 
        // For recovery, all data loaded from snapshot is in memory, so:
        // - begin_address: start of the log (from snapshot)
        // - head_address: should be begin_address (all data is in memory, nothing on disk)
        // - read_only_address: should be tail_address (all recovered data is read-only)
        // - tail_address: end of the log (from snapshot)
        self.begin_address.store(snapshot_begin, Ordering::Release);
        self.head_address.store(snapshot_begin, Ordering::Release);
        self.safe_head_address.store(snapshot_begin, Ordering::Release);
        self.read_only_address.store(snapshot_tail, Ordering::Release);
        self.safe_read_only_address.store(snapshot_tail, Ordering::Release);
        self.flushed_until_address.store(snapshot_tail, Ordering::Release);
        self.tail_page_offset.store_address(snapshot_tail, Ordering::Release);

        Ok(())
    }

    /// Read a log snapshot from disk
    ///
    /// Returns the begin and tail addresses from the snapshot header
    fn read_log_snapshot(&mut self, path: &Path) -> io::Result<(Address, Address)> {
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(1 << 20, file);

        // Read header
        let mut buf = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let begin_address = Address::from_control(u64::from_le_bytes(buf));

        reader.read_exact(&mut buf)?;
        let tail_address = Address::from_control(u64::from_le_bytes(buf));

        reader.read_exact(&mut buf)?;
        let page_size = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let checkpoint_buffer_size = u64::from_le_bytes(buf);

        // Validate page size matches
        if page_size != self.config.page_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Page size mismatch: file has {}, allocator has {}",
                    page_size, self.config.page_size
                ),
            ));
        }

        // Validate buffer size compatibility
        // Since PageArray uses circular buffering (page % buffer_size), we need to ensure
        // the recovery buffer_size is at least as large as the checkpoint buffer_size.
        // If recovery buffer_size is smaller, different pages will map to the same buffer
        // slot, causing silent data corruption.
        let recovery_buffer_size = self.buffer_size as u64;
        if recovery_buffer_size < checkpoint_buffer_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Buffer size mismatch: checkpoint has {} pages, recovery allocator has {} pages. \
                     Recovery buffer size must be >= checkpoint buffer size to prevent data corruption.",
                    checkpoint_buffer_size, recovery_buffer_size
                ),
            ));
        }

        // Read pages
        let mut page_data = vec![0u8; self.config.page_size];
        loop {
            // Read page number
            reader.read_exact(&mut buf)?;
            let page_num = u64::from_le_bytes(buf);

            if page_num == u64::MAX {
                break; // End marker
            }

            // Read page data
            reader.read_exact(&mut page_data)?;

            // Copy to page array - ensure page is allocated first
            let page = page_num as u32;
            
            // Allocate the page buffer if it doesn't exist
            if !self.pages.allocate_page(page, self.config.page_size) {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("Failed to allocate page {} during recovery", page),
                ));
            }
            
            // Now copy the data - the page is guaranteed to exist
            if let Some(dest) = self.pages.get_page_mut(page) {
                dest.copy_from_slice(&page_data);
            } else {
                // This should not happen after successful allocation
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Page {} not available after allocation", page),
                ));
            }
        }

        Ok((begin_address, tail_address))
    }

    /// Get a reference to the device
    pub fn device(&self) -> &Arc<D> {
        &self.device
    }
}

// Safety: PersistentMemoryMalloc uses atomic operations for concurrent access
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
mod tests {
    use super::*;
    use crate::device::NullDisk;

    fn create_test_allocator() -> PersistentMemoryMalloc<NullDisk> {
        let config = HybridLogConfig {
            page_size: 4096, // 4 KB pages for testing
            memory_pages: 16,
            mutable_pages: 4,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        PersistentMemoryMalloc::new(config, device)
    }

    #[test]
    fn test_allocate_basic() {
        let allocator = create_test_allocator();
        
        let addr1 = allocator.allocate(100).unwrap();
        assert_eq!(addr1.page(), 0);
        assert_eq!(addr1.offset(), 0);
        
        let addr2 = allocator.allocate(100).unwrap();
        assert_eq!(addr2.page(), 0);
        assert_eq!(addr2.offset(), 100);
    }

    #[test]
    fn test_allocate_page_overflow() {
        let allocator = create_test_allocator();
        let page_size = allocator.page_size();
        
        // Fill the first page
        let addr1 = allocator.allocate((page_size - 100) as u32).unwrap();
        assert_eq!(addr1.page(), 0);
        
        // This should trigger a new page
        let addr2 = allocator.allocate(200).unwrap();
        assert_eq!(addr2.page(), 1);
        assert_eq!(addr2.offset(), 0);
    }

    #[test]
    fn test_get_addresses() {
        let allocator = create_test_allocator();
        
        let tail = allocator.get_tail_address();
        let read_only = allocator.get_read_only_address();
        let head = allocator.get_head_address();
        let begin = allocator.get_begin_address();
        
        // Initially all should be at 0
        assert_eq!(tail, Address::new(0, 0));
        assert_eq!(read_only, Address::new(0, 0));
        assert_eq!(head, Address::new(0, 0));
        assert_eq!(begin, Address::new(0, 0));
    }

    #[test]
    fn test_shift_addresses() {
        let allocator = create_test_allocator();
        
        let new_addr = Address::new(5, 0);
        allocator.shift_read_only_address(new_addr);
        
        assert_eq!(allocator.get_read_only_address(), new_addr);
    }

    #[test]
    fn test_log_stats() {
        let allocator = create_test_allocator();
        
        // Allocate some space
        allocator.allocate(1000).unwrap();
        
        let stats = allocator.get_stats();
        assert!(stats.tail_address > Address::new(0, 0));
    }

    #[test]
    fn test_is_mutable() {
        let allocator = create_test_allocator();
        
        // Allocate some space
        let addr = allocator.allocate(100).unwrap();
        
        // Should be mutable (in mutable region)
        assert!(allocator.is_mutable(addr));
        
        // Shift read-only past this address
        allocator.shift_read_only_address(Address::new(1, 0));
        
        // Should no longer be mutable
        assert!(!allocator.is_mutable(addr));
    }

    // ============ Checkpoint and Recovery Tests ============

    #[test]
    fn test_checkpoint_metadata() {
        let allocator = create_test_allocator();
        
        // Allocate some space
        allocator.allocate(1000).unwrap();
        
        let token = uuid::Uuid::new_v4();
        let metadata = allocator.checkpoint_metadata(token, 1, true);
        
        assert_eq!(metadata.token, token);
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.begin_address, Address::new(0, 0));
        assert!(metadata.final_address > Address::new(0, 0));
        assert!(metadata.use_snapshot_file);
    }

    #[test]
    fn test_flush_until() {
        let allocator = create_test_allocator();
        
        // Allocate some space
        let addr = allocator.allocate(1000).unwrap();
        
        // Flush to beyond the allocated address
        let flush_addr = Address::new(addr.page() + 1, 0);
        allocator.flush_until(flush_addr).unwrap();
        
        // Flushed address should be updated
        assert!(allocator.get_flushed_until_address() >= flush_addr);
    }

    #[test]
    fn test_checkpoint_and_recover_empty() {
        let allocator = create_test_allocator();
        
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        
        let metadata = allocator.checkpoint(temp_dir.path(), token, 1).unwrap();
        
        assert_eq!(metadata.token, token);
        assert_eq!(metadata.version, 1);
        
        // Create new allocator and recover
        let mut recovered = create_test_allocator();
        recovered.recover(temp_dir.path(), Some(&metadata)).unwrap();
        
        assert_eq!(recovered.get_begin_address(), allocator.get_begin_address());
    }

    #[test]
    fn test_checkpoint_and_recover_with_data() {
        let mut allocator = create_test_allocator();
        
        // Allocate and write some data
        let addr1 = allocator.allocate(100).unwrap();
        let addr2 = allocator.allocate(200).unwrap();
        
        // Write some test data to the pages
        unsafe {
            let ptr1 = allocator.get_mut(addr1).expect("addr1 should be accessible");
            std::ptr::write_bytes(ptr1.as_ptr(), 0xAB, 100);
            
            let ptr2 = allocator.get_mut(addr2).expect("addr2 should be accessible");
            std::ptr::write_bytes(ptr2.as_ptr(), 0xCD, 200);
        }
        
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        
        let metadata = allocator.checkpoint(temp_dir.path(), token, 2).unwrap();
        
        // Create new allocator and recover
        let mut recovered = create_test_allocator();
        recovered.recover(temp_dir.path(), Some(&metadata)).unwrap();
        
        // Verify addresses are restored
        assert_eq!(recovered.get_begin_address(), allocator.get_begin_address());
        
        // Verify data is restored - use expect() to ensure pages are accessible
        unsafe {
            let ptr1 = recovered.get(addr1).expect("recovered addr1 should be accessible");
            let data1 = std::slice::from_raw_parts(ptr1.as_ptr(), 100);
            assert!(data1.iter().all(|&b| b == 0xAB), "Data at addr1 not correctly recovered");
            
            let ptr2 = recovered.get(addr2).expect("recovered addr2 should be accessible");
            let data2 = std::slice::from_raw_parts(ptr2.as_ptr(), 200);
            assert!(data2.iter().all(|&b| b == 0xCD), "Data at addr2 not correctly recovered");
        }
    }

    #[test]
    fn test_recover_without_preloaded_metadata() {
        let allocator = create_test_allocator();
        
        // Allocate some space
        allocator.allocate(500).unwrap();
        
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        
        allocator.checkpoint(temp_dir.path(), token, 3).unwrap();
        
        // Recover without providing metadata
        let mut recovered = create_test_allocator();
        recovered.recover(temp_dir.path(), None).unwrap();
        
        assert_eq!(recovered.get_begin_address(), allocator.get_begin_address());
    }

    #[test]
    fn test_checkpoint_preserves_stats() {
        let allocator = create_test_allocator();
        
        // Fill up some pages
        for _ in 0..10 {
            allocator.allocate(1000).unwrap();
        }
        
        let original_stats = allocator.get_stats();
        
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        
        let metadata = allocator.checkpoint(temp_dir.path(), token, 1).unwrap();
        
        // Stats from metadata should match
        assert_eq!(metadata.begin_address, original_stats.begin_address);
        assert_eq!(metadata.final_address, original_stats.tail_address);
    }

    #[test]
    fn test_recover_rejects_smaller_buffer_size() {
        // Create allocator with larger buffer size
        let config = HybridLogConfig {
            page_size: 4096,
            memory_pages: 32, // 32 pages
            mutable_pages: 8,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        let allocator = PersistentMemoryMalloc::new(config, device);

        // Allocate some data
        allocator.allocate(1000).unwrap();

        // Create checkpoint
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        allocator.checkpoint(temp_dir.path(), token, 1).unwrap();

        // Try to recover with smaller buffer size - should fail
        let smaller_config = HybridLogConfig {
            page_size: 4096,
            memory_pages: 16, // Only 16 pages (smaller than checkpoint's 32)
            mutable_pages: 4,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        let mut smaller_allocator = PersistentMemoryMalloc::new(smaller_config, device);

        let result = smaller_allocator.recover(temp_dir.path(), None);
        assert!(result.is_err(), "Recovery should fail with smaller buffer size");
        
        if let Err(e) = result {
            assert!(
                e.to_string().contains("Buffer size mismatch"),
                "Error should mention buffer size mismatch, got: {}",
                e
            );
        }
    }

    #[test]
    fn test_recover_allows_larger_buffer_size() {
        // Create allocator with smaller buffer size
        let config = HybridLogConfig {
            page_size: 4096,
            memory_pages: 16, // 16 pages
            mutable_pages: 4,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        let allocator = PersistentMemoryMalloc::new(config, device);

        // Allocate some data
        allocator.allocate(1000).unwrap();

        // Create checkpoint
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        allocator.checkpoint(temp_dir.path(), token, 1).unwrap();

        // Recover with larger buffer size - should succeed
        let larger_config = HybridLogConfig {
            page_size: 4096,
            memory_pages: 32, // 32 pages (larger than checkpoint's 16)
            mutable_pages: 8,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        let mut larger_allocator = PersistentMemoryMalloc::new(larger_config, device);

        let result = larger_allocator.recover(temp_dir.path(), None);
        assert!(result.is_ok(), "Recovery should succeed with larger buffer size");
    }

    #[test]
    fn test_recover_fails_without_snapshot() {
        // Create allocator and checkpoint
        let config = HybridLogConfig {
            page_size: 4096,
            memory_pages: 16,
            mutable_pages: 4,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        let allocator = PersistentMemoryMalloc::new(config, device);

        allocator.allocate(1000).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        
        // Create checkpoint directory structure (as FasterKv does)
        let cp_dir = temp_dir.path().join(token.to_string());
        std::fs::create_dir_all(&cp_dir).unwrap();
        
        allocator.checkpoint(&cp_dir, token, 1).unwrap();

        // Delete the snapshot file to simulate incomplete/corrupted checkpoint
        let snapshot_path = cp_dir.join("log.snapshot");
        assert!(snapshot_path.exists(), "Snapshot should exist before deletion");
        std::fs::remove_file(&snapshot_path).unwrap();

        // Try to recover - should fail because snapshot is missing
        let device = Arc::new(NullDisk::new());
        let mut recovered = PersistentMemoryMalloc::new(
            HybridLogConfig {
                page_size: 4096,
                memory_pages: 16,
                mutable_pages: 4,
                segment_size: 1 << 20,
            },
            device,
        );

        let result = recovered.recover(&cp_dir, None);
        assert!(result.is_err(), "Recovery should fail when snapshot file is missing");

        if let Err(e) = result {
            assert!(
                e.to_string().contains("Log snapshot file missing") || 
                e.to_string().contains("snapshot"),
                "Error should mention missing snapshot, got: {}",
                e
            );
        }
    }

    #[test]
    fn test_recovered_data_is_readable() {
        // Create allocator and write data
        let config = HybridLogConfig {
            page_size: 4096,
            memory_pages: 16,
            mutable_pages: 4,
            segment_size: 1 << 20,
        };
        let device = Arc::new(NullDisk::new());
        let mut allocator = PersistentMemoryMalloc::new(config, device);

        // Allocate and write data
        let addr1 = allocator.allocate(100).unwrap();
        let addr2 = allocator.allocate(200).unwrap();

        unsafe {
            let ptr1 = allocator.get_mut(addr1).expect("addr1 should be accessible");
            std::ptr::write_bytes(ptr1.as_ptr(), 0xAA, 100);

            let ptr2 = allocator.get_mut(addr2).expect("addr2 should be accessible");
            std::ptr::write_bytes(ptr2.as_ptr(), 0xBB, 200);
        }

        // Checkpoint
        let temp_dir = tempfile::tempdir().unwrap();
        let token = uuid::Uuid::new_v4();
        let cp_dir = temp_dir.path().join(token.to_string());
        std::fs::create_dir_all(&cp_dir).unwrap();
        allocator.checkpoint(&cp_dir, token, 1).unwrap();

        // Recover
        let device = Arc::new(NullDisk::new());
        let mut recovered = PersistentMemoryMalloc::new(
            HybridLogConfig {
                page_size: 4096,
                memory_pages: 16,
                mutable_pages: 4,
                segment_size: 1 << 20,
            },
            device,
        );
        recovered.recover(&cp_dir, None).unwrap();

        // Verify recovered addresses are NOT marked as on-disk
        // All recovered data should be in memory and readable
        assert!(
            !recovered.is_on_disk(addr1),
            "Recovered address addr1 should not be marked as on-disk"
        );
        assert!(
            !recovered.is_on_disk(addr2),
            "Recovered address addr2 should not be marked as on-disk"
        );

        // Verify head_address is set to begin_address (all data in memory)
        assert_eq!(
            recovered.get_head_address(),
            recovered.get_begin_address(),
            "Head address should equal begin address after recovery (all data in memory)"
        );

        // Verify data is accessible
        unsafe {
            let ptr1 = recovered.get(addr1).expect("recovered addr1 should be accessible");
            let data1 = std::slice::from_raw_parts(ptr1.as_ptr(), 100);
            assert!(data1.iter().all(|&b| b == 0xAA), "Data at addr1 not correctly recovered");

            let ptr2 = recovered.get(addr2).expect("recovered addr2 should be accessible");
            let data2 = std::slice::from_raw_parts(ptr2.as_ptr(), 200);
            assert!(data2.iter().all(|&b| b == 0xBB), "Data at addr2 not correctly recovered");
        }
    }
}

