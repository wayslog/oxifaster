//! Delta Log implementation
//!
//! The DeltaLog provides append-only storage for incremental checkpoint records.
//! It supports both writing new entries during checkpoint and reading entries
//! during recovery.
//!
//! Based on C# FASTER's DeltaLog implementation.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::device::StorageDevice;

use super::entry::{
    compute_entry_checksum, verify_checksum, DeltaLogEntry, DeltaLogEntryType, DeltaLogHeader,
    DELTA_LOG_HEADER_SIZE,
};

/// Configuration for DeltaLog
#[derive(Debug, Clone)]
pub struct DeltaLogConfig {
    /// Log page size in bits (e.g., 22 for 4MB pages)
    pub page_size_bits: u32,
    /// Sector size for alignment (typically 512 or 4096)
    pub sector_size: usize,
}

impl Default for DeltaLogConfig {
    fn default() -> Self {
        Self {
            page_size_bits: 22, // 4MB pages
            sector_size: 512,
        }
    }
}

impl DeltaLogConfig {
    /// Create a new configuration with the given page size bits
    pub fn new(page_size_bits: u32) -> Self {
        Self {
            page_size_bits,
            ..Default::default()
        }
    }

    /// Get the page size in bytes
    pub fn page_size(&self) -> usize {
        1 << self.page_size_bits
    }

    /// Get the page size mask
    pub fn page_size_mask(&self) -> usize {
        self.page_size() - 1
    }

    /// Align a value to sector boundary
    pub fn align(&self, value: i64) -> i64 {
        (value + self.sector_size as i64 - 1) & !(self.sector_size as i64 - 1)
    }
}

/// Write buffer for DeltaLog
struct WriteBuffer {
    /// Buffer data
    data: Vec<u8>,
    /// Current write offset within buffer
    offset: usize,
    /// Page size
    page_size: usize,
}

impl WriteBuffer {
    fn new(page_size: usize) -> Self {
        Self {
            data: vec![0u8; page_size],
            offset: 0,
            page_size,
        }
    }

    fn clear(&mut self) {
        self.data.fill(0);
        self.offset = 0;
    }

    fn remaining(&self) -> usize {
        self.page_size - self.offset
    }

    fn is_empty(&self) -> bool {
        self.offset == 0
    }
}

/// Delta Log for incremental checkpoints
///
/// Provides append-only storage for delta records during incremental checkpoints.
/// Each record consists of a header with checksum, length, and type, followed by
/// the payload data.
pub struct DeltaLog<D: StorageDevice> {
    /// Storage device (public for iterator access)
    pub device: Arc<D>,
    /// Configuration
    config: DeltaLogConfig,
    /// Tail address (next write position)
    tail_address: AtomicI64,
    /// Flushed until address
    flushed_until_address: AtomicI64,
    /// End address for reads (file size)
    end_address: i64,
    /// Write buffer
    write_buffer: Mutex<Option<WriteBuffer>>,
    /// Whether initialized for writes
    initialized_for_writes: AtomicBool,
    /// Whether initialized for reads
    initialized_for_reads: AtomicBool,
    /// Whether disposed
    disposed: AtomicBool,
}

impl<D: StorageDevice> DeltaLog<D> {
    /// Create a new DeltaLog
    ///
    /// # Arguments
    /// * `device` - Storage device for the delta log
    /// * `config` - Configuration
    /// * `tail_address` - Initial tail address (-1 to use file size)
    pub fn new(device: Arc<D>, config: DeltaLogConfig, tail_address: i64) -> Self {
        let end_address = if tail_address >= 0 {
            tail_address
        } else {
            device.size().unwrap_or(0) as i64
        };

        Self {
            device,
            config,
            tail_address: AtomicI64::new(end_address),
            flushed_until_address: AtomicI64::new(end_address),
            end_address,
            write_buffer: Mutex::new(None),
            initialized_for_writes: AtomicBool::new(false),
            initialized_for_reads: AtomicBool::new(false),
            disposed: AtomicBool::new(false),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(device: Arc<D>, tail_address: i64) -> Self {
        Self::new(device, DeltaLogConfig::default(), tail_address)
    }

    /// Get the configuration
    pub fn config(&self) -> &DeltaLogConfig {
        &self.config
    }

    /// Get the current tail address
    pub fn tail_address(&self) -> i64 {
        self.tail_address.load(Ordering::Acquire)
    }

    /// Get the flushed until address
    pub fn flushed_until_address(&self) -> i64 {
        self.flushed_until_address.load(Ordering::Acquire)
    }

    /// Get the next address after the last entry
    pub fn next_address(&self) -> i64 {
        self.tail_address()
    }

    /// Get the end address (for reads)
    pub fn end_address(&self) -> i64 {
        self.end_address
    }

    /// Check if initialized for writes
    pub fn is_initialized_for_writes(&self) -> bool {
        self.initialized_for_writes.load(Ordering::Acquire)
    }

    /// Check if initialized for reads
    pub fn is_initialized_for_reads(&self) -> bool {
        self.initialized_for_reads.load(Ordering::Acquire)
    }

    /// Initialize for writes
    pub fn init_for_writes(&self) {
        if self.initialized_for_writes.swap(true, Ordering::AcqRel) {
            return; // Already initialized
        }

        let mut buffer = self.write_buffer.lock();
        *buffer = Some(WriteBuffer::new(self.config.page_size()));
    }

    /// Initialize for reads
    pub fn init_for_reads(&self) {
        self.initialized_for_reads.store(true, Ordering::Release);
    }

    /// Allocate space for a new entry
    ///
    /// Returns the maximum entry length and physical address to write to.
    /// After writing, call `seal()` to finalize the entry.
    pub fn allocate(&self) -> io::Result<(usize, *mut u8)> {
        if !self.is_initialized_for_writes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DeltaLog not initialized for writes",
            ));
        }

        let mut buffer_guard = self.write_buffer.lock();
        let buffer = buffer_guard.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Write buffer not available")
        })?;

        let tail = self.tail_address.load(Ordering::Acquire);
        let page_size = self.config.page_size() as i64;
        let page_end_address = ((tail / page_size) + 1) * page_size;
        let data_start_address = tail + DELTA_LOG_HEADER_SIZE as i64;
        let max_entry_length = (page_end_address - data_start_address) as usize;

        let offset_in_page = (data_start_address & self.config.page_size_mask() as i64) as usize;
        let physical_address = buffer.data.as_mut_ptr().wrapping_add(offset_in_page);

        Ok((max_entry_length, physical_address))
    }

    /// Seal an allocated entry
    ///
    /// # Arguments
    /// * `entry_length` - Actual length of the entry payload
    /// * `entry_type` - Type of the entry
    pub fn seal(&self, entry_length: usize, entry_type: DeltaLogEntryType) -> io::Result<()> {
        if entry_length == 0 {
            // Skip to next page if entry doesn't fit
            let tail = self.tail_address.load(Ordering::Acquire);
            let page_size = self.config.page_size() as i64;
            let new_tail = ((tail / page_size) + 1) * page_size;
            self.tail_address.store(new_tail, Ordering::Release);
            self.flush_current_page()?;
            return Ok(());
        }

        let mut buffer_guard = self.write_buffer.lock();
        let buffer = buffer_guard.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Write buffer not available")
        })?;

        let old_tail = self.tail_address.load(Ordering::Acquire);
        let offset_in_page = (old_tail & self.config.page_size_mask() as i64) as usize;

        // Create and write header
        let mut header = DeltaLogHeader::new(entry_length as i32, entry_type);

        // Get payload slice for checksum computation
        let payload_offset = offset_in_page + DELTA_LOG_HEADER_SIZE;
        let payload = &buffer.data[payload_offset..payload_offset + entry_length];
        compute_entry_checksum(&mut header, payload);

        // Write header
        let header_bytes = header.to_bytes();
        buffer.data[offset_in_page..offset_in_page + DELTA_LOG_HEADER_SIZE]
            .copy_from_slice(&header_bytes);

        // Update tail address
        let mut new_tail = old_tail + DELTA_LOG_HEADER_SIZE as i64 + entry_length as i64;
        new_tail = self.config.align(new_tail);

        // Check if we need to move to next page
        let page_size = self.config.page_size() as i64;
        let page_end_address = ((new_tail / page_size) + 1) * page_size;
        if new_tail + DELTA_LOG_HEADER_SIZE as i64 >= page_end_address {
            new_tail = ((new_tail / page_size) + 1) * page_size;
        }

        let old_page = old_tail / page_size;
        let new_page = new_tail / page_size;

        self.tail_address.store(new_tail, Ordering::Release);

        // Flush if we crossed a page boundary
        if old_page < new_page {
            drop(buffer_guard); // Release lock before flush
            self.flush_current_page()?;
        }

        Ok(())
    }

    /// Write an entry directly
    pub fn write_entry(&self, entry: &DeltaLogEntry) -> io::Result<i64> {
        if !self.is_initialized_for_writes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DeltaLog not initialized for writes",
            ));
        }

        let (max_len, ptr) = self.allocate()?;
        let entry_len = entry.payload.len();

        if entry_len > max_len {
            // Entry doesn't fit in current page, skip to next
            self.seal(0, DeltaLogEntryType::Delta)?;
            return self.write_entry(entry);
        }

        // Copy payload to allocated space
        unsafe {
            std::ptr::copy_nonoverlapping(entry.payload.as_ptr(), ptr, entry_len);
        }

        let address = self.tail_address();
        self.seal(
            entry_len,
            entry.entry_type().unwrap_or(DeltaLogEntryType::Delta),
        )?;

        Ok(address)
    }

    /// Flush the current page to device
    fn flush_current_page(&self) -> io::Result<()> {
        let mut buffer_guard = self.write_buffer.lock();
        let buffer = match buffer_guard.as_mut() {
            Some(b) => b,
            None => return Ok(()),
        };

        if buffer.is_empty() {
            return Ok(());
        }

        let tail = self.tail_address.load(Ordering::Acquire);
        let flushed = self.flushed_until_address.load(Ordering::Acquire);
        let page_size = self.config.page_size() as i64;

        // Calculate what to flush
        let page_start = (flushed / page_size) * page_size;
        let start_offset = (flushed & self.config.page_size_mask() as i64) as usize;
        let flush_length = self.config.align(tail - flushed) as usize;

        if flush_length == 0 {
            return Ok(());
        }

        // Write to device synchronously for now
        // In a full implementation, this would be async
        let data_to_write = &buffer.data[start_offset..start_offset + flush_length];

        // Use blocking write
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => {
                handle.block_on(async {
                    self.device
                        .write(page_start as u64 + start_offset as u64, data_to_write)
                        .await
                })?;
            }
            Err(_) => {
                // No runtime, create a temporary one
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    self.device
                        .write(page_start as u64 + start_offset as u64, data_to_write)
                        .await
                })?;
            }
        }

        self.flushed_until_address.store(tail, Ordering::Release);

        // Clear buffer for next page
        buffer.clear();

        Ok(())
    }

    /// Flush all pending writes
    pub fn flush(&self) -> io::Result<()> {
        // Flush any partial page
        let tail = self.tail_address.load(Ordering::Acquire);
        let page_size = self.config.page_size() as i64;
        let page_start = (tail / page_size) * page_size;

        if tail > page_start {
            self.flush_current_page()?;
        }

        Ok(())
    }

    /// Flush asynchronously
    #[allow(clippy::await_holding_lock)]
    pub async fn flush_async(&self) -> io::Result<()> {
        // Extract data while holding lock, then release before await
        let (data_to_write, page_start, start_offset, tail) = {
            let buffer_guard = self.write_buffer.lock();
            let buffer = match buffer_guard.as_ref() {
                Some(b) => b,
                None => return Ok(()),
            };

            if buffer.is_empty() {
                return Ok(());
            }

            let tail = self.tail_address.load(Ordering::Acquire);
            let flushed = self.flushed_until_address.load(Ordering::Acquire);
            let page_size = self.config.page_size() as i64;

            let page_start = (flushed / page_size) * page_size;
            let start_offset = (flushed & self.config.page_size_mask() as i64) as usize;
            let flush_length = self.config.align(tail - flushed) as usize;

            if flush_length == 0 {
                return Ok(());
            }

            let data = buffer.data[start_offset..start_offset + flush_length].to_vec();
            (data, page_start, start_offset, tail)
        }; // buffer_guard dropped here

        self.device
            .write(page_start as u64 + start_offset as u64, &data_to_write)
            .await?;

        self.flushed_until_address.store(tail, Ordering::Release);

        Ok(())
    }

    /// Read an entry at the given address
    pub fn read_entry(&self, address: i64) -> io::Result<Option<DeltaLogEntry>> {
        if address < 0 || address >= self.tail_address() {
            return Ok(None);
        }

        // Read header first
        let mut header_buf = vec![0u8; DELTA_LOG_HEADER_SIZE];

        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => {
                handle
                    .block_on(async { self.device.read(address as u64, &mut header_buf).await })?;
            }
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async { self.device.read(address as u64, &mut header_buf).await })?;
            }
        }

        let header = DeltaLogHeader::from_bytes(&header_buf)?;

        if header.length <= 0 {
            return Ok(None);
        }

        // Read payload
        let mut payload = vec![0u8; header.length as usize];
        let payload_address = address + DELTA_LOG_HEADER_SIZE as i64;

        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => {
                handle.block_on(async {
                    self.device.read(payload_address as u64, &mut payload).await
                })?;
            }
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    self.device.read(payload_address as u64, &mut payload).await
                })?;
            }
        }

        // Verify checksum
        if !verify_checksum(&header, &payload) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Delta log entry checksum mismatch",
            ));
        }

        Ok(Some(DeltaLogEntry { header, payload }))
    }

    /// Read an entry asynchronously
    pub async fn read_entry_async(&self, address: i64) -> io::Result<Option<DeltaLogEntry>> {
        if address < 0 || address >= self.tail_address() {
            return Ok(None);
        }

        // Read header
        let mut header_buf = vec![0u8; DELTA_LOG_HEADER_SIZE];
        self.device.read(address as u64, &mut header_buf).await?;

        let header = DeltaLogHeader::from_bytes(&header_buf)?;

        if header.length <= 0 {
            return Ok(None);
        }

        // Read payload
        let mut payload = vec![0u8; header.length as usize];
        let payload_address = address + DELTA_LOG_HEADER_SIZE as i64;
        self.device
            .read(payload_address as u64, &mut payload)
            .await?;

        // Verify checksum
        if !verify_checksum(&header, &payload) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Delta log entry checksum mismatch",
            ));
        }

        Ok(Some(DeltaLogEntry { header, payload }))
    }

    /// Dispose the delta log
    pub fn dispose(&self) {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return; // Already disposed
        }

        // Flush any remaining data
        let _ = self.flush();

        // Clear write buffer
        let mut buffer = self.write_buffer.lock();
        *buffer = None;
    }
}

impl<D: StorageDevice> Drop for DeltaLog<D> {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;

    fn create_test_delta_log() -> DeltaLog<NullDisk> {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12); // 4KB pages for testing
        DeltaLog::new(device, config, 0)
    }

    #[test]
    fn test_delta_log_creation() {
        let log = create_test_delta_log();
        assert_eq!(log.tail_address(), 0);
        assert!(!log.is_initialized_for_writes());
    }

    #[test]
    fn test_init_for_writes() {
        let log = create_test_delta_log();
        log.init_for_writes();
        assert!(log.is_initialized_for_writes());
    }

    #[test]
    fn test_write_entry() {
        let log = create_test_delta_log();
        log.init_for_writes();

        let entry = DeltaLogEntry::delta(b"test data".to_vec());
        let addr = log.write_entry(&entry).unwrap();
        assert_eq!(addr, 0);
        assert!(log.tail_address() > 0);
    }

    #[test]
    fn test_multiple_writes() {
        let log = create_test_delta_log();
        log.init_for_writes();

        for i in 0..10 {
            let entry = DeltaLogEntry::delta(format!("entry {i}").into_bytes());
            log.write_entry(&entry).unwrap();
        }

        assert!(log.tail_address() > 0);
    }

    #[test]
    fn test_config() {
        let config = DeltaLogConfig::new(20);
        assert_eq!(config.page_size(), 1 << 20); // 1MB
        assert_eq!(config.page_size_mask(), (1 << 20) - 1);

        let aligned = config.align(1000);
        assert_eq!(aligned % config.sector_size as i64, 0);
    }
}
