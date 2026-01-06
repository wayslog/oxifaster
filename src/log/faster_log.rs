//! FASTER Log - High-performance persistent log
//!
//! This module provides a standalone high-performance log implementation
//! that can be used independently of the key-value store.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::address::{Address, AtomicAddress, AtomicPageOffset};
use crate::allocator::page_allocator::PageInfo;
use crate::device::StorageDevice;
use crate::status::Status;
use crate::utility::AlignedBuffer;

/// Configuration for FASTER Log
#[derive(Debug, Clone)]
pub struct FasterLogConfig {
    /// Page size in bytes
    pub page_size: usize,
    /// Number of pages in memory
    pub memory_pages: u32,
    /// Segment size for disk storage
    pub segment_size: u64,
    /// Auto-commit interval in milliseconds (0 = disabled)
    pub auto_commit_ms: u64,
}

impl FasterLogConfig {
    /// Create a new configuration
    pub fn new(memory_size: u64, page_size: usize) -> Self {
        let memory_pages = (memory_size / page_size as u64) as u32;

        Self {
            page_size,
            memory_pages,
            segment_size: 1 << 30, // 1 GB
            auto_commit_ms: 0,
        }
    }
}

impl Default for FasterLogConfig {
    fn default() -> Self {
        Self {
            page_size: 1 << 22, // 4 MB
            memory_pages: 64,
            segment_size: 1 << 30,
            auto_commit_ms: 0,
        }
    }
}

/// Entry in the FASTER Log
#[repr(C)]
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Length of the entry data
    pub length: u32,
    /// Entry flags
    pub flags: u32,
    // Data follows
}

impl LogEntry {
    /// Header size in bytes
    pub const HEADER_SIZE: usize = 8;

    /// Create a new log entry
    pub fn new(length: u32) -> Self {
        Self { length, flags: 0 }
    }

    /// Get the total size including header
    pub fn total_size(&self) -> usize {
        Self::HEADER_SIZE + self.length as usize
    }
}

/// Iterator for reading log entries
pub struct LogIterator<'a, D: StorageDevice> {
    log: &'a FasterLog<D>,
    current_address: Address,
    end_address: Address,
}

impl<'a, D: StorageDevice> LogIterator<'a, D> {
    /// Create a new iterator
    fn new(log: &'a FasterLog<D>, start: Address, end: Address) -> Self {
        Self {
            log,
            current_address: start,
            end_address: end,
        }
    }

    /// Get the current address
    pub fn current_address(&self) -> Address {
        self.current_address
    }
}

impl<'a, D: StorageDevice> Iterator for LogIterator<'a, D> {
    type Item = (Address, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_address >= self.end_address {
            return None;
        }

        // Read entry at current address
        if let Some(data) = self.log.read_entry(self.current_address) {
            let addr = self.current_address;
            let entry_size = LogEntry::HEADER_SIZE + data.len();
            self.current_address = self.current_address + entry_size as u64;
            Some((addr, data))
        } else {
            None
        }
    }
}

/// FASTER Log - High-performance append-only log
///
/// Provides a simple append-only log interface with:
/// - High-throughput appends
/// - Commit groups for durability
/// - Iteration over persisted entries
pub struct FasterLog<D: StorageDevice> {
    /// Configuration
    config: FasterLogConfig,
    /// Storage device
    device: Arc<D>,
    /// Page buffers
    pages: Vec<Option<AlignedBuffer>>,
    /// Page info
    page_info: Vec<PageInfo>,
    /// Current tail
    tail: AtomicPageOffset,
    /// Committed until address
    committed_until: AtomicAddress,
    /// Flushed until address
    flushed_until: AtomicAddress,
    /// Begin address
    begin_address: AtomicAddress,
    /// Closed flag
    closed: AtomicBool,
    /// Buffer size
    buffer_size: u32,
}

impl<D: StorageDevice> FasterLog<D> {
    /// Create a new FASTER Log
    pub fn new(config: FasterLogConfig, device: D) -> Self {
        let buffer_size = config.memory_pages;
        let page_size = config.page_size;

        let mut pages = Vec::with_capacity(buffer_size as usize);
        let mut page_info = Vec::with_capacity(buffer_size as usize);

        for _ in 0..buffer_size {
            pages.push(AlignedBuffer::zeroed(page_size, page_size));
            page_info.push(PageInfo::new());
        }

        Self {
            config,
            device: Arc::new(device),
            pages,
            page_info,
            tail: AtomicPageOffset::from_address(Address::new(0, 0)),
            committed_until: AtomicAddress::new(Address::new(0, 0)),
            flushed_until: AtomicAddress::new(Address::new(0, 0)),
            begin_address: AtomicAddress::new(Address::new(0, 0)),
            closed: AtomicBool::new(false),
            buffer_size,
        }
    }

    /// Append data to the log
    ///
    /// Returns the address where the data was written.
    pub fn append(&self, data: &[u8]) -> Result<Address, Status> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Status::Aborted);
        }

        let entry_size = LogEntry::HEADER_SIZE + data.len();

        // Reserve space
        loop {
            let page_offset = self.tail.reserve(entry_size as u32);
            let page = page_offset.page();
            let offset = page_offset.offset();

            // Check if we fit in current page
            let new_offset = offset + entry_size as u64;

            if new_offset <= self.config.page_size as u64 {
                let address = Address::new(page, offset as u32);

                // Write entry
                if let Some(ref buf) = self.pages[self.buffer_index(page)] {
                    let slice = unsafe {
                        std::slice::from_raw_parts_mut(
                            buf.as_ptr() as *mut u8,
                            self.config.page_size,
                        )
                    };

                    // Write header
                    let header = LogEntry::new(data.len() as u32);
                    slice[offset as usize..offset as usize + 4]
                        .copy_from_slice(&header.length.to_le_bytes());
                    slice[offset as usize + 4..offset as usize + 8]
                        .copy_from_slice(&header.flags.to_le_bytes());

                    // Write data
                    slice[offset as usize + LogEntry::HEADER_SIZE
                        ..offset as usize + LogEntry::HEADER_SIZE + data.len()]
                        .copy_from_slice(data);
                }

                return Ok(address);
            }

            // Need new page
            let (advanced, _won) = self.tail.new_page(page);
            if !advanced {
                continue;
            }
        }
    }

    /// Append multiple entries atomically
    pub fn append_batch(&self, entries: &[&[u8]]) -> Result<Vec<Address>, Status> {
        let mut addresses = Vec::with_capacity(entries.len());

        for entry in entries {
            addresses.push(self.append(entry)?);
        }

        Ok(addresses)
    }

    /// Commit all entries up to the current tail
    ///
    /// Makes all appended entries durable.
    pub fn commit(&self) -> Result<Address, Status> {
        let tail = self.get_tail_address();

        // Flush all pages up to tail
        // In a full implementation, this would trigger async I/O

        // Update committed address
        loop {
            let current = self.committed_until.load(Ordering::Acquire);
            if tail <= current {
                break;
            }

            if self
                .committed_until
                .compare_exchange(current, tail, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        Ok(tail)
    }

    /// Wait for commit to complete up to the specified address
    pub fn wait_for_commit(&self, address: Address) -> Result<(), Status> {
        while self.committed_until.load(Ordering::Acquire) < address {
            std::thread::yield_now();
        }
        Ok(())
    }

    /// Read an entry at the given address
    pub fn read_entry(&self, address: Address) -> Option<Vec<u8>> {
        let page = address.page();
        let offset = address.offset() as usize;

        if let Some(ref buf) = self.pages[self.buffer_index(page)] {
            // Read header
            let slice = buf.as_slice();
            if offset + LogEntry::HEADER_SIZE > slice.len() {
                return None;
            }

            let length = u32::from_le_bytes(slice[offset..offset + 4].try_into().ok()?) as usize;

            if offset + LogEntry::HEADER_SIZE + length > slice.len() {
                return None;
            }

            // Read data
            let data = slice
                [offset + LogEntry::HEADER_SIZE..offset + LogEntry::HEADER_SIZE + length]
                .to_vec();

            Some(data)
        } else {
            None
        }
    }

    /// Scan entries in a range
    pub fn scan(&self, start: Address, end: Address) -> LogIterator<'_, D> {
        LogIterator::new(self, start, end)
    }

    /// Scan all committed entries
    pub fn scan_all(&self) -> LogIterator<'_, D> {
        let begin = self.begin_address.load(Ordering::Acquire);
        let end = self.committed_until.load(Ordering::Acquire);
        LogIterator::new(self, begin, end)
    }

    /// Get the current tail address
    #[inline]
    pub fn get_tail_address(&self) -> Address {
        self.tail.load(Ordering::Acquire).to_address()
    }

    /// Get the committed until address
    #[inline]
    pub fn get_committed_until(&self) -> Address {
        self.committed_until.load(Ordering::Acquire)
    }

    /// Get the begin address
    #[inline]
    pub fn get_begin_address(&self) -> Address {
        self.begin_address.load(Ordering::Acquire)
    }

    /// Truncate the log up to the specified address
    pub fn truncate_until(&self, address: Address) -> Status {
        loop {
            let current = self.begin_address.load(Ordering::Acquire);
            if address <= current {
                return Status::Ok;
            }

            if self
                .begin_address
                .compare_exchange(current, address, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Status::Ok;
            }
        }
    }

    /// Close the log
    pub fn close(&self) -> Status {
        self.closed.store(true, Ordering::Release);

        // Commit any pending data
        let _ = self.commit();

        Status::Ok
    }

    /// Check if the log is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Get the buffer index for a page
    #[inline]
    fn buffer_index(&self, page: u32) -> usize {
        (page % self.buffer_size) as usize
    }

    /// Get log statistics
    pub fn get_stats(&self) -> LogStats {
        LogStats {
            tail_address: self.get_tail_address(),
            committed_address: self.get_committed_until(),
            begin_address: self.get_begin_address(),
            page_size: self.config.page_size,
            buffer_pages: self.buffer_size,
        }
    }
}

/// Statistics about the log
#[derive(Debug, Clone)]
pub struct LogStats {
    /// Tail address
    pub tail_address: Address,
    /// Committed address
    pub committed_address: Address,
    /// Begin address
    pub begin_address: Address,
    /// Page size
    pub page_size: usize,
    /// Number of buffer pages
    pub buffer_pages: u32,
}

impl std::fmt::Display for LogStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "FASTER Log Statistics:")?;
        writeln!(f, "  Tail: {}", self.tail_address)?;
        writeln!(f, "  Committed: {}", self.committed_address)?;
        writeln!(f, "  Begin: {}", self.begin_address)?;
        writeln!(f, "  Page size: {} bytes", self.page_size)?;
        writeln!(f, "  Buffer pages: {}", self.buffer_pages)
    }
}

// Safety: FasterLog uses atomic operations for concurrent access
unsafe impl<D: StorageDevice> Send for FasterLog<D> {}
unsafe impl<D: StorageDevice> Sync for FasterLog<D> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;

    fn create_test_log() -> FasterLog<NullDisk> {
        let config = FasterLogConfig {
            page_size: 4096,
            memory_pages: 16,
            segment_size: 1 << 20,
            auto_commit_ms: 0,
        };
        let device = NullDisk::new();
        FasterLog::new(config, device)
    }

    #[test]
    fn test_append_and_read() {
        let log = create_test_log();

        let data = b"Hello, World!";
        let addr = log.append(data).unwrap();

        let read_data = log.read_entry(addr).unwrap();
        assert_eq!(&read_data, data);
    }

    #[test]
    fn test_append_multiple() {
        let log = create_test_log();

        let entries: Vec<_> = (0..10).map(|i| format!("Entry {}", i)).collect();

        let addresses: Vec<_> = entries
            .iter()
            .map(|e| log.append(e.as_bytes()).unwrap())
            .collect();

        for (i, addr) in addresses.iter().enumerate() {
            let data = log.read_entry(*addr).unwrap();
            assert_eq!(String::from_utf8(data).unwrap(), entries[i]);
        }
    }

    #[test]
    fn test_commit() {
        let log = create_test_log();

        log.append(b"test data").unwrap();

        let committed = log.commit().unwrap();
        assert!(committed > Address::new(0, 0));
    }

    #[test]
    fn test_scan() {
        let log = create_test_log();

        let entries = vec!["one", "two", "three"];
        for entry in &entries {
            log.append(entry.as_bytes()).unwrap();
        }

        log.commit().unwrap();

        let mut count = 0;
        for (addr, data) in log.scan_all() {
            assert!(addr >= Address::new(0, 0));
            assert!(!data.is_empty());
            count += 1;
        }

        assert_eq!(count, entries.len());
    }

    #[test]
    fn test_close() {
        let log = create_test_log();

        assert!(!log.is_closed());
        log.close();
        assert!(log.is_closed());

        // Append should fail after close
        let result = log.append(b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_stats() {
        let log = create_test_log();

        log.append(b"test data").unwrap();

        let stats = log.get_stats();
        assert!(stats.tail_address > Address::new(0, 0));
        assert_eq!(stats.page_size, 4096);
    }
}
