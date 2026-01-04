//! Log iterator implementation for scanning records in the hybrid log.
//!
//! Based on C++ FASTER's log_scan.h implementation.
//!
//! The log iterator provides several modes:
//! - Single page: Reads one page at a time (simple, low memory)
//! - Double buffering: Prefetches next page while scanning current (better performance)
//! - Concurrent: Multiple threads can scan different pages simultaneously

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::ptr::NonNull;

use crate::address::Address;
use crate::record::{Key, Record, RecordInfo, Value};
use crate::utility::AlignedBuffer;

/// Status of a log page buffer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogPageStatus {
    /// Page buffer not initialized
    Uninitialized = 0,
    /// Page data ready for reading
    Ready = 1,
    /// Page read is pending (async I/O in progress)
    Pending = 2,
    /// No more data available (past end of log)
    Unavailable = 3,
}

impl Default for LogPageStatus {
    fn default() -> Self {
        LogPageStatus::Uninitialized
    }
}

/// A range to scan in the log
#[derive(Debug, Clone, Copy)]
pub struct ScanRange {
    /// Start address (inclusive)
    pub begin: Address,
    /// End address (exclusive)
    pub end: Address,
}

impl ScanRange {
    /// Create a new scan range
    pub fn new(begin: Address, end: Address) -> Self {
        Self { begin, end }
    }

    /// Check if the range is empty
    pub fn is_empty(&self) -> bool {
        self.begin >= self.end
    }

    /// Get the number of pages in this range
    pub fn page_count(&self) -> u32 {
        if self.is_empty() {
            return 0;
        }
        self.end.page().saturating_sub(self.begin.page()) + 1
    }
}

/// A page buffer for log scanning
pub struct LogPage {
    /// Page data buffer
    buffer: Option<AlignedBuffer>,
    /// Current read position within the page
    current_offset: u32,
    /// End offset within this page
    until_offset: u32,
    /// Base address of this page
    page_address: Address,
    /// Page status
    status: LogPageStatus,
    /// Page size
    page_size: usize,
}

impl LogPage {
    /// Create a new log page buffer
    pub fn new(page_size: usize, alignment: usize) -> Self {
        let buffer = AlignedBuffer::zeroed(page_size, alignment);
        Self {
            buffer,
            current_offset: 0,
            until_offset: 0,
            page_address: Address::INVALID,
            status: LogPageStatus::Uninitialized,
            page_size,
        }
    }

    /// Get the page buffer
    pub fn buffer(&self) -> Option<&[u8]> {
        self.buffer.as_ref().map(|b| b.as_slice())
    }

    /// Get the mutable page buffer
    pub fn buffer_mut(&mut self) -> Option<&mut [u8]> {
        self.buffer.as_mut().map(|b| b.as_mut_slice())
    }

    /// Get the page status
    pub fn status(&self) -> LogPageStatus {
        self.status
    }

    /// Set the page status
    pub fn set_status(&mut self, status: LogPageStatus) {
        self.status = status;
    }

    /// Update the page address and range
    pub fn update(&mut self, page_addr: Address, current: Address, until: Address) {
        self.page_address = page_addr;
        self.current_offset = current.offset();
        
        // Calculate until_offset based on whether until is in this page
        if until.page() > page_addr.page() {
            // until is in a later page, scan to end of this page
            self.until_offset = self.page_size as u32;
        } else {
            // until is in this page
            self.until_offset = until.offset();
        }
    }

    /// Reset the page for reuse
    pub fn reset(&mut self) {
        if let Some(ref mut buf) = self.buffer {
            buf.as_mut_slice().fill(0);
        }
        self.current_offset = 0;
        self.until_offset = 0;
        self.page_address = Address::INVALID;
        self.status = LogPageStatus::Uninitialized;
    }

    /// Get the next record from this page
    ///
    /// Returns the record and its address, or None if no more records.
    pub fn get_next<K, V>(&mut self) -> Option<(Address, NonNull<Record<K, V>>)>
    where
        K: Key,
        V: Value,
    {
        if self.status != LogPageStatus::Ready {
            return None;
        }

        if self.current_offset >= self.until_offset {
            return None;
        }

        let buffer = self.buffer.as_ref()?;
        let slice = buffer.as_slice();

        if self.current_offset as usize >= slice.len() {
            return None;
        }

        // Get record at current offset
        let ptr = unsafe { slice.as_ptr().add(self.current_offset as usize) };
        let record_ptr = ptr as *const Record<K, V>;
        let record = unsafe { &*record_ptr };

        // Check for null/invalid record (indicates end of valid data)
        if record.header.is_null() {
            self.current_offset = self.until_offset; // Mark as exhausted
            return None;
        }

        // Calculate record address
        let record_addr = Address::new(self.page_address.page(), self.current_offset);
        
        // Advance to next record
        let record_size = Record::<K, V>::size();
        self.current_offset += record_size as u32;

        Some((record_addr, unsafe { NonNull::new_unchecked(ptr as *mut _) }))
    }

    /// Check if there are more records to read
    pub fn has_more(&self) -> bool {
        self.status == LogPageStatus::Ready && self.current_offset < self.until_offset
    }
}

/// Iterator for scanning log pages
pub struct LogPageIterator {
    /// Current position
    current: Address,
    /// End position
    until: Address,
    /// Page size
    page_size: usize,
}

impl LogPageIterator {
    /// Create a new log page iterator
    pub fn new(range: ScanRange, page_size: usize) -> Self {
        Self {
            current: range.begin,
            until: range.end,
            page_size,
        }
    }

    /// Get the next page address to read
    /// 
    /// Returns (page_address, start_address) or None if no more pages.
    pub fn next_page(&mut self) -> Option<(Address, Address)> {
        if self.current >= self.until {
            return None;
        }

        let start_addr = self.current;
        let page_addr = Address::new(self.current.page(), 0);
        
        // Move to next page
        self.current = Address::new(self.current.page() + 1, 0);
        
        Some((page_addr, start_addr))
    }

    /// Check if there are more pages to scan
    pub fn has_more(&self) -> bool {
        self.current < self.until
    }

    /// Get the remaining page count
    pub fn remaining_pages(&self) -> u32 {
        if self.current >= self.until {
            return 0;
        }
        self.until.page().saturating_sub(self.current.page()) + 1
    }
}

/// Log scan iterator for reading records from the hybrid log
///
/// This iterator provides a simple way to scan through all records
/// in a range of the log. It handles page boundaries automatically.
pub struct LogScanIterator<K, V>
where
    K: Key,
    V: Value,
{
    /// Page iterator
    page_iter: LogPageIterator,
    /// Current page buffer
    current_page: LogPage,
    /// Scan range
    range: ScanRange,
    /// Records scanned
    records_scanned: u64,
    /// Type markers
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> LogScanIterator<K, V>
where
    K: Key,
    V: Value,
{
    /// Create a new log scan iterator
    pub fn new(range: ScanRange, page_size: usize) -> Self {
        let alignment = 4096; // Default sector alignment
        Self {
            page_iter: LogPageIterator::new(range, page_size),
            current_page: LogPage::new(page_size, alignment),
            range,
            records_scanned: 0,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the scan range
    pub fn range(&self) -> ScanRange {
        self.range
    }

    /// Get the number of records scanned
    pub fn records_scanned(&self) -> u64 {
        self.records_scanned
    }

    /// Check if there are more records
    pub fn has_more(&self) -> bool {
        self.current_page.has_more() || self.page_iter.has_more()
    }

    /// Get the current page buffer (for loading data)
    pub fn current_page_mut(&mut self) -> &mut LogPage {
        &mut self.current_page
    }

    /// Advance to the next page
    /// 
    /// Returns the page address and start address to read,
    /// or None if no more pages.
    pub fn advance_page(&mut self) -> Option<(Address, Address)> {
        self.page_iter.next_page()
    }

    /// Get the next record from the current page
    pub fn next_record(&mut self) -> Option<(Address, NonNull<Record<K, V>>)> {
        let result = self.current_page.get_next::<K, V>();
        if result.is_some() {
            self.records_scanned += 1;
        }
        result
    }

    /// Copy data into the current page buffer
    /// 
    /// This is used to load page data from memory or disk.
    pub fn load_page_data(&mut self, data: &[u8], start_addr: Address, until: Address) {
        if let Some(buf) = self.current_page.buffer_mut() {
            let offset = start_addr.offset() as usize;
            let copy_len = data.len().min(buf.len() - offset);
            buf[offset..offset + copy_len].copy_from_slice(&data[..copy_len]);
        }
        
        let page_addr = Address::new(start_addr.page(), 0);
        self.current_page.update(page_addr, start_addr, until);
        self.current_page.set_status(LogPageStatus::Ready);
    }

    /// Mark current page as exhausted and ready for next page
    pub fn finish_page(&mut self) {
        self.current_page.reset();
    }
}

/// Double-buffered log iterator for better performance
pub struct DoubleBufferedLogIterator<K, V>
where
    K: Key,
    V: Value,
{
    /// Two page buffers for double buffering
    pages: [LogPage; 2],
    /// Current page index (0 or 1)
    current_idx: usize,
    /// Page iterator
    page_iter: LogPageIterator,
    /// Scan range
    range: ScanRange,
    /// Records scanned
    records_scanned: u64,
    /// Type markers
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> DoubleBufferedLogIterator<K, V>
where
    K: Key,
    V: Value,
{
    /// Create a new double-buffered log iterator
    pub fn new(range: ScanRange, page_size: usize) -> Self {
        let alignment = 4096;
        Self {
            pages: [
                LogPage::new(page_size, alignment),
                LogPage::new(page_size, alignment),
            ],
            current_idx: 0,
            page_iter: LogPageIterator::new(range, page_size),
            range,
            records_scanned: 0,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the current page
    pub fn current_page(&self) -> &LogPage {
        &self.pages[self.current_idx]
    }

    /// Get the current page mutably
    pub fn current_page_mut(&mut self) -> &mut LogPage {
        &mut self.pages[self.current_idx]
    }

    /// Get the next (prefetch) page
    pub fn next_page(&self) -> &LogPage {
        &self.pages[1 - self.current_idx]
    }

    /// Get the next page mutably
    pub fn next_page_mut(&mut self) -> &mut LogPage {
        &mut self.pages[1 - self.current_idx]
    }

    /// Switch to the next page buffer
    pub fn switch_pages(&mut self) {
        self.pages[self.current_idx].reset();
        self.current_idx = 1 - self.current_idx;
    }

    /// Get the next page address to prefetch
    pub fn next_prefetch_address(&mut self) -> Option<(Address, Address)> {
        self.page_iter.next_page()
    }

    /// Get the next record from the current page
    pub fn next_record(&mut self) -> Option<(Address, NonNull<Record<K, V>>)> {
        let result = self.pages[self.current_idx].get_next::<K, V>();
        if result.is_some() {
            self.records_scanned += 1;
        }
        result
    }

    /// Get the number of records scanned
    pub fn records_scanned(&self) -> u64 {
        self.records_scanned
    }

    /// Check if there are more records
    pub fn has_more(&self) -> bool {
        self.pages[self.current_idx].has_more() 
            || self.pages[1 - self.current_idx].has_more()
            || self.page_iter.has_more()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_range() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(10, 0));
        assert!(!range.is_empty());
        assert_eq!(range.page_count(), 11);
    }

    #[test]
    fn test_scan_range_empty() {
        let range = ScanRange::new(Address::new(5, 0), Address::new(5, 0));
        assert!(range.is_empty());
        assert_eq!(range.page_count(), 0);
    }

    #[test]
    fn test_log_page() {
        let page = LogPage::new(4096, 4096);
        assert_eq!(page.status(), LogPageStatus::Uninitialized);
        assert!(page.buffer().is_some());
    }

    #[test]
    fn test_log_page_iterator() {
        // Range from page 0 offset 100 to page 3 offset 0 (exclusive)
        // This means we scan pages 0, 1, 2
        let range = ScanRange::new(Address::new(0, 100), Address::new(3, 0));
        let mut iter = LogPageIterator::new(range, 4096);

        // First page
        let (page_addr, start_addr) = iter.next_page().unwrap();
        assert_eq!(page_addr, Address::new(0, 0));
        assert_eq!(start_addr, Address::new(0, 100));

        // Second page
        let (page_addr, start_addr) = iter.next_page().unwrap();
        assert_eq!(page_addr, Address::new(1, 0));
        assert_eq!(start_addr, Address::new(1, 0));

        // Third page
        let (page_addr, start_addr) = iter.next_page().unwrap();
        assert_eq!(page_addr, Address::new(2, 0));
        assert_eq!(start_addr, Address::new(2, 0));

        // No more pages (page 3 is past the end)
        assert!(iter.next_page().is_none());
    }

    #[test]
    fn test_log_scan_iterator() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
        let iter: LogScanIterator<u64, u64> = LogScanIterator::new(range, 4096);

        assert_eq!(iter.records_scanned(), 0);
        assert_eq!(iter.range().begin, Address::new(0, 0));
        assert_eq!(iter.range().end, Address::new(2, 0));
    }

    #[test]
    fn test_double_buffered_iterator() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let iter: DoubleBufferedLogIterator<u64, u64> = DoubleBufferedLogIterator::new(range, 4096);

        assert_eq!(iter.records_scanned(), 0);
        assert!(iter.has_more());
    }
}
