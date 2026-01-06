//! Log iterator implementation for scanning records in the hybrid log.
//!
//! Based on C++ FASTER's log_scan.h implementation.
//!
//! The log iterator provides several modes:
//! - Single page: Reads one page at a time (simple, low memory)
//! - Double buffering: Prefetches next page while scanning current (better performance)
//! - Concurrent: Multiple threads can scan different pages simultaneously

use std::io;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::address::Address;
use crate::device::SyncStorageDevice;
use crate::record::{Key, Record, Value};
use crate::utility::AlignedBuffer;

/// Status of a log page buffer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LogPageStatus {
    /// Page buffer not initialized
    #[default]
    Uninitialized = 0,
    /// Page data ready for reading
    Ready = 1,
    /// Page read is pending (async I/O in progress)
    Pending = 2,
    /// No more data available (past end of log)
    Unavailable = 3,
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

        Some((record_addr, unsafe {
            NonNull::new_unchecked(ptr as *mut _)
        }))
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
pub struct LogScanIterator<K, V, D = ()>
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
    /// Optional storage device for disk I/O
    device: Option<Arc<D>>,
    /// Page size
    page_size: usize,
    /// Type markers
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> LogScanIterator<K, V, ()>
where
    K: Key,
    V: Value,
{
    /// Create a new log scan iterator without a device (memory-only mode)
    pub fn new(range: ScanRange, page_size: usize) -> Self {
        let alignment = 4096; // Default sector alignment
        Self {
            page_iter: LogPageIterator::new(range, page_size),
            current_page: LogPage::new(page_size, alignment),
            range,
            records_scanned: 0,
            device: None,
            page_size,
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

impl<K, V, D> LogScanIterator<K, V, D>
where
    K: Key,
    V: Value,
    D: SyncStorageDevice,
{
    /// Create a new log scan iterator with a device for disk I/O
    pub fn with_device(range: ScanRange, page_size: usize, device: Arc<D>) -> Self {
        let alignment = device.alignment().max(4096);
        Self {
            page_iter: LogPageIterator::new(range, page_size),
            current_page: LogPage::new(page_size, alignment),
            range,
            records_scanned: 0,
            device: Some(device),
            page_size,
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

    /// Load a page from the device synchronously
    ///
    /// Reads a full page from the storage device into the current page buffer.
    ///
    /// # Arguments
    /// * `page_num` - The page number to read
    /// * `start_addr` - The starting address within the page
    /// * `until` - The end address for scanning
    ///
    /// # Returns
    /// * `Ok(())` - If the page was loaded successfully
    /// * `Err(io::Error)` - If the I/O operation failed
    pub fn load_page_from_device(
        &mut self,
        page_num: u32,
        start_addr: Address,
        until: Address,
    ) -> io::Result<()> {
        let device = self
            .device
            .as_ref()
            .ok_or_else(|| io::Error::other("No device configured for iterator"))?;

        // Calculate the offset in the device
        let device_offset = page_num as u64 * self.page_size as u64;

        // Get the buffer and read from device
        let buf = self
            .current_page
            .buffer_mut()
            .ok_or_else(|| io::Error::other("Page buffer not available"))?;

        // Read the entire page from device
        let bytes_read = device.read_sync(device_offset, buf)?;

        if bytes_read < self.page_size {
            // Zero out the rest if we didn't read a full page
            buf[bytes_read..].fill(0);
        }

        // Update page metadata
        let page_addr = Address::new(page_num, 0);
        self.current_page.update(page_addr, start_addr, until);
        self.current_page.set_status(LogPageStatus::Ready);

        Ok(())
    }

    /// Scan and process all records in the range using a callback
    ///
    /// This method handles all the page loading and iteration internally,
    /// calling the provided callback for each record.
    ///
    /// # Arguments
    /// * `callback` - A function called for each record: (address, record) -> bool
    ///   Return false to stop iteration early.
    ///
    /// # Returns
    /// * `Ok(records_processed)` - Number of records processed
    /// * `Err(io::Error)` - If an I/O error occurred
    pub fn scan_all<F>(&mut self, mut callback: F) -> io::Result<u64>
    where
        F: FnMut(Address, &Record<K, V>) -> bool,
    {
        let mut processed = 0u64;

        while let Some((page_addr, start_addr)) = self.advance_page() {
            // Determine the end address for this page
            let page_end = Address::new(page_addr.page() + 1, 0);
            let until = if page_end < self.range.end {
                page_end
            } else {
                self.range.end
            };

            // Load the page from device
            self.load_page_from_device(page_addr.page(), start_addr, until)?;

            // Process all records in the page
            while let Some((addr, record_ptr)) = self.next_record() {
                let record = unsafe { record_ptr.as_ref() };
                processed += 1;

                // Call the user callback
                if !callback(addr, record) {
                    return Ok(processed);
                }
            }

            // Reset page for next iteration
            self.finish_page();
        }

        Ok(processed)
    }

    /// Mark current page as exhausted and ready for next page
    pub fn finish_page(&mut self) {
        self.current_page.reset();
    }
}

/// Double-buffered log iterator for better performance
///
/// Uses two page buffers to enable prefetching while scanning.
/// While one buffer is being processed, the next page can be loaded
/// into the other buffer.
pub struct DoubleBufferedLogIterator<K, V, D = ()>
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
    /// Optional storage device for disk I/O
    device: Option<Arc<D>>,
    /// Page size
    page_size: usize,
    /// Pending prefetch info: (page_num, start_addr, until_addr)
    pending_prefetch: Option<(u32, Address, Address)>,
    /// Type markers
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> DoubleBufferedLogIterator<K, V, ()>
where
    K: Key,
    V: Value,
{
    /// Create a new double-buffered log iterator without a device
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
            device: None,
            page_size,
            pending_prefetch: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the scan range
    pub fn range(&self) -> ScanRange {
        self.range
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
    pub fn prefetch_page(&self) -> &LogPage {
        &self.pages[1 - self.current_idx]
    }

    /// Get the next page mutably
    pub fn prefetch_page_mut(&mut self) -> &mut LogPage {
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

impl<K, V, D> DoubleBufferedLogIterator<K, V, D>
where
    K: Key,
    V: Value,
    D: SyncStorageDevice,
{
    /// Create a new double-buffered log iterator with a device
    pub fn with_device(range: ScanRange, page_size: usize, device: Arc<D>) -> Self {
        let alignment = device.alignment().max(4096);
        Self {
            pages: [
                LogPage::new(page_size, alignment),
                LogPage::new(page_size, alignment),
            ],
            current_idx: 0,
            page_iter: LogPageIterator::new(range, page_size),
            range,
            records_scanned: 0,
            device: Some(device),
            page_size,
            pending_prefetch: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the scan range
    pub fn range(&self) -> ScanRange {
        self.range
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
    pub fn prefetch_page(&self) -> &LogPage {
        &self.pages[1 - self.current_idx]
    }

    /// Get the next page mutably
    pub fn prefetch_page_mut(&mut self) -> &mut LogPage {
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

    /// Load a page from the device into the specified buffer
    fn load_page_into_buffer(
        device: &D,
        page: &mut LogPage,
        page_num: u32,
        start_addr: Address,
        until: Address,
        page_size: usize,
    ) -> io::Result<()> {
        let device_offset = page_num as u64 * page_size as u64;

        let buf = page
            .buffer_mut()
            .ok_or_else(|| io::Error::other("Page buffer not available"))?;

        let bytes_read = device.read_sync(device_offset, buf)?;

        if bytes_read < page_size {
            buf[bytes_read..].fill(0);
        }

        let page_addr = Address::new(page_num, 0);
        page.update(page_addr, start_addr, until);
        page.set_status(LogPageStatus::Ready);

        Ok(())
    }

    /// Start prefetching the next page into the secondary buffer
    ///
    /// This prepares the next page to be loaded. Call `complete_prefetch`
    /// to actually load the data.
    pub fn start_prefetch(&mut self) -> Option<(u32, Address, Address)> {
        if let Some((page_addr, start_addr)) = self.page_iter.next_page() {
            let page_end = Address::new(page_addr.page() + 1, 0);
            let until = if page_end < self.range.end {
                page_end
            } else {
                self.range.end
            };

            self.pending_prefetch = Some((page_addr.page(), start_addr, until));
            Some((page_addr.page(), start_addr, until))
        } else {
            self.pending_prefetch = None;
            None
        }
    }

    /// Complete the prefetch operation by loading data into the prefetch buffer
    pub fn complete_prefetch(&mut self) -> io::Result<bool> {
        let device = match self.device.as_ref() {
            Some(d) => d,
            None => return Err(io::Error::other("No device configured for iterator")),
        };

        if let Some((page_num, start_addr, until)) = self.pending_prefetch.take() {
            let prefetch_idx = 1 - self.current_idx;
            Self::load_page_into_buffer(
                device,
                &mut self.pages[prefetch_idx],
                page_num,
                start_addr,
                until,
                self.page_size,
            )?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Load the current page from the device
    pub fn load_current_page(
        &mut self,
        page_num: u32,
        start_addr: Address,
        until: Address,
    ) -> io::Result<()> {
        let device = self
            .device
            .as_ref()
            .ok_or_else(|| io::Error::other("No device configured for iterator"))?;

        Self::load_page_into_buffer(
            device,
            &mut self.pages[self.current_idx],
            page_num,
            start_addr,
            until,
            self.page_size,
        )
    }

    /// Scan all records with double buffering
    ///
    /// This method loads pages using double buffering for better I/O performance.
    /// While processing one page, the next page is loaded in the background.
    ///
    /// # Arguments
    /// * `callback` - A function called for each record: (address, record) -> bool
    ///   Return false to stop iteration early.
    ///
    /// # Returns
    /// * `Ok(records_processed)` - Number of records processed
    /// * `Err(io::Error)` - If an I/O error occurred
    pub fn scan_all<F>(&mut self, mut callback: F) -> io::Result<u64>
    where
        F: FnMut(Address, &Record<K, V>) -> bool,
    {
        let mut processed = 0u64;

        // Load the first page
        if let Some((page_addr, start_addr)) = self.page_iter.next_page() {
            let page_end = Address::new(page_addr.page() + 1, 0);
            let until = if page_end < self.range.end {
                page_end
            } else {
                self.range.end
            };

            self.load_current_page(page_addr.page(), start_addr, until)?;
        } else {
            return Ok(0);
        }

        // Start prefetching the next page
        self.start_prefetch();
        let _ = self.complete_prefetch();

        loop {
            // Process all records in the current page
            while let Some((addr, record_ptr)) = self.next_record() {
                let record = unsafe { record_ptr.as_ref() };
                processed += 1;

                if !callback(addr, record) {
                    return Ok(processed);
                }
            }

            // Check if there's a prefetched page ready
            if !self.pages[1 - self.current_idx].has_more() && !self.page_iter.has_more() {
                break;
            }

            // Switch to the prefetched page
            self.switch_pages();

            // If the new current page is ready, start prefetching the next
            if self.pages[self.current_idx].status() == LogPageStatus::Ready {
                self.start_prefetch();
                let _ = self.complete_prefetch();
            }
        }

        Ok(processed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;

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
        let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

        assert_eq!(iter.records_scanned(), 0);
        // range() is only available when D: SyncStorageDevice
    }

    #[test]
    fn test_log_scan_iterator_with_device() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
        let device = Arc::new(NullDisk::new());
        let iter: LogScanIterator<u64, u64, NullDisk> =
            LogScanIterator::with_device(range, 4096, device);

        assert_eq!(iter.records_scanned(), 0);
        assert_eq!(iter.range().begin, Address::new(0, 0));
        assert_eq!(iter.range().end, Address::new(2, 0));
    }

    #[test]
    fn test_double_buffered_iterator() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let iter: DoubleBufferedLogIterator<u64, u64, ()> =
            DoubleBufferedLogIterator::new(range, 4096);

        assert_eq!(iter.records_scanned(), 0);
        assert!(iter.has_more());
    }

    #[test]
    fn test_double_buffered_iterator_with_device() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let device = Arc::new(NullDisk::new());
        let iter: DoubleBufferedLogIterator<u64, u64, NullDisk> =
            DoubleBufferedLogIterator::with_device(range, 4096, device);

        assert_eq!(iter.records_scanned(), 0);
        assert!(iter.has_more());
        assert_eq!(iter.range().begin, Address::new(0, 0));
    }

    #[test]
    fn test_log_page_status_values() {
        assert_eq!(LogPageStatus::default(), LogPageStatus::Uninitialized);
        assert_ne!(LogPageStatus::Ready, LogPageStatus::Pending);
        assert_ne!(LogPageStatus::Unavailable, LogPageStatus::Ready);
    }

    #[test]
    fn test_log_page_set_status() {
        let mut page = LogPage::new(4096, 4096);
        assert_eq!(page.status(), LogPageStatus::Uninitialized);

        page.set_status(LogPageStatus::Ready);
        assert_eq!(page.status(), LogPageStatus::Ready);

        page.set_status(LogPageStatus::Pending);
        assert_eq!(page.status(), LogPageStatus::Pending);

        page.set_status(LogPageStatus::Unavailable);
        assert_eq!(page.status(), LogPageStatus::Unavailable);
    }

    #[test]
    fn test_log_page_buffer_mut() {
        let mut page = LogPage::new(4096, 4096);
        let buf = page.buffer_mut().unwrap();
        assert_eq!(buf.len(), 4096);

        // Can write to the buffer
        buf[0] = 42;
        assert_eq!(page.buffer().unwrap()[0], 42);
    }

    #[test]
    fn test_log_page_update() {
        let mut page = LogPage::new(4096, 4096);

        // Update to scan from offset 100 to end of page
        let page_addr = Address::new(5, 0);
        let current = Address::new(5, 100);
        let until = Address::new(6, 0);
        page.update(page_addr, current, until);

        // until is in the next page, so until_offset should be page_size
        // This is an internal detail, but we can check has_more behavior
    }

    #[test]
    fn test_log_page_update_same_page() {
        let mut page = LogPage::new(4096, 4096);

        // Update where until is in same page
        let page_addr = Address::new(5, 0);
        let current = Address::new(5, 100);
        let until = Address::new(5, 500);
        page.update(page_addr, current, until);
    }

    #[test]
    fn test_log_page_reset() {
        let mut page = LogPage::new(4096, 4096);

        // Modify the page
        page.set_status(LogPageStatus::Ready);
        if let Some(buf) = page.buffer_mut() {
            buf[0] = 42;
        }

        // Reset
        page.reset();

        // Check status is reset
        assert_eq!(page.status(), LogPageStatus::Uninitialized);

        // Buffer should be zeroed
        assert_eq!(page.buffer().unwrap()[0], 0);
    }

    #[test]
    fn test_log_page_has_more() {
        let page = LogPage::new(4096, 4096);

        // Uninitialized page has no more records
        assert!(!page.has_more());
    }

    #[test]
    fn test_log_page_iterator_has_more() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
        let mut iter = LogPageIterator::new(range, 4096);

        assert!(iter.has_more());

        // Exhaust all pages
        while iter.next_page().is_some() {}

        assert!(!iter.has_more());
    }

    #[test]
    fn test_log_page_iterator_remaining_pages() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
        let mut iter = LogPageIterator::new(range, 4096);

        // Range from page 0 to page 3 (exclusive end)
        // Pages to scan: 0, 1, 2
        let initial = iter.remaining_pages();
        assert!(initial > 0);

        iter.next_page();
        let after_one = iter.remaining_pages();
        assert!(after_one < initial);

        // Exhaust all pages
        while iter.next_page().is_some() {}

        assert_eq!(iter.remaining_pages(), 0);
    }

    #[test]
    fn test_scan_range_debug() {
        let range = ScanRange::new(Address::new(1, 100), Address::new(5, 200));
        let debug_str = format!("{:?}", range);
        assert!(debug_str.contains("begin"));
        assert!(debug_str.contains("end"));
    }

    #[test]
    fn test_scan_range_clone_copy() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(10, 0));
        let cloned = range.clone();
        let copied = range;

        assert_eq!(range.begin, cloned.begin);
        assert_eq!(range.end, cloned.end);
        assert_eq!(range.begin, copied.begin);
        assert_eq!(range.end, copied.end);
    }

    #[test]
    fn test_log_scan_iterator_range() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

        assert_eq!(iter.range().begin, Address::new(0, 0));
        assert_eq!(iter.range().end, Address::new(5, 0));
    }

    #[test]
    fn test_log_scan_iterator_has_more() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

        assert!(iter.has_more());
    }

    #[test]
    fn test_log_scan_iterator_current_page_mut() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

        let page = iter.current_page_mut();
        assert_eq!(page.status(), LogPageStatus::Uninitialized);
    }

    #[test]
    fn test_log_scan_iterator_advance_page() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

        let result = iter.advance_page();
        assert!(result.is_some());

        let (page_addr, start_addr) = result.unwrap();
        assert_eq!(page_addr, Address::new(0, 0));
        assert_eq!(start_addr, Address::new(0, 0));
    }

    #[test]
    fn test_log_scan_iterator_finish_page() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

        iter.finish_page();
        // After finishing, page should be reset
        assert_eq!(iter.current_page_mut().status(), LogPageStatus::Uninitialized);
    }

    #[test]
    fn test_double_buffered_current_page() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let iter: DoubleBufferedLogIterator<u64, u64, ()> =
            DoubleBufferedLogIterator::new(range, 4096);

        let page = iter.current_page();
        assert_eq!(page.status(), LogPageStatus::Uninitialized);
    }

    #[test]
    fn test_double_buffered_prefetch_page() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let iter: DoubleBufferedLogIterator<u64, u64, ()> =
            DoubleBufferedLogIterator::new(range, 4096);

        let page = iter.prefetch_page();
        assert_eq!(page.status(), LogPageStatus::Uninitialized);
    }

    #[test]
    fn test_double_buffered_prefetch_page_mut() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let mut iter: DoubleBufferedLogIterator<u64, u64, ()> =
            DoubleBufferedLogIterator::new(range, 4096);

        let page = iter.prefetch_page_mut();
        page.set_status(LogPageStatus::Ready);
        assert_eq!(iter.prefetch_page().status(), LogPageStatus::Ready);
    }

    #[test]
    fn test_double_buffered_switch_pages() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let mut iter: DoubleBufferedLogIterator<u64, u64, ()> =
            DoubleBufferedLogIterator::new(range, 4096);

        // Mark current page as ready
        iter.current_page_mut().set_status(LogPageStatus::Ready);

        // Mark prefetch page as pending
        iter.prefetch_page_mut().set_status(LogPageStatus::Pending);

        // Switch pages
        iter.switch_pages();

        // Now the previously prefetch page (Pending) is current
        assert_eq!(iter.current_page().status(), LogPageStatus::Pending);
    }

    #[test]
    fn test_double_buffered_next_prefetch_address() {
        let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
        let mut iter: DoubleBufferedLogIterator<u64, u64, ()> =
            DoubleBufferedLogIterator::new(range, 4096);

        let result = iter.next_prefetch_address();
        assert!(result.is_some());

        let (page_addr, start_addr) = result.unwrap();
        assert_eq!(page_addr, Address::new(0, 0));
        assert_eq!(start_addr, Address::new(0, 0));
    }

    #[test]
    fn test_log_page_status_clone_copy_debug() {
        let status = LogPageStatus::Ready;
        let cloned = status.clone();
        let copied = status;

        assert_eq!(status, cloned);
        assert_eq!(status, copied);

        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Ready"));
    }
}
