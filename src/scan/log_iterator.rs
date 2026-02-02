//! Log iterator implementation for scanning records in the hybrid log.
//!
//! Based on C++ FASTER's log_scan.h implementation.
//!
//! The log iterator provides several modes:
//! - Single page: Reads one page at a time (simple, low memory)
//! - Double buffering: Prefetches next page while scanning current (better performance)
//! - Concurrent: Multiple threads can scan different pages simultaneously

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::address::Address;
use crate::codec::{PersistKey, PersistValue};
use crate::device::SyncStorageDevice;
use crate::record::RecordInfo;
use crate::store::record_format;
use crate::store::RecordView;
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

impl LogPageStatus {
    /// Get the status as a string
    pub const fn as_str(&self) -> &'static str {
        match self {
            LogPageStatus::Uninitialized => "Uninitialized",
            LogPageStatus::Ready => "Ready",
            LogPageStatus::Pending => "Pending",
            LogPageStatus::Unavailable => "Unavailable",
        }
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
    /// Returns a zero-copy record view and its address, or None if no more records.
    ///
    /// The returned `RecordView` borrows from the page buffer; it is only valid until the next
    /// `get_next_view` call or until the page buffer is reused.
    pub fn get_next_view<'a, K, V>(&'a mut self) -> Option<(Address, RecordView<'a>)>
    where
        K: PersistKey,
        V: PersistValue,
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

        const U32_BYTES: usize = std::mem::size_of::<u32>();
        const VARLEN_LENGTHS_SIZE: usize = 2 * U32_BYTES;
        let record_info_size = std::mem::size_of::<RecordInfo>();
        let varlen_header_size = record_info_size + VARLEN_LENGTHS_SIZE;

        while self.current_offset < self.until_offset {
            // Get record at current offset.
            let offset = self.current_offset as usize;
            let remaining = (self.until_offset as usize).saturating_sub(offset);
            if remaining < record_info_size {
                self.current_offset = self.until_offset;
                return None;
            }

            let control = u64::from_le_bytes(
                slice
                    .get(offset..offset + record_info_size)?
                    .try_into()
                    .ok()?,
            );

            // Check for null record (indicates end of valid data).
            if control == 0 {
                self.current_offset = self.until_offset; // Mark as exhausted
                return None;
            }

            // Calculate record address.
            let record_addr = Address::new(self.page_address.page(), self.current_offset);

            let info = unsafe { record_format::record_info_at(slice.as_ptr().add(offset)) };
            if info.is_invalid() {
                // Invalid records indicate an in-progress append. Skip them without decoding
                // varlen payloads. For varlen records we only skip if the length header is
                // well-formed and fits within this page buffer; otherwise treat as end-of-data.
                let alloc_len = if record_format::is_fixed_record::<K, V>() {
                    record_format::fixed_alloc_len::<K, V>()
                } else {
                    if remaining < varlen_header_size {
                        self.current_offset = self.until_offset;
                        return None;
                    }

                    let len_bytes =
                        slice.get(offset + record_info_size..offset + varlen_header_size)?;
                    let key_len =
                        u32::from_le_bytes(len_bytes[0..U32_BYTES].try_into().ok()?) as usize;
                    let value_len = u32::from_le_bytes(
                        len_bytes[U32_BYTES..VARLEN_LENGTHS_SIZE].try_into().ok()?,
                    ) as usize;

                    let total_disk_len = record_format::varlen_disk_len(key_len, value_len);
                    if total_disk_len > remaining {
                        self.current_offset = self.until_offset;
                        return None;
                    }

                    record_format::varlen_alloc_len(key_len, value_len)
                };

                self.current_offset = self.current_offset.saturating_add(alloc_len as u32);
                continue;
            }

            let view = match unsafe {
                record_format::record_view_from_memory::<K, V>(
                    record_addr,
                    slice.as_ptr().add(offset),
                    remaining,
                )
            } {
                Ok(v) => v,
                Err(_) => {
                    // Treat corruption as end-of-data for this page to avoid spinning.
                    self.current_offset = self.until_offset;
                    return None;
                }
            };

            // Advance to next record.
            let record_alloc_len = if record_format::is_fixed_record::<K, V>() {
                record_format::fixed_alloc_len::<K, V>()
            } else {
                record_format::varlen_alloc_len(view.key_bytes().len(), view.value_len())
            };
            self.current_offset = self.current_offset.saturating_add(record_alloc_len as u32);

            return Some((record_addr, view));
        }

        None
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
    K: PersistKey,
    V: PersistValue,
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
    K: PersistKey,
    V: PersistValue,
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
    pub fn next_record(&mut self) -> Option<(Address, RecordView<'_>)> {
        let result = self.current_page.get_next_view::<K, V>();
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
    K: PersistKey,
    V: PersistValue,
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
    pub fn next_record(&mut self) -> Option<(Address, RecordView<'_>)> {
        let result = self.current_page.get_next_view::<K, V>();
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
        F: FnMut(Address, RecordView<'_>) -> bool,
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
            while let Some((addr, record)) = self.next_record() {
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
    K: PersistKey,
    V: PersistValue,
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
    K: PersistKey,
    V: PersistValue,
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
    pub fn next_record(&mut self) -> Option<(Address, RecordView<'_>)> {
        let result = self.pages[self.current_idx].get_next_view::<K, V>();
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
    K: PersistKey,
    V: PersistValue,
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
    pub fn next_record(&mut self) -> Option<(Address, RecordView<'_>)> {
        let result = self.pages[self.current_idx].get_next_view::<K, V>();
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
        F: FnMut(Address, RecordView<'_>) -> bool,
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
            while let Some((addr, record)) = self.next_record() {
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

/// Thread-safe concurrent page iterator for parallel log scanning.
///
/// Based on C++ FASTER's ConcurrentLogPageIterator.
/// Multiple threads can call `get_next_page()` to atomically claim pages to scan.
pub struct ConcurrentLogScanIterator {
    /// Scan range
    range: ScanRange,
    /// Page size
    page_size: usize,
    /// Next page index to process (atomic for thread-safe distribution)
    next_page: AtomicU64,
    /// Total number of pages
    total_pages: u64,
    /// Whether scanning is complete
    completed: AtomicBool,
}

impl ConcurrentLogScanIterator {
    /// Create a new concurrent log scan iterator
    pub fn new(range: ScanRange, page_size: usize) -> Self {
        let total_pages = if range.is_empty() {
            0
        } else {
            (range.end.page() as u64).saturating_sub(range.begin.page() as u64) + 1
        };
        Self {
            range,
            page_size,
            next_page: AtomicU64::new(0),
            total_pages,
            completed: AtomicBool::new(false),
        }
    }

    /// Get the next page to process.
    /// Returns Some((page_index, page_begin_address, page_end_address)) or None if all pages claimed.
    pub fn get_next_page(&self) -> Option<(u64, Address, Address)> {
        let page_idx = self.next_page.fetch_add(1, Ordering::AcqRel);
        if page_idx >= self.total_pages {
            self.completed.store(true, Ordering::Release);
            return None;
        }

        let page_number = self.range.begin.page() + page_idx as u32;
        let page_begin = if page_idx == 0 {
            self.range.begin
        } else {
            Address::new(page_number, 0)
        };
        let page_end = if page_idx == self.total_pages - 1 {
            self.range.end
        } else {
            Address::new(page_number + 1, 0)
        };

        Some((page_idx, page_begin, page_end))
    }

    /// Check if all pages have been distributed
    pub fn is_complete(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Get progress as (pages_distributed, total_pages)
    pub fn progress(&self) -> (u64, u64) {
        let distributed = self.next_page.load(Ordering::Acquire).min(self.total_pages);
        (distributed, self.total_pages)
    }

    /// Get the scan range
    pub fn range(&self) -> &ScanRange {
        &self.range
    }

    /// Get the total number of pages
    pub fn total_pages(&self) -> u64 {
        self.total_pages
    }

    /// Get the page size
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[cfg(test)]
mod tests;
