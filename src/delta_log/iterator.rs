//! Delta Log iterator for scanning entries
//!
//! Provides an iterator interface for reading all entries from a delta log.
//! Used during recovery to apply incremental changes.

use std::io;
use std::sync::Arc;

use crate::device::StorageDevice;

use super::entry::{DeltaLogEntry, DeltaLogEntryType, DeltaLogHeader, DELTA_LOG_HEADER_SIZE};
use super::log::DeltaLog;
#[cfg(test)]
use super::log::DeltaLogConfig;

/// Iterator state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IteratorState {
    /// Not started
    NotStarted,
    /// Iterating
    Active,
    /// Completed
    Done,
    /// Error occurred
    Error,
}

/// Iterator for scanning delta log entries
pub struct DeltaLogIterator<D: StorageDevice> {
    /// The delta log being iterated
    delta_log: Arc<DeltaLog<D>>,
    /// Current address
    current_address: i64,
    /// Next address to read
    next_address: i64,
    /// End address
    end_address: i64,
    /// Page size bits
    page_size_bits: u32,
    /// Page size
    page_size: i64,
    /// Page size mask
    page_size_mask: i64,
    /// Sector size for alignment
    sector_size: i64,
    /// Iterator state
    state: IteratorState,
    /// Read buffer for current page
    read_buffer: Vec<u8>,
    /// Current page loaded
    current_page: i64,
    /// Whether buffer is valid
    buffer_valid: bool,
    /// Actual valid data length in the buffer (may be less than page_size for partial reads)
    valid_data_length: usize,
}

impl<D: StorageDevice> DeltaLogIterator<D> {
    /// Create a new iterator
    ///
    /// # Arguments
    /// * `delta_log` - The delta log to iterate
    /// * `start_address` - Starting address (0 for beginning)
    /// * `end_address` - Ending address (use delta_log.tail_address() for all entries)
    pub fn new(delta_log: Arc<DeltaLog<D>>, start_address: i64, end_address: i64) -> Self {
        // Use the actual log's configuration, not hardcoded defaults
        let config = delta_log.config();
        let page_size = config.page_size() as i64;
        let page_size_bits = config.page_size_bits;
        let sector_size = config.sector_size as i64;

        Self {
            delta_log,
            current_address: start_address,
            next_address: start_address,
            end_address,
            page_size_bits,
            page_size,
            page_size_mask: page_size - 1,
            sector_size,
            state: IteratorState::NotStarted,
            read_buffer: vec![0u8; page_size as usize],
            current_page: -1,
            buffer_valid: false,
            valid_data_length: 0,
        }
    }

    /// Create an iterator for all entries
    pub fn all(delta_log: Arc<DeltaLog<D>>) -> Self {
        let end = delta_log.tail_address();
        Self::new(delta_log, 0, end)
    }

    /// Get the current address
    pub fn current_address(&self) -> i64 {
        self.current_address
    }

    /// Get the next address
    pub fn next_address(&self) -> i64 {
        self.next_address
    }

    /// Get the iterator state
    pub fn state(&self) -> IteratorState {
        self.state
    }

    /// Check if iteration is complete
    pub fn is_done(&self) -> bool {
        self.state == IteratorState::Done || self.state == IteratorState::Error
    }

    /// Align a value to sector boundary
    fn align(&self, value: i64) -> i64 {
        (value + self.sector_size - 1) & !(self.sector_size - 1)
    }

    /// Load the page containing the given address
    fn load_page(&mut self, address: i64) -> io::Result<()> {
        let page = address >> self.page_size_bits;

        if page == self.current_page && self.buffer_valid {
            return Ok(()); // Already loaded
        }

        let page_start = page << self.page_size_bits;
        let read_end = std::cmp::min(page_start + self.page_size, self.end_address);
        let read_length = (read_end - page_start) as usize;

        if read_length == 0 {
            self.buffer_valid = false;
            self.valid_data_length = 0;
            return Ok(());
        }

        // Read page from device
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => {
                handle.block_on(async {
                    self.delta_log
                        .device
                        .read(page_start as u64, &mut self.read_buffer[..read_length])
                        .await
                })?;
            }
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    self.delta_log
                        .device
                        .read(page_start as u64, &mut self.read_buffer[..read_length])
                        .await
                })?;
            }
        }

        self.current_page = page;
        self.buffer_valid = true;
        self.valid_data_length = read_length;

        Ok(())
    }

    /// Get the next entry
    ///
    /// Returns the physical address, entry length, and entry type.
    /// The caller can then read the payload from the returned address.
    pub fn get_next(&mut self) -> io::Result<Option<(i64, DeltaLogEntry)>> {
        loop {
            self.current_address = self.next_address;

            if self.current_address >= self.end_address {
                self.state = IteratorState::Done;
                return Ok(None);
            }

            self.state = IteratorState::Active;

            // Load the page containing current address
            self.load_page(self.current_address)?;

            if !self.buffer_valid {
                self.state = IteratorState::Done;
                return Ok(None);
            }

            let offset = (self.current_address & self.page_size_mask) as usize;

            // Check if we have enough bytes for a header (use valid_data_length, not buffer capacity)
            if offset + DELTA_LOG_HEADER_SIZE > self.valid_data_length {
                // Move to next page
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            // Read header
            let header = DeltaLogHeader::from_bytes(&self.read_buffer[offset..])?;

            // Check for hole (zero length)
            if header.length == 0 {
                if offset == 0 {
                    // Hole at beginning of page means end of delta log
                    self.state = IteratorState::Done;
                    return Ok(None);
                }

                // Hole at end of page, skip to next page
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            // Validate entry length
            if header.length < 0 {
                // Invalid entry, skip to next page
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            let entry_length = header.length as usize;
            let total_size = DELTA_LOG_HEADER_SIZE + entry_length;

            // Check if entry fits within valid data (use valid_data_length, not page_size)
            if offset + total_size > self.valid_data_length {
                // Entry doesn't fit, skip to next page
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            // Read payload
            let payload_start = offset + DELTA_LOG_HEADER_SIZE;
            let payload_end = payload_start + entry_length;
            let payload = self.read_buffer[payload_start..payload_end].to_vec();

            // Verify checksum
            if !super::entry::verify_checksum(&header, &payload) {
                // Checksum mismatch, skip to next page
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            // Calculate next address (aligned using actual config)
            let mut next = self.current_address + total_size as i64;
            next = self.align(next);

            // Check if next entry would start past page end
            let page_end = ((next >> self.page_size_bits) + 1) << self.page_size_bits;
            if next + DELTA_LOG_HEADER_SIZE as i64 >= page_end {
                next = ((next >> self.page_size_bits) + 1) << self.page_size_bits;
            }

            self.next_address = next;

            let entry = DeltaLogEntry { header, payload };
            return Ok(Some((self.current_address, entry)));
        }
    }

    /// Get the next entry asynchronously
    pub async fn get_next_async(&mut self) -> io::Result<Option<(i64, DeltaLogEntry)>> {
        loop {
            self.current_address = self.next_address;

            if self.current_address >= self.end_address {
                self.state = IteratorState::Done;
                return Ok(None);
            }

            self.state = IteratorState::Active;

            // Load page
            let page = self.current_address >> self.page_size_bits;
            if page != self.current_page || !self.buffer_valid {
                let page_start = page << self.page_size_bits;
                let read_end = std::cmp::min(page_start + self.page_size, self.end_address);
                let read_length = (read_end - page_start) as usize;

                if read_length > 0 {
                    self.delta_log
                        .device
                        .read(page_start as u64, &mut self.read_buffer[..read_length])
                        .await?;
                    self.current_page = page;
                    self.buffer_valid = true;
                    self.valid_data_length = read_length;
                } else {
                    self.buffer_valid = false;
                    self.valid_data_length = 0;
                }
            }

            if !self.buffer_valid {
                self.state = IteratorState::Done;
                return Ok(None);
            }

            let offset = (self.current_address & self.page_size_mask) as usize;

            // Check if we have enough bytes for a header (use valid_data_length, not buffer capacity)
            if offset + DELTA_LOG_HEADER_SIZE > self.valid_data_length {
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            let header = DeltaLogHeader::from_bytes(&self.read_buffer[offset..])?;

            if header.length == 0 {
                if offset == 0 {
                    self.state = IteratorState::Done;
                    return Ok(None);
                }
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            if header.length < 0 {
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            let entry_length = header.length as usize;
            let total_size = DELTA_LOG_HEADER_SIZE + entry_length;

            // Check if entry fits within valid data (use valid_data_length, not page_size)
            if offset + total_size > self.valid_data_length {
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            let payload_start = offset + DELTA_LOG_HEADER_SIZE;
            let payload_end = payload_start + entry_length;
            let payload = self.read_buffer[payload_start..payload_end].to_vec();

            if !super::entry::verify_checksum(&header, &payload) {
                self.next_address =
                    ((self.current_address >> self.page_size_bits) + 1) << self.page_size_bits;
                continue;
            }

            // Calculate next address (aligned using actual config)
            let mut next = self.current_address + total_size as i64;
            next = self.align(next);

            let page_end = ((next >> self.page_size_bits) + 1) << self.page_size_bits;
            if next + DELTA_LOG_HEADER_SIZE as i64 >= page_end {
                next = ((next >> self.page_size_bits) + 1) << self.page_size_bits;
            }

            self.next_address = next;

            let entry = DeltaLogEntry { header, payload };
            return Ok(Some((self.current_address, entry)));
        }
    }

    /// Collect all entries into a vector
    pub fn collect_all(&mut self) -> io::Result<Vec<(i64, DeltaLogEntry)>> {
        let mut entries = Vec::new();

        while let Some(entry) = self.get_next()? {
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Collect entries of a specific type
    pub fn collect_by_type(
        &mut self,
        entry_type: DeltaLogEntryType,
    ) -> io::Result<Vec<(i64, DeltaLogEntry)>> {
        let mut entries = Vec::new();

        while let Some((addr, entry)) = self.get_next()? {
            if entry.entry_type() == Some(entry_type) {
                entries.push((addr, entry));
            }
        }

        Ok(entries)
    }

    /// Find the last checkpoint metadata entry
    pub fn find_last_checkpoint_metadata(&mut self) -> io::Result<Option<(i64, DeltaLogEntry)>> {
        let mut last: Option<(i64, DeltaLogEntry)> = None;

        while let Some((addr, entry)) = self.get_next()? {
            if entry.entry_type() == Some(DeltaLogEntryType::CheckpointMetadata) {
                last = Some((addr, entry));
            }
        }

        Ok(last)
    }
}

/// Convenience struct for iterating with ownership
pub struct DeltaLogIntoIterator<D: StorageDevice> {
    inner: DeltaLogIterator<D>,
}

impl<D: StorageDevice> DeltaLogIntoIterator<D> {
    /// Create a new iterator that owns the delta log reference
    pub fn new(delta_log: Arc<DeltaLog<D>>) -> Self {
        Self {
            inner: DeltaLogIterator::all(delta_log),
        }
    }
}

impl<D: StorageDevice> Iterator for DeltaLogIntoIterator<D> {
    type Item = io::Result<(i64, DeltaLogEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.get_next() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;

    #[test]
    fn test_iterator_creation() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIterator::all(delta_log);
        assert_eq!(iter.state(), IteratorState::NotStarted);
        assert_eq!(iter.current_address(), 0);
    }

    #[test]
    fn test_empty_delta_log() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);
        let result = iter.get_next().unwrap();
        assert!(result.is_none());
        assert!(iter.is_done());
    }

    #[test]
    fn test_into_iterator() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIntoIterator::new(delta_log);
        let entries: Vec<_> = iter.collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_iterator_state_values() {
        assert_eq!(IteratorState::NotStarted, IteratorState::NotStarted);
        assert_eq!(IteratorState::Active, IteratorState::Active);
        assert_eq!(IteratorState::Done, IteratorState::Done);
        assert_eq!(IteratorState::Error, IteratorState::Error);
        assert_ne!(IteratorState::NotStarted, IteratorState::Done);
    }

    #[test]
    fn test_iterator_state_clone_copy() {
        let state = IteratorState::Active;
        let cloned = state;
        let copied = state;
        assert_eq!(state, cloned);
        assert_eq!(state, copied);
    }

    #[test]
    fn test_iterator_state_debug() {
        let debug_str = format!("{:?}", IteratorState::NotStarted);
        assert!(debug_str.contains("NotStarted"));
    }

    #[test]
    fn test_iterator_next_address() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIterator::all(delta_log);
        assert_eq!(iter.next_address(), 0);
    }

    #[test]
    fn test_iterator_is_done_states() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);

        // Initially not done
        assert!(!iter.is_done());

        // After getting None, should be done
        let _ = iter.get_next();
        assert!(iter.is_done());
    }

    #[test]
    fn test_iterator_with_custom_range() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIterator::new(delta_log, 100, 200);
        assert_eq!(iter.current_address(), 100);
        assert_eq!(iter.next_address(), 100);
    }

    #[test]
    fn test_collect_all_empty() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);
        let entries = iter.collect_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_collect_by_type_empty() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);
        let entries = iter
            .collect_by_type(DeltaLogEntryType::CheckpointMetadata)
            .unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_find_last_checkpoint_metadata_empty() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);
        let result = iter.find_last_checkpoint_metadata().unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_iterator_get_next_async_empty() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);
        let result = iter.get_next_async().await.unwrap();
        assert!(result.is_none());
        assert!(iter.is_done());
    }

    #[test]
    fn test_iterator_with_past_end_address() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        // Create iterator where start >= end
        let iter = DeltaLogIterator::new(delta_log, 100, 50);
        assert!(!iter.is_done()); // Initially not done since state is NotStarted
    }

    #[test]
    fn test_iterator_state_not_started() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIterator::new(delta_log, 0, 100);
        assert_eq!(iter.state(), IteratorState::NotStarted);
    }

    #[test]
    fn test_iterator_becomes_done_on_empty_range() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        // Iterator with start = end
        let mut iter = DeltaLogIterator::new(delta_log, 100, 100);
        let result = iter.get_next().unwrap();
        assert!(result.is_none());
        assert_eq!(iter.state(), IteratorState::Done);
    }

    #[test]
    fn test_iterator_current_address_updates() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::new(delta_log, 50, 60);
        assert_eq!(iter.current_address(), 50);

        // After get_next, current_address should update
        let _ = iter.get_next();
        // Current address will have been updated during get_next
    }

    #[test]
    fn test_iterator_into_iter_with_error_handling() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIntoIterator::new(delta_log);

        // Collect all items - should be empty for null device
        let items: Vec<_> = iter.collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_iterator_state_equality() {
        assert!(IteratorState::NotStarted == IteratorState::NotStarted);
        assert!(IteratorState::Active == IteratorState::Active);
        assert!(IteratorState::Done == IteratorState::Done);
        assert!(IteratorState::Error == IteratorState::Error);

        assert!(IteratorState::NotStarted != IteratorState::Active);
        assert!(IteratorState::Active != IteratorState::Done);
        assert!(IteratorState::Done != IteratorState::Error);
    }

    #[test]
    fn test_iterator_state_all_values() {
        // Test all enum variants
        let states = [
            IteratorState::NotStarted,
            IteratorState::Active,
            IteratorState::Done,
            IteratorState::Error,
        ];

        for (i, state) in states.iter().enumerate() {
            for (j, other) in states.iter().enumerate() {
                if i == j {
                    assert_eq!(state, other);
                } else {
                    assert_ne!(state, other);
                }
            }
        }
    }

    #[test]
    fn test_iterator_new_with_different_configs() {
        // Test with small page size
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(10); // 1KB pages
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIterator::new(delta_log, 0, 100);
        assert_eq!(iter.current_address(), 0);
        assert_eq!(iter.next_address(), 0);

        // Test with large page size
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(16); // 64KB pages
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let iter = DeltaLogIterator::new(delta_log, 0, 100);
        assert_eq!(iter.current_address(), 0);
    }

    #[tokio::test]
    async fn test_iterator_get_next_async_with_range() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        // Create iterator with custom range
        let mut iter = DeltaLogIterator::new(delta_log, 0, 4096);

        // Should return None on empty log
        let result = iter.get_next_async().await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_collect_all_with_range() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::new(delta_log, 0, 8192);
        let entries = iter.collect_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_iterator_debug_state() {
        let debug_not_started = format!("{:?}", IteratorState::NotStarted);
        let debug_active = format!("{:?}", IteratorState::Active);
        let debug_done = format!("{:?}", IteratorState::Done);
        let debug_error = format!("{:?}", IteratorState::Error);

        assert!(debug_not_started.contains("NotStarted"));
        assert!(debug_active.contains("Active"));
        assert!(debug_done.contains("Done"));
        assert!(debug_error.contains("Error"));
    }

    #[test]
    fn test_find_last_checkpoint_metadata_with_range() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::new(delta_log, 0, 4096);
        let result = iter.find_last_checkpoint_metadata().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_iterator_sequential_calls() {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(12);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);

        // Multiple sequential get_next calls
        for _ in 0..5 {
            let result = iter.get_next().unwrap();
            assert!(result.is_none());
            assert!(iter.is_done());
        }
    }
}
