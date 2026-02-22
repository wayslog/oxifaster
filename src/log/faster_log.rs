//! FASTER Log - High-performance persistent log
//!
//! This module provides a standalone high-performance log implementation
//! that can be used independently of the key-value store.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam::channel;

use crate::address::{Address, AtomicAddress, AtomicPageOffset, PageOffset};
use crate::device::StorageDevice;
use crate::status::Status;
use crate::utility::AlignedBuffer;

use super::format::{LogEntryHeader, LogMetadata, LogMetadataError};
use super::io::{IoExecutor, block_on_device};
use super::types::status_from_error;
pub use super::types::{
    FasterLogConfig, FasterLogOpenOptions, FasterLogSelfCheckOptions, FasterLogSelfCheckReport,
    LogError, LogErrorKind, LogStats,
};

struct FasterLogShared<D: StorageDevice> {
    config: FasterLogConfig,
    device: Arc<D>,
    pages: Vec<AlignedBuffer>,
    tail: AtomicPageOffset,
    sealed_page: AtomicU32,
    committed_until: AtomicAddress,
    flushed_until: AtomicAddress,
    begin_address: AtomicAddress,
    closed: AtomicBool,
    buffer_size: u32,
    data_offset: u64,
    metadata: Mutex<LogMetadata>,
    last_error: Mutex<Option<LogError>>,
}

enum FlushRequest {
    Commit(Address),
    Shutdown,
}

enum EntryRead {
    Data(Vec<u8>, Address),
    Padding(Address),
    End,
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
        loop {
            if self.current_address >= self.end_address {
                return None;
            }

            match self.log.read_entry_with_meta(self.current_address) {
                Ok(EntryRead::Data(data, next)) => {
                    let addr = self.current_address;
                    self.current_address = next;
                    return Some((addr, data));
                }
                Ok(EntryRead::Padding(next)) => {
                    self.current_address = next;
                }
                Ok(EntryRead::End) => return None,
                Err(error) => {
                    self.log.shared.record_error(error);
                    return None;
                }
            }
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
    shared: Arc<FasterLogShared<D>>,
    flush_tx: channel::Sender<FlushRequest>,
    flush_worker: Mutex<Option<JoinHandle<()>>>,
}

impl<D: StorageDevice> FasterLog<D> {
    /// Create (or open) a FASTER Log with default open options.
    pub fn new(config: FasterLogConfig, device: D) -> Result<Self, Status> {
        Self::open_with_options(config, device, FasterLogOpenOptions::default())
    }

    /// Open a FASTER Log with default open options.
    pub fn open(config: FasterLogConfig, device: D) -> Result<Self, Status> {
        Self::open_with_options(config, device, FasterLogOpenOptions::default())
    }

    /// Open a FASTER Log with custom open options.
    pub fn open_with_options(
        config: FasterLogConfig,
        device: D,
        options: FasterLogOpenOptions,
    ) -> Result<Self, Status> {
        let device = Arc::new(device);
        let mut pending_error: Option<LogError> = None;

        if let Err(error) = Self::validate_config(&config, &device) {
            return Err(status_from_error(&error));
        }

        let data_offset = config.page_size as u64;
        let alignment = device.alignment();
        let io_exec = IoExecutor::new().map_err(|error| {
            let log_error = LogError::new(LogErrorKind::Io, error.to_string());
            status_from_error(&log_error)
        })?;

        let mut metadata = match Self::load_metadata(&device, &io_exec, config.page_size, alignment)
        {
            Ok(Some(meta)) => Some(meta),
            Ok(None) => None,
            Err(error) => {
                pending_error = Some(error);
                None
            }
        };

        let mut recovered = false;
        if metadata.is_none() && options.recover {
            if let Ok(scan_end) =
                Self::max_address_from_device(&device, data_offset, config.page_size)
            {
                if scan_end > Address::new(0, 0) {
                    match Self::recover_scan(
                        &device,
                        &io_exec,
                        data_offset,
                        Address::new(0, 0),
                        scan_end,
                        config.page_size,
                    ) {
                        Ok(scan_result) => {
                            if scan_result.end_address > Address::new(0, 0) {
                                let mut meta = LogMetadata::new(
                                    config.page_size as u32,
                                    config.segment_size,
                                    data_offset,
                                );
                                meta.begin_address = 0;
                                meta.committed_address = u64::from(scan_result.end_address);
                                meta.tail_address = u64::from(scan_result.end_address);
                                metadata = Some(meta);
                                recovered = true;
                                if scan_result.had_corruption {
                                    if tracing::enabled!(tracing::Level::WARN) {
                                        tracing::warn!(
                                            scan_end = %scan_result.end_address,
                                            "fasterlog recovery detected corruption"
                                        );
                                    }
                                    if options.truncate_on_corruption {
                                        let truncate_size = Self::address_to_file_offset(
                                            data_offset,
                                            config.page_size,
                                            scan_result.end_address,
                                        );
                                        if let Err(error) =
                                            Self::truncate_device(&device, &io_exec, truncate_size)
                                        {
                                            if pending_error.is_none() {
                                                pending_error = Some(error);
                                            }
                                        }
                                    }
                                    if pending_error.is_none() {
                                        pending_error = Some(LogError::new(
                                            LogErrorKind::Corruption,
                                            "recovered log after truncating corrupted tail",
                                        ));
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            pending_error = Some(error);
                        }
                    }
                }
            }
        }

        if metadata.is_none() && options.create_if_missing {
            let mut meta =
                LogMetadata::new(config.page_size as u32, config.segment_size, data_offset);
            meta.begin_address = 0;
            meta.committed_address = 0;
            meta.tail_address = 0;
            metadata = Some(meta);
        }

        let mut metadata = match metadata {
            Some(meta) => meta,
            None => {
                return Err(Status::NotFound);
            }
        };

        if metadata.page_size != config.page_size as u32 {
            return Err(Status::InvalidArgument);
        }

        if metadata.data_offset != data_offset {
            return Err(Status::InvalidArgument);
        }

        if options.recover && !recovered {
            let committed = Address::from(metadata.committed_address);
            let begin = Address::from(metadata.begin_address);
            let scan_end = if committed > Address::new(0, 0) {
                committed
            } else {
                Self::max_address_from_device(&device, data_offset, config.page_size)
                    .map_err(|error| status_from_error(&error))?
            };

            match Self::recover_scan(
                &device,
                &io_exec,
                data_offset,
                begin,
                scan_end,
                config.page_size,
            ) {
                Ok(scan_result) => {
                    let mut tail = scan_result.end_address;
                    if committed < tail {
                        tail = committed;
                    }

                    if scan_result.had_corruption {
                        if tracing::enabled!(tracing::Level::WARN) {
                            tracing::warn!(
                                scan_end = %scan_result.end_address,
                                "fasterlog self-check detected corruption"
                            );
                        }
                        if !options.truncate_on_corruption {
                            return Err(Status::Corruption);
                        }
                        pending_error = Some(LogError::new(
                            LogErrorKind::Corruption,
                            "corrupted tail detected, truncated to last valid entry",
                        ));
                        let truncate_size =
                            Self::address_to_file_offset(data_offset, config.page_size, tail);
                        if let Err(error) = Self::truncate_device(&device, &io_exec, truncate_size)
                        {
                            return Err(status_from_error(&error));
                        }
                    }

                    if tail < committed {
                        metadata.committed_address = u64::from(tail);
                    }
                    metadata.tail_address = u64::from(tail);
                    if Address::from(metadata.begin_address) > tail {
                        metadata.begin_address = u64::from(tail);
                    }
                }
                Err(error) => {
                    return Err(status_from_error(&error));
                }
            }
        }

        if let Err(error) =
            Self::persist_metadata(&device, &io_exec, config.page_size, alignment, &metadata)
        {
            return Err(status_from_error(&error));
        }

        let mut pages = Vec::with_capacity(config.memory_pages as usize);
        for _ in 0..config.memory_pages {
            let buffer =
                AlignedBuffer::zeroed(alignment, config.page_size).ok_or(Status::OutOfMemory)?;
            pages.push(buffer);
        }

        let tail_address = Address::from(metadata.tail_address);
        let shared = Arc::new(FasterLogShared {
            config: config.clone(),
            device: Arc::clone(&device),
            pages,
            tail: AtomicPageOffset::from_address(tail_address),
            sealed_page: AtomicU32::new(tail_address.page()),
            committed_until: AtomicAddress::new(Address::from(metadata.committed_address)),
            flushed_until: AtomicAddress::new(Address::from(metadata.committed_address)),
            begin_address: AtomicAddress::new(Address::from(metadata.begin_address)),
            closed: AtomicBool::new(false),
            buffer_size: config.memory_pages,
            data_offset,
            metadata: Mutex::new(metadata),
            last_error: Mutex::new(pending_error),
        });

        let (flush_tx, flush_rx) = channel::unbounded();
        let worker_shared = Arc::clone(&shared);
        let flush_worker = std::thread::spawn(move || {
            let io_exec = match IoExecutor::new() {
                Ok(exec) => exec,
                Err(error) => {
                    worker_shared.record_error(LogError::new(LogErrorKind::Io, error.to_string()));
                    return;
                }
            };

            let mut shutdown = false;
            while let Ok(request) = flush_rx.recv() {
                let mut pending_commit = None;
                match request {
                    FlushRequest::Commit(addr) => pending_commit = Some(addr),
                    FlushRequest::Shutdown => shutdown = true,
                }

                while let Ok(next) = flush_rx.try_recv() {
                    match next {
                        FlushRequest::Commit(addr) => pending_commit = Some(addr),
                        FlushRequest::Shutdown => shutdown = true,
                    }
                }

                if let Some(target) = pending_commit {
                    if let Err(error) = worker_shared.flush_to(&io_exec, target) {
                        worker_shared.record_error(error);
                    }
                }

                if shutdown {
                    break;
                }
            }
        });

        Ok(Self {
            shared,
            flush_tx,
            flush_worker: Mutex::new(Some(flush_worker)),
        })
    }

    /// Run a log self-check without opening the log.
    pub fn self_check(
        config: FasterLogConfig,
        device: D,
    ) -> Result<FasterLogSelfCheckReport, Status> {
        Self::self_check_with_options(config, device, FasterLogSelfCheckOptions::default())
    }

    /// Run a log self-check with custom options (including dry-run and repair).
    pub fn self_check_with_options(
        config: FasterLogConfig,
        device: D,
        options: FasterLogSelfCheckOptions,
    ) -> Result<FasterLogSelfCheckReport, Status> {
        let device = Arc::new(device);
        if let Err(error) = Self::validate_config(&config, &device) {
            return Err(status_from_error(&error));
        }

        let data_offset = config.page_size as u64;
        let alignment = device.alignment();
        let io_exec = IoExecutor::new().map_err(|error| {
            let log_error = LogError::new(LogErrorKind::Io, error.to_string());
            status_from_error(&log_error)
        })?;

        let mut metadata_present = false;
        let mut metadata_created = false;
        let mut metadata = match Self::load_metadata(&device, &io_exec, config.page_size, alignment)
        {
            Ok(Some(meta)) => {
                metadata_present = true;
                Some(meta)
            }
            Ok(None) => None,
            Err(error) => return Err(status_from_error(&error)),
        };

        if let Some(ref meta) = metadata {
            if meta.page_size != config.page_size as u32 {
                return Err(Status::InvalidArgument);
            }
            if meta.data_offset != data_offset {
                return Err(Status::InvalidArgument);
            }
        }

        let begin = metadata
            .as_ref()
            .map(|meta| Address::from(meta.begin_address))
            .unwrap_or_else(|| Address::new(0, 0));
        let committed = metadata
            .as_ref()
            .map(|meta| Address::from(meta.committed_address))
            .unwrap_or_else(|| Address::new(0, 0));
        let scan_end = if committed > Address::new(0, 0) {
            committed
        } else {
            Self::max_address_from_device(&device, data_offset, config.page_size)
                .map_err(|error| status_from_error(&error))?
        };

        let scan_result = Self::recover_scan(
            &device,
            &io_exec,
            data_offset,
            begin,
            scan_end,
            config.page_size,
        )
        .map_err(|error| status_from_error(&error))?;

        let mut tail = scan_result.end_address;
        if committed > Address::new(0, 0) && committed < tail {
            tail = committed;
        }

        let mut truncate_to = None;
        if scan_result.had_corruption {
            truncate_to = Some(tail);
        }

        let mut repaired = false;
        if options.repair && !options.dry_run {
            if let Some(truncate_target) = truncate_to {
                let truncate_size =
                    Self::address_to_file_offset(data_offset, config.page_size, truncate_target);
                Self::truncate_device(&device, &io_exec, truncate_size)
                    .map_err(|error| status_from_error(&error))?;

                if let Some(ref mut meta) = metadata {
                    meta.committed_address = u64::from(truncate_target);
                    meta.tail_address = u64::from(truncate_target);
                    if Address::from(meta.begin_address) > truncate_target {
                        meta.begin_address = u64::from(truncate_target);
                    }
                    Self::persist_metadata(&device, &io_exec, config.page_size, alignment, meta)
                        .map_err(|error| status_from_error(&error))?;
                } else if options.create_if_missing {
                    let mut meta =
                        LogMetadata::new(config.page_size as u32, config.segment_size, data_offset);
                    meta.begin_address = 0;
                    meta.committed_address = u64::from(truncate_target);
                    meta.tail_address = u64::from(truncate_target);
                    Self::persist_metadata(&device, &io_exec, config.page_size, alignment, &meta)
                        .map_err(|error| status_from_error(&error))?;
                    metadata_created = true;
                }

                repaired = true;
                if tracing::enabled!(tracing::Level::WARN) {
                    tracing::warn!(
                        truncate_to = %truncate_target,
                        "fasterlog self-check repaired corruption"
                    );
                }
            } else if metadata.is_none() && options.create_if_missing && tail > Address::new(0, 0) {
                let mut meta =
                    LogMetadata::new(config.page_size as u32, config.segment_size, data_offset);
                meta.begin_address = 0;
                meta.committed_address = u64::from(tail);
                meta.tail_address = u64::from(tail);
                Self::persist_metadata(&device, &io_exec, config.page_size, alignment, &meta)
                    .map_err(|error| status_from_error(&error))?;
                metadata_created = true;
                repaired = true;
                if tracing::enabled!(tracing::Level::WARN) {
                    tracing::warn!(tail = %tail, "fasterlog self-check created metadata");
                }
            }
        }

        Ok(FasterLogSelfCheckReport {
            metadata_present,
            metadata_created,
            scan_end: scan_result.end_address,
            had_corruption: scan_result.had_corruption,
            truncate_to,
            repaired,
            dry_run: options.dry_run,
        })
    }

    /// Append data to the log
    ///
    /// Returns the address where the data was written.
    pub fn append(&self, data: &[u8]) -> Result<Address, Status> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Status::Aborted);
        }

        if data.is_empty() {
            self.shared.record_error(LogError::new(
                LogErrorKind::Config,
                "empty log entries are not supported",
            ));
            return Err(Status::InvalidArgument);
        }

        let entry_size = LogEntryHeader::SIZE + data.len();
        if entry_size > self.shared.config.page_size {
            self.shared.record_error(LogError::new(
                LogErrorKind::Config,
                "log entry exceeds page size",
            ));
            return Err(Status::InvalidArgument);
        }

        let page_size = self.shared.config.page_size as u64;

        loop {
            let current = self.shared.tail.load(Ordering::Acquire);
            let page = current.page();
            let offset = current.offset();

            let new_offset = offset + entry_size as u64;
            if new_offset <= page_size {
                let next = PageOffset::new(page, new_offset);
                if self
                    .shared
                    .tail
                    .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }

                let address = Address::new(page, offset as u32);
                let buffer_index = self.shared.buffer_index(page);
                let page_buf = &self.shared.pages[buffer_index];
                // Safety: the page buffer is stable for the log lifetime; concurrent writers are
                // coordinated via atomic tail reservations and non-overlapping writes.
                let slice = unsafe {
                    std::slice::from_raw_parts_mut(
                        page_buf.as_ptr() as *mut u8,
                        self.shared.config.page_size,
                    )
                };

                let header = LogEntryHeader::new(data.len() as u32, 0, data);
                if let Err(error) = header.encode(&mut slice[offset as usize..]) {
                    self.shared
                        .record_error(LogError::new(LogErrorKind::Entry, error.to_string()));
                    return Err(Status::Corruption);
                }

                let data_start = offset as usize + LogEntryHeader::SIZE;
                let data_end = data_start + data.len();
                slice[data_start..data_end].copy_from_slice(data);

                return Ok(address);
            }

            if page >= Address::MAX_PAGE {
                self.shared.record_error(LogError::new(
                    LogErrorKind::Config,
                    "log exceeded maximum addressable pages",
                ));
                return Err(Status::OutOfMemory);
            }

            let next = PageOffset::new(page + 1, 0);
            if self
                .shared
                .tail
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if let Err(error) = self.write_padding(page, offset) {
                    self.shared.record_error(error);
                    self.shared.closed.store(true, Ordering::Release);
                    return Err(Status::Corruption);
                }
                self.shared.advance_sealed_page(page + 1);
            }
        }
    }

    fn write_padding(&self, page: u32, offset: u64) -> Result<(), LogError> {
        let page_size = self.shared.config.page_size as u64;
        if offset >= page_size {
            return Ok(());
        }

        let remaining = page_size - offset;
        if remaining < LogEntryHeader::SIZE as u64 {
            return Ok(());
        }

        let payload_len = remaining as usize - LogEntryHeader::SIZE;
        let buffer_index = self.shared.buffer_index(page);
        let page_buf = &self.shared.pages[buffer_index];
        let slice = unsafe {
            std::slice::from_raw_parts_mut(
                page_buf.as_ptr() as *mut u8,
                self.shared.config.page_size,
            )
        };

        let header = LogEntryHeader::padding(payload_len as u32);
        header
            .encode(&mut slice[offset as usize..])
            .map_err(|err| LogError::new(LogErrorKind::Entry, err.to_string()))?;

        let data_start = offset as usize + LogEntryHeader::SIZE;
        let data_end = data_start + payload_len;
        slice[data_start..data_end].fill(0);
        Ok(())
    }

    /// Append multiple entries atomically
    pub fn append_batch(&self, entries: &[&[u8]]) -> Result<Vec<Address>, Status> {
        let mut addresses = Vec::with_capacity(entries.len());

        for entry in entries {
            addresses.push(self.append(entry)?);
        }

        Ok(addresses)
    }

    /// Enqueue a commit up to the current tail.
    ///
    /// This schedules a background flush but returns immediately without
    /// waiting for durability. Use [`wait_for_commit`] on the returned
    /// address, or call [`commit_wait`] for a synchronous durable commit.
    pub fn commit(&self) -> Result<Address, Status> {
        let tail = self.get_tail_address();

        loop {
            let current = self.shared.committed_until.load(Ordering::Acquire);
            if tail <= current {
                break;
            }

            if self
                .shared
                .committed_until
                .compare_exchange(current, tail, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        if self.flush_tx.send(FlushRequest::Commit(tail)).is_err() {
            self.shared
                .record_error(LogError::new(LogErrorKind::Io, "flush worker unavailable"));
            return Err(Status::IoError);
        }

        Ok(tail)
    }

    /// Commit and wait until all entries are durable on stable storage.
    ///
    /// Equivalent to calling [`commit`] followed by [`wait_for_commit`].
    pub fn commit_wait(&self) -> Result<Address, Status> {
        let addr = self.commit()?;
        self.wait_for_commit(addr)?;
        Ok(addr)
    }

    /// Wait for commit to complete up to the specified address
    pub fn wait_for_commit(&self, address: Address) -> Result<(), Status> {
        while self.shared.flushed_until.load(Ordering::Acquire) < address {
            std::thread::yield_now();
        }
        Ok(())
    }

    /// Read an entry at the given address
    pub fn read_entry(&self, address: Address) -> Option<Vec<u8>> {
        if address < self.get_begin_address() || address >= self.get_tail_address() {
            return None;
        }

        match self.read_entry_with_meta(address) {
            Ok(EntryRead::Data(data, _)) => Some(data),
            Ok(EntryRead::Padding(_)) | Ok(EntryRead::End) => None,
            Err(error) => {
                self.shared.record_error(error);
                None
            }
        }
    }

    /// Scan entries in a range
    pub fn scan(&self, start: Address, end: Address) -> LogIterator<'_, D> {
        LogIterator::new(self, start, end)
    }

    /// Scan all committed entries
    pub fn scan_all(&self) -> LogIterator<'_, D> {
        let begin = self.get_begin_address();
        let end = self.get_committed_until();
        LogIterator::new(self, begin, end)
    }

    /// Get the current tail address
    #[inline]
    pub fn get_tail_address(&self) -> Address {
        self.shared.tail.load(Ordering::Acquire).to_address()
    }

    /// Get the committed until address
    #[inline]
    pub fn get_committed_until(&self) -> Address {
        self.shared.committed_until.load(Ordering::Acquire)
    }

    /// Get the begin address
    #[inline]
    pub fn get_begin_address(&self) -> Address {
        self.shared.begin_address.load(Ordering::Acquire)
    }

    /// Truncate the log before the specified address.
    ///
    /// This removes all log entries before `before_address` and updates
    /// the log's begin address. The space may not be immediately reclaimed
    /// depending on the storage device.
    ///
    /// # Arguments
    /// - `before_address`: Entries before this address will be removed
    ///
    /// # Returns
    /// The new begin address after truncation
    ///
    /// # Errors
    /// Returns an error if:
    /// - `before_address` is beyond the committed address
    /// - Device I/O fails
    /// - Metadata update fails
    pub fn truncate_before(&self, before_address: Address) -> Result<Address, Status> {
        // Validate address
        let committed = self.shared.committed_until.load(Ordering::Acquire);
        if before_address > committed {
            return Err(Status::InvalidArgument);
        }

        let begin = self.shared.begin_address.load(Ordering::Acquire);
        if before_address <= begin {
            return Ok(begin); // Already truncated
        }

        // Update begin address
        self.shared
            .begin_address
            .store(before_address, Ordering::Release);

        // Persist updated metadata
        let io_exec = IoExecutor::new().map_err(|e| {
            self.shared
                .record_error(LogError::new(LogErrorKind::Io, e.to_string()));
            Status::IoError
        })?;

        let mut meta = self.shared.metadata.lock().map_err(|_| Status::IoError)?;
        meta.begin_address = u64::from(before_address);

        Self::persist_metadata(
            &self.shared.device,
            &io_exec,
            self.shared.config.page_size,
            self.shared.device.alignment(),
            &meta,
        )
        .map_err(|e| {
            self.shared.record_error(e.clone());
            status_from_error(&e)
        })?;

        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(
                from = %begin,
                to = %before_address,
                "log truncated"
            );
        }

        Ok(before_address)
    }

    /// Get the reclaimable space in bytes.
    ///
    /// This is an estimate of space that could be reclaimed by truncating
    /// to the current committed address.
    pub fn get_reclaimable_space(&self) -> u64 {
        let begin = self.shared.begin_address.load(Ordering::Acquire);
        let committed = self.shared.committed_until.load(Ordering::Acquire);

        if committed <= begin {
            return 0;
        }

        // Estimate based on page size
        let begin_page = begin.page();
        let committed_page = committed.page();
        let pages = committed_page.saturating_sub(begin_page);
        pages as u64 * self.shared.config.page_size as u64
    }

    /// Get the last error, if any.
    pub fn last_error(&self) -> Option<LogError> {
        self.shared
            .last_error
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Truncate the log up to the specified address
    pub fn truncate_until(&self, address: Address) -> Status {
        loop {
            let current = self.shared.begin_address.load(Ordering::Acquire);
            if address <= current {
                return Status::Ok;
            }

            if self
                .shared
                .begin_address
                .compare_exchange(current, address, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        let mut meta = match self.shared.metadata.lock() {
            Ok(guard) => guard,
            Err(_) => return Status::Aborted,
        };
        meta.begin_address = u64::from(address);
        if let Err(error) = self.persist_metadata_locked(&meta) {
            self.shared.record_error(error);
            return Status::IoError;
        }

        Status::Ok
    }

    /// Close the log
    pub fn close(&self) -> Status {
        self.shared.closed.store(true, Ordering::Release);

        if let Ok(addr) = self.commit() {
            let _ = self.wait_for_commit(addr);
        }

        self.shutdown_flush_worker();
        Status::Ok
    }

    /// Check if the log is closed
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Get log statistics
    pub fn get_stats(&self) -> LogStats {
        LogStats {
            tail_address: self.get_tail_address(),
            committed_address: self.get_committed_until(),
            begin_address: self.get_begin_address(),
            page_size: self.shared.config.page_size,
            buffer_pages: self.shared.buffer_size,
        }
    }

    fn validate_config(config: &FasterLogConfig, device: &Arc<D>) -> Result<(), LogError> {
        if config.page_size == 0 {
            return Err(LogError::new(
                LogErrorKind::Config,
                "page size must be non-zero",
            ));
        }

        if config.page_size > Address::MAX_OFFSET as usize {
            return Err(LogError::new(
                LogErrorKind::Config,
                "page size exceeds addressable offset",
            ));
        }

        let alignment = device.alignment();
        if alignment == 0 || !alignment.is_power_of_two() {
            return Err(LogError::new(
                LogErrorKind::Config,
                "device alignment must be a power of two",
            ));
        }

        if !config.page_size.is_multiple_of(alignment) {
            return Err(LogError::new(
                LogErrorKind::Config,
                "page size must be aligned to device requirements",
            ));
        }

        if config.page_size < LogEntryHeader::SIZE {
            return Err(LogError::new(
                LogErrorKind::Config,
                "page size must exceed entry header size",
            ));
        }

        Ok(())
    }

    fn load_metadata(
        device: &Arc<D>,
        io_exec: &IoExecutor,
        page_size: usize,
        alignment: usize,
    ) -> Result<Option<LogMetadata>, LogError> {
        let size = device
            .size()
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        if size < page_size as u64 {
            return Ok(None);
        }

        let mut buffer = AlignedBuffer::zeroed(alignment, page_size)
            .ok_or_else(|| LogError::new(LogErrorKind::Io, "metadata buffer allocation failed"))?;
        let read = io_exec
            .block_on(device.read(0, buffer.as_mut_slice()))
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        if read < LogMetadata::ENCODED_SIZE {
            return Ok(None);
        }

        match LogMetadata::decode(&buffer.as_slice()[..LogMetadata::ENCODED_SIZE]) {
            Ok(meta) => Ok(Some(meta)),
            Err(LogMetadataError::MagicMismatch) => Ok(None),
            Err(error) => Err(LogError::new(LogErrorKind::Metadata, error.to_string())),
        }
    }

    fn persist_metadata(
        device: &Arc<D>,
        io_exec: &IoExecutor,
        page_size: usize,
        alignment: usize,
        metadata: &LogMetadata,
    ) -> Result<(), LogError> {
        let mut buffer = AlignedBuffer::zeroed(alignment, page_size)
            .ok_or_else(|| LogError::new(LogErrorKind::Io, "metadata buffer allocation failed"))?;
        metadata
            .encode(buffer.as_mut_slice())
            .map_err(|err| LogError::new(LogErrorKind::Metadata, err.to_string()))?;

        let written = io_exec
            .block_on(device.write(0, buffer.as_slice()))
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        if written != buffer.size() {
            return Err(LogError::new(LogErrorKind::Io, "partial metadata write"));
        }

        io_exec
            .block_on(device.flush())
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;

        Ok(())
    }

    fn persist_metadata_locked(&self, metadata: &LogMetadata) -> Result<(), LogError> {
        let io_exec =
            IoExecutor::new().map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        Self::persist_metadata(
            &self.shared.device,
            &io_exec,
            self.shared.config.page_size,
            self.shared.device.alignment(),
            metadata,
        )
    }

    fn max_address_from_device(
        device: &Arc<D>,
        data_offset: u64,
        page_size: usize,
    ) -> Result<Address, LogError> {
        let size = device
            .size()
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        if size <= data_offset {
            return Ok(Address::new(0, 0));
        }

        let data_size = size - data_offset;
        let page = data_size / page_size as u64;
        let offset = data_size % page_size as u64;
        if page > Address::MAX_PAGE as u64 {
            return Err(LogError::new(
                LogErrorKind::Config,
                "log exceeds maximum addressable size",
            ));
        }
        Ok(Address::new(page as u32, offset as u32))
    }

    fn address_to_file_offset(data_offset: u64, page_size: usize, address: Address) -> u64 {
        data_offset + (address.page() as u64 * page_size as u64) + address.offset() as u64
    }

    fn truncate_device(device: &Arc<D>, io_exec: &IoExecutor, size: u64) -> Result<(), LogError> {
        io_exec
            .block_on(device.truncate(size))
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        io_exec
            .block_on(device.flush())
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        Ok(())
    }

    fn recover_scan(
        device: &Arc<D>,
        io_exec: &IoExecutor,
        data_offset: u64,
        begin: Address,
        end: Address,
        page_size: usize,
    ) -> Result<ScanResult, LogError> {
        let mut current = begin;
        let mut had_corruption = false;

        while current < end {
            let offset = current.offset() as usize;
            if offset + LogEntryHeader::SIZE > page_size {
                let next_page = current.page().saturating_add(1);
                if next_page > Address::MAX_PAGE {
                    had_corruption = true;
                    break;
                }
                current = Address::new(next_page, 0);
                continue;
            }

            let header_offset =
                data_offset + (current.page() as u64 * page_size as u64) + current.offset() as u64;
            let mut header_buf = [0u8; LogEntryHeader::SIZE];
            let read = io_exec
                .block_on(device.read(header_offset, &mut header_buf))
                .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
            if read < LogEntryHeader::SIZE {
                had_corruption = true;
                break;
            }

            let header = LogEntryHeader::decode(&header_buf)
                .map_err(|err| LogError::new(LogErrorKind::Entry, err.to_string()))?;

            if header.length == 0 && !header.is_padding() {
                break;
            }

            let entry_size = LogEntryHeader::SIZE + header.length as usize;
            if offset + entry_size > page_size {
                had_corruption = true;
                break;
            }

            let mut payload = vec![0u8; header.length as usize];
            let payload_offset = header_offset + LogEntryHeader::SIZE as u64;
            let read_payload = io_exec
                .block_on(device.read(payload_offset, &mut payload))
                .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
            if read_payload < payload.len() {
                had_corruption = true;
                break;
            }

            if !header.verify(&payload) {
                had_corruption = true;
                break;
            }

            let next = current + entry_size as u64;
            if next > end {
                had_corruption = true;
                break;
            }
            current = next;
        }

        Ok(ScanResult {
            end_address: current,
            had_corruption,
        })
    }

    fn read_entry_with_meta(&self, address: Address) -> Result<EntryRead, LogError> {
        if address < self.get_begin_address() || address >= self.get_tail_address() {
            return Ok(EntryRead::End);
        }

        let flushed = self.shared.flushed_until.load(Ordering::Acquire);
        if address < flushed {
            self.read_entry_from_device(address)
        } else {
            self.read_entry_from_memory(address)
        }
    }

    fn read_entry_from_memory(&self, address: Address) -> Result<EntryRead, LogError> {
        let buffer_index = self.shared.buffer_index(address.page());
        let slice = self.shared.pages[buffer_index].as_slice();
        let offset = address.offset() as usize;

        if offset + LogEntryHeader::SIZE > slice.len() {
            let next_page = address.page().saturating_add(1);
            if next_page > Address::MAX_PAGE {
                return Ok(EntryRead::End);
            }
            return Ok(EntryRead::Padding(Address::new(next_page, 0)));
        }

        let header = LogEntryHeader::decode(&slice[offset..offset + LogEntryHeader::SIZE])
            .map_err(|err| LogError::new(LogErrorKind::Entry, err.to_string()))?;
        if header.length == 0 && !header.is_padding() {
            return Ok(EntryRead::End);
        }

        let total = LogEntryHeader::SIZE + header.length as usize;
        if offset + total > slice.len() {
            return Err(LogError::new(
                LogErrorKind::Corruption,
                "entry length exceeds page boundary",
            ));
        }

        let data_start = offset + LogEntryHeader::SIZE;
        let data_end = data_start + header.length as usize;
        let payload = slice[data_start..data_end].to_vec();

        if !header.verify(&payload) {
            return Err(LogError::new(
                LogErrorKind::Corruption,
                "checksum mismatch for in-memory entry",
            ));
        }

        let next = address + total as u64;
        if header.is_padding() {
            Ok(EntryRead::Padding(next))
        } else {
            Ok(EntryRead::Data(payload, next))
        }
    }

    fn read_entry_from_device(&self, address: Address) -> Result<EntryRead, LogError> {
        let header_offset = self.address_to_offset(address);
        let mut header_buf = [0u8; LogEntryHeader::SIZE];
        let offset = address.offset() as usize;
        if offset + LogEntryHeader::SIZE > self.shared.config.page_size {
            let next_page = address.page().saturating_add(1);
            if next_page > Address::MAX_PAGE {
                return Ok(EntryRead::End);
            }
            return Ok(EntryRead::Padding(Address::new(next_page, 0)));
        }

        let read = block_on_device(&self.shared.device, async {
            self.shared
                .device
                .read(header_offset, &mut header_buf)
                .await
        })
        .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;

        if read < LogEntryHeader::SIZE {
            return Ok(EntryRead::End);
        }

        let header = LogEntryHeader::decode(&header_buf)
            .map_err(|err| LogError::new(LogErrorKind::Entry, err.to_string()))?;
        if header.length == 0 && !header.is_padding() {
            return Ok(EntryRead::End);
        }

        let entry_size = LogEntryHeader::SIZE + header.length as usize;
        if offset + entry_size > self.shared.config.page_size {
            return Err(LogError::new(
                LogErrorKind::Corruption,
                "entry length exceeds page boundary",
            ));
        }

        let mut payload = vec![0u8; header.length as usize];
        let payload_offset = header_offset + LogEntryHeader::SIZE as u64;
        let read_payload = block_on_device(&self.shared.device, async {
            self.shared.device.read(payload_offset, &mut payload).await
        })
        .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
        if read_payload < payload.len() {
            return Ok(EntryRead::End);
        }

        if !header.verify(&payload) {
            return Err(LogError::new(
                LogErrorKind::Corruption,
                "checksum mismatch for persisted entry",
            ));
        }

        let next = address + entry_size as u64;
        if header.is_padding() {
            Ok(EntryRead::Padding(next))
        } else {
            Ok(EntryRead::Data(payload, next))
        }
    }

    fn address_to_offset(&self, address: Address) -> u64 {
        self.shared.data_offset
            + (address.page() as u64 * self.shared.config.page_size as u64)
            + address.offset() as u64
    }

    fn shutdown_flush_worker(&self) {
        let _ = self.flush_tx.send(FlushRequest::Shutdown);
        if let Ok(mut guard) = self.flush_worker.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }
}

impl<D: StorageDevice> FasterLogShared<D> {
    fn buffer_index(&self, page: u32) -> usize {
        (page % self.buffer_size) as usize
    }

    fn advance_sealed_page(&self, page: u32) {
        let mut current = self.sealed_page.load(Ordering::Acquire);
        while page > current {
            match self.sealed_page.compare_exchange(
                current,
                page,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn record_error(&self, error: LogError) {
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = Some(error.clone());
        }
        tracing::warn!(error = %error.message, kind = ?error.kind, "fasterlog error");
    }

    fn flush_to(&self, io_exec: &IoExecutor, target: Address) -> Result<(), LogError> {
        let mut current = self.flushed_until.load(Ordering::Acquire);
        if target <= current {
            return Ok(());
        }

        let page_size = self.config.page_size as u64;
        while current < target {
            let page = current.page();
            let offset = current.offset() as usize;
            let end_offset = if page == target.page() {
                target.offset() as usize
            } else {
                self.config.page_size
            };

            if end_offset > offset {
                if page < target.page() {
                    while self.sealed_page.load(Ordering::Acquire) <= page {
                        std::thread::yield_now();
                    }
                }
                let buffer_index = self.buffer_index(page);
                let slice = self.pages[buffer_index].as_slice();
                let data = &slice[offset..end_offset];
                let file_offset = self.data_offset + page as u64 * page_size + offset as u64;
                let written = io_exec
                    .block_on(self.device.write(file_offset, data))
                    .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;
                if written != data.len() {
                    return Err(LogError::new(LogErrorKind::Io, "partial page write"));
                }
            }

            if page == target.page() {
                current = target;
            } else {
                current = Address::new(page + 1, 0);
            }
        }

        io_exec
            .block_on(self.device.flush())
            .map_err(|err| LogError::new(LogErrorKind::Io, err.to_string()))?;

        let mut meta = self
            .metadata
            .lock()
            .map_err(|_| LogError::new(LogErrorKind::Io, "metadata lock poisoned"))?;
        meta.committed_address = u64::from(target);
        if u64::from(target) > meta.tail_address {
            meta.tail_address = u64::from(target);
        }

        FasterLog::<D>::persist_metadata(
            &self.device,
            io_exec,
            self.config.page_size,
            self.device.alignment(),
            &meta,
        )?;

        self.flushed_until.store(target, Ordering::Release);
        Ok(())
    }
}

struct ScanResult {
    end_address: Address,
    had_corruption: bool,
}

// Safety: FasterLog uses atomics and internal locking for concurrent access.
unsafe impl<D: StorageDevice> Send for FasterLog<D> {}
unsafe impl<D: StorageDevice> Sync for FasterLog<D> {}

impl<D: StorageDevice> Drop for FasterLog<D> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shutdown_flush_worker();
    }
}

#[cfg(test)]
#[path = "faster_log_tests.rs"]
mod tests;
