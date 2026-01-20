use std::ptr;
use std::sync::Arc;
use std::time::Instant;

use crate::address::Address;
use crate::codec::KeyCodec;
use crate::codec::PersistKey;
use crate::codec::PersistValue;
use crate::compaction::{
    AutoCompactionConfig, AutoCompactionHandle, AutoCompactionTarget, CompactionConfig,
    CompactionResult, CompactionStats,
};
use crate::device::StorageDevice;
use crate::index::KeyHash;
use crate::record::RecordInfo;
use crate::status::Status;
use crate::store::record_format;

use super::FasterKv;

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Simple compaction: shift log begin address and garbage collect index
    pub fn compact(&self, until_address: Address) -> Status {
        // `compact` reclaims space by advancing `begin_address`, not `head_address`.
        // `head_address` represents the on-disk boundary; advancing it affects the `Pending`
        // read-back behavior (see `flush_and_shift_head`).
        //
        // To ensure the reclaimed range is durable (e.g. for diagnostics/read-back), flush first.
        if let Err(_e) = unsafe { self.hlog().flush_until(until_address) } {
            return Status::Corruption;
        }

        // Update begin address
        // SAFETY: shift_begin_address is protected by epoch
        unsafe { self.hlog_mut().shift_begin_address(until_address) };

        // Garbage collect hash index
        self.hash_index.garbage_collect(until_address);

        Status::Ok
    }

    /// Flush data up to the given address and mark it as on-disk (advance `head_address`
    /// without index reclamation).
    ///
    /// This is key to a FASTER-style `Pending + complete_pending` flow:
    /// when `address < head_address`, `read()` returns `Status::Pending`. The caller drives I/O
    /// completion via `complete_pending()` and then retries `read()` to succeed.
    pub fn flush_and_shift_head(&self, new_head: Address) -> Status {
        if let Err(_e) = unsafe { self.hlog().flush_until(new_head) } {
            return Status::Corruption;
        }

        // SAFETY: shift_head_address is protected by epoch
        unsafe { self.hlog_mut().shift_head_address(new_head) };
        Status::Ok
    }

    // ============ Compaction API ============

    /// Get the compactor configuration
    pub fn compaction_config(&self) -> &CompactionConfig {
        self.compactor.config()
    }

    /// Check if compaction is currently in progress
    pub fn is_compaction_in_progress(&self) -> bool {
        self.compactor.is_in_progress()
    }

    /// Perform log compaction
    ///
    /// This scans records from begin_address up to the safe head address,
    /// moves live records to the tail, and shifts the begin address.
    ///
    /// # Returns
    /// CompactionResult with status and statistics
    pub fn log_compact(&self) -> CompactionResult {
        self.log_compact_until(None)
    }

    /// Perform log compaction up to a specific address
    ///
    /// # Arguments
    /// * `target_address` - Optional target address to compact up to
    ///
    /// # Returns
    /// CompactionResult with status and statistics
    pub fn log_compact_until(&self, target_address: Option<Address>) -> CompactionResult {
        // Try to start compaction
        if let Err(status) = self.compactor.try_start() {
            return CompactionResult::failure(status);
        }

        if self.stats_collector.is_enabled() {
            self.stats_collector
                .store_stats
                .operational
                .record_compaction_started();
        }
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(target_address = ?target_address, "compaction start");
        }

        let start_time = Instant::now();
        let result = (|| -> CompactionResult {
            // SAFETY: These are read-only accesses
            let begin_address = unsafe { self.hlog().get_begin_address() };
            let head_address = unsafe { self.hlog().get_head_address() };

            // Calculate scan range
            let scan_range = match self.compactor.calculate_scan_range(
                begin_address,
                head_address,
                target_address,
            ) {
                Some(range) => range,
                None => {
                    self.compactor.complete();
                    return CompactionResult::success(begin_address, CompactionStats::default());
                }
            };

            // Create compaction context
            let _context = self.compactor.create_context(scan_range);

            // Track statistics
            let mut stats = CompactionStats::default();
            let mut current_address = scan_range.begin;
            // Start with the assumption we can reclaim everything up to scan_range.end
            // This will be adjusted if any record fails to copy
            let mut new_begin_address = scan_range.end;
            // Track if we encountered any copy failures - if so, we may need to stop early
            let mut copy_failed = false;

            // Scan records in the range and copy live records to tail
            while current_address < scan_range.end {
                // SAFETY: Address is within the valid range
                let record_ptr = unsafe { self.hlog().get(current_address) };

                if let Some(ptr) = record_ptr {
                    let view = match unsafe {
                        let page_size = self.hlog().page_size();
                        let limit = page_size.saturating_sub(current_address.offset() as usize);
                        record_format::record_view_from_memory::<K, V>(
                            current_address,
                            ptr.as_ptr(),
                            limit,
                        )
                    } {
                        Ok(v) => v,
                        Err(status) => {
                            self.compactor.complete();
                            return CompactionResult::failure(status);
                        }
                    };

                    let info = unsafe { record_format::record_info_at(ptr.as_ptr()) };
                    let record_alloc_len = if record_format::is_fixed_record::<K, V>() {
                        record_format::fixed_alloc_len::<K, V>()
                    } else {
                        record_format::varlen_alloc_len(view.key_bytes().len(), view.value_len())
                    } as u64;

                    if info.is_invalid() {
                        current_address += record_alloc_len;
                        continue;
                    }

                    stats.records_scanned += 1;
                    stats.bytes_scanned += record_alloc_len;

                    // Check if this is a tombstone
                    let is_tombstone = view.is_tombstone();
                    if is_tombstone {
                        stats.tombstones_found += 1;
                    }

                    // Get the key and check if this record is the latest version
                    let hash =
                        KeyHash::new(<K as PersistKey>::Codec::hash_encoded(view.key_bytes()));
                    let index_result = self.hash_index.find_entry(hash);

                    if index_result.found() {
                        let index_address = index_result.entry.address();

                        // Check if record should be compacted (moved to tail)
                        if self.compactor.should_compact_record(
                            current_address,
                            index_address,
                            is_tombstone,
                        ) {
                            // This record is still live - MUST copy to tail before reclaiming
                            if !is_tombstone {
                                // Copy live record to the tail of the log
                                let value_bytes = match view.value_bytes() {
                                    Some(b) => b,
                                    None => {
                                        self.compactor.complete();
                                        return CompactionResult::failure(Status::Corruption);
                                    }
                                };
                                match self.copy_record_to_tail(view.key_bytes(), value_bytes, hash)
                                {
                                    Ok(new_addr) => {
                                        // Successfully copied - update index to point to new location
                                        // Use CAS to atomically update the index entry
                                        let update_result = self.hash_index.try_update_address(
                                            hash,
                                            current_address,
                                            new_addr,
                                        );

                                        if update_result == Status::Ok {
                                            // Index successfully updated to point to new location
                                            stats.records_compacted += 1;
                                            stats.bytes_compacted += record_alloc_len;
                                        } else {
                                            // CAS failed - index was modified concurrently
                                            // CRITICAL: We copied the record but couldn't update the index.
                                            // The index might still point to the old address, OR another
                                            // thread might have updated it to point elsewhere.
                                            //
                                            // For safety, we must preserve the original record because:
                                            // - If index still points to old address, reclaiming would corrupt data
                                            // - The new copy at new_addr becomes a "garbage" record that will
                                            //   eventually be compacted away
                                            //
                                            // This is the conservative approach - we lose some space but
                                            // guarantee data integrity.
                                            if !copy_failed {
                                                new_begin_address = current_address;
                                                copy_failed = true;
                                            }
                                            stats.records_skipped += 1;
                                        }
                                    }
                                    Err(_) => {
                                        // Failed to copy - cannot reclaim this record
                                        // CRITICAL: Adjust new_begin_address to preserve this record
                                        //
                                        // We only need to record the FIRST failure's address because:
                                        // 1. Records are scanned in ascending address order
                                        // 2. shift_begin_address(X) only reclaims addresses < X
                                        // 3. All addresses >= X (including any later failures) are
                                        //    automatically preserved
                                        //
                                        // Example: if failures occur at addresses 140 and 160,
                                        // setting new_begin_address=140 preserves BOTH records
                                        // because shift_begin_address(140) keeps everything >= 140
                                        if !copy_failed {
                                            new_begin_address = current_address;
                                            copy_failed = true;
                                        }
                                        stats.records_skipped += 1;
                                    }
                                }
                            } else {
                                // Tombstone that is still the latest - can be reclaimed
                                // (tombstones don't need to be preserved during compaction)
                                stats.tombstones_found += 1;
                                stats.bytes_reclaimed += record_alloc_len;
                            }
                        } else {
                            // Record is obsolete (superseded by newer version) - can be reclaimed
                            stats.records_skipped += 1;
                            stats.bytes_reclaimed += record_alloc_len;
                        }
                    } else {
                        // Key not in index - record can be reclaimed
                        stats.records_skipped += 1;
                        stats.bytes_reclaimed += record_alloc_len;
                    }

                    // Move to next record
                    current_address =
                        Address::from_control(current_address.control() + record_alloc_len);
                } else {
                    // No more records in this page, move to next page
                    let page_size = unsafe { self.hlog().page_size() } as u64;
                    let next_page = (current_address.control() / page_size + 1) * page_size;
                    current_address = Address::from_control(next_page);
                }
            }

            // Now safe to shift begin address - all live records before new_begin_address
            // have either been copied or the address has been adjusted to preserve them
            unsafe { self.hlog_mut().shift_begin_address(new_begin_address) };

            // Garbage collect index entries pointing to reclaimed region
            self.hash_index.garbage_collect(new_begin_address);

            stats.duration_ms = start_time.elapsed().as_millis() as u64;

            self.compactor.complete();
            CompactionResult::success(new_begin_address, stats)
        })();

        if self.stats_collector.is_enabled() {
            if result.status == Status::Ok {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_compaction_completed();
            } else {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_compaction_failed();
            }
        }
        if tracing::enabled!(tracing::Level::INFO) {
            let duration_ms = start_time.elapsed().as_millis();
            if result.status == Status::Ok {
                tracing::info!(
                    duration_ms,
                    bytes_reclaimed = result.stats.bytes_reclaimed,
                    records_scanned = result.stats.records_scanned,
                    "compaction completed"
                );
            } else {
                tracing::warn!(duration_ms, status = ?result.status, "compaction failed");
            }
        }

        result
    }

    /// Copy a record to the tail of the log during compaction
    ///
    /// # Arguments
    /// * `key_bytes` - Encoded key bytes from the source record
    /// * `value_bytes` - Encoded value bytes from the source record
    /// * `_hash` - Pre-computed hash of the key (unused but kept for future use)
    ///
    /// # Returns
    /// The new address of the copied record, or an error
    fn copy_record_to_tail(
        &self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        _hash: KeyHash,
    ) -> Result<Address, Status> {
        let (disk_len, alloc_len) = if record_format::is_fixed_record::<K, V>() {
            (
                record_format::fixed_disk_len::<K, V>(),
                record_format::fixed_alloc_len::<K, V>(),
            )
        } else {
            (
                record_format::varlen_disk_len(key_bytes.len(), value_bytes.len()),
                record_format::varlen_alloc_len(key_bytes.len(), value_bytes.len()),
            )
        };

        // Allocate space at tail
        // SAFETY: Allocation is protected by epoch
        let new_address = unsafe { self.hlog_mut().allocate(alloc_len as u32)? };

        if new_address == Address::INVALID {
            return Err(Status::OutOfMemory);
        }

        // Get pointer to new location
        // SAFETY: Address was just allocated and is valid
        let new_ptr = unsafe { self.hlog().get(new_address) };

        if let Some(ptr) = new_ptr {
            unsafe {
                const U32_BYTES: usize = std::mem::size_of::<u32>();
                const VARLEN_LENGTHS_SIZE: usize = 2 * U32_BYTES;
                let record_info_size = std::mem::size_of::<RecordInfo>();
                let varlen_header_size = record_info_size + VARLEN_LENGTHS_SIZE;

                // Initialize header (not a tombstone, valid record)
                ptr::write(ptr.as_ptr().cast::<RecordInfo>(), RecordInfo::default());

                if record_format::is_fixed_record::<K, V>() {
                    let key_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(record_info_size),
                        key_bytes.len(),
                    );
                    key_dst.copy_from_slice(key_bytes);
                    let val_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(record_info_size + key_bytes.len()),
                        value_bytes.len(),
                    );
                    val_dst.copy_from_slice(value_bytes);
                } else {
                    let len_ptr = ptr.as_ptr().add(record_info_size);
                    let key_len_le = (key_bytes.len() as u32).to_le_bytes();
                    std::ptr::copy_nonoverlapping(key_len_le.as_ptr(), len_ptr, U32_BYTES);
                    let val_len_le = (value_bytes.len() as u32).to_le_bytes();
                    std::ptr::copy_nonoverlapping(
                        val_len_le.as_ptr(),
                        len_ptr.add(U32_BYTES),
                        U32_BYTES,
                    );

                    let key_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(varlen_header_size),
                        key_bytes.len(),
                    );
                    key_dst.copy_from_slice(key_bytes);
                    let val_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(varlen_header_size + key_bytes.len()),
                        value_bytes.len(),
                    );
                    val_dst.copy_from_slice(value_bytes);
                }
            }

            // Record stats
            if self.stats_collector.is_enabled() {
                self.stats_collector
                    .store_stats
                    .hybrid_log
                    .record_allocation(disk_len as u64);
            }

            Ok(new_address)
        } else {
            Err(Status::Corruption)
        }
    }

    /// Compact the log by a specified amount of bytes
    ///
    /// # Arguments
    /// * `bytes_to_compact` - Approximate number of bytes to compact
    ///
    /// # Returns
    /// CompactionResult with status and statistics
    pub fn compact_bytes(&self, bytes_to_compact: u64) -> CompactionResult {
        // SAFETY: Read-only access
        let begin_address = unsafe { self.hlog().get_begin_address() };
        let target = Address::from_control(begin_address.control() + bytes_to_compact);
        self.log_compact_until(Some(target))
    }

    /// Estimated liveness ratio for on-disk records.
    ///
    /// This is a heuristic that assumes approximately 70% of on-disk records
    /// are still live (not superseded by newer versions). In practice, this
    /// depends on workload characteristics:
    /// - Update-heavy workloads may have lower liveness (more dead records)
    /// - Insert-heavy workloads may have higher liveness (fewer updates)
    ///
    /// This value is used in `log_utilization()` to estimate space efficiency.
    const ESTIMATED_ON_DISK_LIVENESS: f64 = 0.7;

    /// Get estimated log utilization
    ///
    /// Estimates the fraction of log space containing live (non-obsolete) records.
    /// The in-memory portion is assumed to be 100% live, while the on-disk portion
    /// uses `ESTIMATED_ON_DISK_LIVENESS` as a heuristic.
    ///
    /// # Returns
    /// Utilization ratio between 0.0 and 1.0
    ///
    /// # Note
    /// This is an estimate. For accurate utilization, perform a full log scan
    /// using the compaction infrastructure.
    pub fn log_utilization(&self) -> f64 {
        // SAFETY: Read-only accesses
        let begin_address = unsafe { self.hlog().get_begin_address() };
        let tail_address = unsafe { self.hlog().get_tail_address() };
        let head_address = unsafe { self.hlog().get_head_address() };

        let total_size = tail_address
            .control()
            .saturating_sub(begin_address.control());
        let on_disk_size = head_address
            .control()
            .saturating_sub(begin_address.control());

        if total_size == 0 {
            return 1.0;
        }

        // In-memory records are assumed 100% live (mutable region)
        // On-disk records use the estimated liveness ratio
        let in_memory_size = total_size.saturating_sub(on_disk_size);
        (in_memory_size as f64 + on_disk_size as f64 * Self::ESTIMATED_ON_DISK_LIVENESS)
            / total_size as f64
    }

    /// Check if compaction is recommended based on configuration
    pub fn should_compact(&self) -> bool {
        self.log_utilization() < self.compactor.config().target_utilization
    }

    /// Start an automatic background compaction worker for this store.
    ///
    /// This can be called multiple times to start multiple independent workers.
    /// Dropping the returned handle stops the worker thread and joins it.
    ///
    /// Notes:
    /// - The worker triggers based on [`FasterKv::log_utilization`] and `AutoCompactionConfig`.
    /// - Actual compaction uses the existing [`FasterKv::log_compact_until`] path, so at most
    ///   one compaction can run at a time (subsequent triggers will be skipped while busy).
    pub fn start_auto_compaction(
        self: &Arc<Self>,
        config: AutoCompactionConfig,
    ) -> AutoCompactionHandle<Self> {
        AutoCompactionHandle::start_new(Arc::downgrade(self), config)
    }

    /// Get the hybrid log begin address.
    pub fn log_begin_address(&self) -> Address {
        // SAFETY: Read-only access.
        unsafe { self.hlog().get_begin_address() }
    }

    /// Get the hybrid log head address.
    pub fn log_head_address(&self) -> Address {
        // SAFETY: Read-only access.
        unsafe { self.hlog().get_head_address() }
    }

    /// Get the hybrid log tail address.
    pub fn log_tail_address(&self) -> Address {
        // SAFETY: Read-only access.
        unsafe { self.hlog().get_tail_address() }
    }

    /// Get the hybrid log size in bytes (tail - begin).
    pub fn log_size_bytes(&self) -> u64 {
        let begin = self.log_begin_address();
        let tail = self.log_tail_address();
        tail.control().saturating_sub(begin.control())
    }
}

impl<K, V, D> AutoCompactionTarget for FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    fn begin_address(&self) -> Address {
        self.log_begin_address()
    }

    fn safe_head_address(&self) -> Address {
        self.log_head_address()
    }

    fn log_size_bytes(&self) -> u64 {
        FasterKv::<K, V, D>::log_size_bytes(self)
    }

    fn log_utilization(&self) -> f64 {
        FasterKv::<K, V, D>::log_utilization(self)
    }

    fn compact_until(&self, until_address: Address) -> CompactionResult {
        self.log_compact_until(Some(until_address))
    }
}
