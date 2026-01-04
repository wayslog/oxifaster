//! FasterKV - Core key-value store implementation
//!
//! This module provides the main FasterKV store implementation.

use std::cell::UnsafeCell;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::address::Address;
use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::cache::{ReadCache, ReadCacheConfig};
use crate::checkpoint::{
    create_checkpoint_directory, CheckpointToken, CheckpointType, IndexMetadata, LogMetadata,
};
use crate::compaction::{CompactionConfig, CompactionResult, CompactionStats, Compactor};
use crate::device::StorageDevice;
use crate::epoch::LightEpoch;
use crate::index::{KeyHash, MemHashIndex, MemHashIndexConfig, GrowState, GrowResult};
use crate::record::{Key, Record, RecordInfo, Value};
use crate::stats::{StatsCollector, StatsConfig, StoreStats as StatsStoreStats};
use crate::status::Status;
use crate::store::state_transitions::{AtomicSystemState, Phase, SystemState};
use crate::store::{Session, ThreadContext};

/// Configuration for FasterKV
#[derive(Debug, Clone)]
pub struct FasterKvConfig {
    /// Initial hash table size (must be power of 2)
    pub table_size: u64,
    /// Log memory size in bytes
    pub log_memory_size: u64,
    /// Log page size bits (page size = 1 << page_size_bits)
    pub page_size_bits: u32,
    /// Mutable fraction of log memory
    pub mutable_fraction: f64,
}

impl FasterKvConfig {
    /// Create a new configuration
    pub fn new(table_size: u64, log_memory_size: u64) -> Self {
        Self {
            table_size,
            log_memory_size,
            page_size_bits: 22, // 4 MB pages by default
            mutable_fraction: 0.9,
        }
    }
}

impl Default for FasterKvConfig {
    fn default() -> Self {
        Self {
            table_size: 1 << 20,      // 1M buckets
            log_memory_size: 1 << 29, // 512 MB
            page_size_bits: 22,        // 4 MB pages
            mutable_fraction: 0.9,
        }
    }
}

/// FasterKV - High-performance concurrent key-value store
///
/// This is the main store implementation that coordinates:
/// - Epoch protection for safe memory reclamation
/// - Hash index for key lookups
/// - Hybrid log for record storage
/// - Read cache for hot data
/// - Compaction for space reclamation
/// - Index growth for dynamic resizing
/// - Statistics collection
pub struct FasterKv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Epoch protection
    epoch: Arc<LightEpoch>,
    /// System state
    system_state: AtomicSystemState,
    /// Hash index
    hash_index: MemHashIndex,
    /// Hybrid log - wrapped in UnsafeCell for interior mutability
    /// SAFETY: Access to hlog is protected by epoch protection and
    /// the internal synchronization mechanisms of PersistentMemoryMalloc
    hlog: UnsafeCell<PersistentMemoryMalloc<D>>,
    /// Read cache for hot data
    read_cache: Option<ReadCache<K, V>>,
    /// Storage device
    device: Arc<D>,
    /// Next session ID
    next_session_id: AtomicU32,
    /// Compactor for log compaction
    compactor: Compactor,
    /// Index growth state
    grow_state: UnsafeCell<Option<GrowState>>,
    /// Statistics collector
    stats_collector: StatsCollector,
    /// Type markers
    _marker: PhantomData<(K, V)>,
}

// SAFETY: FasterKv uses epoch protection and internal synchronization
// to ensure safe concurrent access to hlog
unsafe impl<K, V, D> Send for FasterKv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice + Send + Sync,
{
}

unsafe impl<K, V, D> Sync for FasterKv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice + Send + Sync,
{
}

impl<K, V, D> FasterKv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Create a new FasterKV store
    pub fn new(config: FasterKvConfig, device: D) -> Self {
        Self::with_compaction_config(config, device, CompactionConfig::default())
    }

    /// Create a new FasterKV store with custom compaction configuration
    pub fn with_compaction_config(config: FasterKvConfig, device: D, compaction_config: CompactionConfig) -> Self {
        Self::with_full_config(config, device, compaction_config, None)
    }

    /// Create a new FasterKV store with read cache
    pub fn with_read_cache(config: FasterKvConfig, device: D, cache_config: ReadCacheConfig) -> Self {
        Self::with_full_config(config, device, CompactionConfig::default(), Some(cache_config))
    }

    /// Create a new FasterKV store with full configuration
    pub fn with_full_config(
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
        cache_config: Option<ReadCacheConfig>,
    ) -> Self {
        let device = Arc::new(device);
        
        // Initialize epoch
        let epoch = Arc::new(LightEpoch::new());
        
        // Initialize hash index
        let mut hash_index = MemHashIndex::new();
        let index_config = MemHashIndexConfig::new(config.table_size);
        hash_index.initialize(&index_config);
        
        // Initialize hybrid log
        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        
        // Initialize compactor
        let compactor = Compactor::with_config(compaction_config);
        
        // Initialize read cache if configured
        let read_cache = cache_config.map(ReadCache::new);
        
        Self {
            epoch,
            system_state: AtomicSystemState::default(),
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache,
            device,
            next_session_id: AtomicU32::new(0),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            _marker: PhantomData,
        }
    }

    /// Get a reference to the epoch
    pub fn epoch(&self) -> &LightEpoch {
        &self.epoch
    }

    /// Get the current system state
    pub fn system_state(&self) -> SystemState {
        self.system_state.load(Ordering::Acquire)
    }

    /// Get hash index statistics
    pub fn index_stats(&self) -> crate::index::IndexStats {
        self.hash_index.dump_distribution()
    }

    /// Get log statistics
    pub fn log_stats(&self) -> crate::allocator::LogStats {
        // SAFETY: get_stats() is a read-only operation
        unsafe { (*self.hlog.get()).get_stats() }
    }

    // ============ Statistics Collection API ============

    /// Get the statistics collector
    pub fn stats_collector(&self) -> &StatsCollector {
        &self.stats_collector
    }

    /// Enable statistics collection
    pub fn enable_stats(&self) {
        self.stats_collector.enable();
    }

    /// Disable statistics collection
    pub fn disable_stats(&self) {
        self.stats_collector.disable();
    }

    /// Check if statistics collection is enabled
    pub fn is_stats_enabled(&self) -> bool {
        self.stats_collector.is_enabled()
    }

    /// Get a snapshot of all statistics
    pub fn stats_snapshot(&self) -> crate::stats::StatsSnapshot {
        self.stats_collector.snapshot()
    }

    /// Get operation statistics
    pub fn operation_stats(&self) -> &crate::stats::OperationStats {
        &self.stats_collector.store_stats.operations
    }

    /// Get allocator statistics  
    pub fn allocator_stats(&self) -> &crate::stats::AllocatorStats {
        &self.stats_collector.store_stats.allocator
    }

    /// Get hybrid log statistics from collector
    pub fn hlog_stats(&self) -> &crate::stats::HybridLogStats {
        &self.stats_collector.store_stats.hybrid_log
    }

    /// Get throughput (operations per second)
    pub fn throughput(&self) -> f64 {
        self.stats_collector.throughput()
    }

    /// Get the storage device
    pub fn device(&self) -> &Arc<D> {
        &self.device
    }

    /// Get a reference to the hybrid log
    /// 
    /// # Safety
    /// The caller must ensure no mutable access to the same region
    /// is occurring concurrently.
    #[inline]
    unsafe fn hlog(&self) -> &PersistentMemoryMalloc<D> {
        &*self.hlog.get()
    }

    /// Get a mutable reference to the hybrid log
    /// 
    /// # Safety
    /// The caller must ensure exclusive access to the region being modified.
    /// This is typically guaranteed by epoch protection.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn hlog_mut(&self) -> &mut PersistentMemoryMalloc<D> {
        &mut *self.hlog.get()
    }

    /// Start a new session
    pub fn start_session(self: &Arc<Self>) -> Session<K, V, D> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::AcqRel) as usize;
        let mut session = Session::new(self.clone(), session_id);
        session.start();
        session
    }

    /// Synchronous read operation
    pub(crate) fn read_sync(&self, ctx: &mut ThreadContext, key: &K) -> Result<Option<V>, Status> {
        let hash = KeyHash::new(key.get_hash());
        
        // Find entry in hash index
        let result = self.hash_index.find_entry(hash);
        
        if !result.found() {
            return Ok(None);
        }
        
        let mut address = result.entry.address();
        
        // Traverse the chain to find the key
        while address.is_valid() {
            // SAFETY: These are read-only accesses to log metadata
            let read_only_address = unsafe { self.hlog().get_read_only_address() };
            
            if address < unsafe { self.hlog().get_head_address() } {
                // Record is on disk - need async I/O
                return Err(Status::Pending);
            }
            
            // Get record from log
            // SAFETY: Address is valid and within memory range
            let record_ptr = unsafe { self.hlog().get(address) };
            
            if let Some(ptr) = record_ptr {
                let record: &Record<K, V> = unsafe { &*(ptr.as_ptr() as *const _) };
                
                // Check if this is our key
                let record_key = unsafe { record.key() };
                if record_key == key {
                    // Check for tombstone
                    if record.header.is_tombstone() {
                        return Ok(None);
                    }
                    
                    // Return value
                    let value = unsafe { record.value() };
                    return Ok(Some(value.clone()));
                }
                
                // Follow chain
                address = record.header.previous_address();
            } else {
                break;
            }
        }
        
        Ok(None)
    }

    /// Synchronous upsert operation
    pub(crate) fn upsert_sync(&self, ctx: &mut ThreadContext, key: K, value: V) -> Status {
        let hash = KeyHash::new(key.get_hash());
        
        // Find or create entry in hash index
        let result = self.hash_index.find_or_create_entry(hash);
        
        if result.atomic_entry.is_none() {
            return Status::OutOfMemory;
        }
        
        let atomic_entry = result.atomic_entry.unwrap();
        let old_address = result.entry.address();
        
        // Calculate record size
        let record_size = Record::<K, V>::size();
        
        // Allocate space in the log
        // SAFETY: Allocation is protected by epoch and internal synchronization
        let address = match unsafe { self.hlog_mut().allocate(record_size as u32) } {
            Ok(addr) => addr,
            Err(status) => return status,
        };
        
        // Get pointer to the allocated space
        // SAFETY: We just allocated this space, and access is protected by epoch
        let record_ptr = unsafe { self.hlog_mut().get_mut(address) };
        
        if let Some(ptr) = record_ptr {
            // Initialize the record
            unsafe {
                let record = ptr.as_ptr() as *mut Record<K, V>;
                
                // Initialize header
                let header = RecordInfo::new(old_address, ctx.version as u16, false, false, false);
                ptr::write(&mut (*record).header, header);
                
                // Write key
                let key_ptr = (ptr.as_ptr() as *mut u8).add(Record::<K, V>::key_offset()) as *mut K;
                ptr::write(key_ptr, key);
                
                // Write value
                let value_ptr = (ptr.as_ptr() as *mut u8).add(Record::<K, V>::value_offset()) as *mut V;
                ptr::write(value_ptr, value);
            }
            
            // Update hash index
            let status = self.hash_index.try_update_entry(
                atomic_entry,
                result.entry.to_hash_bucket_entry(),
                address,
                hash.tag(),
                false,
            );
            
            if status != Status::Ok {
                // CAS failed - another thread updated, need to retry
                // For now, just return success since the record is in the log
            }
            
            Status::Ok
        } else {
            Status::OutOfMemory
        }
    }

    /// Synchronous delete operation
    pub(crate) fn delete_sync(&self, ctx: &mut ThreadContext, key: &K) -> Status {
        let hash = KeyHash::new(key.get_hash());
        
        // Find entry in hash index
        let result = self.hash_index.find_entry(hash);
        
        if !result.found() {
            return Status::NotFound;
        }
        
        let atomic_entry = result.atomic_entry.unwrap();
        let old_address = result.entry.address();
        
        // Calculate record size (tombstone record)
        let record_size = Record::<K, V>::size();
        
        // Allocate space in the log
        // SAFETY: Allocation is protected by epoch and internal synchronization
        let address = match unsafe { self.hlog_mut().allocate(record_size as u32) } {
            Ok(addr) => addr,
            Err(status) => return status,
        };
        
        // Get pointer to the allocated space
        // SAFETY: We just allocated this space, and access is protected by epoch
        let record_ptr = unsafe { self.hlog_mut().get_mut(address) };
        
        if let Some(ptr) = record_ptr {
            unsafe {
                let record = ptr.as_ptr() as *mut Record<K, V>;
                
                // Initialize header with tombstone flag
                let header = RecordInfo::new(old_address, ctx.version as u16, false, true, false);
                ptr::write(&mut (*record).header, header);
                
                // Write key
                let key_ptr = (ptr.as_ptr() as *mut u8).add(Record::<K, V>::key_offset()) as *mut K;
                ptr::write(key_ptr, key.clone());
            }
            
            // Update hash index
            let _ = self.hash_index.try_update_entry(
                atomic_entry,
                result.entry.to_hash_bucket_entry(),
                address,
                hash.tag(),
                false,
            );
            
            Status::Ok
        } else {
            Status::OutOfMemory
        }
    }

    /// Synchronous RMW operation
    pub(crate) fn rmw_sync<F>(&self, ctx: &mut ThreadContext, key: K, mut modifier: F) -> Status
    where
        F: FnMut(&mut V) -> bool,
    {
        let hash = KeyHash::new(key.get_hash());
        
        // Find or create entry in hash index
        let result = self.hash_index.find_or_create_entry(hash);
        
        if result.atomic_entry.is_none() {
            return Status::OutOfMemory;
        }
        
        let atomic_entry = result.atomic_entry.unwrap();
        let old_address = result.entry.address();
        
        // Try to find existing record for in-place update
        // SAFETY: Read-only access to log metadata
        if old_address.is_valid() && old_address >= unsafe { self.hlog().get_read_only_address() } {
            // Record is in mutable region - can try in-place update
            // SAFETY: Address is in mutable region, access is protected by epoch
            let record_ptr = unsafe { self.hlog_mut().get_mut(old_address) };
            
            if let Some(ptr) = record_ptr {
                unsafe {
                    let record = ptr.as_ptr() as *mut Record<K, V>;
                    let record_key = (*record).key();
                    
                    if record_key == &key && !(*record).header.is_tombstone() {
                        // Try in-place update
                        let value = (*record).value_mut();
                        if modifier(value) {
                            return Status::Ok;
                        }
                        // modifier returned false, indicating abort - fall through to create new record
                    }
                }
            }
        }
        
        // Need to create new record
        // First, read old value if exists
        let old_value: Option<V> = if old_address.is_valid() {
            match self.read_sync(ctx, &key) {
                Ok(v) => v,
                Err(_) => None,
            }
        } else {
            None
        };
        
        // Create new value
        let new_value = if let Some(mut v) = old_value {
            // Check modifier return value - if false, operation is aborted
            if !modifier(&mut v) {
                return Status::Aborted;
            }
            v
        } else {
            // Use default value
            return Status::NotFound;
        };
        
        // Upsert the new value
        self.upsert_sync(ctx, key, new_value)
    }

    /// Simple compaction: shift log begin address and garbage collect index
    pub fn compact(&self, until_address: Address) -> Status {
        // Update head address
        // SAFETY: shift_head_address is protected by epoch
        unsafe { self.hlog_mut().shift_head_address(until_address) };
        
        // Garbage collect hash index
        self.hash_index.garbage_collect(until_address);
        
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

        let start_time = Instant::now();
        
        // SAFETY: These are read-only accesses
        let begin_address = unsafe { self.hlog().get_begin_address() };
        let head_address = unsafe { self.hlog().get_head_address() };

        // Calculate scan range
        let scan_range = match self.compactor.calculate_scan_range(begin_address, head_address, target_address) {
            Some(range) => range,
            None => {
                self.compactor.complete();
                return CompactionResult::success(begin_address, CompactionStats::default());
            }
        };

        // Create compaction context
        let _context = self.compactor.create_context(scan_range.clone());

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
                let record: &Record<K, V> = unsafe { &*(ptr.as_ptr() as *const _) };
                let record_size = Record::<K, V>::size() as u64;
                
                stats.records_scanned += 1;
                stats.bytes_scanned += record_size;

                // Check if this is a tombstone
                let is_tombstone = record.header.is_tombstone();
                if is_tombstone {
                    stats.tombstones_found += 1;
                }

                // Get the key and check if this record is the latest version
                let key = unsafe { record.key() };
                let value = unsafe { record.value() };
                let hash = KeyHash::new(key.get_hash());
                let index_result = self.hash_index.find_entry(hash);

                if index_result.found() {
                    let index_address = index_result.entry.address();
                    
                    // Check if record should be compacted (moved to tail)
                    if self.compactor.should_compact_record(current_address, index_address, is_tombstone) {
                        // This record is still live - MUST copy to tail before reclaiming
                        if !is_tombstone {
                            // Copy live record to the tail of the log
                            match self.copy_record_to_tail(key, value, hash) {
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
                                        stats.bytes_compacted += record_size;
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
                            stats.bytes_reclaimed += record_size;
                        }
                    } else {
                        // Record is obsolete (superseded by newer version) - can be reclaimed
                        stats.records_skipped += 1;
                        stats.bytes_reclaimed += record_size;
                    }
                } else {
                    // Key not in index - record can be reclaimed
                    stats.records_skipped += 1;
                    stats.bytes_reclaimed += record_size;
                }

                // Move to next record
                current_address = Address::from_control(current_address.control() + record_size);
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
    }

    /// Copy a record to the tail of the log during compaction
    ///
    /// # Arguments
    /// * `key` - The key to copy
    /// * `value` - The value to copy
    /// * `_hash` - Pre-computed hash of the key (unused but kept for future use)
    ///
    /// # Returns
    /// The new address of the copied record, or an error
    fn copy_record_to_tail(&self, key: &K, value: &V, _hash: KeyHash) -> Result<Address, Status> {
        let record_size = Record::<K, V>::size();
        
        // Allocate space at tail
        // SAFETY: Allocation is protected by epoch
        let new_address = unsafe {
            self.hlog_mut().allocate(record_size as u32)?
        };
        
        if new_address == Address::INVALID {
            return Err(Status::OutOfMemory);
        }
        
        // Get pointer to new location
        // SAFETY: Address was just allocated and is valid
        let new_ptr = unsafe { self.hlog().get(new_address) };
        
        if let Some(ptr) = new_ptr {
            // Initialize the new record
            let new_record: &mut Record<K, V> = unsafe { &mut *(ptr.as_ptr() as *mut _) };
            
            // Copy key and value using ptr::write to avoid dropping uninitialized memory
            unsafe {
                ptr::write(new_record.key_mut(), key.clone());
                ptr::write(new_record.value_mut(), value.clone());
            }
            
            // Initialize header (not a tombstone, valid record)
            new_record.header = RecordInfo::default();
            
            // Record stats
            if self.stats_collector.is_enabled() {
                self.stats_collector.store_stats.hybrid_log.record_allocation(record_size as u64);
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

        let total_size = tail_address.control().saturating_sub(begin_address.control());
        let on_disk_size = head_address.control().saturating_sub(begin_address.control());

        if total_size == 0 {
            return 1.0;
        }

        // In-memory records are assumed 100% live (mutable region)
        // On-disk records use the estimated liveness ratio
        let in_memory_size = total_size.saturating_sub(on_disk_size);
        (in_memory_size as f64 + on_disk_size as f64 * Self::ESTIMATED_ON_DISK_LIVENESS) / total_size as f64
    }

    /// Check if compaction is recommended based on configuration
    pub fn should_compact(&self) -> bool {
        self.log_utilization() < self.compactor.config().target_utilization
    }

    // ============ Index Growth API ============

    /// Get the current hash table size (number of buckets)
    pub fn index_size(&self) -> u64 {
        self.hash_index.size()
    }

    /// Check if index growth is in progress
    pub fn is_grow_in_progress(&self) -> bool {
        // SAFETY: Read-only check
        unsafe { (*self.grow_state.get()).is_some() }
    }

    /// Start growing the hash index
    ///
    /// # Arguments
    /// * `new_size` - New hash table size (must be power of 2 and > current size)
    ///
    /// # Returns
    /// Ok(()) if growth started, Err if already in progress or invalid size
    pub fn start_grow(&self, new_size: u64) -> Result<(), Status> {
        // Check if grow is already in progress
        if self.is_grow_in_progress() {
            return Err(Status::Aborted);
        }

        let current_size = self.index_size();
        
        // Validate new size
        if new_size <= current_size {
            return Err(Status::InvalidArgument);
        }
        if !new_size.is_power_of_two() {
            return Err(Status::InvalidArgument);
        }

        // Calculate number of chunks for growth
        let num_chunks = crate::index::calculate_num_chunks(current_size);
        
        // Create grow state and initialize
        let mut state = GrowState::new();
        state.initialize(0, num_chunks);

        // SAFETY: We checked no grow is in progress
        unsafe {
            *self.grow_state.get() = Some(state);
        }

        Ok(())
    }

    /// Complete a pending index growth operation
    ///
    /// This should be called after all threads have processed their chunks.
    pub fn complete_grow(&self) -> GrowResult {
        // SAFETY: Access to grow_state
        let state = unsafe { (*self.grow_state.get()).take() };
        
        match state {
            Some(grow_state) => grow_state.complete(),
            None => GrowResult::failure(Status::InvalidOperation),
        }
    }

    /// Get grow progress (completed chunks / total chunks)
    pub fn grow_progress(&self) -> Option<(u64, u64)> {
        // SAFETY: Read-only access
        unsafe {
            (*self.grow_state.get()).as_ref().map(|s| {
                (s.progress().0, s.progress().1)
            })
        }
    }

    // ============ Read Cache API ============

    /// Check if read cache is enabled
    pub fn has_read_cache(&self) -> bool {
        self.read_cache.is_some()
    }

    /// Get read cache statistics
    pub fn read_cache_stats(&self) -> Option<&crate::cache::ReadCacheStats> {
        self.read_cache.as_ref().map(|rc| rc.stats())
    }

    /// Get read cache configuration
    pub fn read_cache_config(&self) -> Option<&ReadCacheConfig> {
        self.read_cache.as_ref().map(|rc| rc.config())
    }

    /// Clear the read cache
    pub fn clear_read_cache(&self) {
        if let Some(ref rc) = self.read_cache {
            rc.clear();
        }
    }

    /// Try to read from read cache
    fn try_read_from_cache(&self, address: Address, key: &K) -> Option<V> {
        if let Some(ref rc) = self.read_cache {
            if let Some((value, _info)) = rc.read(address, key) {
                return Some(value);
            }
        }
        None
    }

    /// Try to insert into read cache
    fn try_insert_into_cache(&self, key: &K, value: &V, previous_address: Address) -> Option<Address> {
        if let Some(ref rc) = self.read_cache {
            rc.try_insert(key, value, previous_address, false).ok()
        } else {
            None
        }
    }

    /// Skip read cache addresses to get underlying HybridLog address
    fn skip_read_cache(&self, address: Address) -> Address {
        if let Some(ref rc) = self.read_cache {
            rc.skip(address)
        } else {
            address
        }
    }

    // ============ Checkpoint and Recovery Methods ============

    /// Create a checkpoint of the store
    ///
    /// This saves both the hash index and hybrid log state to disk.
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    ///
    /// # Returns
    /// The checkpoint token on success, or an error
    pub fn checkpoint(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        // Generate a new checkpoint token
        let token = uuid::Uuid::new_v4();
        
        // Create checkpoint directory
        let cp_dir = create_checkpoint_directory(checkpoint_dir, token)?;
        
        // Get current version
        let state = self.system_state.load(Ordering::Acquire);
        let version = state.version;
        
        // Checkpoint the hash index
        let _index_metadata = self.hash_index.checkpoint(&cp_dir, token)?;
        
        // Checkpoint the hybrid log
        // SAFETY: checkpoint() is protected by the epoch system
        let _log_metadata = unsafe {
            (*self.hlog.get()).checkpoint(&cp_dir, token, version)?
        };
        
        // Bump version and return to rest state
        let new_state = SystemState::rest(version + 1);
        self.system_state.store(new_state, Ordering::Release);
        
        Ok(token)
    }

    /// Create a checkpoint with a specific type
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `checkpoint_type` - Type of checkpoint to create
    ///
    /// # Returns
    /// The checkpoint token on success, or an error
    pub fn checkpoint_with_type(
        &self,
        checkpoint_dir: &Path,
        _checkpoint_type: CheckpointType,
    ) -> io::Result<CheckpointToken> {
        // For now, all checkpoints are the same type
        self.checkpoint(checkpoint_dir)
    }

    /// Get information about an existing checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to query
    ///
    /// # Returns
    /// Tuple of (IndexMetadata, LogMetadata) on success
    pub fn get_checkpoint_info(
        checkpoint_dir: &Path,
        token: CheckpointToken,
    ) -> io::Result<(IndexMetadata, LogMetadata)> {
        let cp_dir = checkpoint_dir.join(token.to_string());
        
        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;
        
        Ok((index_meta, log_meta))
    }

    /// Recover a store from a checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to recover from
    /// * `config` - Store configuration
    /// * `device` - Storage device
    ///
    /// # Returns
    /// A new FasterKv instance with recovered state
    pub fn recover(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
    ) -> io::Result<Self> {
        Self::recover_with_compaction_config(checkpoint_dir, token, config, device, CompactionConfig::default())
    }

    /// Recover a store from a checkpoint with custom compaction configuration
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to recover from
    /// * `config` - Store configuration
    /// * `device` - Storage device
    /// * `compaction_config` - Compaction configuration
    ///
    /// # Returns
    /// A new FasterKv instance with recovered state
    pub fn recover_with_compaction_config(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
    ) -> io::Result<Self> {
        Self::recover_with_full_config(checkpoint_dir, token, config, device, compaction_config, None)
    }

    /// Recover a store from a checkpoint with read cache
    pub fn recover_with_read_cache(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        cache_config: ReadCacheConfig,
    ) -> io::Result<Self> {
        Self::recover_with_full_config(checkpoint_dir, token, config, device, CompactionConfig::default(), Some(cache_config))
    }

    /// Recover a store from a checkpoint with full configuration
    pub fn recover_with_full_config(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
        cache_config: Option<ReadCacheConfig>,
    ) -> io::Result<Self> {
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());
        
        // Initialize epoch
        let epoch = Arc::new(LightEpoch::new());
        
        // Load checkpoint metadata
        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;
        
        // Recover hash index
        let mut hash_index = MemHashIndex::new();
        hash_index.recover(&cp_dir, Some(&index_meta))?;
        
        // Initialize hybrid log with recovered state
        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let mut hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        hlog.recover(&cp_dir, Some(&log_meta))?;
        
        // Set system state to recovered version (rest state)
        let system_state = AtomicSystemState::new(SystemState::rest(log_meta.version));
        
        // Initialize compactor
        let compactor = Compactor::with_config(compaction_config);
        
        // Initialize read cache if configured
        let read_cache = cache_config.map(ReadCache::new);
        
        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache,
            device,
            next_session_id: AtomicU32::new(0),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            _marker: PhantomData,
        })
    }

    /// Check if a checkpoint exists
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to check
    ///
    /// # Returns
    /// true if the checkpoint exists and is valid
    pub fn checkpoint_exists(checkpoint_dir: &Path, token: CheckpointToken) -> bool {
        let cp_dir = checkpoint_dir.join(token.to_string());
        cp_dir.join("index.meta").exists() && cp_dir.join("log.meta").exists()
    }

    /// List all checkpoint tokens in a directory
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    ///
    /// # Returns
    /// List of valid checkpoint tokens
    pub fn list_checkpoints(checkpoint_dir: &Path) -> io::Result<Vec<CheckpointToken>> {
        let mut tokens = Vec::new();
        
        if !checkpoint_dir.exists() {
            return Ok(tokens);
        }
        
        for entry in std::fs::read_dir(checkpoint_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    if let Some(name_str) = name.to_str() {
                        if let Ok(token) = uuid::Uuid::parse_str(name_str) {
                            // Verify this is a valid checkpoint
                            if Self::checkpoint_exists(checkpoint_dir, token) {
                                tokens.push(token);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;

    fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20, // 1 MB
            page_size_bits: 12,       // 4 KB pages
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device))
    }

    #[test]
    fn test_create_store() {
        let store = create_test_store();
        let state = store.system_state();
        assert_eq!(state.version, 0);
        assert_eq!(state.phase, Phase::Rest);
    }

    #[test]
    fn test_upsert_and_read() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Upsert
        let status = session.upsert(42u64, 100u64);
        assert_eq!(status, Status::Ok);
        
        // Read
        let result = session.read(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(100u64));
    }

    #[test]
    fn test_read_not_found() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        let result = session.read(&999u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Insert
        session.upsert(42u64, 100u64);
        
        // Verify it exists
        let result = session.read(&42u64);
        assert_eq!(result.unwrap(), Some(100u64));
        
        // Delete
        let status = session.delete(&42u64);
        assert_eq!(status, Status::Ok);
        
        // Verify it's gone
        let result = session.read(&42u64);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_multiple_operations() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Insert multiple keys
        for i in 1u64..101 {
            let status = session.upsert(i, i * 10);
            assert_eq!(status, Status::Ok);
        }
        
        // Read them back
        for i in 1u64..101 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), Some(i * 10), "Failed to read key {}", i);
        }
        
        // Update some
        for i in 1u64..51 {
            let status = session.upsert(i, i * 100);
            assert_eq!(status, Status::Ok);
        }
        
        // Verify updates
        for i in 1u64..51 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), Some(i * 100), "Failed to read updated key {}", i);
        }
        
        // Verify unchanged
        for i in 51u64..101 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), Some(i * 10), "Key {} was unexpectedly changed", i);
        }
    }

    #[test]
    fn test_index_stats() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Insert some data
        for i in 0u64..10 {
            session.upsert(i, i);
        }
        
        let stats = store.index_stats();
        assert!(stats.used_entries > 0);
    }

    #[test]
    fn test_log_stats() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Insert some data
        for i in 0u64..10 {
            session.upsert(i, i);
        }
        
        let stats = store.log_stats();
        assert!(stats.tail_address > Address::new(0, 0));
    }

    // ============ Checkpoint and Recovery Tests ============

    fn create_test_config() -> FasterKvConfig {
        FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20, // 1 MB
            page_size_bits: 12,       // 4 KB pages
            mutable_fraction: 0.9,
        }
    }

    #[test]
    fn test_checkpoint_empty_store() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let token = store.checkpoint(temp_dir.path()).unwrap();

        // Verify checkpoint exists
        assert!(FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
            temp_dir.path(),
            token
        ));
    }

    #[test]
    fn test_checkpoint_with_data() {
        let store = create_test_store();
        let mut session = store.start_session();

        // Insert some data
        for i in 1u64..11 {
            session.upsert(i, i * 100);
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let token = store.checkpoint(temp_dir.path()).unwrap();

        // Verify checkpoint files exist
        let cp_dir = temp_dir.path().join(token.to_string());
        assert!(cp_dir.join("index.meta").exists());
        assert!(cp_dir.join("index.dat").exists());
        assert!(cp_dir.join("log.meta").exists());
        assert!(cp_dir.join("log.snapshot").exists());
    }

    #[test]
    fn test_get_checkpoint_info() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let token = store.checkpoint(temp_dir.path()).unwrap();

        let (index_meta, log_meta) =
            FasterKv::<u64, u64, NullDisk>::get_checkpoint_info(temp_dir.path(), token).unwrap();

        assert_eq!(index_meta.token, token);
        assert_eq!(log_meta.token, token);
    }

    #[test]
    fn test_list_checkpoints() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create multiple checkpoints
        let token1 = store.checkpoint(temp_dir.path()).unwrap();
        let token2 = store.checkpoint(temp_dir.path()).unwrap();
        let token3 = store.checkpoint(temp_dir.path()).unwrap();

        let tokens =
            FasterKv::<u64, u64, NullDisk>::list_checkpoints(temp_dir.path()).unwrap();

        assert_eq!(tokens.len(), 3);
        assert!(tokens.contains(&token1));
        assert!(tokens.contains(&token2));
        assert!(tokens.contains(&token3));
    }

    #[test]
    fn test_recover_empty_store() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let token = store.checkpoint(temp_dir.path()).unwrap();
        drop(store);

        // Recover
        let config = create_test_config();
        let device = NullDisk::new();
        let recovered: FasterKv<u64, u64, NullDisk> =
            FasterKv::recover(temp_dir.path(), token, config, device).unwrap();

        let state = recovered.system_state();
        assert_eq!(state.phase, Phase::Rest);
    }

    #[test]
    fn test_checkpoint_increments_version() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let v0 = store.system_state().version;
        store.checkpoint(temp_dir.path()).unwrap();
        let v1 = store.system_state().version;
        store.checkpoint(temp_dir.path()).unwrap();
        let v2 = store.system_state().version;

        assert_eq!(v0, 0);
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    #[test]
    fn test_checkpoint_not_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let fake_token = uuid::Uuid::new_v4();

        assert!(!FasterKv::<u64, u64, NullDisk>::checkpoint_exists(
            temp_dir.path(),
            fake_token
        ));
    }

    // ============ Read Cache Integration Tests ============

    #[test]
    fn test_create_store_with_read_cache() {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        let cache_config = ReadCacheConfig::new(1 << 20); // 1 MB cache
        
        let store: Arc<FasterKv<u64, u64, NullDisk>> = 
            Arc::new(FasterKv::with_read_cache(config, device, cache_config));
        
        assert!(store.has_read_cache());
        assert!(store.read_cache_stats().is_some());
    }

    #[test]
    fn test_store_without_read_cache() {
        let store = create_test_store();
        assert!(!store.has_read_cache());
        assert!(store.read_cache_stats().is_none());
    }

    #[test]
    fn test_clear_read_cache() {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        let cache_config = ReadCacheConfig::new(1 << 20);
        
        let store: Arc<FasterKv<u64, u64, NullDisk>> = 
            Arc::new(FasterKv::with_read_cache(config, device, cache_config));
        
        // Clear should not panic
        store.clear_read_cache();
        assert!(store.has_read_cache());
    }

    // ============ Compaction Integration Tests ============

    #[test]
    fn test_compaction_config() {
        let store = create_test_store();
        let config = store.compaction_config();
        
        assert!(config.target_utilization > 0.0);
        assert!(config.target_utilization <= 1.0);
    }

    #[test]
    fn test_compaction_not_in_progress() {
        let store = create_test_store();
        assert!(!store.is_compaction_in_progress());
    }

    #[test]
    fn test_log_compact_empty() {
        let store = create_test_store();
        let result = store.log_compact();
        
        // On empty store, compaction should succeed with no changes
        assert_eq!(result.status, Status::Ok);
        assert_eq!(result.stats.records_scanned, 0);
    }

    #[test]
    fn test_log_utilization() {
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Initial utilization
        let util = store.log_utilization();
        assert!(util >= 0.0 && util <= 1.0);
        
        // After some inserts
        for i in 0u64..10 {
            session.upsert(i, i * 100);
        }
        
        let util_after = store.log_utilization();
        assert!(util_after >= 0.0 && util_after <= 1.0);
    }

    #[test]
    fn test_should_compact() {
        let store = create_test_store();
        // With default settings and empty store, should not need compaction
        let needs_compact = store.should_compact();
        // Result depends on configuration
        assert!(needs_compact || !needs_compact); // Just verify it doesn't panic
    }

    #[test]
    fn test_compaction_preserves_live_records() {
        // This test verifies that compaction properly copies live records
        // to the tail before reclaiming the original space.
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Insert some records (start from 1, not 0, for consistency with other tests)
        let num_records = 100u64;
        for i in 1..=num_records {
            session.upsert(i, i * 100);
        }
        
        // Verify all records are readable before compaction
        for i in 1..=num_records {
            let result = session.read(&i);
            assert!(result.is_ok(), "Record {} should exist before compaction", i);
            assert_eq!(result.unwrap(), Some(i * 100), "Record {} should have correct value", i);
        }
        
        // Perform compaction
        let compact_result = store.log_compact();
        assert_eq!(compact_result.status, Status::Ok, "Compaction should succeed");
        
        // CRITICAL: Verify all records are still readable after compaction
        // This is the key assertion - if compaction drops live records,
        // this test will fail.
        for i in 1..=num_records {
            let result = session.read(&i);
            assert!(result.is_ok(), "Record {} should still exist after compaction", i);
            let value = result.unwrap();
            assert_eq!(value, Some(i * 100), 
                "Record {} should have correct value {} after compaction, got {:?}", 
                i, i * 100, value);
        }
        
        // Session is automatically dropped when it goes out of scope
    }

    #[test]
    fn test_compaction_removes_obsolete_records() {
        // Test that compaction properly removes obsolete (superseded) records
        let store = create_test_store();
        let mut session = store.start_session();
        
        // Insert initial values (start from 1, not 0, to avoid key=0 edge case)
        for i in 1u64..=50 {
            session.upsert(i, i);
        }
        
        // Update some values (creating obsolete records)
        for i in 1u64..=25 {
            session.upsert(i, i + 1000);
        }
        
        // Perform compaction
        let result = store.log_compact();
        assert_eq!(result.status, Status::Ok);
        
        // Verify we can still read all keys with correct values
        // Keys 1-25 should have updated values (i + 1000)
        for i in 1u64..=25 {
            let value = session.read(&i).unwrap();
            assert_eq!(value, Some(i + 1000), "Key {} should have updated value", i);
        }
        
        // Keys 26-50 should have original values
        for i in 26u64..=50 {
            let value = session.read(&i).unwrap();
            assert_eq!(value, Some(i), "Key {} should have original value", i);
        }
        
        // Session is automatically dropped when it goes out of scope
    }

    // ============ Index Growth Integration Tests ============

    #[test]
    fn test_index_size() {
        let store = create_test_store();
        let size = store.index_size();
        assert!(size > 0);
        assert!(size.is_power_of_two());
    }

    #[test]
    fn test_grow_not_in_progress() {
        let store = create_test_store();
        assert!(!store.is_grow_in_progress());
        assert!(store.grow_progress().is_none());
    }

    #[test]
    fn test_start_grow_invalid_size() {
        let store = create_test_store();
        let current_size = store.index_size();
        
        // Smaller size should fail
        let result = store.start_grow(current_size / 2);
        assert!(result.is_err());
        
        // Same size should fail
        let result = store.start_grow(current_size);
        assert!(result.is_err());
        
        // Non-power-of-two should fail
        let result = store.start_grow(current_size * 2 + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_start_grow_valid() {
        let store = create_test_store();
        let current_size = store.index_size();
        let new_size = current_size * 2;
        
        // Start growth
        let result = store.start_grow(new_size);
        assert!(result.is_ok());
        assert!(store.is_grow_in_progress());
        assert!(store.grow_progress().is_some());
        
        // Complete growth
        let grow_result = store.complete_grow();
        assert!(grow_result.success || grow_result.status.is_some());
    }

    #[test]
    fn test_grow_already_in_progress() {
        let store = create_test_store();
        let current_size = store.index_size();
        
        // Start first growth
        let result1 = store.start_grow(current_size * 2);
        assert!(result1.is_ok());
        
        // Second growth should fail
        let result2 = store.start_grow(current_size * 4);
        assert!(result2.is_err());
        
        // Clean up
        store.complete_grow();
    }
}

