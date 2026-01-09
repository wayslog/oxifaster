//! FasterKV - Core key-value store implementation
//!
//! This module provides the main FasterKV store implementation.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use crate::address::Address;
use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::cache::{ReadCache, ReadCacheConfig};
use crate::checkpoint::{
    create_checkpoint_directory, delta_log_path, delta_metadata_path, CheckpointState,
    CheckpointToken, CheckpointType, DeltaLogMetadata, IndexMetadata, LogMetadata, RecoveryState,
    SessionState,
};
use crate::compaction::{CompactionConfig, CompactionResult, CompactionStats, Compactor};
use crate::delta_log::{DeltaLog, DeltaLogConfig};
use crate::device::StorageDevice;
use crate::epoch::{current_thread_tag_for, get_thread_id, get_thread_tag, LightEpoch};
use crate::index::{GrowResult, GrowState, KeyHash, MemHashIndex, MemHashIndexConfig};
use crate::record::{Key, Record, RecordInfo, Value};
use crate::stats::StatsCollector;
use crate::status::Status;
use crate::store::pending_io::PendingIoManager;
use crate::store::state_transitions::{Action, AtomicSystemState, Phase, SystemState};
use crate::store::{AsyncSession, Session, ThreadContext};

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
            page_size_bits: 22,       // 4 MB pages
            mutable_fraction: 0.9,
        }
    }
}

#[derive(Clone)]
struct DiskReadResult<K, V> {
    key: K,
    value: Option<V>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingIoKey {
    thread_id: usize,
    thread_tag: u64,
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
/// - Session persistence for checkpoint/recovery
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
    /// Pending I/O manager (background I/O for the `Pending + complete_pending` read-back path).
    pending_io: PendingIoManager<D>,
    /// Disk read result cache (keyed by record address).
    ///
    /// Convention: `read()` returns `Pending` when it hits a disk address. Once the background
    /// read completes, the parsed result is stored here; the caller calls `complete_pending()`
    /// and then retries `read()` to hit the cache.
    disk_read_results: Mutex<HashMap<u64, DiskReadResult<K, V>>>,
    /// Background I/O completion counter (aggregated by `thread_id` and consumed by sessions in
    /// `complete_pending()`).
    pending_io_completed: Mutex<HashMap<PendingIoKey, u32>>,
    /// Next session ID
    next_session_id: AtomicU32,
    /// Compactor for log compaction
    compactor: Compactor,
    /// Index growth state
    grow_state: UnsafeCell<Option<GrowState>>,
    /// Statistics collector
    stats_collector: StatsCollector,
    /// Active session states registry
    /// Maps session GUID to session state for checkpoint persistence
    session_registry: RwLock<HashMap<Uuid, SessionState>>,
    /// Last snapshot checkpoint state for incremental checkpoints
    /// Used to track the base snapshot for incremental checkpoints
    last_snapshot_checkpoint: RwLock<Option<CheckpointState>>,
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
    pub fn with_compaction_config(
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
    ) -> Self {
        Self::with_full_config(config, device, compaction_config, None)
    }

    /// Create a new FasterKV store with read cache
    pub fn with_read_cache(
        config: FasterKvConfig,
        device: D,
        cache_config: ReadCacheConfig,
    ) -> Self {
        Self::with_full_config(
            config,
            device,
            CompactionConfig::default(),
            Some(cache_config),
        )
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

        // Pending I/O manager (dedicated thread, does not depend on an external Tokio runtime).
        let pending_io = PendingIoManager::new(device.clone());

        Self {
            epoch,
            system_state: AtomicSystemState::default(),
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(0),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(HashMap::new()),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: PhantomData,
        }
    }

    /// Drive and process background I/O completions (global).
    fn process_pending_io_completions(&self) {
        let completions = self.pending_io.drain_completions();
        if completions.is_empty() {
            return;
        }

        let mut completed = self.pending_io_completed.lock();

        for c in completions {
            match c {
                crate::store::pending_io::IoCompletion::ReadBytesDone {
                    thread_id,
                    thread_tag,
                    address,
                    result,
                } => {
                    if let Ok(bytes) = result {
                        if let Some(parsed) = self.parse_disk_record(&bytes) {
                            self.disk_read_results
                                .lock()
                                .insert(address.control(), parsed);
                        }
                    }

                    if current_thread_tag_for(thread_id) != thread_tag {
                        continue;
                    }

                    *completed
                        .entry(PendingIoKey {
                            thread_id,
                            thread_tag,
                        })
                        .or_insert(0) += 1;
                }
            }
        }
    }

    /// Consume and return the number of completed I/Os for the given thread.
    pub(crate) fn take_completed_io_for_thread(&self, thread_id: usize, thread_tag: u64) -> u32 {
        self.process_pending_io_completions();
        self.pending_io_completed
            .lock()
            .remove(&PendingIoKey {
                thread_id,
                thread_tag,
            })
            .unwrap_or(0)
    }

    /// Parse key/value from disk-read record bytes (producing an owned result).
    ///
    /// # Safety Note
    ///
    /// This method is only safe for POD (Plain Old Data) types. For types that contain pointers
    /// (e.g. `String`, `Vec`), bytes read from disk would contain invalid pointers, and
    /// interpreting them directly would be undefined behavior.
    ///
    /// Therefore, this method checks at runtime whether `K` and `V` require drop:
    /// - If they do, it returns `None` and disables disk parsing for this store.
    /// - Only POD types (e.g. `u64`, `i64`, `[u8; N]`) are actually parsed.
    fn parse_disk_record(&self, bytes: &[u8]) -> Option<DiskReadResult<K, V>> {
        // Safety check: only POD types can be safely reconstructed from raw bytes.
        if std::mem::needs_drop::<K>() || std::mem::needs_drop::<V>() {
            // Non-POD types are not supported; the caller will keep returning `Pending`
            // because the cache will never be filled (expected behavior).
            return None;
        }

        let min_len = Record::<K, V>::disk_size() as usize;
        if bytes.len() < min_len {
            return None;
        }

        let header_control = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
        let header = RecordInfo::from_control(header_control);

        let key_offset = Record::<K, V>::key_offset();
        let value_offset = Record::<K, V>::value_offset();

        // SAFETY:
        // 1. `needs_drop` checks ensure K and V are POD (no internal pointers)
        // 2. bytes come from a read of `Record::<K,V>::disk_size()` and the length is checked
        // 3. offsets are derived from `Record` layout
        unsafe {
            let key_ptr = bytes.as_ptr().add(key_offset) as *const K;
            let value_ptr = bytes.as_ptr().add(value_offset) as *const V;

            // For POD types, a bitwise copy is sufficient.
            let key: K = ptr::read_unaligned(key_ptr);

            let value = if header.is_tombstone() {
                None
            } else {
                let value: V = ptr::read_unaligned(value_ptr);
                Some(value)
            };

            Some(DiskReadResult { key, value })
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

    // ============ Session Management API ============

    /// Start a new session
    ///
    /// Creates a new session bound to the current thread. The session uses the
    /// thread's ID for epoch protection, ensuring correct memory safety guarantees.
    ///
    /// # Note
    ///
    /// Sessions are not thread-safe and should only be used from the thread that
    /// created them. Each thread should have at most one active session.
    pub fn start_session(self: &Arc<Self>) -> Session<K, V, D> {
        // Increment session counter for tracking purposes
        let _session_id = self.next_session_id.fetch_add(1, Ordering::AcqRel);
        // Use the thread-local ID for epoch protection, not the session counter
        let thread_id = get_thread_id();
        let mut session = Session::new(self.clone(), thread_id);
        session.start();

        // Register session in the registry
        self.register_session(&session.to_session_state());

        session
    }

    /// Start a new async session for this store
    ///
    /// Async sessions provide non-blocking operations using Rust's async/await.
    /// Each async session gets a unique GUID and is bound to the calling thread
    /// for epoch protection.
    ///
    /// # Note
    ///
    /// Async sessions are not thread-safe and should only be used from the thread
    /// that created them.
    pub fn start_async_session(self: &Arc<Self>) -> AsyncSession<K, V, D> {
        let session = self.start_session();
        AsyncSession::new(session)
    }

    /// Continue an async session from a saved state (for recovery)
    ///
    /// Restores an async session using the GUID and serial number from a previous checkpoint.
    pub fn continue_async_session(self: &Arc<Self>, state: SessionState) -> AsyncSession<K, V, D> {
        let session = self.continue_session(state);
        AsyncSession::new(session)
    }

    /// Continue a session from a saved state (for recovery)
    ///
    /// Restores a session using the GUID and serial number from a previous checkpoint.
    /// The session is bound to the current thread for epoch protection.
    ///
    /// # Note
    ///
    /// The restored session should be used from the same thread that calls this method.
    pub fn continue_session(self: &Arc<Self>, state: SessionState) -> Session<K, V, D> {
        // Increment session counter for tracking purposes
        let _session_id = self.next_session_id.fetch_add(1, Ordering::AcqRel);
        // Use the thread-local ID for epoch protection
        let thread_id = get_thread_id();
        let mut session = Session::from_state(self.clone(), thread_id, &state);
        session.start();

        // Register restored session
        self.register_session(&session.to_session_state());

        session
    }

    /// Register a session state in the registry
    ///
    /// Called when a session starts or updates its state.
    pub fn register_session(&self, state: &SessionState) {
        self.session_registry
            .write()
            .insert(state.guid, state.clone());
    }

    /// Update a session's state in the registry
    ///
    /// Should be called periodically by sessions to keep checkpoint state current.
    pub fn update_session(&self, state: &SessionState) {
        if let Some(entry) = self.session_registry.write().get_mut(&state.guid) {
            entry.serial_num = state.serial_num;
        }
    }

    /// Unregister a session from the registry
    ///
    /// Called when a session ends.
    pub fn unregister_session(&self, guid: Uuid) {
        self.session_registry.write().remove(&guid);
    }

    /// Get all active session states (for checkpointing)
    ///
    /// Returns a snapshot of all registered session states.
    pub fn get_session_states(&self) -> Vec<SessionState> {
        self.session_registry.read().values().cloned().collect()
    }

    /// Get the number of active sessions
    pub fn active_session_count(&self) -> usize {
        self.session_registry.read().len()
    }

    /// Get a specific session state by GUID
    pub fn get_session_state(&self, guid: Uuid) -> Option<SessionState> {
        self.session_registry.read().get(&guid).cloned()
    }

    /// Synchronous read operation
    pub(crate) fn read_sync(&self, ctx: &mut ThreadContext, key: &K) -> Result<Option<V>, Status> {
        let start = Instant::now();
        let hash = KeyHash::new(key.get_hash());

        // Find entry in hash index
        let result = self.hash_index.find_entry(hash);

        if !result.found() {
            // Record miss statistics
            if self.stats_collector.is_enabled() {
                self.stats_collector
                    .store_stats
                    .operations
                    .record_read(false);
                self.stats_collector
                    .store_stats
                    .operations
                    .record_latency(start.elapsed());
            }
            return Ok(None);
        }

        let mut address = result.entry.address();

        // Check read cache first if enabled
        if address.in_readcache() {
            if let Some(value) = self.try_read_from_cache(address, key) {
                // Record hit statistics
                if self.stats_collector.is_enabled() {
                    self.stats_collector
                        .store_stats
                        .operations
                        .record_read(true);
                    self.stats_collector
                        .store_stats
                        .operations
                        .record_latency(start.elapsed());
                }
                return Ok(Some(value));
            }
            // If cache miss, skip to underlying HybridLog address
            address = self.skip_read_cache(address);
        }

        // Traverse the chain to find the key
        while address.is_valid() {
            // Check read cache for chain entries
            if address.in_readcache() {
                if let Some(value) = self.try_read_from_cache(address, key) {
                    // Record hit statistics
                    if self.stats_collector.is_enabled() {
                        self.stats_collector
                            .store_stats
                            .operations
                            .record_read(true);
                        self.stats_collector
                            .store_stats
                            .operations
                            .record_latency(start.elapsed());
                    }
                    return Ok(Some(value));
                }
                // Skip to underlying address
                address = self.skip_read_cache(address);
                continue;
            }

            // SAFETY: read-only access to hybrid log metadata.
            let head_address = unsafe { self.hlog().get_head_address() };

            if address < head_address {
                // Prefer hitting the "disk read result cache" to avoid duplicate I/O submissions.
                if let Some(result) = self.disk_read_results.lock().remove(&address.control()) {
                    if result.key == *key {
                        // The cache stores tombstone(None) or value(Some) after a read completes.
                        if self.stats_collector.is_enabled() {
                            self.stats_collector
                                .store_stats
                                .operations
                                .record_read(result.value.is_some());
                            self.stats_collector
                                .store_stats
                                .operations
                                .record_latency(start.elapsed());
                        }
                        return Ok(result.value);
                    }
                }

                // Record pending statistics
                if self.stats_collector.is_enabled() {
                    self.stats_collector.store_stats.operations.record_pending();
                }
                // Record is on disk: submit an async read request and return Pending.
                let record_len = Record::<K, V>::disk_size() as usize;
                if self.pending_io.submit_read_bytes(
                    ctx.thread_id,
                    get_thread_tag(),
                    address,
                    record_len,
                ) {
                    ctx.pending_count = ctx.pending_count.saturating_add(1);
                }
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
                        // Record miss (tombstone found)
                        if self.stats_collector.is_enabled() {
                            self.stats_collector
                                .store_stats
                                .operations
                                .record_read(false);
                            self.stats_collector
                                .store_stats
                                .operations
                                .record_latency(start.elapsed());
                        }
                        return Ok(None);
                    }

                    // Return value
                    let value = unsafe { record.value() };

                    // If address is in read-only region, consider inserting into cache
                    // for future reads (only for cold data from read-only region)
                    let safe_read_only = unsafe { self.hlog().get_safe_read_only_address() };
                    if address < safe_read_only && self.read_cache.is_some() {
                        // Insert into read cache for future reads
                        let _ = self.try_insert_into_cache(key, value, address);
                    }

                    // Record hit statistics
                    if self.stats_collector.is_enabled() {
                        self.stats_collector
                            .store_stats
                            .operations
                            .record_read(true);
                        self.stats_collector
                            .store_stats
                            .operations
                            .record_latency(start.elapsed());
                    }

                    return Ok(Some(value.clone()));
                }

                // Follow chain
                address = record.header.previous_address();
            } else {
                break;
            }
        }

        // Record miss statistics
        if self.stats_collector.is_enabled() {
            self.stats_collector
                .store_stats
                .operations
                .record_read(false);
            self.stats_collector
                .store_stats
                .operations
                .record_latency(start.elapsed());
        }
        Ok(None)
    }

    /// Internal upsert implementation without statistics recording.
    ///
    /// This is used by both `upsert_sync` and `rmw_sync` to avoid double-counting
    /// operation statistics when RMW creates a new record via upsert.
    fn upsert_internal(&self, ctx: &mut ThreadContext, key: K, value: V) -> (Status, usize) {
        let hash = KeyHash::new(key.get_hash());

        // Find or create entry in hash index
        let result = self.hash_index.find_or_create_entry(hash);

        let atomic_entry = match result.atomic_entry {
            Some(entry) => entry,
            None => return (Status::OutOfMemory, 0),
        };
        let old_address = result.entry.address();

        // Invalidate any existing read cache entry for this key
        if old_address.in_readcache() {
            self.invalidate_cache_entry(old_address, &key);
        }

        // Calculate record size
        let record_size = Record::<K, V>::size();

        // Allocate space in the log
        // SAFETY: Allocation is protected by epoch and internal synchronization
        let address = match unsafe { self.hlog_mut().allocate(record_size as u32) } {
            Ok(addr) => addr,
            Err(status) => return (status, 0),
        };

        // Get pointer to the allocated space
        // SAFETY: We just allocated this space, and access is protected by epoch
        let record_ptr = unsafe { self.hlog_mut().get_mut(address) };

        if let Some(ptr) = record_ptr {
            // Initialize the record
            unsafe {
                let record = ptr.as_ptr() as *mut Record<K, V>;

                // Initialize header - use the underlying HybridLog address, not cache address
                let hlog_old_address = self.skip_read_cache(old_address);
                let header =
                    RecordInfo::new(hlog_old_address, ctx.version as u16, false, false, false);
                ptr::write(&mut (*record).header, header);

                // Write key
                let key_ptr = ptr.as_ptr().add(Record::<K, V>::key_offset()) as *mut K;
                ptr::write(key_ptr, key);

                // Write value
                let value_ptr = ptr.as_ptr().add(Record::<K, V>::value_offset()) as *mut V;
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
                // Record retry statistics
                if self.stats_collector.is_enabled() {
                    self.stats_collector.store_stats.operations.record_retry();
                }
            }

            (Status::Ok, record_size)
        } else {
            (Status::OutOfMemory, 0)
        }
    }

    /// Synchronous upsert operation
    ///
    /// Inserts or updates a key-value pair.
    pub(crate) fn upsert_sync(&self, ctx: &mut ThreadContext, key: K, value: V) -> Status {
        let start = Instant::now();

        let (status, record_size) = self.upsert_internal(ctx, key, value);

        // Record upsert statistics
        if self.stats_collector.is_enabled() {
            self.stats_collector.store_stats.operations.record_upsert();
            self.stats_collector
                .store_stats
                .operations
                .record_latency(start.elapsed());
            if record_size > 0 {
                self.stats_collector
                    .store_stats
                    .hybrid_log
                    .record_allocation(record_size as u64);
            }
        }

        status
    }

    /// Synchronous delete operation
    pub(crate) fn delete_sync(&self, ctx: &mut ThreadContext, key: &K) -> Status {
        let start = Instant::now();
        let hash = KeyHash::new(key.get_hash());

        // Find entry in hash index
        let result = self.hash_index.find_entry(hash);

        let atomic_entry = match result.atomic_entry {
            Some(entry) => entry,
            None => return Status::NotFound,
        };
        let old_address = result.entry.address();

        // Invalidate any existing read cache entry for this key
        if old_address.in_readcache() {
            self.invalidate_cache_entry(old_address, key);
        }

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

                // Initialize header with tombstone flag - use underlying HybridLog address
                let hlog_old_address = self.skip_read_cache(old_address);
                let header =
                    RecordInfo::new(hlog_old_address, ctx.version as u16, false, true, false);
                ptr::write(&mut (*record).header, header);

                // Write key
                let key_ptr = ptr.as_ptr().add(Record::<K, V>::key_offset()) as *mut K;
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

            // Record delete statistics
            if self.stats_collector.is_enabled() {
                self.stats_collector.store_stats.operations.record_delete();
                self.stats_collector
                    .store_stats
                    .operations
                    .record_latency(start.elapsed());
                self.stats_collector
                    .store_stats
                    .hybrid_log
                    .record_allocation(record_size as u64);
            }

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
        let start = Instant::now();
        let hash = KeyHash::new(key.get_hash());

        // Find or create entry in hash index
        let result = self.hash_index.find_or_create_entry(hash);

        let _atomic_entry = match result.atomic_entry {
            Some(entry) => entry,
            None => return Status::OutOfMemory,
        };
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
                            // Record RMW statistics (in-place update)
                            if self.stats_collector.is_enabled() {
                                self.stats_collector.store_stats.operations.record_rmw();
                                self.stats_collector
                                    .store_stats
                                    .operations
                                    .record_latency(start.elapsed());
                            }
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
            self.read_sync(ctx, &key).unwrap_or_default()
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

        // Upsert the new value using the internal method to avoid double-counting
        // statistics. We only want to record RMW stats, not both upsert and RMW.
        let (status, record_size) = self.upsert_internal(ctx, key, new_value);

        // Record RMW statistics only (not upsert stats)
        if self.stats_collector.is_enabled() {
            self.stats_collector.store_stats.operations.record_rmw();
            self.stats_collector
                .store_stats
                .operations
                .record_latency(start.elapsed());
            if record_size > 0 {
                self.stats_collector
                    .store_stats
                    .hybrid_log
                    .record_allocation(record_size as u64);
            }
        }

        status
    }

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

        let start_time = Instant::now();

        // SAFETY: These are read-only accesses
        let begin_address = unsafe { self.hlog().get_begin_address() };
        let head_address = unsafe { self.hlog().get_head_address() };

        // Calculate scan range
        let scan_range =
            match self
                .compactor
                .calculate_scan_range(begin_address, head_address, target_address)
            {
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
                    if self.compactor.should_compact_record(
                        current_address,
                        index_address,
                        is_tombstone,
                    ) {
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
        let new_address = unsafe { self.hlog_mut().allocate(record_size as u32)? };

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
                self.stats_collector
                    .store_stats
                    .hybrid_log
                    .record_allocation(record_size as u64);
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
            (*self.grow_state.get())
                .as_ref()
                .map(|s| (s.progress().0, s.progress().1))
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
        if let Some(read_cache) = self.read_cache.as_ref() {
            read_cache.clear();
        }
    }

    /// Try to read from read cache
    fn try_read_from_cache(&self, address: Address, key: &K) -> Option<V> {
        let read_cache = self.read_cache.as_ref()?;
        let (value, _info) = read_cache.read(address, key)?;
        Some(value)
    }

    /// Try to insert into read cache
    fn try_insert_into_cache(
        &self,
        key: &K,
        value: &V,
        previous_address: Address,
    ) -> Option<Address> {
        let read_cache = self.read_cache.as_ref()?;
        read_cache
            .try_insert(key, value, previous_address, false)
            .ok()
    }

    /// Skip read cache addresses to get underlying HybridLog address
    fn skip_read_cache(&self, address: Address) -> Address {
        self.read_cache
            .as_ref()
            .map_or(address, |read_cache| read_cache.skip(address))
    }

    /// Invalidate a cache entry for a key
    fn invalidate_cache_entry(&self, address: Address, key: &K) {
        if let Some(read_cache) = self.read_cache.as_ref() {
            let _ = read_cache.invalidate(address, key);
        }
    }

    // ============ Checkpoint and Recovery Methods ============

    /// Create a checkpoint of the store using the CPR (Concurrent Prefix Recovery) protocol
    ///
    /// This saves both the hash index and hybrid log state to disk.
    /// The checkpoint uses the CPR state machine to ensure consistent
    /// concurrent access during checkpointing.
    ///
    /// # CPR Protocol Phases
    /// 1. PrepIndexChkpt - Prepare index checkpoint, synchronize threads
    /// 2. IndexChkpt - Write index to disk
    /// 3. Prepare - Prepare HybridLog checkpoint
    /// 4. InProgress - Increment version, mark checkpoint in progress
    /// 5. WaitPending - Wait for pending operations
    /// 6. WaitFlush - Wait for log flush
    /// 7. PersistenceCallback - Invoke persistence callback
    /// 8. Rest - Return to rest state
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    ///
    /// # Returns
    /// The checkpoint token on success, or an error
    pub fn checkpoint(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointFull)
    }

    /// Create an index-only checkpoint
    pub fn checkpoint_index(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointIndex)
    }

    /// Create a hybrid log-only checkpoint
    pub fn checkpoint_hybrid_log(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointHybridLog)
    }

    /// Create an incremental checkpoint
    ///
    /// If there's a previous snapshot available, this creates an incremental checkpoint
    /// that only stores changes since the last snapshot. Otherwise, it falls back to
    /// creating a full snapshot checkpoint.
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    ///
    /// # Returns
    /// The checkpoint token on success, or an error
    pub fn checkpoint_incremental(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        // Check if we have a previous snapshot to base the incremental checkpoint on
        let prev_snapshot = self.last_snapshot_checkpoint.read();
        let can_use_incremental = prev_snapshot.is_some();
        drop(prev_snapshot);

        if can_use_incremental {
            self.checkpoint_with_action(checkpoint_dir, Action::CheckpointIncremental)
        } else {
            // No previous snapshot, create a full checkpoint instead
            // This will also set up the last_snapshot_checkpoint for future incrementals
            let token = self.checkpoint(checkpoint_dir)?;
            Ok(token)
        }
    }

    /// Force a full snapshot checkpoint and set it as the base for future incrementals
    ///
    /// This is useful when you want to start a new incremental chain.
    pub fn checkpoint_full_snapshot(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointFull)
    }

    /// Create a checkpoint with a specific action
    fn checkpoint_with_action(
        &self,
        checkpoint_dir: &Path,
        action: Action,
    ) -> io::Result<CheckpointToken> {
        let token = Uuid::new_v4();
        self.checkpoint_with_action_internal(checkpoint_dir, action, token)
    }

    fn checkpoint_with_action_internal(
        &self,
        checkpoint_dir: &Path,
        action: Action,
        token: CheckpointToken,
    ) -> io::Result<CheckpointToken> {
        // Try to start the checkpoint action
        let start_result = self.system_state.try_start_action(action);
        let start_state = match start_result {
            Ok(prev) => prev,
            Err(current_state) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "Cannot start checkpoint: action {:?} phase {:?} already in progress",
                        current_state.action, current_state.phase
                    ),
                ));
            }
        };
        let start_version = start_state.version;

        // Create checkpoint directory
        let result = create_checkpoint_directory(checkpoint_dir, token)
            .and_then(|cp_dir| self.execute_checkpoint_state_machine(&cp_dir, token, action));

        // On failure, we must roll back to the version before checkpoint started
        // to prevent the CPR version from being incorrectly incremented.
        if result.is_err() {
            self.system_state
                .store(SystemState::rest(start_version), Ordering::Release);
        }

        result.map(|_| token)
    }

    /// Execute the checkpoint state machine
    fn execute_checkpoint_state_machine(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        action: Action,
    ) -> io::Result<()> {
        let mut index_metadata: Option<IndexMetadata> = None;
        let mut log_metadata: Option<LogMetadata> = None;
        let is_incremental = action == Action::CheckpointIncremental;

        loop {
            let current_state = self.system_state.load(Ordering::Acquire);

            // Handle current phase
            match current_state.phase {
                Phase::PrepIndexChkpt => {
                    // Phase 1: Prepare index checkpoint
                    // In a full implementation, this would synchronize all threads
                    // For now, we just advance to the next phase
                    self.handle_prep_index_checkpoint()?;
                }

                Phase::IndexChkpt => {
                    // Phase 2: Write index to disk
                    index_metadata = Some(self.handle_index_checkpoint(cp_dir, token)?);
                }

                Phase::Prepare => {
                    // Phase 3: Prepare hybrid log checkpoint
                    self.handle_prepare_checkpoint()?;
                }

                Phase::InProgress => {
                    // Phase 4: Checkpoint in progress
                    // Version is already incremented by state machine
                    if is_incremental {
                        log_metadata = Some(self.handle_incremental_checkpoint(
                            cp_dir,
                            token,
                            current_state.version,
                        )?);
                    } else {
                        log_metadata = Some(self.handle_in_progress_checkpoint(
                            cp_dir,
                            token,
                            current_state.version,
                        )?);
                    }
                }

                Phase::WaitPending => {
                    // Phase 5: Wait for pending operations
                    self.handle_wait_pending()?;
                }

                Phase::WaitFlush => {
                    // Phase 6: Wait for flush to complete
                    if is_incremental {
                        self.handle_incremental_wait_flush(cp_dir, token, current_state.version)?;
                    } else {
                        self.handle_wait_flush()?;
                    }
                }

                Phase::PersistenceCallback => {
                    // Phase 7: Invoke persistence callback
                    self.handle_persistence_callback(
                        cp_dir,
                        token,
                        index_metadata.as_ref(),
                        log_metadata.as_ref(),
                    )?;

                    // Save checkpoint state for future incremental checkpoints
                    if !is_incremental && log_metadata.is_some() {
                        let mut state = CheckpointState::new(CheckpointType::Snapshot);
                        if let Some(ref log_meta) = log_metadata {
                            state.log_metadata = log_meta.clone();
                        }
                        if let Some(ref index_meta) = index_metadata {
                            state.index_metadata = index_meta.clone();
                        }
                        // Set tokens AFTER cloning to ensure they're not overwritten
                        // Both index and log metadata must have the same token for consistency
                        state.index_metadata.token = token;
                        state.log_metadata.token = token;
                        *self.last_snapshot_checkpoint.write() = Some(state);
                    }
                }

                Phase::Rest => {
                    // Phase 8: Back to rest state - checkpoint complete
                    return Ok(());
                }

                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Unexpected phase {:?} during checkpoint",
                            current_state.phase
                        ),
                    ));
                }
            }

            // Advance to the next phase
            if let Err(err_state) = self.system_state.try_advance() {
                // If we can't advance and we're in rest state, we're done
                if err_state.phase == Phase::Rest {
                    return Ok(());
                }
                // Otherwise, retry (another thread may have advanced)
            }
        }
    }

    /// Handle PREP_INDEX_CHKPT phase
    fn handle_prep_index_checkpoint(&self) -> io::Result<()> {
        // In a multi-threaded implementation, this would:
        // 1. Signal all threads that index checkpoint is starting
        // 2. Wait for threads to reach a safe point
        // For now, we assume single-threaded checkpoint
        Ok(())
    }

    /// Handle INDEX_CHKPT phase
    fn handle_index_checkpoint(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
    ) -> io::Result<IndexMetadata> {
        self.hash_index.checkpoint(cp_dir, token)
    }

    /// Handle PREPARE phase
    fn handle_prepare_checkpoint(&self) -> io::Result<()> {
        // Prepare for hybrid log checkpoint
        // Get flushed addresses, etc.
        Ok(())
    }

    /// Handle IN_PROGRESS phase - checkpoint the hybrid log
    fn handle_in_progress_checkpoint(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        version: u32,
    ) -> io::Result<LogMetadata> {
        // Collect session states for persistence
        let session_states = self.get_session_states();

        // SAFETY: checkpoint() is protected by the epoch system
        unsafe {
            (*self.hlog.get()).checkpoint_with_sessions(cp_dir, token, version, session_states)
        }
    }

    /// Handle WAIT_PENDING phase
    fn handle_wait_pending(&self) -> io::Result<()> {
        // Wait for all pending operations to complete
        // In async implementation, this would drain pending I/Os
        Ok(())
    }

    /// Handle WAIT_FLUSH phase
    fn handle_wait_flush(&self) -> io::Result<()> {
        // Wait for log flush to complete
        // SAFETY: Flush is protected by epoch
        unsafe {
            (*self.hlog.get()).flush_to_disk()?;
        }
        Ok(())
    }

    /// Handle IN_PROGRESS phase for incremental checkpoint
    fn handle_incremental_checkpoint(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        version: u32,
    ) -> io::Result<LogMetadata> {
        // Get the previous snapshot state
        let prev_snapshot = self.last_snapshot_checkpoint.read();
        let prev_state = prev_snapshot.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "No previous snapshot for incremental checkpoint",
            )
        })?;

        let prev_token = prev_state.token();
        let prev_final_address = prev_state.log_metadata.final_address;

        drop(prev_snapshot);

        // Collect session states for persistence
        let session_states = self.get_session_states();

        // Create metadata for incremental checkpoint
        let mut metadata = unsafe { (*self.hlog.get()).checkpoint_metadata(token, version, true) };
        metadata.session_states = session_states;
        metadata.num_threads = metadata.session_states.len() as u32;

        // Mark as incremental
        metadata.is_incremental = true;
        metadata.prev_snapshot_token = Some(prev_token);
        metadata.start_logical_address = prev_final_address;

        // Write log metadata
        let meta_path = cp_dir.join("log.meta");
        metadata.write_to_file(&meta_path)?;

        Ok(metadata)
    }

    /// Handle WAIT_FLUSH phase for incremental checkpoint
    fn handle_incremental_wait_flush(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        version: u32,
    ) -> io::Result<()> {
        // Get the previous snapshot state
        let prev_snapshot = self.last_snapshot_checkpoint.read();
        let prev_state = prev_snapshot.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "No previous snapshot for incremental checkpoint",
            )
        })?;

        let prev_token = prev_state.token();
        let prev_final_address = prev_state.log_metadata.final_address;

        drop(prev_snapshot);

        // Get current log addresses
        let (flushed_address, final_address) = unsafe {
            let hlog = &*self.hlog.get();
            (hlog.get_flushed_until_address(), hlog.get_tail_address())
        };

        // Create delta log device (using a file in the checkpoint directory)
        let delta_path = delta_log_path(cp_dir, 0);
        let delta_device = Arc::new(crate::device::FileSystemDisk::single_file(&delta_path)?);
        let delta_config = DeltaLogConfig::new(22); // 4MB pages
        let delta_log = DeltaLog::new(delta_device.clone(), delta_config, 0);

        // Flush delta records
        let num_entries = unsafe {
            (*self.hlog.get()).flush_delta_to_device(
                flushed_address,
                final_address,
                prev_final_address,
                version,
                &delta_log,
            )?
        };

        // Write delta log metadata (use checkpoint token, not a random UUID)
        let delta_meta = DeltaLogMetadata {
            token: token.to_string(),
            base_snapshot_token: prev_token.to_string(),
            version,
            delta_tail_address: delta_log.tail_address(),
            prev_snapshot_final_address: prev_final_address.control(),
            current_final_address: final_address.control(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            num_entries,
        };
        delta_meta.write_to_file(&delta_metadata_path(cp_dir))?;

        // Flush the regular log as well
        unsafe {
            (*self.hlog.get()).flush_to_disk()?;
        }

        Ok(())
    }

    /// Handle PERSISTENCE_CALLBACK phase
    fn handle_persistence_callback(
        &self,
        _cp_dir: &Path,
        _token: CheckpointToken,
        _index_metadata: Option<&IndexMetadata>,
        _log_metadata: Option<&LogMetadata>,
    ) -> io::Result<()> {
        // Invoke user persistence callback if registered
        // For now, this is a no-op
        Ok(())
    }

    /// Create a checkpoint with a specific type (deprecated, use checkpoint_index or checkpoint_hybrid_log)
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
        checkpoint_type: CheckpointType,
    ) -> io::Result<CheckpointToken> {
        let action = match checkpoint_type {
            // FoldOver and Snapshot are both full checkpoints, just with different snapshot modes
            CheckpointType::Full | CheckpointType::Snapshot | CheckpointType::FoldOver => {
                Action::CheckpointFull
            }
            CheckpointType::IndexOnly => Action::CheckpointIndex,
            CheckpointType::HybridLogOnly => Action::CheckpointHybridLog,
            // IncrementalSnapshot is handled via checkpoint_incremental()
            CheckpointType::IncrementalSnapshot => Action::CheckpointFull,
        };
        self.checkpoint_with_action(checkpoint_dir, action)
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
        Self::recover_with_compaction_config(
            checkpoint_dir,
            token,
            config,
            device,
            CompactionConfig::default(),
        )
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
        Self::recover_with_full_config(
            checkpoint_dir,
            token,
            config,
            device,
            compaction_config,
            None,
        )
    }

    /// Recover a store from a checkpoint with read cache
    pub fn recover_with_read_cache(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        cache_config: ReadCacheConfig,
    ) -> io::Result<Self> {
        Self::recover_with_full_config(
            checkpoint_dir,
            token,
            config,
            device,
            CompactionConfig::default(),
            Some(cache_config),
        )
    }

    /// Recover a store from a checkpoint with full configuration
    ///
    /// Uses RecoveryState to track recovery progress and restores session states
    /// for continuation with Concurrent Prefix Recovery (CPR).
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

        // Initialize recovery state tracker
        let mut recovery_state = RecoveryState::new();
        let status = recovery_state.start_recovery(checkpoint_dir, token);
        if status != crate::Status::Ok {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                recovery_state
                    .error_message()
                    .unwrap_or("Unknown recovery error"),
            ));
        }

        // Initialize epoch
        let epoch = Arc::new(LightEpoch::new());

        // Load checkpoint metadata (recovery_state already validated these exist)
        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;

        // Phase 2: Recover hash index
        let mut hash_index = MemHashIndex::new();
        hash_index.recover(&cp_dir, Some(&index_meta))?;
        recovery_state.index_recovered();

        // Phase 3: Recover hybrid log
        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let mut hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        hlog.recover(&cp_dir, Some(&log_meta))?;
        recovery_state.log_recovered();

        // Set system state to recovered version (rest state)
        let system_state = AtomicSystemState::new(SystemState::rest(log_meta.version));

        // Initialize compactor
        let compactor = Compactor::with_config(compaction_config);

        // Initialize read cache if configured
        let read_cache = cache_config.map(ReadCache::new);
        let pending_io = PendingIoManager::new(device.clone());

        // Phase 4: Build session registry from recovered session states
        let mut session_registry = HashMap::new();
        for session_state in &log_meta.session_states {
            session_registry.insert(session_state.guid, session_state.clone());
        }
        // Set next session ID based on number of recovered sessions
        let next_session_id = log_meta.num_threads;

        recovery_state.complete_recovery();

        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(next_session_id),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(session_registry),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: PhantomData,
        })
    }

    /// Get recovered session states from a checkpoint
    ///
    /// This can be used to restore sessions after recovery.
    /// Call `continue_session()` with each state to restore sessions.
    pub fn get_recovered_sessions(&self) -> Vec<SessionState> {
        self.get_session_states()
    }

    /// Check if a full checkpoint exists (both index and log)
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to check
    ///
    /// # Returns
    /// true if a complete checkpoint (both index.meta and log.meta) exists
    pub fn checkpoint_exists(checkpoint_dir: &Path, token: CheckpointToken) -> bool {
        Self::get_checkpoint_kind(checkpoint_dir, token) == CheckpointKind::Full
    }

    /// Get the kind of checkpoint that exists
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to check
    ///
    /// # Returns
    /// The kind of checkpoint (Full, IndexOnly, LogOnly, or None)
    pub fn get_checkpoint_kind(checkpoint_dir: &Path, token: CheckpointToken) -> CheckpointKind {
        let cp_dir = checkpoint_dir.join(token.to_string());
        let has_index = cp_dir.join("index.meta").exists();
        let has_log = cp_dir.join("log.meta").exists();

        match (has_index, has_log) {
            (true, true) => CheckpointKind::Full,
            (true, false) => CheckpointKind::IndexOnly,
            (false, true) => CheckpointKind::LogOnly,
            (false, false) => CheckpointKind::None,
        }
    }

    /// Check if any checkpoint (full or partial) exists
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to check
    ///
    /// # Returns
    /// true if any checkpoint files exist
    pub fn any_checkpoint_exists(checkpoint_dir: &Path, token: CheckpointToken) -> bool {
        Self::get_checkpoint_kind(checkpoint_dir, token) != CheckpointKind::None
    }

    /// List all checkpoint tokens in a directory (full checkpoints only)
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    ///
    /// # Returns
    /// List of valid full checkpoint tokens
    pub fn list_checkpoints(checkpoint_dir: &Path) -> io::Result<Vec<CheckpointToken>> {
        Self::list_checkpoints_by_kind(checkpoint_dir, Some(CheckpointKind::Full))
    }

    /// List all checkpoint tokens of a specific kind
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `kind` - If Some, only list checkpoints of this kind; if None, list all
    ///
    /// # Returns
    /// List of checkpoint tokens matching the criteria
    pub fn list_checkpoints_by_kind(
        checkpoint_dir: &Path,
        kind: Option<CheckpointKind>,
    ) -> io::Result<Vec<CheckpointToken>> {
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
                            let cp_kind = Self::get_checkpoint_kind(checkpoint_dir, token);
                            // Include if kind matches or if no filter specified (and not None)
                            let include = match kind {
                                Some(k) => cp_kind == k,
                                None => cp_kind != CheckpointKind::None,
                            };
                            if include {
                                tokens.push(token);
                            }
                        }
                    }
                }
            }
        }

        Ok(tokens)
    }

    /// Recover only the hash index from a checkpoint
    ///
    /// This is useful for index-only checkpoints or when you want to
    /// recover the index separately from the log.
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to recover from
    /// * `config` - Store configuration
    /// * `device` - Storage device
    ///
    /// # Returns
    /// A new FasterKv instance with recovered index (log starts empty)
    pub fn recover_index_only(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
    ) -> io::Result<Self> {
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());

        // Check that index checkpoint exists
        let kind = Self::get_checkpoint_kind(checkpoint_dir, token);
        if kind != CheckpointKind::Full && kind != CheckpointKind::IndexOnly {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Index checkpoint not found",
            ));
        }

        // Initialize epoch
        let epoch = Arc::new(LightEpoch::new());

        // Load index metadata
        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;

        // Recover hash index
        let mut hash_index = MemHashIndex::new();
        hash_index.recover(&cp_dir, Some(&index_meta))?;

        // Initialize fresh hybrid log (no recovery)
        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let hlog = PersistentMemoryMalloc::new(log_config, device.clone());

        // Set system state to recovered version
        let system_state = AtomicSystemState::new(SystemState::rest(index_meta.version));

        // Initialize compactor
        let compactor = Compactor::with_config(CompactionConfig::default());
        let pending_io = PendingIoManager::new(device.clone());

        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache: None,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(0),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(HashMap::new()),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: PhantomData,
        })
    }

    /// Recover only the hybrid log from a checkpoint
    ///
    /// This is useful for log-only checkpoints. Note that the hash index
    /// will be empty/fresh, so lookups won't work until the index is rebuilt.
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Base directory for checkpoints
    /// * `token` - Checkpoint token to recover from
    /// * `config` - Store configuration
    /// * `device` - Storage device
    ///
    /// # Returns
    /// A new FasterKv instance with recovered log (index is fresh/empty)
    pub fn recover_log_only(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
    ) -> io::Result<Self> {
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());

        // Check that log checkpoint exists
        let kind = Self::get_checkpoint_kind(checkpoint_dir, token);
        if kind != CheckpointKind::Full && kind != CheckpointKind::LogOnly {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Log checkpoint not found",
            ));
        }

        // Initialize epoch
        let epoch = Arc::new(LightEpoch::new());

        // Initialize fresh hash index (no recovery)
        let mut hash_index = MemHashIndex::new();
        let index_config = MemHashIndexConfig::new(config.table_size);
        hash_index.initialize(&index_config);

        // Load log metadata
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;

        // Recover hybrid log
        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let mut hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        hlog.recover(&cp_dir, Some(&log_meta))?;

        // Set system state to recovered version
        let system_state = AtomicSystemState::new(SystemState::rest(log_meta.version));

        // Initialize compactor
        let compactor = Compactor::with_config(CompactionConfig::default());
        let pending_io = PendingIoManager::new(device.clone());

        // Restore session states
        let mut session_registry = HashMap::new();
        for session_state in &log_meta.session_states {
            session_registry.insert(session_state.guid, session_state.clone());
        }

        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache: None,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(log_meta.num_threads),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(session_registry),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: PhantomData,
        })
    }
}

/// Kind of checkpoint that exists
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointKind {
    /// No checkpoint exists
    None,
    /// Full checkpoint (both index and log)
    Full,
    /// Index-only checkpoint
    IndexOnly,
    /// Log-only checkpoint
    LogOnly,
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
            assert_eq!(result.unwrap(), Some(i * 10), "Failed to read key {i}");
        }

        // Update some
        for i in 1u64..51 {
            let status = session.upsert(i, i * 100);
            assert_eq!(status, Status::Ok);
        }

        // Verify updates
        for i in 1u64..51 {
            let result = session.read(&i);
            assert_eq!(
                result.unwrap(),
                Some(i * 100),
                "Failed to read updated key {i}"
            );
        }

        // Verify unchanged
        for i in 51u64..101 {
            let result = session.read(&i);
            assert_eq!(
                result.unwrap(),
                Some(i * 10),
                "Key {i} was unexpectedly changed"
            );
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

        let tokens = FasterKv::<u64, u64, NullDisk>::list_checkpoints(temp_dir.path()).unwrap();

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
    fn test_checkpoint_failure_rolls_back_version() {
        let store = create_test_store();
        let temp_dir = tempfile::tempdir().unwrap();

        let v0 = store.system_state().version;
        let token = uuid::Uuid::new_v4();

        // Create a path that will cause log.snapshot write to fail: make it a directory.
        let cp_dir = temp_dir.path().join(token.to_string());
        std::fs::create_dir_all(cp_dir.join("log.snapshot")).unwrap();

        assert!(store
            .checkpoint_with_action_internal(temp_dir.path(), Action::CheckpointFull, token)
            .is_err());

        let state = store.system_state();
        assert_eq!(state.phase, Phase::Rest);
        assert_eq!(state.version, v0);
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
        assert!((0.0..=1.0).contains(&util));

        // After some inserts
        for i in 0u64..10 {
            session.upsert(i, i * 100);
        }

        let util_after = store.log_utilization();
        assert!((0.0..=1.0).contains(&util_after));
    }

    #[test]
    fn test_should_compact() {
        let store = create_test_store();
        // With default settings and empty store, should not need compaction
        let _needs_compact = store.should_compact();
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
            assert!(result.is_ok(), "Record {i} should exist before compaction");
            assert_eq!(
                result.unwrap(),
                Some(i * 100),
                "Record {i} should have correct value"
            );
        }

        // Perform compaction
        let compact_result = store.log_compact();
        assert_eq!(
            compact_result.status,
            Status::Ok,
            "Compaction should succeed"
        );

        // CRITICAL: Verify all records are still readable after compaction
        // This is the key assertion - if compaction drops live records,
        // this test will fail.
        for i in 1..=num_records {
            let result = session.read(&i);
            assert!(
                result.is_ok(),
                "Record {i} should still exist after compaction"
            );
            let value = result.unwrap();
            assert_eq!(
                value,
                Some(i * 100),
                "Record {} should have correct value {} after compaction, got {:?}",
                i,
                i * 100,
                value
            );
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
            assert_eq!(value, Some(i + 1000), "Key {i} should have updated value");
        }

        // Keys 26-50 should have original values
        for i in 26u64..=50 {
            let value = session.read(&i).unwrap();
            assert_eq!(value, Some(i), "Key {i} should have original value");
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
