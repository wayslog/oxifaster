//! FasterKV - Core key-value store implementation
//!
//! This module provides the main FasterKV store implementation.

mod checkpoint;
mod compaction;
mod cpr;
mod index_grow;
mod pending_io_support;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use crate::address::Address;
use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::cache::ReadCacheRecordInfo;
use crate::cache::{ReadCache, ReadCacheConfig};
use crate::checkpoint::{CheckpointState, SessionState};
use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::compaction::{CompactionConfig, Compactor};
use crate::constants::MAX_THREADS;
use crate::device::StorageDevice;
use crate::epoch::EpochGuard;
use crate::epoch::{get_thread_tag, try_get_thread_id, LightEpoch};
use crate::index::{GrowState, KeyHash, MemHashIndex, MemHashIndexConfig};
use crate::record::RecordInfo;
use crate::stats::StatsCollector;
use crate::status::Status;
use crate::store::pending_io::PendingIoManager;
use crate::store::state_transitions::{AtomicSystemState, SystemState};
use crate::store::{AsyncSession, RecordView, Session, ThreadContext};

use super::record_format;
use cpr::CprCoordinator;
pub use cpr::{CheckpointDurability, LogCheckpointBackend};

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

#[derive(Debug, Clone)]
pub(crate) struct DiskReadResult<K, V> {
    /// Decoded key for the record. `None` means the record was marked invalid and should be skipped.
    pub(crate) key: Option<K>,
    pub(crate) value: Option<V>,
    pub(crate) previous_address: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingIoKey {
    thread_id: usize,
    thread_tag: u64,
    ctx_id: u64,
}

#[derive(Debug, Clone)]
struct DiskReadCacheEntry<K, V> {
    parsed: Result<DiskReadResult<K, V>, Status>,
}

type DiskReadCache<K, V> = HashMap<u64, DiskReadCacheEntry<K, V>>;

pub(crate) enum ChainReadOutcome<V> {
    Completed(Option<V>),
    NeedDiskRead(Address),
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
    K: PersistKey,
    V: PersistValue,
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
    read_cache: Option<Arc<dyn ReadCacheOps<K, V>>>,
    /// Storage device
    device: Arc<D>,
    /// Pending I/O manager (background I/O for the `Pending + complete_pending` read-back path).
    pending_io: PendingIoManager<D>,
    /// Disk read result cache (keyed by record address).
    ///
    /// Convention: `read()` returns `Pending` when it hits a disk address. Once the background
    /// read completes, the parsed result is stored here; the caller calls `complete_pending()`
    /// and then retries `read()` to hit the cache.
    disk_read_results: Mutex<DiskReadCache<K, V>>,
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
    /// Active thread set (bitmask by `thread_id`) for CPR rendezvous.
    active_threads: Mutex<u128>,
    /// Cooperative CPR coordinator state for the currently running checkpoint (if any).
    cpr: CprCoordinator,
    /// Default log checkpoint backend for `checkpoint()`.
    default_log_checkpoint_backend: AtomicU32,
    /// Default durability mode for checkpoint artifacts.
    default_checkpoint_durability: AtomicU32,
    /// Last snapshot checkpoint state for incremental checkpoints
    /// Used to track the base snapshot for incremental checkpoints
    last_snapshot_checkpoint: RwLock<Option<CheckpointState>>,
    /// Type markers
    _marker: PhantomData<(K, V)>,
}

trait ReadCacheOps<K, V>: Send + Sync {
    fn read(&self, cache_address: Address, key: &K) -> Option<(V, ReadCacheRecordInfo)>;
    fn try_insert(
        &self,
        key: &K,
        value: &V,
        previous_address: Address,
        is_cold_log_record: bool,
    ) -> Result<Address, Status>;
    fn skip(&self, address: Address) -> Address;
    fn invalidate(&self, address: Address, key: &K) -> Address;
    fn stats(&self) -> &crate::cache::ReadCacheStats;
    fn config(&self) -> &ReadCacheConfig;
    fn clear(&self);
}

impl<K, V> ReadCacheOps<K, V> for ReadCache<K, V>
where
    K: PersistKey,
    V: PersistValue,
{
    fn read(&self, cache_address: Address, key: &K) -> Option<(V, ReadCacheRecordInfo)> {
        ReadCache::read(self, cache_address, key)
    }

    fn try_insert(
        &self,
        key: &K,
        value: &V,
        previous_address: Address,
        is_cold_log_record: bool,
    ) -> Result<Address, Status> {
        ReadCache::try_insert(self, key, value, previous_address, is_cold_log_record)
    }

    fn skip(&self, address: Address) -> Address {
        ReadCache::skip(self, address)
    }

    fn invalidate(&self, address: Address, key: &K) -> Address {
        ReadCache::invalidate(self, address, key)
    }

    fn stats(&self) -> &crate::cache::ReadCacheStats {
        ReadCache::stats(self)
    }

    fn config(&self) -> &ReadCacheConfig {
        ReadCache::config(self)
    }

    fn clear(&self) {
        ReadCache::clear(self)
    }
}

// SAFETY: FasterKv uses epoch protection and internal synchronization
// to ensure safe concurrent access to hlog
unsafe impl<K, V, D> Send for FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice + Send + Sync,
{
}

unsafe impl<K, V, D> Sync for FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice + Send + Sync,
{
}

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
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
        Self::with_full_config_impl(config, device, compaction_config, None)
    }

    /// Create a new FasterKV store with read cache
    pub fn with_read_cache(
        config: FasterKvConfig,
        device: D,
        cache_config: ReadCacheConfig,
    ) -> Self {
        let read_cache: Arc<dyn ReadCacheOps<K, V>> =
            Arc::new(ReadCache::<K, V>::new(cache_config));
        Self::with_full_config_impl(
            config,
            device,
            CompactionConfig::default(),
            Some(read_cache),
        )
    }

    fn with_full_config_impl(
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
        read_cache: Option<Arc<dyn ReadCacheOps<K, V>>>,
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

        // Pending I/O manager (dedicated thread, does not depend on an external Tokio runtime).
        let pending_io = PendingIoManager::new(device.clone(), hlog.page_size());

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
            active_threads: Mutex::new(0),
            cpr: CprCoordinator::default(),
            default_log_checkpoint_backend: AtomicU32::new(LogCheckpointBackend::Snapshot as u32),
            default_checkpoint_durability: AtomicU32::new(CheckpointDurability::FasterLike as u32),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: PhantomData,
        }
    }

    /// Get a reference to the epoch
    pub fn epoch(&self) -> &LightEpoch {
        &self.epoch
    }

    /// Get an `Arc` handle to the epoch (for constructing guards that outlive a store borrow).
    pub fn epoch_arc(&self) -> Arc<LightEpoch> {
        self.epoch.clone()
    }

    /// Get the current system state
    pub fn system_state(&self) -> SystemState {
        self.system_state.load(Ordering::Acquire)
    }

    /// Get the log checkpoint backend used by `checkpoint()`.
    pub fn log_checkpoint_backend(&self) -> LogCheckpointBackend {
        LogCheckpointBackend::from(self.default_log_checkpoint_backend.load(Ordering::Acquire) as u8)
    }

    /// Configure the log checkpoint backend used by `checkpoint()`.
    pub fn set_log_checkpoint_backend(&self, backend: LogCheckpointBackend) {
        self.default_log_checkpoint_backend
            .store(u32::from(backend as u8), Ordering::Release);
    }

    /// Get the durability mode for checkpoint artifacts.
    pub fn checkpoint_durability(&self) -> CheckpointDurability {
        CheckpointDurability::from(self.default_checkpoint_durability.load(Ordering::Acquire) as u8)
    }

    /// Configure the durability mode for checkpoint artifacts.
    pub fn set_checkpoint_durability(&self, durability: CheckpointDurability) {
        self.default_checkpoint_durability
            .store(u32::from(durability as u8), Ordering::Release);
    }

    pub(crate) fn cpr_refresh(&self, ctx: &mut ThreadContext) {
        let state = self.system_state.load(Ordering::Acquire);

        if state.phase == crate::store::Phase::Rest || state.action == crate::store::Action::None {
            ctx.version = state.version;
            ctx.current.version = state.version;
            return;
        }

        let state_sig = ThreadContext::cpr_state_sig(state.action, state.phase, state.version);
        if ctx.last_cpr_state == state_sig {
            return;
        }

        let Some(is_participant) = self
            .cpr
            .with_active(|active| active.is_participant(ctx.thread_id))
        else {
            // The store is in a CPR phase, but the checkpoint coordinator isn't fully initialized
            // yet. Do not advance `last_cpr_state`, otherwise the caller could miss an ack.
            ctx.version = state.version;
            ctx.current.version = state.version;
            return;
        };

        if !is_participant {
            if ctx.version != state.version {
                ctx.swap_contexts_for_checkpoint(state.version);
            } else {
                ctx.version = state.version;
                ctx.current.version = state.version;
            }
            ctx.last_cpr_state = state_sig;
            return;
        }

        match state.phase {
            crate::store::Phase::PrepIndexChkpt | crate::store::Phase::Prepare => {
                if self
                    .cpr
                    .with_active_mut(|active| {
                        active.set_phase(state.phase, state.version);
                        active.ack(ctx.thread_id);
                    })
                    .is_some()
                {
                    ctx.last_cpr_state = state_sig;
                }
            }
            crate::store::Phase::InProgress => {
                if ctx.version != state.version {
                    ctx.swap_contexts_for_checkpoint(state.version);
                } else {
                    ctx.current.version = state.version;
                }

                if self
                    .cpr
                    .with_active_mut(|active| {
                        active.set_phase(state.phase, state.version);
                        active.ack(ctx.thread_id);
                    })
                    .is_some()
                {
                    ctx.last_cpr_state = state_sig;
                }
            }
            crate::store::Phase::WaitPending => {
                if ctx.prev_ctx_is_drained()
                    && self
                        .cpr
                        .with_active_mut(|active| {
                            active.set_phase(state.phase, state.version);
                            active.ack(ctx.thread_id);
                        })
                        .is_some()
                {
                    ctx.last_cpr_state = state_sig;
                }
            }
            crate::store::Phase::WaitFlush => {
                let ready = self.cpr.with_active(|active| match active.backend {
                    LogCheckpointBackend::Snapshot => {
                        active.snapshot_written && active.log_metadata.is_some()
                    }
                    LogCheckpointBackend::FoldOver => unsafe {
                        active.log_metadata.is_some()
                            && (*self.hlog.get()).get_flushed_until_address()
                                >= active.final_address
                    },
                });

                if ready.unwrap_or(false)
                    && self
                        .cpr
                        .with_active_mut(|active| {
                            active.set_phase(state.phase, state.version);
                            active.ack(ctx.thread_id);
                        })
                        .is_some()
                {
                    ctx.last_cpr_state = state_sig;
                }
            }
            _ => {}
        }
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

    /// Current hybrid log head address (records below this are considered "on disk").
    pub(crate) fn head_address(&self) -> Address {
        // SAFETY: reading the head address is a read-only operation.
        unsafe { self.hlog().get_head_address() }
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

    /// Get operational statistics (checkpoint/compaction/growth/recovery)
    pub fn operational_stats(&self) -> &crate::stats::OperationalStats {
        &self.stats_collector.store_stats.operational
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
    pub fn start_session(self: &Arc<Self>) -> Result<Session<K, V, D>, Status> {
        self.start_session_with_thread_id(try_get_thread_id())
    }

    fn start_session_with_thread_id(
        self: &Arc<Self>,
        thread_id: Option<usize>,
    ) -> Result<Session<K, V, D>, Status> {
        // Increment session counter for tracking purposes.
        let _ = self.next_session_id.fetch_add(1, Ordering::AcqRel);
        // Use the thread-local ID for epoch protection, not the session counter.
        let thread_id = thread_id.ok_or(Status::TooManyThreads)?;
        let mut session = Session::new(self.clone(), thread_id);
        session.start();

        Ok(session)
    }

    pub(crate) fn register_thread(&self, thread_id: usize) {
        assert!(
            thread_id < MAX_THREADS,
            "register_thread: thread_id {thread_id} exceeds MAX_THREADS={MAX_THREADS}"
        );
        let mut mask = self.active_threads.lock();
        let bit = 1u128
            .checked_shl(thread_id as u32)
            .expect("register_thread: thread_id must be < 128 for u128 bitmask");
        *mask |= bit;
    }

    pub(crate) fn unregister_thread(&self, thread_id: usize) {
        assert!(
            thread_id < MAX_THREADS,
            "unregister_thread: thread_id {thread_id} exceeds MAX_THREADS={MAX_THREADS}"
        );
        let mut mask = self.active_threads.lock();
        let bit = 1u128
            .checked_shl(thread_id as u32)
            .expect("unregister_thread: thread_id must be < 128 for u128 bitmask");
        *mask &= !bit;

        let _ = self
            .cpr
            .with_active_mut(|active| active.mark_thread_inactive(thread_id));
    }

    pub(crate) fn active_threads_snapshot(&self) -> u128 {
        *self.active_threads.lock()
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
    pub fn start_async_session(self: &Arc<Self>) -> Result<AsyncSession<K, V, D>, Status> {
        let session = self.start_session()?;
        Ok(AsyncSession::new(session))
    }

    /// Continue an async session from a saved state (for recovery)
    ///
    /// Restores an async session using the GUID and serial number from a previous checkpoint.
    pub fn continue_async_session(
        self: &Arc<Self>,
        state: SessionState,
    ) -> Result<AsyncSession<K, V, D>, Status> {
        let session = self.continue_session(state)?;
        Ok(AsyncSession::new(session))
    }

    /// Continue a session from a saved state (for recovery)
    ///
    /// Restores a session using the GUID and serial number from a previous checkpoint.
    /// The session is bound to the current thread for epoch protection.
    ///
    /// # Note
    ///
    /// The restored session should be used from the same thread that calls this method.
    pub fn continue_session(
        self: &Arc<Self>,
        state: SessionState,
    ) -> Result<Session<K, V, D>, Status> {
        // Increment session counter for tracking purposes
        let _ = self.next_session_id.fetch_add(1, Ordering::AcqRel);
        // Use the thread-local ID for epoch protection
        let thread_id = try_get_thread_id().ok_or(Status::TooManyThreads)?;
        let mut session = Session::from_state(self.clone(), thread_id, &state);
        session.start();

        Ok(session)
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
        let hash = KeyHash::new(<K as PersistKey>::Codec::hash(key)?);

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
                    match result.parsed {
                        Ok(parsed) => {
                            if parsed.key.as_ref() == Some(key) {
                                // The cache stores tombstone(None) or value(Some) after a read completes.
                                if self.stats_collector.is_enabled() {
                                    self.stats_collector
                                        .store_stats
                                        .operations
                                        .record_read(parsed.value.is_some());
                                    self.stats_collector
                                        .store_stats
                                        .operations
                                        .record_latency(start.elapsed());
                                }
                                return Ok(parsed.value);
                            }
                            address = parsed.previous_address;
                            continue;
                        }
                        Err(status) => return Err(status),
                    }
                }

                // Record pending statistics
                if self.stats_collector.is_enabled() {
                    self.stats_collector.store_stats.operations.record_pending();
                }
                // Record is on disk: submit an async read request and return Pending.
                let submitted = self.submit_disk_read_for_context(
                    ctx.thread_id,
                    get_thread_tag(),
                    ctx.current_ctx_id(),
                    address,
                );

                ctx.last_pending_read = Some(address);
                if submitted {
                    ctx.on_pending_io_submitted();
                }
                return Err(Status::Pending);
            }

            // Get record from log
            // SAFETY: Address is valid and within memory range
            let record_ptr = unsafe { self.hlog().get(address) };

            if let Some(ptr) = record_ptr {
                let info = unsafe { record_format::record_info_at(ptr.as_ptr()) };
                if info.is_invalid() {
                    address = info.previous_address();
                    continue;
                }
                let page_size = unsafe { self.hlog().page_size() };
                let limit = page_size.saturating_sub(address.offset() as usize);
                let view: RecordView<'_> = unsafe {
                    record_format::record_view_from_memory::<K, V>(address, ptr.as_ptr(), limit)?
                };

                // Check if this is our key
                if <K as PersistKey>::Codec::equals_encoded(view.key_bytes(), key)? {
                    // Check for tombstone
                    if view.is_tombstone() {
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
                    let value_bytes = view.value_bytes().ok_or(Status::Corruption)?;
                    let value = <V as PersistValue>::Codec::decode(value_bytes)?;

                    // If address is in read-only region, consider inserting into cache
                    // for future reads (only for cold data from read-only region)
                    let safe_read_only = unsafe { self.hlog().get_safe_read_only_address() };
                    if address < safe_read_only && self.read_cache.is_some() {
                        // Insert into read cache for future reads
                        let _ = self.try_insert_into_cache(key, &value, address);
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

                    return Ok(Some(value));
                }

                // Follow chain
                address = info.previous_address();
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

    /// Continue a read by traversing an in-memory hash chain starting at `address`.
    ///
    /// This helper is used to resume collision traversal after a disk read yields a mismatched key.
    pub(crate) fn read_chain_from_memory(
        &self,
        key: &K,
        mut address: Address,
    ) -> Result<ChainReadOutcome<V>, Status> {
        while address.is_valid() {
            if address.in_readcache() {
                if let Some(value) = self.try_read_from_cache(address, key) {
                    return Ok(ChainReadOutcome::Completed(Some(value)));
                }
                address = self.skip_read_cache(address);
                continue;
            }

            // SAFETY: read-only access to hybrid log metadata.
            let head_address = unsafe { self.hlog().get_head_address() };
            if address < head_address {
                return Ok(ChainReadOutcome::NeedDiskRead(address));
            }

            let record_ptr = unsafe { self.hlog().get(address) };
            if let Some(ptr) = record_ptr {
                let info = unsafe { record_format::record_info_at(ptr.as_ptr()) };
                if info.is_invalid() {
                    address = info.previous_address();
                    continue;
                }

                let page_size = unsafe { self.hlog().page_size() };
                let limit = page_size.saturating_sub(address.offset() as usize);
                let view: RecordView<'_> = unsafe {
                    record_format::record_view_from_memory::<K, V>(address, ptr.as_ptr(), limit)?
                };

                if <K as PersistKey>::Codec::equals_encoded(view.key_bytes(), key)? {
                    if view.is_tombstone() {
                        return Ok(ChainReadOutcome::Completed(None));
                    }

                    let value_bytes = view.value_bytes().ok_or(Status::Corruption)?;
                    let value = <V as PersistValue>::Codec::decode(value_bytes)?;
                    return Ok(ChainReadOutcome::Completed(Some(value)));
                }

                address = info.previous_address();
            } else {
                break;
            }
        }

        Ok(ChainReadOutcome::Completed(None))
    }

    /// Zero-copy read that returns a `RecordView` into the hybrid log.
    ///
    /// The returned view is only valid while `guard` is held.
    pub(crate) fn read_view_sync<'g>(
        &'g self,
        _ctx: &mut ThreadContext,
        _guard: &'g EpochGuard,
        key: &K,
    ) -> Result<Option<RecordView<'g>>, Status> {
        let hash = KeyHash::new(<K as PersistKey>::Codec::hash(key)?);

        let result = self.hash_index.find_entry(hash);
        if !result.found() {
            return Ok(None);
        }

        let mut address = result.entry.address();
        address = self.skip_read_cache(address);

        while address.is_valid() {
            address = self.skip_read_cache(address);

            // SAFETY: read-only access to hybrid log metadata.
            let head_address = unsafe { self.hlog().get_head_address() };
            if address < head_address {
                // `RecordView` borrows from the in-memory hybrid log. It cannot be backed by
                // disk-resident records without changing the API to return owned bytes.
                return Err(Status::NotSupported);
            }

            let record_ptr = unsafe { self.hlog().get(address) };
            let Some(ptr) = record_ptr else { break };

            let info = unsafe { record_format::record_info_at(ptr.as_ptr()) };
            if info.is_invalid() {
                address = info.previous_address();
                continue;
            }
            let page_size = unsafe { self.hlog().page_size() };
            let limit = page_size.saturating_sub(address.offset() as usize);
            let view: RecordView<'g> = unsafe {
                record_format::record_view_from_memory::<K, V>(address, ptr.as_ptr(), limit)?
            };
            if <K as PersistKey>::Codec::equals_encoded(view.key_bytes(), key)? {
                return Ok(Some(view));
            }
            address = info.previous_address();
        }

        Ok(None)
    }

    /// Internal upsert implementation without statistics recording.
    ///
    /// This is used by both `upsert_sync` and `rmw_sync` to avoid double-counting
    /// operation statistics when RMW creates a new record via upsert.
    fn upsert_internal(&self, ctx: &mut ThreadContext, key: K, value: V) -> (Status, usize) {
        let hash = match <K as PersistKey>::Codec::hash(&key) {
            Ok(h) => KeyHash::new(h),
            Err(s) => return (s, 0),
        };

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

        let (disk_len, alloc_len, key_len, value_len) =
            match record_format::layout_for_ops::<K, V>(&key, Some(&value)) {
                Ok(v) => v,
                Err(s) => return (s, 0),
            };

        // Allocate space in the log
        // SAFETY: Allocation is protected by epoch and internal synchronization
        let address = match unsafe { self.hlog_mut().allocate(alloc_len as u32) } {
            Ok(addr) => addr,
            Err(status) => return (status, 0),
        };

        // Get pointer to the allocated space
        // SAFETY: We just allocated this space, and access is protected by epoch
        let record_ptr = unsafe { self.hlog_mut().get_mut(address) };

        if let Some(ptr) = record_ptr {
            // Initialize the record
            unsafe {
                const U32_BYTES: usize = std::mem::size_of::<u32>();
                const VARLEN_LENGTHS_SIZE: usize = 2 * U32_BYTES;
                let record_info_size = std::mem::size_of::<RecordInfo>();
                let varlen_header_size = record_info_size + VARLEN_LENGTHS_SIZE;

                // Initialize header - use the underlying HybridLog address, not cache address
                let hlog_old_address = self.skip_read_cache(old_address);
                let header =
                    RecordInfo::new(hlog_old_address, ctx.version as u16, true, false, false);
                ptr::write(ptr.as_ptr().cast::<RecordInfo>(), header);

                if record_format::is_fixed_record::<K, V>() {
                    let key_bytes =
                        std::slice::from_raw_parts_mut(ptr.as_ptr().add(record_info_size), key_len);
                    if let Err(s) = <K as PersistKey>::Codec::encode_into(&key, key_bytes) {
                        return (s, 0);
                    }

                    let val_bytes = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(record_info_size + key_len),
                        value_len,
                    );
                    if let Err(s) = <V as PersistValue>::Codec::encode_into(&value, val_bytes) {
                        return (s, 0);
                    }
                } else {
                    // Write lengths (little-endian) then payload bytes.
                    let len_ptr = ptr.as_ptr().add(record_info_size);
                    let key_len_le = (key_len as u32).to_le_bytes();
                    std::ptr::copy_nonoverlapping(key_len_le.as_ptr(), len_ptr, U32_BYTES);
                    let val_len_le = (value_len as u32).to_le_bytes();
                    std::ptr::copy_nonoverlapping(
                        val_len_le.as_ptr(),
                        len_ptr.add(U32_BYTES),
                        U32_BYTES,
                    );

                    let key_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(varlen_header_size),
                        key_len,
                    );
                    if let Err(s) = <K as PersistKey>::Codec::encode_into(&key, key_dst) {
                        return (s, 0);
                    }

                    let val_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(varlen_header_size + key_len),
                        value_len,
                    );
                    if let Err(s) = <V as PersistValue>::Codec::encode_into(&value, val_dst) {
                        return (s, 0);
                    }
                }

                // The record is now fully initialized and can be scanned safely.
                record_format::record_info_at(ptr.as_ptr()).set_invalid(false);
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

            (Status::Ok, disk_len)
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
        let hash = match <K as PersistKey>::Codec::hash(key) {
            Ok(h) => KeyHash::new(h),
            Err(s) => return s,
        };

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

        let (disk_len, alloc_len, key_len, _) =
            match record_format::layout_for_ops::<K, V>(key, None) {
                Ok(v) => v,
                Err(s) => return s,
            };

        // Allocate space in the log
        // SAFETY: Allocation is protected by epoch and internal synchronization
        let address = match unsafe { self.hlog_mut().allocate(alloc_len as u32) } {
            Ok(addr) => addr,
            Err(status) => return status,
        };

        // Get pointer to the allocated space
        // SAFETY: We just allocated this space, and access is protected by epoch
        let record_ptr = unsafe { self.hlog_mut().get_mut(address) };

        if let Some(ptr) = record_ptr {
            unsafe {
                const U32_BYTES: usize = std::mem::size_of::<u32>();
                const VARLEN_LENGTHS_SIZE: usize = 2 * U32_BYTES;
                let record_info_size = std::mem::size_of::<RecordInfo>();
                let varlen_header_size = record_info_size + VARLEN_LENGTHS_SIZE;

                // Initialize header with tombstone flag - use underlying HybridLog address
                let hlog_old_address = self.skip_read_cache(old_address);
                let header =
                    RecordInfo::new(hlog_old_address, ctx.version as u16, true, true, false);
                ptr::write(ptr.as_ptr().cast::<RecordInfo>(), header);

                if record_format::is_fixed_record::<K, V>() {
                    let key_bytes =
                        std::slice::from_raw_parts_mut(ptr.as_ptr().add(record_info_size), key_len);
                    if let Err(s) = <K as PersistKey>::Codec::encode_into(key, key_bytes) {
                        return s;
                    }
                    // Zero the value bytes to avoid persisting uninitialized padding.
                    let val_bytes = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(record_info_size + key_len),
                        <V as PersistValue>::Codec::FIXED_LEN,
                    );
                    val_bytes.fill(0);
                } else {
                    let len_ptr = ptr.as_ptr().add(record_info_size);
                    let key_len_le = (key_len as u32).to_le_bytes();
                    std::ptr::copy_nonoverlapping(key_len_le.as_ptr(), len_ptr, U32_BYTES);
                    let val_len_le = 0u32.to_le_bytes();
                    std::ptr::copy_nonoverlapping(
                        val_len_le.as_ptr(),
                        len_ptr.add(U32_BYTES),
                        U32_BYTES,
                    );

                    let key_dst = std::slice::from_raw_parts_mut(
                        ptr.as_ptr().add(varlen_header_size),
                        key_len,
                    );
                    if let Err(s) = <K as PersistKey>::Codec::encode_into(key, key_dst) {
                        return s;
                    }
                }

                // The tombstone is now fully initialized and can be scanned safely.
                record_format::record_info_at(ptr.as_ptr()).set_invalid(false);
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
                    .record_allocation(disk_len as u64);
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
        let hash = match <K as PersistKey>::Codec::hash(&key) {
            Ok(h) => KeyHash::new(h),
            Err(s) => return s,
        };

        // Find or create entry in hash index
        let result = self.hash_index.find_or_create_entry(hash);

        let _atomic_entry = match result.atomic_entry {
            Some(entry) => entry,
            None => return Status::OutOfMemory,
        };
        let old_address = result.entry.address();

        // Try to find existing record for in-place update (fixed format only).
        if record_format::is_fixed_record::<K, V>()
            && old_address.is_valid()
            && old_address >= unsafe { self.hlog().get_read_only_address() }
        {
            // SAFETY: Address is in mutable region, access is protected by epoch.
            let record_ptr = unsafe { self.hlog_mut().get_mut(old_address) };
            if let Some(ptr) = record_ptr {
                unsafe {
                    let info = record_format::record_info_at(ptr.as_ptr());
                    if !info.is_invalid() {
                        let page_size = self.hlog().page_size();
                        let limit = page_size.saturating_sub(old_address.offset() as usize);
                        let view = match record_format::record_view_from_memory::<K, V>(
                            old_address,
                            ptr.as_ptr(),
                            limit,
                        ) {
                            Ok(v) => v,
                            Err(s) => return s,
                        };
                        let key_matches = match <K as PersistKey>::Codec::equals_encoded(
                            view.key_bytes(),
                            &key,
                        ) {
                            Ok(b) => b,
                            Err(s) => return s,
                        };
                        if key_matches && !view.is_tombstone() {
                            let value_bytes = match view.value_bytes() {
                                Some(b) => b,
                                None => return Status::Corruption,
                            };
                            let mut current = match <V as PersistValue>::Codec::decode(value_bytes)
                            {
                                Ok(v) => v,
                                Err(s) => return s,
                            };
                            if modifier(&mut current) {
                                let val_dst = std::slice::from_raw_parts_mut(
                                    ptr.as_ptr().add(8 + <K as PersistKey>::Codec::FIXED_LEN),
                                    <V as PersistValue>::Codec::FIXED_LEN,
                                );
                                if let Err(s) =
                                    <V as PersistValue>::Codec::encode_into(&current, val_dst)
                                {
                                    return s;
                                }

                                if self.stats_collector.is_enabled() {
                                    self.stats_collector.store_stats.operations.record_rmw();
                                    self.stats_collector
                                        .store_stats
                                        .operations
                                        .record_latency(start.elapsed());
                                }
                                return Status::Ok;
                            }
                        }
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
include!("faster_kv/tests.rs");
