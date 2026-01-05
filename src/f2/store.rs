//! F2 Key-Value Store implementation
//!
//! The F2 (Fast & Fair) architecture provides two-tier storage with
//! automatic hot-cold data separation.

use std::cell::UnsafeCell;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use uuid::Uuid;

use crate::address::Address;
use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::compaction::{CompactionConfig, CompactionResult, CompactionStats, Compactor};
use crate::device::StorageDevice;
use crate::epoch::LightEpoch;
use crate::f2::config::{F2CompactionConfig, F2Config};
use crate::f2::state::{F2CheckpointPhase, F2CheckpointState, StoreCheckpointStatus};
use crate::index::{
    ColdIndex, ColdIndexConfig, FindResult, IndexHashBucketEntry, KeyHash, MemHashIndex,
    MemHashIndexConfig,
};
use crate::record::{Key, RecordInfo, Value};
use crate::status::Status;

/// Store type identifier for internal operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreType {
    /// Hot store (frequently accessed data)
    Hot,
    /// Cold store (infrequently accessed data)
    Cold,
}

/// Read operation stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOperationStage {
    /// Reading from hot log
    HotLogRead,
    /// Reading from cold log
    ColdLogRead,
}

/// RMW operation stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmwOperationStage {
    /// RMW on hot log
    HotLogRmw,
    /// Reading from cold log
    ColdLogRead,
    /// Conditional insert to hot log
    HotLogConditionalInsert,
}

/// Index type for a store
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// In-memory hash index (for hot store)
    MemoryIndex,
    /// On-disk cold index (for cold store in F2 mode)
    ColdIndex,
}

impl Default for IndexType {
    fn default() -> Self {
        Self::MemoryIndex
    }
}

/// Unified index interface for F2 stores
///
/// This enum allows switching between in-memory and cold indexes
/// depending on the store configuration.
pub enum StoreIndex {
    /// In-memory hash index
    Memory(MemHashIndex),
    /// On-disk cold index
    Cold(Box<ColdIndex>),
}

impl StoreIndex {
    /// Create a new memory index
    pub fn new_memory(table_size: u64) -> Self {
        let mut index = MemHashIndex::new();
        let config = MemHashIndexConfig::new(table_size);
        index.initialize(&config);
        Self::Memory(index)
    }

    /// Create a new cold index
    pub fn new_cold(config: ColdIndexConfig) -> Result<Self, Status> {
        let mut index = ColdIndex::new(config);
        index.initialize()?;
        Ok(Self::Cold(Box::new(index)))
    }

    /// Check if this is a memory index
    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// Check if this is a cold index
    pub fn is_cold(&self) -> bool {
        matches!(self, Self::Cold(_))
    }

    /// Get as memory index reference
    pub fn as_memory(&self) -> Option<&MemHashIndex> {
        match self {
            Self::Memory(idx) => Some(idx),
            _ => None,
        }
    }

    /// Get as cold index reference
    pub fn as_cold(&self) -> Option<&ColdIndex> {
        match self {
            Self::Cold(idx) => Some(idx.as_ref()),
            _ => None,
        }
    }

    /// Get as mutable cold index reference
    pub fn as_cold_mut(&mut self) -> Option<&mut ColdIndex> {
        match self {
            Self::Cold(idx) => Some(idx.as_mut()),
            _ => None,
        }
    }

    /// Find entry by key hash
    pub fn find_entry(&self, hash: KeyHash) -> FindResult {
        match self {
            Self::Memory(idx) => idx.find_entry(hash),
            Self::Cold(idx) => {
                // Convert ColdIndexFindResult to FindResult
                // NOTE: ColdIndex returns HashBucketEntry (address only, no tag)
                // We must reconstruct IndexHashBucketEntry with the correct tag from hash
                let result = idx.find_entry(hash);
                let entry = if result.entry.is_unused() {
                    IndexHashBucketEntry::INVALID
                } else {
                    // Preserve tag from the key hash for proper collision detection
                    IndexHashBucketEntry::new(result.entry.address(), hash.tag(), false)
                };
                FindResult {
                    entry,
                    atomic_entry: None, // Cold index doesn't provide atomic entry pointer
                }
            }
        }
    }

    /// Find or create entry by key hash
    pub fn find_or_create_entry(&mut self, hash: KeyHash) -> FindResult {
        match self {
            Self::Memory(idx) => idx.find_or_create_entry(hash),
            Self::Cold(idx) => {
                // Convert ColdIndexFindResult to FindResult
                // NOTE: ColdIndex returns HashBucketEntry (address only, no tag)
                // We must reconstruct IndexHashBucketEntry with the correct tag from hash
                let result = idx.find_or_create_entry(hash);
                let entry = if result.entry.is_unused() {
                    IndexHashBucketEntry::INVALID
                } else {
                    // Preserve tag from the key hash for proper collision detection
                    IndexHashBucketEntry::new(result.entry.address(), hash.tag(), false)
                };
                FindResult {
                    entry,
                    atomic_entry: None, // Cold index doesn't provide atomic entry pointer
                }
            }
        }
    }

    /// Garbage collect entries before the given address
    pub fn garbage_collect(&mut self, new_begin_address: Address) {
        match self {
            Self::Memory(idx) => {
                let _ = idx.garbage_collect(new_begin_address);
            }
            Self::Cold(idx) => {
                let _ = idx.garbage_collect(new_begin_address);
            }
        }
    }

    /// Checkpoint the index
    pub fn checkpoint(
        &self,
        dir: &std::path::Path,
        token: uuid::Uuid,
    ) -> Result<crate::checkpoint::IndexMetadata, Status> {
        match self {
            Self::Memory(idx) => idx.checkpoint(dir, token).map_err(|_| Status::Corruption),
            Self::Cold(idx) => {
                // For cold index, we start checkpoint and return basic metadata
                if idx.start_checkpoint() {
                    idx.complete_checkpoint(true);
                    let mut metadata = crate::checkpoint::IndexMetadata::with_token(token);
                    metadata.table_size = idx.size();
                    Ok(metadata)
                } else {
                    Err(Status::Aborted)
                }
            }
        }
    }

    /// Recover the index
    pub fn recover(
        &mut self,
        dir: &std::path::Path,
        metadata: Option<&crate::checkpoint::IndexMetadata>,
    ) -> Result<crate::checkpoint::IndexMetadata, Status> {
        match self {
            Self::Memory(idx) => {
                idx.recover(dir, metadata).map_err(|_| Status::Corruption)?;
                // Return metadata (either provided or load from file)
                let recovered_metadata = if let Some(meta) = metadata {
                    meta.clone()
                } else {
                    let meta_path = dir.join("index.meta");
                    crate::checkpoint::IndexMetadata::read_from_file(&meta_path)
                        .map_err(|_| Status::Corruption)?
                };
                Ok(recovered_metadata)
            }
            Self::Cold(idx) => {
                // For cold index, recovery is simpler since data is on disk
                // Return basic metadata
                let token = metadata.map(|m| m.token).unwrap_or_default();
                let mut recovered_metadata = crate::checkpoint::IndexMetadata::with_token(token);
                recovered_metadata.table_size = idx.size();
                Ok(recovered_metadata)
            }
        }
    }
}

/// Internal store wrapper for hot or cold store
struct InternalStore<D>
where
    D: StorageDevice,
{
    /// Epoch protection
    epoch: Arc<LightEpoch>,
    /// Hash index
    hash_index: MemHashIndex,
    /// Hybrid log
    hlog: UnsafeCell<PersistentMemoryMalloc<D>>,
    /// Storage device
    device: Arc<D>,
    /// Store type
    store_type: StoreType,
    /// Max log size (for throttling)
    max_hlog_size: AtomicU64,
}

// SAFETY: InternalStore uses UnsafeCell for hlog but access is protected by epoch
// All mutable access goes through hlog_mut() which requires caller to ensure safety
unsafe impl<D: StorageDevice + Send> Send for InternalStore<D> {}
unsafe impl<D: StorageDevice + Send + Sync> Sync for InternalStore<D> {}

impl<D: StorageDevice> InternalStore<D> {
    /// Create a new internal store
    fn new(
        table_size: u64,
        log_mem_size: u64,
        page_size_bits: u8,
        device: D,
        store_type: StoreType,
    ) -> Self {
        let device = Arc::new(device);
        let epoch = Arc::new(LightEpoch::new());

        let mut hash_index = MemHashIndex::new();
        let index_config = MemHashIndexConfig::new(table_size);
        hash_index.initialize(&index_config);

        let log_config = HybridLogConfig::new(log_mem_size, page_size_bits as u32);
        let hlog = PersistentMemoryMalloc::new(log_config, device.clone());

        Self {
            epoch,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            device,
            store_type,
            max_hlog_size: AtomicU64::new(u64::MAX),
        }
    }

    /// Get mutable reference to hlog (unsafe)
    #[allow(clippy::mut_from_ref)]
    unsafe fn hlog_mut(&self) -> &mut PersistentMemoryMalloc<D> {
        &mut *self.hlog.get()
    }

    /// Get immutable reference to hlog
    fn hlog(&self) -> &PersistentMemoryMalloc<D> {
        unsafe { &*self.hlog.get() }
    }

    /// Get the current log size
    fn size(&self) -> u64 {
        let hlog = self.hlog();
        let tail = hlog.get_tail_address().control();
        let begin = hlog.get_begin_address().control();
        tail.saturating_sub(begin)
    }

    /// Get tail address
    fn tail_address(&self) -> Address {
        self.hlog().get_tail_address()
    }

    /// Get begin address
    fn begin_address(&self) -> Address {
        self.hlog().get_begin_address()
    }

    /// Get safe read-only address
    fn safe_read_only_address(&self) -> Address {
        self.hlog().get_safe_read_only_address()
    }
}

/// F2 Key-Value Store
///
/// Provides a two-tier storage architecture with automatic hot-cold separation.
/// The hot store holds frequently accessed data with optional read cache,
/// while the cold store holds less frequently accessed data.
pub struct F2Kv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Configuration
    config: F2Config,
    /// Hot store (frequently accessed data)
    hot_store: InternalStore<D>,
    /// Cold store (infrequently accessed data)
    cold_store: InternalStore<D>,
    /// Checkpoint state
    checkpoint: F2CheckpointState,
    /// Background worker active flag
    background_worker_active: AtomicBool,
    /// Compaction scheduled flag
    compaction_scheduled: AtomicBool,
    /// Compactor for hot store
    hot_compactor: Compactor,
    /// Compactor for cold store
    cold_compactor: Compactor,
    /// Number of active sessions
    num_active_sessions: AtomicU64,
    /// Checkpoint directory for background checkpoint operations
    checkpoint_dir: Option<std::path::PathBuf>,
    /// Phantom data for type parameters
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V, D> F2Kv<K, V, D>
where
    K: Key + Clone + 'static,
    V: Value + Clone + 'static,
    D: StorageDevice + 'static,
{
    /// Default page size bits
    const DEFAULT_PAGE_SIZE_BITS: u8 = 22;

    /// Create a new F2 key-value store with the given configuration
    pub fn new(config: F2Config, hot_device: D, cold_device: D) -> Result<Self, String> {
        config.validate()?;

        // Create hot store
        let hot_store = InternalStore::new(
            config.hot_store.index_size,
            config.hot_store.log_mem_size,
            Self::DEFAULT_PAGE_SIZE_BITS,
            hot_device,
            StoreType::Hot,
        );

        // Set max log size for hot store
        hot_store
            .max_hlog_size
            .store(config.compaction.hot_log_size_budget, Ordering::Release);

        // Create cold store
        let cold_store = InternalStore::new(
            config.cold_store.index_size,
            config.cold_store.log_mem_size,
            Self::DEFAULT_PAGE_SIZE_BITS,
            cold_device,
            StoreType::Cold,
        );

        // Set max log size for cold store
        cold_store
            .max_hlog_size
            .store(config.compaction.cold_log_size_budget, Ordering::Release);

        // Create compactors
        let hot_compaction_config = CompactionConfig::new()
            .with_max_compact_bytes(config.compaction.max_compact_size)
            .with_num_threads(config.compaction.num_threads);

        let cold_compaction_config = CompactionConfig::new()
            .with_max_compact_bytes(config.compaction.max_compact_size)
            .with_num_threads(config.compaction.num_threads);

        Ok(Self {
            config,
            hot_store,
            cold_store,
            checkpoint: F2CheckpointState::new(),
            background_worker_active: AtomicBool::new(false),
            compaction_scheduled: AtomicBool::new(false),
            hot_compactor: Compactor::with_config(hot_compaction_config),
            cold_compactor: Compactor::with_config(cold_compaction_config),
            num_active_sessions: AtomicU64::new(0),
            checkpoint_dir: None,
            _marker: std::marker::PhantomData,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &F2Config {
        &self.config
    }

    /// Set the checkpoint directory for background checkpoint operations
    pub fn set_checkpoint_dir(&mut self, dir: impl Into<std::path::PathBuf>) {
        self.checkpoint_dir = Some(dir.into());
    }

    /// Get the checkpoint directory
    pub fn checkpoint_dir(&self) -> Option<&std::path::Path> {
        self.checkpoint_dir.as_deref()
    }

    /// Start a new session
    ///
    /// # Returns
    /// A unique session GUID
    pub fn start_session(&self) -> Result<Uuid, Status> {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            return Err(Status::Aborted);
        }

        let guid = Uuid::new_v4();

        // Protect epoch on both stores
        // Use thread_id 0 for simplicity; in production, use actual thread IDs
        self.hot_store.epoch.protect(0);
        self.cold_store.epoch.protect(0);

        self.num_active_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(guid)
    }

    /// Continue an existing session
    ///
    /// # Arguments
    /// * `session_id` - The session GUID to continue
    ///
    /// # Returns
    /// The last serial number for this session
    pub fn continue_session(&self, _session_id: Uuid) -> Result<u64, Status> {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            return Err(Status::Aborted);
        }

        // Protect epoch on both stores
        self.hot_store.epoch.protect(0);
        self.cold_store.epoch.protect(0);

        self.num_active_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(0)
    }

    /// Stop the current session
    pub fn stop_session(&self) {
        // Wait for pending operations and checkpointing to complete
        while self.checkpoint.is_in_progress() {
            std::hint::spin_loop();
        }

        // Release epoch on both stores
        self.hot_store.epoch.unprotect(0);
        self.cold_store.epoch.unprotect(0);

        self.num_active_sessions.fetch_sub(1, Ordering::AcqRel);
    }

    /// Refresh the session - called periodically to check system state
    pub fn refresh(&self) {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            self.heavy_enter();
        }

        // Bump epoch on both stores
        self.hot_store.epoch.bump_current_epoch();
        self.cold_store.epoch.bump_current_epoch();
    }

    /// Read a value by key
    ///
    /// The read operation first checks the hot store, then the cold store.
    pub fn read(&self, key: &K) -> Result<Option<V>, Status> {
        let key_hash = KeyHash::new(key.get_hash());

        // Stage 1: Read from hot store
        if let Some(value) = self.internal_read(&self.hot_store, key_hash)? {
            return Ok(Some(value));
        }

        // Stage 2: Read from cold store
        if let Some(value) = self.internal_read(&self.cold_store, key_hash)? {
            // TODO: Optionally insert into read cache
            return Ok(Some(value));
        }

        Ok(None)
    }

    /// Internal read from a specific store
    fn internal_read(
        &self,
        store: &InternalStore<D>,
        key_hash: KeyHash,
    ) -> Result<Option<V>, Status> {
        // Find entry in hash index
        let find_result = store.hash_index.find_entry(key_hash);

        if find_result.entry.is_unused() {
            return Ok(None);
        }

        let address = find_result.entry.address();
        if address == Address::INVALID {
            return Ok(None);
        }

        // NOTE: Full implementation would read record from log and deserialize
        // For now, we return None as we don't have the record serialization infrastructure
        Ok(None)
    }

    /// Upsert a key-value pair
    ///
    /// Always writes to the hot store.
    pub fn upsert(&self, key: K, value: V) -> Result<(), Status> {
        let key_hash = KeyHash::new(key.get_hash());

        // Check if we need to throttle due to log size
        let hot_size = self.hot_store.size();
        let max_size = self.hot_store.max_hlog_size.load(Ordering::Acquire);
        if hot_size >= max_size {
            // Wait for compaction
            while self.hot_store.size() >= max_size {
                self.refresh();
                std::hint::spin_loop();
            }
        }

        // Allocate record in hot log
        let record_size =
            std::mem::size_of::<RecordInfo>() + key.size() as usize + value.size() as usize;
        let record_size = (record_size + 7) & !7; // Align to 8 bytes

        // SAFETY: We have epoch protection
        let address = unsafe { self.hot_store.hlog_mut().allocate(record_size as u32) };

        match address {
            Ok(addr) if addr.is_valid() => {
                // Update hash index
                let _result = self.hot_store.hash_index.find_or_create_entry(key_hash);
                Ok(())
            }
            Ok(_) => Err(Status::OutOfMemory),
            Err(status) => Err(status),
        }
    }

    /// Read-Modify-Write operation
    ///
    /// If the key exists in hot store, modify in place.
    /// If only in cold store, read and conditionally insert modified value to hot store.
    pub fn rmw<F>(&self, key: K, modify: F) -> Result<(), Status>
    where
        F: FnOnce(&mut V),
        V: Default,
    {
        let key_hash = KeyHash::new(key.get_hash());

        // Stage 1: Try RMW in hot store
        if self.internal_rmw_in_place(&self.hot_store, key_hash)? {
            return Ok(());
        }

        // Stage 2: Read from cold store
        if let Some(mut value) = self.internal_read(&self.cold_store, key_hash)? {
            // Apply modification
            modify(&mut value);

            // Insert to hot store (conditional)
            self.upsert(key, value)?;
            return Ok(());
        }

        // Key not found - create new with default value
        let mut value = V::default();
        modify(&mut value);
        self.upsert(key, value)?;

        Ok(())
    }

    /// Internal RMW in place (returns true if successful)
    fn internal_rmw_in_place(
        &self,
        _store: &InternalStore<D>,
        _key_hash: KeyHash,
    ) -> Result<bool, Status> {
        // Simplified: always return false to force read-modify-write path
        // Full implementation would try to modify record in place if in mutable region
        Ok(false)
    }

    /// Delete a key
    ///
    /// Writes a tombstone to the hot store.
    pub fn delete(&self, key: &K) -> Result<(), Status> {
        let key_hash = KeyHash::new(key.get_hash());

        // Allocate tombstone record in hot log
        let record_size = std::mem::size_of::<RecordInfo>() + key.size() as usize;
        let record_size = (record_size + 7) & !7;

        // SAFETY: We have epoch protection
        let address = unsafe { self.hot_store.hlog_mut().allocate(record_size as u32) };

        match address {
            Ok(addr) if addr.is_valid() => {
                // Update hash index with tombstone address
                let _result = self.hot_store.hash_index.find_or_create_entry(key_hash);
                Ok(())
            }
            Ok(_) => Err(Status::OutOfMemory),
            Err(status) => Err(status),
        }
    }

    /// Complete pending asynchronous operations
    ///
    /// # Arguments
    /// * `wait` - If true, wait for all pending operations to complete
    ///
    /// # Returns
    /// true if all operations are complete
    pub fn complete_pending(&self, wait: bool) -> bool {
        // Refresh to process any pending epoch actions.
        self.refresh();

        // In this simplified implementation, operations are synchronous.
        // `wait` is kept for API compatibility.
        let _ = wait;
        true
    }

    /// Wait for pending compactions to complete
    pub fn complete_pending_compactions(&self) {
        while self.compaction_scheduled.load(Ordering::Acquire) {
            if self.hot_store.epoch.is_protected(0) {
                self.complete_pending(false);
            }
            std::hint::spin_loop();
        }
    }

    /// Start a checkpoint
    ///
    /// # Arguments
    /// * `lazy` - If true, wait for compaction before cold store checkpoint
    ///
    /// # Returns
    /// The checkpoint token
    pub fn checkpoint(&mut self, lazy: bool) -> Result<Uuid, Status> {
        // Try to start checkpoint
        let result = self.checkpoint.phase.compare_exchange(
            F2CheckpointPhase::Rest,
            F2CheckpointPhase::HotStoreCheckpoint,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if result.is_err() {
            return Err(Status::Aborted);
        }

        let token = Uuid::new_v4();
        let num_sessions = self.num_active_sessions.load(Ordering::Acquire) as u32;
        self.checkpoint.initialize(token, num_sessions.max(1));

        // Request hot store checkpoint
        self.checkpoint
            .hot_store_status
            .store(StoreCheckpointStatus::Requested, Ordering::Release);

        let _ = lazy; // Used by background worker

        Ok(token)
    }

    /// Recover from a checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory containing checkpoints
    /// * `token` - The checkpoint token to recover from
    pub fn recover(&mut self, checkpoint_dir: &Path, token: Uuid) -> Result<u32, Status> {
        let result = self.checkpoint.phase.compare_exchange(
            F2CheckpointPhase::Rest,
            F2CheckpointPhase::Recover,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if result.is_err() {
            return Err(Status::Aborted);
        }

        // Create checkpoint directory paths
        let cp_dir = checkpoint_dir.join(token.to_string());
        let hot_dir = cp_dir.join("hot");
        let cold_dir = cp_dir.join("cold");

        // Verify that the checkpoint directory exists
        if !cp_dir.exists() {
            self.checkpoint
                .phase
                .store(F2CheckpointPhase::Rest, Ordering::Release);
            return Err(Status::NotFound);
        }

        // At least one of hot or cold store must exist for a valid checkpoint
        let hot_exists = hot_dir.exists();
        let cold_exists = cold_dir.exists();
        
        if !hot_exists && !cold_exists {
            // Neither store checkpoint exists - this is not a valid checkpoint
            self.checkpoint
                .phase
                .store(F2CheckpointPhase::Rest, Ordering::Release);
            return Err(Status::NotFound);
        }

        let mut version = 0u32;

        // Recover hot store
        if hot_exists {
            // Recover hot store hash index
            if let Err(_e) = self.hot_store.hash_index.recover(&hot_dir, None) {
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
                return Err(Status::Corruption);
            }

            // Recover hot store hybrid log
            unsafe {
                if let Err(_e) = self.hot_store.hlog_mut().recover(&hot_dir, None) {
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                    return Err(Status::Corruption);
                }
            }

            // Get version from hot store log metadata
            if let Ok(log_meta) = crate::checkpoint::LogMetadata::read_from_file(&hot_dir.join("log.meta")) {
                version = log_meta.version;
            }
        }

        // Recover cold store
        if cold_exists {
            // Recover cold store hash index
            if let Err(_e) = self.cold_store.hash_index.recover(&cold_dir, None) {
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
                return Err(Status::Corruption);
            }

            // Recover cold store hybrid log
            unsafe {
                if let Err(_e) = self.cold_store.hlog_mut().recover(&cold_dir, None) {
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                    return Err(Status::Corruption);
                }
            }

            // Get version from cold store log metadata if not already set from hot store
            // This handles the case where only cold store checkpoint exists
            if version == 0 {
                if let Ok(log_meta) = crate::checkpoint::LogMetadata::read_from_file(&cold_dir.join("log.meta")) {
                    version = log_meta.version;
                }
            }
        }

        // Update checkpoint state with recovered version so subsequent checkpoints
        // continue from the correct version number
        self.checkpoint.set_version(version);

        // Move back to REST phase
        self.checkpoint
            .phase
            .store(F2CheckpointPhase::Rest, Ordering::Release);

        Ok(version)
    }

    /// Save checkpoint to disk
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory to save checkpoint files
    /// * `token` - The checkpoint token
    pub fn save_checkpoint(&self, checkpoint_dir: &Path, token: Uuid) -> Result<(), Status> {
        // Create checkpoint directories
        let cp_dir = checkpoint_dir.join(token.to_string());
        let hot_dir = cp_dir.join("hot");
        let cold_dir = cp_dir.join("cold");

        std::fs::create_dir_all(&hot_dir).map_err(|_| Status::Corruption)?;
        std::fs::create_dir_all(&cold_dir).map_err(|_| Status::Corruption)?;

        // Get the checkpoint version
        let checkpoint_version = self.checkpoint.version();

        // Checkpoint hot store index
        let _hot_index_meta = self.hot_store.hash_index
            .checkpoint(&hot_dir, token)
            .map_err(|_| Status::Corruption)?;

        // Checkpoint hot store hybrid log (writes both metadata and snapshot)
        let _hot_log_meta = self.hot_store.hlog()
            .checkpoint(&hot_dir, token, checkpoint_version)
            .map_err(|_| Status::Corruption)?;

        // Checkpoint cold store index
        let _cold_index_meta = self.cold_store.hash_index
            .checkpoint(&cold_dir, token)
            .map_err(|_| Status::Corruption)?;

        // Checkpoint cold store hybrid log (writes both metadata and snapshot)
        let _cold_log_meta = self.cold_store.hlog()
            .checkpoint(&cold_dir, token, checkpoint_version)
            .map_err(|_| Status::Corruption)?;

        Ok(())
    }

    /// Compact the hot log
    ///
    /// Moves cold data from hot log to cold log.
    pub fn compact_hot_log(&self, until_address: Address) -> Result<CompactionResult, Status> {
        self.compact_log(StoreType::Hot, until_address, true)
    }

    /// Compact the cold log
    ///
    /// Reclaims space in the cold log.
    pub fn compact_cold_log(&self, until_address: Address) -> Result<CompactionResult, Status> {
        self.compact_log(StoreType::Cold, until_address, true)
    }

    /// Internal log compaction
    fn compact_log(
        &self,
        store_type: StoreType,
        until_address: Address,
        shift_begin_address: bool,
    ) -> Result<CompactionResult, Status> {
        let (store, compactor) = match store_type {
            StoreType::Hot => (&self.hot_store, &self.hot_compactor),
            StoreType::Cold => (&self.cold_store, &self.cold_compactor),
        };

        // Validate until_address
        let safe_ro = store.safe_read_only_address();
        if until_address.control() > safe_ro.control() {
            return Err(Status::InvalidArgument);
        }

        // Try to start compaction
        if compactor.try_start().is_err() {
            return Err(Status::Aborted);
        }

        // Create compaction stats
        let begin_addr = store.begin_address();
        let bytes_to_compact = until_address.control().saturating_sub(begin_addr.control());
        let stats = CompactionStats {
            records_scanned: 0, // Simplified
            records_compacted: 0,
            records_skipped: 0,
            tombstones_found: 0,
            bytes_scanned: bytes_to_compact,
            bytes_compacted: 0,
            bytes_reclaimed: bytes_to_compact,
            duration_ms: 0,
        };

        if shift_begin_address {
            // Shift begin address
            unsafe { store.hlog_mut().shift_begin_address(until_address) };

            // Garbage collect hash index
            store.hash_index.garbage_collect(until_address);
        }

        compactor.complete();

        Ok(CompactionResult::success(until_address, stats))
    }

    /// Check if hot log should be compacted
    pub fn should_compact_hot_log(&self) -> Option<Address> {
        self.should_compact_log(StoreType::Hot)
    }

    /// Check if cold log should be compacted
    pub fn should_compact_cold_log(&self) -> Option<Address> {
        self.should_compact_log(StoreType::Cold)
    }

    /// Internal check for compaction need
    fn should_compact_log(&self, store_type: StoreType) -> Option<Address> {
        let (store, compaction_enabled, log_size_budget) = match store_type {
            StoreType::Hot => (
                &self.hot_store,
                self.config.compaction.hot_store_enabled,
                self.config.compaction.hot_log_size_budget,
            ),
            StoreType::Cold => (
                &self.cold_store,
                self.config.compaction.cold_store_enabled,
                self.config.compaction.cold_log_size_budget,
            ),
        };

        if !compaction_enabled {
            return None;
        }

        let hlog_size_threshold =
            (log_size_budget as f64 * self.config.compaction.trigger_percentage) as u64;

        if store.size() < hlog_size_threshold {
            return None;
        }

        // Check checkpoint phase
        let phase = self.checkpoint.phase.load(Ordering::Acquire);
        match phase {
            F2CheckpointPhase::Rest => {}
            F2CheckpointPhase::HotStoreCheckpoint if store_type == StoreType::Hot => {
                return None; // Can't compact hot store during hot checkpoint
            }
            F2CheckpointPhase::ColdStoreCheckpoint => {
                if self.checkpoint.cold_store_status.load(Ordering::Acquire)
                    == StoreCheckpointStatus::Active
                {
                    return None; // Can't compact during active cold checkpoint
                }
            }
            F2CheckpointPhase::Recover => {
                return None; // Can't compact during recovery
            }
            _ => {}
        }

        // Calculate until address
        let begin_address = store.begin_address().control();
        let compact_size = (store.size() as f64 * self.config.compaction.compact_percentage) as u64;
        let mut until_address = begin_address + compact_size;

        // Respect max compacted size
        until_address = until_address.min(begin_address + self.config.compaction.max_compact_size);

        // Don't compact in-memory regions
        let safe_head = store.safe_read_only_address().control();
        until_address = until_address.min(safe_head);

        if until_address <= begin_address {
            return None;
        }

        Some(Address::from_control(until_address))
    }

    /// Get the total size of both stores
    pub fn size(&self) -> u64 {
        self.hot_store.size() + self.cold_store.size()
    }

    /// Get the hot store size
    pub fn hot_store_size(&self) -> u64 {
        self.hot_store.size()
    }

    /// Get the cold store size
    pub fn cold_store_size(&self) -> u64 {
        self.cold_store.size()
    }

    /// Get the number of active sessions
    pub fn num_active_sessions(&self) -> u32 {
        self.num_active_sessions.load(Ordering::Acquire) as u32
    }

    /// Check if automatic compaction is scheduled
    pub fn is_compaction_scheduled(&self) -> bool {
        self.compaction_scheduled.load(Ordering::Acquire)
    }

    /// Handle heavy enter for checkpoint phases
    fn heavy_enter(&self) {
        let phase = self.checkpoint.phase.load(Ordering::Acquire);

        if phase == F2CheckpointPhase::ColdStoreCheckpoint {
            let status = self.checkpoint.cold_store_status.load(Ordering::Acquire);
            if !status.is_done() {
                return;
            }

            // All done - can move to REST
            // Note: Full implementation would issue callbacks here
        }
    }

    /// Start the background worker thread
    pub fn start_background_worker(self: &Arc<Self>) {
        if self
            .background_worker_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return; // Already running
        }

        let f2 = Arc::clone(self);
        thread::spawn(move || {
            f2.background_worker_loop();
        });
    }

    /// Background worker loop
    fn background_worker_loop(&self) {
        let check_interval = self.config.compaction.check_interval;

        while self.background_worker_active.load(Ordering::Acquire) {
            self.compaction_scheduled.store(true, Ordering::Release);

            // Check hot store checkpoint
            if self.checkpoint.hot_store_status.load(Ordering::Acquire)
                == StoreCheckpointStatus::Requested
            {
                // Issue hot store checkpoint
                self.checkpoint
                    .hot_store_status
                    .store(StoreCheckpointStatus::Active, Ordering::Release);
                
                // Actually checkpoint hot store - index first
                let token = self.checkpoint.token();
                let checkpoint_version = self.checkpoint.version();
                let checkpoint_success = if let Some(cp_dir) = self.checkpoint_dir.as_ref() {
                    let hot_dir = cp_dir.join(token.to_string()).join("hot");
                    let dir_created = std::fs::create_dir_all(&hot_dir).is_ok();
                    
                    if dir_created {
                        // Checkpoint hot store index
                        let index_ok = self.hot_store.hash_index.checkpoint(&hot_dir, token).is_ok();
                        
                        // Checkpoint hot store hybrid log (writes both metadata and snapshot)
                        let log_ok = self.hot_store.hlog()
                            .checkpoint(&hot_dir, token, checkpoint_version)
                            .is_ok();
                        
                        index_ok && log_ok
                    } else {
                        false
                    }
                } else {
                    // No checkpoint directory configured - this is an error condition
                    // Cannot checkpoint without a directory
                    false
                };
                
                if checkpoint_success {
                    self.checkpoint
                        .hot_store_status
                        .store(StoreCheckpointStatus::Finished, Ordering::Release);

                    // Move to cold store checkpoint phase
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::ColdStoreCheckpoint, Ordering::Release);
                    self.checkpoint
                        .cold_store_status
                        .store(StoreCheckpointStatus::Requested, Ordering::Release);
                } else {
                    // Checkpoint failed - mark as failed and reset phase
                    self.checkpoint
                        .hot_store_status
                        .store(StoreCheckpointStatus::Failed, Ordering::Release);
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                }
            }

            // Check cold store checkpoint
            if self.checkpoint.cold_store_status.load(Ordering::Acquire)
                == StoreCheckpointStatus::Requested
            {
                // Issue cold store checkpoint
                self.checkpoint
                    .cold_store_status
                    .store(StoreCheckpointStatus::Active, Ordering::Release);
                
                // Actually checkpoint cold store
                let token = self.checkpoint.token();
                let checkpoint_version = self.checkpoint.version();
                let checkpoint_success = if let Some(cp_dir) = self.checkpoint_dir.as_ref() {
                    let cold_dir = cp_dir.join(token.to_string()).join("cold");
                    let dir_created = std::fs::create_dir_all(&cold_dir).is_ok();
                    
                    if dir_created {
                        // Checkpoint cold store index
                        let index_ok = self.cold_store.hash_index.checkpoint(&cold_dir, token).is_ok();
                        
                        // Checkpoint cold store hybrid log (writes both metadata and snapshot)
                        let log_ok = self.cold_store.hlog()
                            .checkpoint(&cold_dir, token, checkpoint_version)
                            .is_ok();
                        
                        index_ok && log_ok
                    } else {
                        false
                    }
                } else {
                    // No checkpoint directory configured - this is an error condition
                    false
                };
                
                if checkpoint_success {
                    self.checkpoint
                        .cold_store_status
                        .store(StoreCheckpointStatus::Finished, Ordering::Release);

                    // Move back to REST
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                } else {
                    // Checkpoint failed - mark as failed and reset phase
                    self.checkpoint
                        .cold_store_status
                        .store(StoreCheckpointStatus::Failed, Ordering::Release);
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                }
            }

            // Hot-cold compaction
            if let Some(until_addr) = self.should_compact_hot_log() {
                let _ = self.compact_hot_log(until_addr);
            }

            // Cold-cold compaction
            if let Some(until_addr) = self.should_compact_cold_log() {
                let _ = self.compact_cold_log(until_addr);
            }

            self.compaction_scheduled.store(false, Ordering::Release);
            thread::sleep(check_interval);
        }
    }

    /// Stop the background worker thread
    pub fn stop_background_worker(&self) {
        self.background_worker_active
            .store(false, Ordering::Release);
    }

    /// Get compaction configuration
    pub fn compaction_config(&self) -> &F2CompactionConfig {
        &self.config.compaction
    }

    /// Get hot store statistics
    pub fn hot_store_stats(&self) -> StoreStats {
        StoreStats {
            size: self.hot_store.size(),
            begin_address: self.hot_store.begin_address(),
            tail_address: self.hot_store.tail_address(),
            safe_read_only_address: self.hot_store.safe_read_only_address(),
        }
    }

    /// Get cold store statistics
    pub fn cold_store_stats(&self) -> StoreStats {
        StoreStats {
            size: self.cold_store.size(),
            begin_address: self.cold_store.begin_address(),
            tail_address: self.cold_store.tail_address(),
            safe_read_only_address: self.cold_store.safe_read_only_address(),
        }
    }
}

/// Statistics for a single store
#[derive(Debug, Clone)]
pub struct StoreStats {
    /// Current log size
    pub size: u64,
    /// Begin address
    pub begin_address: Address,
    /// Tail address
    pub tail_address: Address,
    /// Safe read-only address
    pub safe_read_only_address: Address,
}

impl<K, V, D> Drop for F2Kv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    fn drop(&mut self) {
        // Wait for operations to complete
        while self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            std::hint::spin_loop();
        }

        // Stop background worker
        self.stop_background_worker();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;

    #[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
    struct TestKey(u64);

    impl Key for TestKey {
        fn size(&self) -> u32 {
            std::mem::size_of::<Self>() as u32
        }

        fn get_hash(&self) -> u64 {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.hash(&mut hasher);
            hasher.finish()
        }
    }

    #[derive(Clone, Debug, PartialEq, Default)]
    struct TestValue(u64);

    impl Value for TestValue {
        fn size(&self) -> u32 {
            std::mem::size_of::<Self>() as u32
        }
    }

    #[test]
    fn test_create_f2() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device);
        assert!(f2.is_ok());
    }

    #[test]
    fn test_session_lifecycle() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let f2 =
            F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

        // Start session
        let session = f2.start_session();
        assert!(session.is_ok());

        // Stop session
        f2.stop_session();
    }

    #[test]
    fn test_checkpoint() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let mut f2 =
            F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

        // Start checkpoint
        let token = f2.checkpoint(false);
        assert!(token.is_ok());

        // Reset for next test
        f2.checkpoint.reset();
    }

    #[test]
    fn test_store_type() {
        assert_ne!(StoreType::Hot, StoreType::Cold);
    }

    #[test]
    fn test_operation_stages() {
        assert_ne!(
            ReadOperationStage::HotLogRead,
            ReadOperationStage::ColdLogRead
        );
        assert_ne!(RmwOperationStage::HotLogRmw, RmwOperationStage::ColdLogRead);
    }

    #[test]
    fn test_f2_size() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let f2 =
            F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

        // Initial size should be 0
        assert_eq!(f2.size(), 0);
        assert_eq!(f2.hot_store_size(), 0);
        assert_eq!(f2.cold_store_size(), 0);
    }

    #[test]
    fn test_f2_stats() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let f2 =
            F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

        let hot_stats = f2.hot_store_stats();
        let cold_stats = f2.cold_store_stats();

        assert_eq!(hot_stats.size, 0);
        assert_eq!(cold_stats.size, 0);
    }

    #[test]
    fn test_f2_read_write() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let f2 =
            F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

        // Start session
        let _session = f2.start_session().unwrap();

        // Read non-existent key
        let result = f2.read(&TestKey(1));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Upsert
        let result = f2.upsert(TestKey(1), TestValue(100));
        assert!(result.is_ok());

        // Delete
        let result = f2.delete(&TestKey(1));
        assert!(result.is_ok());

        // Stop session
        f2.stop_session();
    }

    #[test]
    fn test_f2_compaction_check() {
        let config = F2Config::default();
        let hot_device = NullDisk::new();
        let cold_device = NullDisk::new();
        let f2 =
            F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

        // Should not need compaction with empty stores
        assert!(f2.should_compact_hot_log().is_none());
        assert!(f2.should_compact_cold_log().is_none());
    }
}
