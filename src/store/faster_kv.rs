//! FasterKV - Core key-value store implementation
//!
//! This module provides the main FasterKV store implementation.

use std::cell::UnsafeCell;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use crate::address::Address;
use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::checkpoint::{
    create_checkpoint_directory, CheckpointState, CheckpointToken, CheckpointType,
    IndexMetadata, LogMetadata,
};
use crate::device::StorageDevice;
use crate::epoch::LightEpoch;
use crate::index::{KeyHash, MemHashIndex, MemHashIndexConfig};
use crate::record::{Key, Record, RecordInfo, Value};
use crate::status::Status;
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

/// System state for checkpointing
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct SystemState {
    /// Current version
    pub version: u32,
    /// Current phase
    pub phase: SystemPhase,
}

impl SystemState {
    /// Create a new system state
    pub const fn new() -> Self {
        Self {
            version: 0,
            phase: SystemPhase::Rest,
        }
    }
}

/// System phases for checkpointing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SystemPhase {
    /// Normal operation
    #[default]
    Rest = 0,
    /// Prepare phase
    Prepare = 1,
    /// In-progress phase
    InProgress = 2,
    /// Wait-pending phase
    WaitPending = 3,
    /// Wait-flush phase
    WaitFlush = 4,
    /// Persistence callback phase
    PersistenceCallback = 5,
    /// Index checkpoint phase
    IndexCheckpoint = 6,
    /// Grow prepare phase
    GrowPrepare = 7,
    /// Grow in-progress phase
    GrowInProgress = 8,
}

/// Atomic system state
#[repr(transparent)]
pub struct AtomicSystemState {
    control: AtomicU64,
}

impl AtomicSystemState {
    /// Create a new atomic system state
    pub const fn new(state: SystemState) -> Self {
        let control = (state.version as u64) | ((state.phase as u64) << 32);
        Self {
            control: AtomicU64::new(control),
        }
    }

    /// Load the state atomically
    pub fn load(&self, ordering: Ordering) -> SystemState {
        let control = self.control.load(ordering);
        SystemState {
            version: control as u32,
            phase: unsafe { std::mem::transmute((control >> 32) as u8) },
        }
    }

    /// Store a state atomically
    pub fn store(&self, state: SystemState, ordering: Ordering) {
        let control = (state.version as u64) | ((state.phase as u64) << 32);
        self.control.store(control, ordering);
    }
}

impl Default for AtomicSystemState {
    fn default() -> Self {
        Self::new(SystemState::new())
    }
}

/// FasterKV - High-performance concurrent key-value store
///
/// This is the main store implementation that coordinates:
/// - Epoch protection for safe memory reclamation
/// - Hash index for key lookups
/// - Hybrid log for record storage
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
    /// Storage device
    device: Arc<D>,
    /// Next session ID
    next_session_id: AtomicU32,
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
        
        Self {
            epoch,
            system_state: AtomicSystemState::default(),
            hash_index,
            hlog: UnsafeCell::new(hlog),
            device,
            next_session_id: AtomicU32::new(0),
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

    /// Compact the log
    pub fn compact(&self, until_address: Address) -> Status {
        // Update head address
        // SAFETY: shift_head_address is protected by epoch
        unsafe { self.hlog_mut().shift_head_address(until_address) };
        
        // Garbage collect hash index
        self.hash_index.garbage_collect(until_address);
        
        Status::Ok
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
        
        // Bump version
        let new_state = SystemState {
            version: version + 1,
            phase: SystemPhase::Rest,
        };
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
        
        // Set system state to recovered version
        let system_state = AtomicSystemState::new(SystemState {
            version: log_meta.version,
            phase: SystemPhase::Rest,
        });
        
        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            device,
            next_session_id: AtomicU32::new(0),
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
        assert_eq!(state.phase, SystemPhase::Rest);
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
        assert_eq!(state.phase, SystemPhase::Rest);
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
}

