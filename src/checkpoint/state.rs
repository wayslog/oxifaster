//! Checkpoint state management for FASTER
//!
//! This module provides checkpoint state tracking and metadata management.
//! 
//! Based on C++ FASTER's checkpoint_state.h implementation.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use uuid::Uuid;

use crate::address::Address;
use crate::checkpoint::CheckpointToken;

/// Index metadata for checkpoint
/// 
/// Corresponds to C++ FASTER's IndexMetadata class.
#[derive(Debug, Clone, Default)]
pub struct IndexMetadata {
    /// Checkpoint token
    pub token: CheckpointToken,
    /// Index version
    pub version: u32,
    /// Hash table size (number of buckets)
    pub table_size: u64,
    /// Number of bytes in the hash table
    pub num_ht_bytes: u64,
    /// Number of bytes in overflow buckets
    pub num_ofb_bytes: u64,
    /// Number of overflow buckets used
    pub num_buckets: u64,
    /// Number of entries (keys) in the index
    pub num_entries: u64,
    /// Earliest valid address in the log
    pub log_begin_address: Address,
    /// Address at which this checkpoint was taken
    pub checkpoint_start_address: Address,
}

impl IndexMetadata {
    /// Create a new index metadata
    pub fn new() -> Self {
        Self {
            token: Uuid::new_v4(),
            version: 0,
            table_size: 0,
            num_ht_bytes: 0,
            num_ofb_bytes: 0,
            num_buckets: 0,
            num_entries: 0,
            log_begin_address: Address::INVALID,
            checkpoint_start_address: Address::INVALID,
        }
    }

    /// Initialize with a specific token
    pub fn with_token(token: CheckpointToken) -> Self {
        Self {
            token,
            version: 0,
            table_size: 0,
            num_ht_bytes: 0,
            num_ofb_bytes: 0,
            num_buckets: 0,
            num_entries: 0,
            log_begin_address: Address::INVALID,
            checkpoint_start_address: Address::INVALID,
        }
    }

    /// Initialize the metadata with checkpoint parameters
    pub fn initialize(
        &mut self,
        version: u32,
        table_size: u64,
        log_begin_address: Address,
        checkpoint_start_address: Address,
    ) {
        self.version = version;
        self.table_size = table_size;
        self.log_begin_address = log_begin_address;
        self.checkpoint_start_address = checkpoint_start_address;
        self.num_ht_bytes = 0;
        self.num_ofb_bytes = 0;
    }

    /// Reset the metadata to default values
    pub fn reset(&mut self) {
        self.version = 0;
        self.table_size = 0;
        self.num_ht_bytes = 0;
        self.num_ofb_bytes = 0;
        self.num_buckets = 0;
        self.num_entries = 0;
        self.log_begin_address = Address::INVALID;
        self.checkpoint_start_address = Address::INVALID;
    }
}

/// Session state for checkpoint persistence
/// 
/// Stores the state of a single session (thread) at checkpoint time.
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    /// Session GUID
    pub guid: Uuid,
    /// Monotonic serial number for this session
    pub serial_num: u64,
}

impl SessionState {
    /// Create a new session state
    pub fn new(guid: Uuid, serial_num: u64) -> Self {
        Self { guid, serial_num }
    }
}

/// Log metadata for checkpoint
/// 
/// Corresponds to C++ FASTER's LogMetadata class.
#[derive(Debug, Clone, Default)]
pub struct LogMetadata {
    /// Checkpoint token
    pub token: CheckpointToken,
    /// Whether to use snapshot file (vs fold-over)
    pub use_snapshot_file: bool,
    /// Version at checkpoint
    pub version: u32,
    /// Number of active threads at checkpoint
    pub num_threads: u32,
    /// Begin address (start of log)
    pub begin_address: Address,
    /// Final address at checkpoint (tail)
    pub final_address: Address,
    /// Flushed until address
    pub flushed_until_address: Address,
    /// Object log exists
    pub use_object_log: bool,
    /// Session states (guid + serial_num for each thread)
    pub session_states: Vec<SessionState>,
}

impl LogMetadata {
    /// Create a new log metadata
    pub fn new() -> Self {
        Self {
            token: Uuid::new_v4(),
            use_snapshot_file: false,
            version: 0,
            num_threads: 0,
            begin_address: Address::INVALID,
            final_address: Address::INVALID,
            flushed_until_address: Address::INVALID,
            use_object_log: false,
            session_states: Vec::new(),
        }
    }

    /// Initialize with a specific token
    pub fn with_token(token: CheckpointToken) -> Self {
        Self {
            token,
            use_snapshot_file: false,
            version: 0,
            num_threads: 0,
            begin_address: Address::INVALID,
            final_address: Address::INVALID,
            flushed_until_address: Address::INVALID,
            use_object_log: false,
            session_states: Vec::new(),
        }
    }

    /// Initialize the metadata with checkpoint parameters
    pub fn initialize(
        &mut self,
        use_snapshot_file: bool,
        version: u32,
        flushed_address: Address,
    ) {
        self.use_snapshot_file = use_snapshot_file;
        self.version = version;
        self.num_threads = 0;
        self.flushed_until_address = flushed_address;
        self.final_address = Address::INVALID;
        self.session_states.clear();
    }

    /// Reset the metadata to default values
    /// 
    /// This restores the metadata to the same state as `new()` creates,
    /// except the token is preserved.
    pub fn reset(&mut self) {
        self.use_snapshot_file = false;
        self.version = 0;  // Consistent with new() and with_token()
        self.num_threads = 0;
        self.begin_address = Address::INVALID;  // Was missing
        self.final_address = Address::INVALID;
        self.flushed_until_address = Address::INVALID;
        self.use_object_log = false;  // Was missing
        self.session_states.clear();
    }

    /// Add a session state to the metadata
    pub fn add_session(&mut self, guid: Uuid, serial_num: u64) {
        self.session_states.push(SessionState::new(guid, serial_num));
        self.num_threads = self.session_states.len() as u32;
    }

    /// Get the session state for a given guid
    pub fn get_session(&self, guid: &Uuid) -> Option<&SessionState> {
        self.session_states.iter().find(|s| &s.guid == guid)
    }
}

/// Checkpoint type
///
/// Note: The discriminant values are part of the on-disk format.
/// FoldOver=0 and Snapshot=1 are the original values and must be preserved
/// for backward compatibility with existing checkpoint files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CheckpointType {
    /// Fold-over checkpoint (no snapshot file, original default)
    /// Preserved as 0 for backward compatibility
    FoldOver = 0,
    /// Snapshot checkpoint (uses snapshot file)
    /// Preserved as 1 for backward compatibility
    Snapshot = 1,
    /// Full checkpoint (index + hybrid log), equivalent to Snapshot
    Full = 2,
    /// Index-only checkpoint
    IndexOnly = 3,
    /// HybridLog-only checkpoint
    HybridLogOnly = 4,
}

impl Default for CheckpointType {
    fn default() -> Self {
        // Default to Snapshot (Full) for new checkpoints
        CheckpointType::Snapshot
    }
}

/// State of an active checkpoint operation
#[derive(Debug)]
pub struct CheckpointState {
    /// Index metadata
    pub index_metadata: IndexMetadata,
    /// Log metadata
    pub log_metadata: LogMetadata,
    /// Checkpoint type
    pub checkpoint_type: CheckpointType,
    /// Index checkpoint started
    pub index_checkpoint_started: AtomicBool,
    /// Index checkpoint completed
    pub index_checkpoint_completed: AtomicBool,
    /// Log flush started
    pub log_flush_started: AtomicBool,
    /// Log flush completed
    pub log_flush_completed: AtomicBool,
    /// Persistence callback invoked
    pub persistence_callback_invoked: AtomicBool,
    /// Number of pending persistence calls
    pub pending_persistence_calls: AtomicU64,
}

impl CheckpointState {
    /// Create a new checkpoint state
    pub fn new(checkpoint_type: CheckpointType) -> Self {
        let token = Uuid::new_v4();
        Self {
            index_metadata: IndexMetadata::with_token(token),
            log_metadata: LogMetadata::with_token(token),
            checkpoint_type,
            index_checkpoint_started: AtomicBool::new(false),
            index_checkpoint_completed: AtomicBool::new(false),
            log_flush_started: AtomicBool::new(false),
            log_flush_completed: AtomicBool::new(false),
            persistence_callback_invoked: AtomicBool::new(false),
            pending_persistence_calls: AtomicU64::new(0),
        }
    }

    /// Get the checkpoint token
    pub fn token(&self) -> CheckpointToken {
        self.index_metadata.token
    }

    /// Check if checkpoint is complete
    pub fn is_complete(&self) -> bool {
        self.index_checkpoint_completed.load(Ordering::Acquire)
            && self.log_flush_completed.load(Ordering::Acquire)
    }

    /// Reset the checkpoint state
    pub fn reset(&mut self) {
        let token = Uuid::new_v4();
        self.index_metadata = IndexMetadata::with_token(token);
        self.log_metadata = LogMetadata::with_token(token);
        self.index_checkpoint_started.store(false, Ordering::Release);
        self.index_checkpoint_completed.store(false, Ordering::Release);
        self.log_flush_started.store(false, Ordering::Release);
        self.log_flush_completed.store(false, Ordering::Release);
        self.persistence_callback_invoked.store(false, Ordering::Release);
        self.pending_persistence_calls.store(0, Ordering::Release);
    }

    /// Mark index checkpoint as started
    pub fn start_index_checkpoint(&self) -> bool {
        !self.index_checkpoint_started.swap(true, Ordering::AcqRel)
    }

    /// Mark index checkpoint as completed
    pub fn complete_index_checkpoint(&self) {
        self.index_checkpoint_completed.store(true, Ordering::Release);
    }

    /// Mark log flush as started
    pub fn start_log_flush(&self) -> bool {
        !self.log_flush_started.swap(true, Ordering::AcqRel)
    }

    /// Mark log flush as completed
    pub fn complete_log_flush(&self) {
        self.log_flush_completed.store(true, Ordering::Release);
    }

    /// Increment pending persistence calls
    pub fn increment_pending(&self) {
        self.pending_persistence_calls.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement pending persistence calls
    ///
    /// Returns true if this was the last pending call
    pub fn decrement_pending(&self) -> bool {
        self.pending_persistence_calls.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Check if there are pending persistence calls
    pub fn has_pending(&self) -> bool {
        self.pending_persistence_calls.load(Ordering::Acquire) > 0
    }
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self::new(CheckpointType::Full)
    }
}

impl Clone for CheckpointState {
    fn clone(&self) -> Self {
        Self {
            index_metadata: self.index_metadata.clone(),
            log_metadata: self.log_metadata.clone(),
            checkpoint_type: self.checkpoint_type,
            index_checkpoint_started: AtomicBool::new(self.index_checkpoint_started.load(Ordering::Relaxed)),
            index_checkpoint_completed: AtomicBool::new(self.index_checkpoint_completed.load(Ordering::Relaxed)),
            log_flush_started: AtomicBool::new(self.log_flush_started.load(Ordering::Relaxed)),
            log_flush_completed: AtomicBool::new(self.log_flush_completed.load(Ordering::Relaxed)),
            persistence_callback_invoked: AtomicBool::new(self.persistence_callback_invoked.load(Ordering::Relaxed)),
            pending_persistence_calls: AtomicU64::new(self.pending_persistence_calls.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_metadata() {
        let meta = IndexMetadata::new();
        assert_eq!(meta.table_size, 0);
        assert_eq!(meta.num_buckets, 0);
        assert_eq!(meta.version, 0);
        assert!(meta.log_begin_address.is_invalid());
        assert!(meta.checkpoint_start_address.is_invalid());
    }

    #[test]
    fn test_index_metadata_initialize() {
        let mut meta = IndexMetadata::new();
        meta.initialize(5, 1024, Address::new(0, 0), Address::new(10, 500));
        
        assert_eq!(meta.version, 5);
        assert_eq!(meta.table_size, 1024);
        assert_eq!(meta.log_begin_address, Address::new(0, 0));
        assert_eq!(meta.checkpoint_start_address, Address::new(10, 500));
    }

    #[test]
    fn test_log_metadata() {
        let meta = LogMetadata::new();
        assert_eq!(meta.version, 0);
        assert_eq!(meta.num_threads, 0);
        assert!(!meta.use_snapshot_file);
        assert!(meta.begin_address.is_invalid());
        assert!(meta.session_states.is_empty());
    }

    #[test]
    fn test_log_metadata_sessions() {
        let mut meta = LogMetadata::new();
        let guid1 = Uuid::new_v4();
        let guid2 = Uuid::new_v4();
        
        meta.add_session(guid1, 100);
        meta.add_session(guid2, 200);
        
        assert_eq!(meta.num_threads, 2);
        assert_eq!(meta.session_states.len(), 2);
        
        let session1 = meta.get_session(&guid1).unwrap();
        assert_eq!(session1.serial_num, 100);
        
        let session2 = meta.get_session(&guid2).unwrap();
        assert_eq!(session2.serial_num, 200);
    }

    #[test]
    fn test_session_state() {
        let guid = Uuid::new_v4();
        let state = SessionState::new(guid, 42);
        assert_eq!(state.guid, guid);
        assert_eq!(state.serial_num, 42);
    }

    #[test]
    fn test_checkpoint_state() {
        let state = CheckpointState::new(CheckpointType::Full);
        assert!(!state.is_complete());
        
        // Start and complete index checkpoint
        assert!(state.start_index_checkpoint());
        assert!(!state.start_index_checkpoint()); // Second call should return false
        state.complete_index_checkpoint();
        
        // Start and complete log flush
        assert!(state.start_log_flush());
        state.complete_log_flush();
        
        assert!(state.is_complete());
    }

    #[test]
    fn test_pending_persistence() {
        let state = CheckpointState::new(CheckpointType::Full);
        
        assert!(!state.has_pending());
        
        state.increment_pending();
        state.increment_pending();
        assert!(state.has_pending());
        
        assert!(!state.decrement_pending());
        assert!(state.decrement_pending()); // Last one
        assert!(!state.has_pending());
    }
}

