//! Checkpoint state management for FASTER
//!
//! This module provides checkpoint state tracking and metadata management.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use uuid::Uuid;

use crate::address::Address;
use crate::checkpoint::CheckpointToken;

/// Index metadata for checkpoint
#[derive(Debug, Clone, Default)]
pub struct IndexMetadata {
    /// Checkpoint token
    pub token: CheckpointToken,
    /// Hash table size
    pub table_size: u64,
    /// Number of overflow buckets
    pub num_buckets: u64,
    /// Number of overflow entries
    pub num_entries: u64,
}

impl IndexMetadata {
    /// Create a new index metadata
    pub fn new() -> Self {
        Self {
            token: Uuid::new_v4(),
            table_size: 0,
            num_buckets: 0,
            num_entries: 0,
        }
    }

    /// Initialize with a specific token
    pub fn with_token(token: CheckpointToken) -> Self {
        Self {
            token,
            table_size: 0,
            num_buckets: 0,
            num_entries: 0,
        }
    }
}

/// Log metadata for checkpoint
#[derive(Debug, Clone, Default)]
pub struct LogMetadata {
    /// Checkpoint token
    pub token: CheckpointToken,
    /// Version at checkpoint
    pub version: u32,
    /// Begin address (start of log)
    pub begin_address: Address,
    /// Final address at checkpoint
    pub final_address: Address,
    /// Flushed until address
    pub flushed_until_address: Address,
    /// Object log exists
    pub use_object_log: bool,
}

impl LogMetadata {
    /// Create a new log metadata
    pub fn new() -> Self {
        Self {
            token: Uuid::new_v4(),
            version: 0,
            begin_address: Address::INVALID,
            final_address: Address::INVALID,
            flushed_until_address: Address::INVALID,
            use_object_log: false,
        }
    }

    /// Initialize with a specific token
    pub fn with_token(token: CheckpointToken) -> Self {
        Self {
            token,
            version: 0,
            begin_address: Address::INVALID,
            final_address: Address::INVALID,
            flushed_until_address: Address::INVALID,
            use_object_log: false,
        }
    }
}

/// Checkpoint type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CheckpointType {
    /// Fold-over checkpoint
    FoldOver = 0,
    /// Snapshot checkpoint
    Snapshot = 1,
}

impl Default for CheckpointType {
    fn default() -> Self {
        CheckpointType::FoldOver
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
        Self::new(CheckpointType::FoldOver)
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
    }

    #[test]
    fn test_log_metadata() {
        let meta = LogMetadata::new();
        assert_eq!(meta.version, 0);
        assert!(meta.begin_address.is_invalid());
    }

    #[test]
    fn test_checkpoint_state() {
        let state = CheckpointState::new(CheckpointType::FoldOver);
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
        let state = CheckpointState::new(CheckpointType::Snapshot);
        
        assert!(!state.has_pending());
        
        state.increment_pending();
        state.increment_pending();
        assert!(state.has_pending());
        
        assert!(!state.decrement_pending());
        assert!(state.decrement_pending()); // Last one
        assert!(!state.has_pending());
    }
}

