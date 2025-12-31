//! Recovery tests for oxifaster
//!
//! This module contains tests for checkpoint and recovery functionality.

use oxifaster::checkpoint::{CheckpointState, CheckpointType, IndexMetadata, LogMetadata, RecoveryStatus};

#[test]
fn test_checkpoint_state_creation() {
    let state = CheckpointState::new(CheckpointType::FoldOver);
    
    assert!(!state.is_complete());
    assert_eq!(state.checkpoint_type, CheckpointType::FoldOver);
}

#[test]
fn test_checkpoint_state_lifecycle() {
    let state = CheckpointState::new(CheckpointType::Snapshot);
    
    // Start index checkpoint
    assert!(state.start_index_checkpoint());
    assert!(!state.start_index_checkpoint()); // Should return false on second call
    
    // Complete index checkpoint
    state.complete_index_checkpoint();
    
    // Start log flush
    assert!(state.start_log_flush());
    
    // Complete log flush
    state.complete_log_flush();
    
    // Should be complete now
    assert!(state.is_complete());
}

#[test]
fn test_checkpoint_pending_operations() {
    let state = CheckpointState::new(CheckpointType::FoldOver);
    
    // Add pending operations
    state.increment_pending();
    state.increment_pending();
    state.increment_pending();
    
    assert!(state.has_pending());
    
    // Complete operations
    assert!(!state.decrement_pending());
    assert!(!state.decrement_pending());
    assert!(state.decrement_pending()); // Last one
    
    assert!(!state.has_pending());
}

#[test]
fn test_index_metadata() {
    let meta = IndexMetadata::new();
    
    assert_eq!(meta.table_size, 0);
    assert_eq!(meta.num_buckets, 0);
    assert_eq!(meta.num_entries, 0);
}

#[test]
fn test_log_metadata() {
    let meta = LogMetadata::new();
    
    assert_eq!(meta.version, 0);
    assert!(meta.begin_address.is_invalid());
    assert!(meta.final_address.is_invalid());
    assert!(!meta.use_object_log);
}

#[test]
fn test_recovery_status() {
    assert_eq!(RecoveryStatus::default(), RecoveryStatus::NotStarted);
}

#[test]
fn test_checkpoint_reset() {
    let mut state = CheckpointState::new(CheckpointType::FoldOver);
    
    // Do some operations
    state.start_index_checkpoint();
    state.complete_index_checkpoint();
    state.start_log_flush();
    state.complete_log_flush();
    
    assert!(state.is_complete());
    
    // Reset
    state.reset();
    
    // Should be back to initial state
    assert!(!state.is_complete());
}

#[test]
fn test_checkpoint_types() {
    let fold_over = CheckpointState::new(CheckpointType::FoldOver);
    let snapshot = CheckpointState::new(CheckpointType::Snapshot);
    
    assert_eq!(fold_over.checkpoint_type, CheckpointType::FoldOver);
    assert_eq!(snapshot.checkpoint_type, CheckpointType::Snapshot);
}

#[test]
fn test_checkpoint_token() {
    let state1 = CheckpointState::new(CheckpointType::FoldOver);
    let state2 = CheckpointState::new(CheckpointType::FoldOver);
    
    // Tokens should be unique
    assert_ne!(state1.token(), state2.token());
}

