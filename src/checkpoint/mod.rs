//! Checkpoint and recovery for FASTER
//!
//! This module provides checkpointing and recovery functionality
//! for FasterKV stores.

mod state;
mod recovery;

pub use state::{CheckpointState, CheckpointType, IndexMetadata, LogMetadata};
pub use recovery::RecoveryStatus;

use uuid::Uuid;

/// Token identifying a checkpoint
pub type CheckpointToken = Uuid;

/// Callback for index persistence completion
pub type IndexPersistenceCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;

/// Callback for hybrid log persistence completion
pub type HybridLogPersistenceCallback = Box<dyn FnOnce(crate::Status, u64) + Send + 'static>;

