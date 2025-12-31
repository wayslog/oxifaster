//! Checkpoint and recovery for FASTER
//!
//! This module provides checkpointing and recovery functionality
//! for FasterKV stores.

mod recovery;
mod serialization;
mod state;

pub use recovery::{CheckpointInfo, RecoveryState, RecoveryStatus};
pub use serialization::{
    create_checkpoint_directory, index_data_path, index_metadata_path, log_metadata_path,
    log_snapshot_path, SerializableCheckpointInfo, SerializableIndexMetadata,
    SerializableLogMetadata,
};
pub use state::{CheckpointState, CheckpointType, IndexMetadata, LogMetadata};

use uuid::Uuid;

/// Token identifying a checkpoint
pub type CheckpointToken = Uuid;

/// Callback for index persistence completion
pub type IndexPersistenceCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;

/// Callback for hybrid log persistence completion
pub type HybridLogPersistenceCallback = Box<dyn FnOnce(crate::Status, u64) + Send + 'static>;

