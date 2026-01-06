//! Checkpoint and recovery for FASTER
//!
//! This module provides checkpointing and recovery functionality
//! for FasterKV stores.

mod locks;
mod recovery;
pub mod serialization;
mod state;

pub use locks::{
    AtomicCheckpointLock, CheckpointLock, CheckpointLockGuard, CheckpointLocks, LockType,
};
pub use recovery::{
    find_latest_checkpoint, list_checkpoints, validate_checkpoint, CheckpointInfo, RecoveryState,
    RecoveryStatus,
};
pub use serialization::{
    create_checkpoint_directory, delta_log_path, delta_metadata_path, incremental_info_path,
    index_data_path, index_metadata_path, log_metadata_path, log_snapshot_path, DeltaLogMetadata,
    IncrementalCheckpointChain, SerializableCheckpointInfo, SerializableIndexMetadata,
    SerializableLogMetadata,
};
pub use state::{CheckpointState, CheckpointType, IndexMetadata, LogMetadata, SessionState};

use uuid::Uuid;

/// Token identifying a checkpoint
pub type CheckpointToken = Uuid;

/// Callback for index persistence completion
pub type IndexPersistenceCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;

/// Callback for hybrid log persistence completion
pub type HybridLogPersistenceCallback = Box<dyn FnOnce(crate::Status, u64) + Send + 'static>;
