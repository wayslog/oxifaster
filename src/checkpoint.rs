//! Checkpoint and recovery for FASTER
//!
//! This module provides checkpointing and recovery functionality
//! for FasterKV stores.
//!
//! # Overview
//!
//! FASTER uses the CPR (Concurrent Prefix Recovery) protocol for checkpointing,
//! which allows checkpoints to be taken without stopping normal operations.
//!
//! # Checkpoint Types
//!
//! - **Full Checkpoint**: Saves both index and hybrid log state
//! - **Index-Only Checkpoint**: Saves only the hash index
//! - **HybridLog-Only Checkpoint**: Saves only the log state
//! - **Incremental Checkpoint**: Saves only changes since last snapshot
//!
//! # CPR Protocol Phases
//!
//! 1. **PrepIndexChkpt**: Prepare index checkpoint, synchronize threads
//! 2. **IndexChkpt**: Write index to disk
//! 3. **Prepare**: Prepare HybridLog checkpoint
//! 4. **InProgress**: Increment version, mark checkpoint in progress
//! 5. **WaitPending**: Wait for pending operations
//! 6. **WaitFlush**: Wait for log flush
//! 7. **PersistenceCallback**: Invoke persistence callback
//! 8. **Rest**: Return to rest state
//!
//! # File Structure
//!
//! Each checkpoint creates a directory with the following files:
//!
//! - `index.meta`: Index checkpoint metadata (JSON)
//! - `index.dat`: Hash index data (binary)
//! - `log.meta`: Log checkpoint metadata (JSON)
//! - `log.snapshot`: Log snapshot data (binary)
//!
//! # Usage
//!
//! ```rust,ignore
//! use std::path::Path;
//!
//! // Create checkpoint
//! let token = store.checkpoint(Path::new("/checkpoints"))?;
//!
//! // Recover from checkpoint
//! let recovered = FasterKv::recover(
//!     Path::new("/checkpoints"),
//!     token,
//!     config,
//!     device
//! )?;
//! ```

/// C-style binary format serialization for FASTER C++ compatibility
pub mod binary_format;
mod locks;
mod recovery;
pub mod serialization;
mod state;

pub use locks::{
    AtomicCheckpointLock, CheckpointLock, CheckpointLockGuard, CheckpointLocks, LockType,
};
pub use recovery::{
    CheckpointInfo, PageRecoveryStatus, RecoveryState, RecoveryStatus, find_latest_checkpoint,
    list_checkpoints, validate_checkpoint,
};
pub(crate) use recovery::{build_incremental_chain, validate_incremental_checkpoint};
pub use serialization::{
    DeltaLogMetadata, IncrementalCheckpointChain, SerializableCheckpointInfo,
    SerializableIndexMetadata, SerializableLogMetadata, create_checkpoint_directory,
    delta_log_path, delta_metadata_path, incremental_info_path, index_data_path,
    index_metadata_path, log_metadata_path, log_snapshot_path,
};
pub use state::{CheckpointState, CheckpointType, IndexMetadata, LogMetadata, SessionState};

use uuid::Uuid;

/// Token identifying a checkpoint
pub type CheckpointToken = Uuid;

/// Callback for index persistence completion
pub type IndexPersistenceCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;

/// Callback for hybrid log persistence completion
pub type HybridLogPersistenceCallback = Box<dyn FnOnce(crate::Status, u64) + Send + 'static>;
