//! F2 Hot-Cold separation architecture for FASTER
//!
//! F2 (Fast & Fair) is a two-tier storage architecture that separates
//! hot (frequently accessed) data from cold (infrequently accessed) data.
//!
//! Key features:
//! - Hot store: Fast in-memory store with read cache
//! - Cold store: Larger on-disk store for less frequently accessed data
//! - Automatic compaction: Background thread moves data between stores
//! - Unified interface: Read/Write/RMW operations work across both stores
//!
//! Based on C++ FASTER's f2.h implementation.

mod config;
mod store;
mod state;

pub use config::{F2Config, F2CompactionConfig, HotStoreConfig, ColdStoreConfig};
pub use store::{F2Kv, StoreType, ReadOperationStage, RmwOperationStage, StoreStats};
pub use state::{F2CheckpointPhase, F2CheckpointState, StoreCheckpointStatus};
