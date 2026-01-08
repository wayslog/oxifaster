//! F2 Hot-Cold separation architecture for FASTER
//!
//! F2 (Fast & Fair) is a two-tier storage architecture that separates
//! hot (frequently accessed) data from cold (infrequently accessed) data.
//!
//! # Overview
//!
//! The F2 architecture provides optimized storage by automatically tiering
//! data based on access patterns:
//!
//! - **Hot Store**: Fast in-memory store for frequently accessed data
//! - **Cold Store**: Larger on-disk store for infrequently accessed data
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────────────────────────────┐
//! │              F2Kv Store                    │
//! ├───────────────────┬───────────────────────┤
//! │    Hot Store      │     Cold Store        │
//! │  (MemHashIndex)   │ (默认 MemHashIndex；   │
//! │                   │  可选 ColdIndex)      │
//! │   (HybridLog)     │   (HybridLog)         │
//! │   + Read Cache    │                       │
//! └───────────────────┴───────────────────────┘
//!         ↑                     ↑
//!    Frequent Access      Infrequent Access
//! ```
//!
//! # Key Features
//!
//! - **Automatic Migration**: Background thread moves cold data to cold store
//! - **Unified Interface**: Read/Write/RMW operations work transparently
//! - **Checkpoint/Recovery**: Both stores can be checkpointed together
//! - **Cold Index**: 冷端可选使用 ColdIndex（磁盘索引）以降低内存占用
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::f2::{F2Kv, F2Config};
//!
//! let config = F2Config::default();
//! let f2_store = F2Kv::new(config, hot_device, cold_device);
//!
//! // Checkpoint both stores
//! let token = f2_store.checkpoint(checkpoint_dir)?;
//!
//! // Recovery
//! let recovered = F2Kv::recover(checkpoint_dir, token)?;
//! ```
//!
//! Based on C++ FASTER's f2.h implementation.

mod config;
mod state;
mod store;

pub use config::{
    ColdStoreConfig, F2CompactionConfig, F2Config, HotStoreConfig, HotToColdMigrationStrategy,
};
pub use state::{F2CheckpointPhase, F2CheckpointState, StoreCheckpointStatus};
pub use store::{F2Kv, ReadOperationStage, RmwOperationStage, StoreStats, StoreType};
