//! oxifaster - A high-performance concurrent key-value store and log engine
//!
//! This is a Rust port of Microsoft's FASTER project, providing:
//! - **FASTER KV**: A concurrent key-value store supporting data larger than memory
//! - **FASTER Log**: A high-performance persistent recoverable log
//!
//! # Features
//!
//! - High-performance concurrent operations with epoch-based memory reclamation
//! - Hybrid log architecture (in-memory + disk)
//! - Non-blocking checkpointing and recovery
//! - Async I/O with Tokio runtime
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use oxifaster::{FasterKv, Status};
//!
//! // Create a new store
//! let store = FasterKv::new(config)?;
//!
//! // Start a session
//! let session = store.start_session();
//!
//! // Perform operations
//! session.upsert(key, value).await?;
//! let result = session.read(key).await?;
//! ```

#![warn(missing_docs)]
#![allow(dead_code)]

pub mod address;
pub mod allocator;
pub mod checkpoint;
pub mod device;
pub mod epoch;
pub mod index;
pub mod log;
pub mod record;
pub mod status;
pub mod store;
mod utility;

// Re-exports for convenience
pub use address::{Address, AtomicAddress};
pub use record::{Record, RecordInfo};
pub use status::{OperationStatus, Status};

/// Constants used throughout the library
pub mod constants {
    /// Size of a cache line in bytes
    pub const CACHE_LINE_BYTES: usize = 64;

    /// Number of merge chunks for checkpoint writes
    pub const NUM_MERGE_CHUNKS: u32 = 256;

    /// Maximum number of threads supported
    pub const MAX_THREADS: usize = 96;

    /// Page size (32 MB)
    pub const PAGE_SIZE: usize = 1 << 25;
}

/// Utility for size literals (e.g., 1_GiB)
pub mod size {
    /// 1 KiB in bytes
    pub const KIB: u64 = 1024;
    /// 1 MiB in bytes
    pub const MIB: u64 = 1024 * KIB;
    /// 1 GiB in bytes
    pub const GIB: u64 = 1024 * MIB;
}

/// Prelude module for common imports
pub mod prelude {
    pub use crate::address::{Address, AtomicAddress};
    pub use crate::record::{Record, RecordInfo};
    pub use crate::status::{OperationStatus, Status};
    pub use crate::store::FasterKv;
}

