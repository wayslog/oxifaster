//! # oxifaster - A High-Performance Concurrent Key-Value Store and Log Engine
//!
//! `oxifaster` is a Rust port of Microsoft's [FASTER](https://github.com/microsoft/FASTER) project,
//! providing a high-performance concurrent key-value store and log engine designed for
//! data-intensive applications.
//!
//! ## Overview
//!
//! This library provides two main components:
//!
//! - **FasterKV**: A concurrent key-value store that supports data larger than memory
//!   with seamless disk spillover via a hybrid log architecture.
//! - **FasterLog**: A high-performance persistent recoverable append-only log.
//!
//! ## Key Features
#![allow(clippy::collapsible_if)]
//!
//! - **High-Performance Concurrent Operations**: Lock-free data structures with epoch-based
//!   memory reclamation ensure safe and efficient concurrent access.
//! - **Hybrid Log Architecture**: Automatic tiering between in-memory (mutable + read-only)
//!   and on-disk storage regions.
//! - **Non-Blocking Checkpointing**: CPR (Concurrent Prefix Recovery) protocol enables
//!   checkpoints without stopping operations.
//! - **Read Cache**: Optional in-memory cache for frequently accessed cold data.
//! - **Log Compaction**: Space reclamation through background compaction of obsolete records.
//! - **Dynamic Index Growth**: Hash table can grow dynamically with minimal disruption.
//! - **F2 Architecture**: Two-tier hot-cold data separation for optimized access patterns.
//! - **Async I/O**: Full async/await support with Tokio runtime.
//!
//! ## Quick Start
//!
//! ### Basic Key-Value Operations
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use oxifaster::device::NullDisk;
//! use oxifaster::store::{FasterKv, FasterKvConfig};
//!
//! // Create a store with default configuration
//! let config = FasterKvConfig::default();
//! let device = NullDisk::new();
//! let store = Arc::new(FasterKv::new(config, device));
//!
//! // Start a session (required for all operations)
//! let mut session = store.start_session().expect("failed to start session");
//!
//! // Insert or update a key-value pair
//! session.upsert(42u64, 100u64);
//!
//! // Read a value
//! if let Ok(Some(value)) = session.read(&42u64) {
//!     println!("Value: {}", value);
//! }
//!
//! // Delete a key
//! session.delete(&42u64);
//!
//! // Read-Modify-Write operation
//! session.rmw(42u64, |value| {
//!     *value += 1;
//!     true // return true to apply the modification
//! });
//! ```
//!
//! ### Checkpoint and Recovery
//!
//! ```rust,ignore
//! use std::path::Path;
//!
//! // Create a checkpoint
//! let checkpoint_dir = Path::new("/path/to/checkpoints");
//! let token = store.checkpoint(checkpoint_dir)?;
//!
//! // Later, recover from the checkpoint
//! let recovered_store = FasterKv::recover(
//!     checkpoint_dir,
//!     token,
//!     config,
//!     device
//! )?;
//!
//! // Continue sessions from checkpoint
//! for session_state in recovered_store.get_recovered_sessions() {
//!     let session = recovered_store
//!         .continue_session(session_state)
//!         .expect("failed to continue session");
//!     // Use the recovered session...
//! }
//! ```
//!
//! ### With Read Cache
//!
//! ```rust,ignore
//! use oxifaster::cache::ReadCacheConfig;
//!
//! let cache_config = ReadCacheConfig::new(256 * 1024 * 1024); // 256 MB cache
//! let store = FasterKv::with_read_cache(config, device, cache_config);
//! ```
//!
//! ### Log Compaction
//!
//! ```rust,ignore
//! // Manual compaction
//! let result = store.log_compact();
//! println!("Compacted {} records", result.stats.records_compacted);
//!
//! // Check if compaction is recommended
//! if store.should_compact() {
//!     store.log_compact();
//! }
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        Application                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Session / AsyncSession                    │
//! ├──────────────────────────┬──────────────────────────────────┤
//! │       FasterKV           │         FasterLog                │
//! ├──────────────────────────┼──────────────────────────────────┤
//! │       Read Cache         │       Log Compaction             │
//! ├──────────────────────────┴──────────────────────────────────┤
//! │                     Epoch Protection                        │
//! ├───────────────────────────┬─────────────────────────────────┤
//! │    Hash Index             │     Hybrid Log                  │
//! │   (MemHashIndex)          │  ┌────────┬────────┬────────┐   │
//! │                           │  │Mutable │ReadOnly│On-Disk │   │
//! │                           │  │Region  │Region  │Region  │   │
//! │                           │  └────────┴────────┴────────┘   │
//! ├─────────────────────────────────────────────────────────────┤
//! │                    Storage Device Layer                     │
//! │         (NullDisk / FileSystemDisk / IoUring)               │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Module Organization
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`address`] | 48-bit logical address system for hybrid log |
//! | [`allocator`] | Hybrid log memory allocator (PersistentMemoryMalloc) |
//! | [`cache`] | Read cache for hot data acceleration |
//! | [`checkpoint`] | Checkpoint and recovery with CPR protocol |
//! | [`compaction`] | Log compaction for space reclamation |
//! | [`config`] | Configuration loading helpers |
//! | [`delta_log`] | Delta log for incremental checkpoints |
//! | [`device`] | Storage device abstraction layer |
//! | [`epoch`] | Epoch-based memory reclamation framework |
//! | [`f2`] | F2 two-tier hot-cold storage architecture |
//! | [`index`] | High-performance in-memory hash index |
//! | [`log`] | FasterLog append-only log |
//! | [`ops`] | Operational helpers for self-check and recovery |
//! | [`record`] | Record format and Key/Value traits |
//! | [`scan`] | Log scanning and iteration |
//! | [`stats`] | Statistics collection and reporting |
//! | [`status`] | Operation status codes |
//! | [`store`] | FasterKV core implementation |
//! | [`varlen`] | Variable-length record support |
//!
//! ## Performance Considerations
//!
//! - **Session Affinity**: Each thread should have its own session for optimal performance.
//!   Sessions are not thread-safe and are designed for single-threaded use.
//! - **Epoch Protection**: Operations are protected by epochs. Long-running operations
//!   may delay memory reclamation.
//! - **Page Size**: Larger pages reduce metadata overhead but may increase I/O latency.
//! - **Mutable Fraction**: Controls how much of the log is available for in-place updates.
//!
//! ## Feature Flags
//!
//! - `statistics`: Enable comprehensive statistics collection (slight performance overhead)
//! - `prometheus`: Enable Prometheus text exposition rendering for statistics snapshots

#![warn(missing_docs)]
#![allow(dead_code)]

pub mod address;
pub mod allocator;
pub mod buffer_pool;
pub mod cache;
pub mod checkpoint;
pub mod codec;
pub mod compaction;
pub mod config;
pub mod delta_log;
pub mod device;
pub mod epoch;
#[cfg(feature = "f2")]
pub mod f2;
pub mod format;
pub mod gc;
pub mod index;
pub mod log;
pub mod ops;
pub mod record;
pub mod scan;
pub mod stats;
pub mod status;
pub mod store;
mod utility;
pub mod varlen;

// Re-exports for convenience
pub use address::{Address, AtomicAddress};
pub use record::RecordInfo;
pub use status::{OperationStatus, Status};

/// Constants used throughout the library
pub mod constants {
    /// Size of a cache line in bytes
    pub const CACHE_LINE_BYTES: usize = 64;

    /// Number of merge chunks for checkpoint writes
    pub const NUM_MERGE_CHUNKS: u32 = 256;

    /// Maximum number of threads supported
    pub const MAX_THREADS: usize = 96;

    // NOTE: CPR uses a `u128` bitmask to track active thread participants.
    // Keep this constraint explicit to avoid panics from shifting by >= 128.
    const _: () = assert!(MAX_THREADS <= 128);

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

/// Thread pool configuration for background operations
#[derive(Debug, Clone)]
pub struct ThreadPoolConfig {
    /// Number of worker threads (0 = use number of CPUs)
    pub num_threads: usize,
    /// Thread name prefix
    pub thread_name_prefix: String,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        Self {
            num_threads: 0,
            thread_name_prefix: "oxifaster-worker".to_string(),
        }
    }
}

impl ThreadPoolConfig {
    /// Create a new thread pool configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of worker threads
    ///
    /// If set to 0, the number of CPUs will be used.
    pub fn with_num_threads(mut self, num: usize) -> Self {
        self.num_threads = num;
        self
    }

    /// Set the thread name prefix
    pub fn with_thread_name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.thread_name_prefix = prefix.into();
        self
    }

    /// Get the effective number of threads
    ///
    /// Returns `num_threads` if non-zero, otherwise the number of CPUs.
    pub fn effective_threads(&self) -> usize {
        if self.num_threads == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        } else {
            self.num_threads
        }
    }
}

/// Prelude module for common imports
pub mod prelude {
    pub use crate::address::{Address, AtomicAddress};
    pub use crate::record::RecordInfo;
    pub use crate::status::{OperationStatus, Status};
    pub use crate::store::FasterKv;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_constants() {
        assert_eq!(size::KIB, 1024);
        assert_eq!(size::MIB, 1024 * 1024);
        assert_eq!(size::GIB, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_thread_pool_config_default() {
        let config = ThreadPoolConfig::default();
        assert_eq!(config.num_threads, 0);
        assert_eq!(config.thread_name_prefix, "oxifaster-worker");
    }

    #[test]
    fn test_thread_pool_config_new() {
        let config = ThreadPoolConfig::new();
        assert_eq!(config.num_threads, 0);
    }

    #[test]
    fn test_thread_pool_config_with_num_threads() {
        let config = ThreadPoolConfig::new().with_num_threads(4);
        assert_eq!(config.num_threads, 4);
    }

    #[test]
    fn test_thread_pool_config_with_thread_name_prefix() {
        let config = ThreadPoolConfig::new().with_thread_name_prefix("my-worker");
        assert_eq!(config.thread_name_prefix, "my-worker");
    }

    #[test]
    fn test_thread_pool_config_effective_threads() {
        // Test with explicit thread count
        let config = ThreadPoolConfig::new().with_num_threads(8);
        assert_eq!(config.effective_threads(), 8);

        // Test with default (0 = use CPU count)
        let config = ThreadPoolConfig::new();
        assert!(config.effective_threads() > 0);
    }

    #[test]
    fn test_thread_pool_config_clone() {
        let config = ThreadPoolConfig::new()
            .with_num_threads(4)
            .with_thread_name_prefix("test");
        let cloned = config.clone();
        assert_eq!(cloned.num_threads, 4);
        assert_eq!(cloned.thread_name_prefix, "test");
    }

    #[test]
    fn test_thread_pool_config_debug() {
        let config = ThreadPoolConfig::new();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ThreadPoolConfig"));
        assert!(debug_str.contains("num_threads"));
    }
}
