//! Log compaction for FASTER
//!
//! This module provides log compaction functionality to reclaim space
//! by removing obsolete records from the log.
//!
//! # Overview
//!
//! As updates and deletes occur, old versions of records become obsolete.
//! Compaction scans these old records, copies live ones to the tail, and
//! reclaims the space by advancing the begin address.
//!
//! # Compaction Process
//!
//! 1. **Scan**: Read old records from begin_address to target_address
//! 2. **Filter**: Check if each record is the latest version (via hash index)
//! 3. **Copy**: Conditionally insert live records into the tail of the log
//! 4. **Update Index**: Point hash index to the new location
//! 5. **Reclaim**: Shift begin_address to reclaim space
//!
//! # Compaction Types
//!
//! - **Single-threaded**: Basic compaction via [`Compactor`]
//! - **Concurrent**: Multi-threaded compaction via [`ConcurrentCompactor`]
//! - **Auto**: Background compaction via the auto-compaction worker
//!
//! # Configuration
//!
//! ```rust,ignore
//! use oxifaster::compaction::CompactionConfig;
//!
//! let config = CompactionConfig::new()
//!     .with_target_utilization(0.5)  // Compact when < 50% live data
//!     .with_num_threads(4);          // Use 4 threads for concurrent compaction
//! ```
//!
//! # Safety Guarantees
//!
//! The compaction algorithm ensures data safety by:
//!
//! - Using CAS to update index entries atomically
//! - Preserving records that fail to copy or update
//! - Only reclaiming space after all copies complete successfully

mod auto_compact;
mod compact;
mod concurrent;
mod contexts;

pub use auto_compact::{
    AutoCompactionConfig, AutoCompactionHandle, AutoCompactionState, AutoCompactionStats,
};
pub use compact::{CompactionConfig, CompactionResult, CompactionStats, Compactor};
pub use concurrent::{
    CompactionWorkerHandle, ConcurrentCompactionConfig, ConcurrentCompactionContext,
    ConcurrentCompactionResult, ConcurrentCompactionState, ConcurrentCompactor,
    ConcurrentLogPageIterator, PageChunk,
};
pub use contexts::{CompactionContext, CompactionInsertContext};

pub(crate) use auto_compact::AutoCompactionTarget;
