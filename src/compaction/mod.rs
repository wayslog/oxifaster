//! Log compaction for FASTER
//!
//! This module provides log compaction functionality to reclaim space
//! by removing obsolete records from the log.
//!
//! Compaction process:
//! 1. Scan old records from begin_address to new_begin_address
//! 2. For each live record, check if it's the latest version (in hash index)
//! 3. If latest, conditionally insert into the tail of the log
//! 4. Update hash index to point to new location
//! 5. Shift begin_address to reclaim space
//!
//! # Concurrent Compaction
//!
//! The module also provides concurrent compaction support via `ConcurrentCompactor`,
//! which distributes work across multiple threads for improved throughput.

mod compact;
mod concurrent;
mod contexts;

pub use compact::{CompactionConfig, CompactionResult, CompactionStats, Compactor};
pub use concurrent::{
    CompactionWorkerHandle, ConcurrentCompactionConfig, ConcurrentCompactionContext,
    ConcurrentCompactionResult, ConcurrentCompactionState, ConcurrentCompactor,
    ConcurrentLogPageIterator, PageChunk,
};
pub use contexts::{CompactionContext, CompactionInsertContext};
