//! Delta Log for Incremental Checkpoints
//!
//! This module provides the Delta Log implementation for storing incremental
//! checkpoint records. A delta log captures only the changes since the last
//! full (or incremental) snapshot, significantly reducing checkpoint time
//! and storage space.
//!
//! ## Architecture
//!
//! The delta log is an append-only log that stores:
//! - **Delta records**: Changed data records since the last snapshot
//! - **Checkpoint metadata**: Metadata about each incremental checkpoint
//!
//! ## Entry Format
//!
//! Each entry in the delta log has a fixed-size header (16 bytes) followed
//! by variable-length payload:
//!
//! ```text
//! +----------------+----------------+----------------+
//! |   Checksum     |    Length      |     Type       |
//! |    8 bytes     |    4 bytes     |    4 bytes     |
//! +----------------+----------------+----------------+
//! |                    Payload                       |
//! |                  (variable)                      |
//! +--------------------------------------------------+
//! ```
//!
//! ## Usage
//!
//! ### Writing entries during checkpoint
//!
//! ```ignore
//! use oxifaster::delta_log::{DeltaLog, DeltaLogConfig, DeltaLogEntry};
//! use std::sync::Arc;
//!
//! let device = Arc::new(create_device());
//! let delta_log = DeltaLog::new(device, DeltaLogConfig::default(), 0);
//! delta_log.init_for_writes();
//!
//! // Write a delta entry
//! let entry = DeltaLogEntry::delta(b"changed data".to_vec());
//! delta_log.write_entry(&entry)?;
//!
//! // Flush to disk
//! delta_log.flush()?;
//! ```
//!
//! ### Reading entries during recovery
//!
//! ```ignore
//! use oxifaster::delta_log::{DeltaLog, DeltaLogIterator};
//! use std::sync::Arc;
//!
//! let delta_log = Arc::new(DeltaLog::with_defaults(device, -1));
//! delta_log.init_for_reads();
//!
//! let mut iter = DeltaLogIterator::all(delta_log);
//! while let Some((addr, entry)) = iter.get_next()? {
//!     match entry.entry_type() {
//!         Some(DeltaLogEntryType::Delta) => {
//!             // Apply delta record
//!         }
//!         Some(DeltaLogEntryType::CheckpointMetadata) => {
//!             // Process checkpoint metadata
//!         }
//!         None => {}
//!     }
//! }
//! ```
//!
//! ## Based on C# FASTER
//!
//! This implementation is based on C# FASTER's `DeltaLog` class, providing
//! similar functionality for incremental snapshots.

mod entry;
mod iterator;
mod log;

pub use entry::{
    DELTA_LOG_HEADER_SIZE, DeltaLogEntry, DeltaLogEntryType, DeltaLogHeader,
    compute_entry_checksum, compute_xor_checksum, verify_checksum,
};
pub use iterator::{DeltaLogIntoIterator, DeltaLogIterator, IteratorState};
pub use log::{DeltaLog, DeltaLogConfig};
