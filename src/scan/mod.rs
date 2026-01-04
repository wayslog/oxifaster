//! Log scanning and iteration for FASTER
//!
//! This module provides iterators for scanning records in the hybrid log.
//! Used for compaction, recovery, and data export operations.

mod log_iterator;

pub use log_iterator::{
    LogScanIterator, LogPageIterator, LogPage, LogPageStatus, ScanRange,
};
