//! Log scanning and iteration for FASTER
//!
//! This module provides iterators for scanning records in the hybrid log.
//! Used for compaction, recovery, and data export operations.
//!
//! # Overview
//!
//! Log scanning allows sequential access to records in the hybrid log,
//! which is useful for operations like compaction, data export, and
//! rebuilding indexes.
//!
//! # Iterators
//!
//! - [`LogScanIterator`]: Basic iterator for scanning a range of addresses
//! - [`LogPageIterator`]: Page-by-page iterator with buffering
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::scan::{LogScanIterator, ScanRange};
//!
//! let range = ScanRange::new(begin_address, end_address);
//! let mut iterator = LogScanIterator::new(&hlog, range);
//!
//! while let Some((address, record)) = iterator.next() {
//!     // Process record...
//! }
//! ```
//!
//! # Performance Considerations
//!
//! - Use page-aligned scan ranges for optimal I/O performance
//! - Consider using double-buffered iterator for overlapped I/O

mod log_iterator;

pub use log_iterator::{
    ConcurrentLogScanIterator, LogPage, LogPageIterator, LogPageStatus, LogScanIterator, ScanRange,
};
