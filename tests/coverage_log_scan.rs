//! Integration tests for FasterLog and scan modules to improve code coverage.
//!
//! Covers: FasterLog creation, append, commit, scan, truncate, error paths,
//! LogConfig, LogStats, scan iterators, and concurrent scan.

use std::sync::Arc;
use std::thread;

use oxifaster::Address;
use oxifaster::Status;
use oxifaster::device::NullDisk;
use oxifaster::log::{
    FasterLog, FasterLogConfig, FasterLogOpenOptions, FasterLogSelfCheckOptions, LogErrorKind,
    LogStats,
};
use oxifaster::scan::{
    ConcurrentLogScanIterator, LogPage, LogPageIterator, LogPageStatus, LogScanIterator, ScanRange,
};

// ---------------------------------------------------------------------------
// Helper: create a small FasterLog on NullDisk suitable for unit tests.
// ---------------------------------------------------------------------------

fn make_log() -> FasterLog<NullDisk> {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    FasterLog::new(config, NullDisk::new()).unwrap()
}

fn make_config() -> FasterLogConfig {
    FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    }
}

// ===========================================================================
// FasterLogConfig tests
// ===========================================================================

include!("coverage_log_scan/part1.rs");
include!("coverage_log_scan/part2.rs");
include!("coverage_log_scan/part3.rs");
