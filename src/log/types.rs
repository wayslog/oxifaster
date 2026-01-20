//! Shared types for FasterLog.

use std::fmt;

use crate::address::Address;
use crate::status::Status;

/// Configuration for FASTER Log
#[derive(Debug, Clone)]
pub struct FasterLogConfig {
    /// Page size in bytes
    pub page_size: usize,
    /// Number of pages in memory
    pub memory_pages: u32,
    /// Segment size for disk storage
    pub segment_size: u64,
    /// Auto-commit interval in milliseconds (0 = disabled)
    pub auto_commit_ms: u64,
}

impl FasterLogConfig {
    /// Create a new configuration
    pub fn new(memory_size: u64, page_size: usize) -> Self {
        let memory_pages = (memory_size / page_size as u64) as u32;

        Self {
            page_size,
            memory_pages,
            segment_size: 1 << 30, // 1 GB
            auto_commit_ms: 0,
        }
    }
}

impl Default for FasterLogConfig {
    fn default() -> Self {
        Self {
            page_size: 1 << 22, // 4 MB
            memory_pages: 64,
            segment_size: 1 << 30,
            auto_commit_ms: 0,
        }
    }
}

/// Options that control how the log is opened.
#[derive(Debug, Clone)]
pub struct FasterLogOpenOptions {
    /// Whether to run self-check and recovery scans on open.
    pub recover: bool,
    /// Whether to create a new log if metadata is missing.
    pub create_if_missing: bool,
    /// Whether to trim corrupt tails during self-check/recovery.
    pub truncate_on_corruption: bool,
}

impl Default for FasterLogOpenOptions {
    fn default() -> Self {
        Self {
            recover: true,
            create_if_missing: true,
            truncate_on_corruption: true,
        }
    }
}

impl FasterLogOpenOptions {
    /// Enable or disable self-check during open.
    pub fn with_self_check(mut self, enabled: bool) -> Self {
        self.recover = enabled;
        self
    }

    /// Enable or disable auto-repair (trim) on corruption.
    pub fn with_self_repair(mut self, enabled: bool) -> Self {
        self.truncate_on_corruption = enabled;
        self
    }

    /// Enable or disable create-if-missing behavior.
    pub fn with_create_if_missing(mut self, enabled: bool) -> Self {
        self.create_if_missing = enabled;
        self
    }
}

/// Options for running a log self-check without opening the log.
#[derive(Debug, Clone, Default)]
pub struct FasterLogSelfCheckOptions {
    /// Whether to apply repairs when issues are detected.
    pub repair: bool,
    /// Whether to perform a dry run (report only, no writes).
    pub dry_run: bool,
    /// Whether to create metadata when missing during repair.
    pub create_if_missing: bool,
}

/// Report from a log self-check operation.
#[derive(Debug, Clone)]
pub struct FasterLogSelfCheckReport {
    /// Whether metadata was present on disk.
    pub metadata_present: bool,
    /// Whether metadata was created during repair.
    pub metadata_created: bool,
    /// Last valid address found by scanning.
    pub scan_end: Address,
    /// Whether corruption was detected.
    pub had_corruption: bool,
    /// Proposed or applied trim address.
    pub truncate_to: Option<Address>,
    /// Whether a repair was applied.
    pub repaired: bool,
    /// Whether the run was dry-run.
    pub dry_run: bool,
}

/// Detailed error information for log operations.
#[derive(Debug, Clone)]
pub struct LogError {
    /// Error category.
    pub kind: LogErrorKind,
    /// Human-readable error message.
    pub message: String,
}

impl LogError {
    pub(crate) fn new(kind: LogErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Error categories for log operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogErrorKind {
    /// I/O failures.
    Io,
    /// Metadata validation failures.
    Metadata,
    /// Entry header or payload errors.
    Entry,
    /// Configuration or argument errors.
    Config,
    /// Data corruption detected during recovery or reads.
    Corruption,
}

pub(crate) fn status_from_error(error: &LogError) -> Status {
    match error.kind {
        LogErrorKind::Io => Status::IoError,
        LogErrorKind::Metadata | LogErrorKind::Entry | LogErrorKind::Corruption => {
            Status::Corruption
        }
        LogErrorKind::Config => Status::InvalidArgument,
    }
}

/// Statistics about the log
#[derive(Debug, Clone)]
pub struct LogStats {
    /// Tail address
    pub tail_address: Address,
    /// Committed address
    pub committed_address: Address,
    /// Begin address
    pub begin_address: Address,
    /// Page size
    pub page_size: usize,
    /// Number of buffer pages
    pub buffer_pages: u32,
}

impl std::fmt::Display for LogStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "FASTER Log Statistics:")?;
        writeln!(f, "  Tail: {}", self.tail_address)?;
        writeln!(f, "  Committed: {}", self.committed_address)?;
        writeln!(f, "  Begin: {}", self.begin_address)?;
        writeln!(f, "  Page size: {} bytes", self.page_size)?;
        writeln!(f, "  Buffer pages: {}", self.buffer_pages)
    }
}
