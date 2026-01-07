//! io_uring 公共类型定义（与平台实现解耦）

use std::io;
use std::path::{Path, PathBuf};

use crate::status::Status;

/// Error type for io_uring operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoUringError {
    /// io_uring is not available on this system
    NotAvailable,
    /// Device not initialized
    NotInitialized,
    /// Submission queue is full
    SubmissionQueueFull,
    /// Invalid argument
    InvalidArgument,
    /// I/O operation failed
    IoError(String),
    /// Feature not implemented (mock mode)
    NotImplemented,
}

/// Configuration for io_uring
#[derive(Debug, Clone)]
pub struct IoUringConfig {
    /// Number of submission queue entries
    pub sq_entries: u32,
    /// Number of completion queue entries (typically 2x sq_entries)
    pub cq_entries: u32,
    /// Whether to use SQPOLL mode (kernel-side submission polling)
    pub sqpoll: bool,
    /// SQPOLL idle timeout in milliseconds
    pub sqpoll_idle_ms: u32,
    /// Whether to register files for faster access
    pub register_files: bool,
    /// Maximum number of registered files
    pub max_registered_files: u32,
    /// Whether to use fixed buffers
    pub use_fixed_buffers: bool,
    /// Size of each fixed buffer
    pub fixed_buffer_size: usize,
    /// Number of fixed buffers
    pub num_fixed_buffers: usize,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            cq_entries: 512,
            sqpoll: false,
            sqpoll_idle_ms: 2000,
            register_files: true,
            max_registered_files: 64,
            use_fixed_buffers: true,
            fixed_buffer_size: 4096,
            num_fixed_buffers: 256,
        }
    }
}

impl IoUringConfig {
    /// Create a new io_uring configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the submission queue size
    pub fn with_sq_entries(mut self, entries: u32) -> Self {
        self.sq_entries = entries;
        self
    }

    /// Set the completion queue size
    pub fn with_cq_entries(mut self, entries: u32) -> Self {
        self.cq_entries = entries;
        self
    }

    /// Enable or disable SQPOLL mode
    pub fn with_sqpoll(mut self, enabled: bool) -> Self {
        self.sqpoll = enabled;
        self
    }

    /// Enable or disable fixed buffers
    pub fn with_fixed_buffers(mut self, enabled: bool) -> Self {
        self.use_fixed_buffers = enabled;
        self
    }

    /// Set the fixed buffer size
    pub fn with_fixed_buffer_size(mut self, size: usize) -> Self {
        self.fixed_buffer_size = size;
        self
    }
}

/// Statistics for io_uring operations
#[derive(Debug, Clone, Default)]
pub struct IoUringStats {
    /// Total reads submitted
    pub reads_submitted: u64,
    /// Total reads completed
    pub reads_completed: u64,
    /// Total writes submitted
    pub writes_submitted: u64,
    /// Total writes completed
    pub writes_completed: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Read errors
    pub read_errors: u64,
    /// Write errors
    pub write_errors: u64,
    /// Average read latency in microseconds
    pub avg_read_latency_us: u64,
    /// Average write latency in microseconds
    pub avg_write_latency_us: u64,
    /// Total read latency in nanoseconds (for calculating average)
    total_read_latency_ns: u64,
    /// Total write latency in nanoseconds (for calculating average)
    total_write_latency_ns: u64,
    /// SQ full events (submission queue was full)
    pub sq_full_events: u64,
    /// CQ overflow events (completion queue overflowed)
    pub cq_overflow_events: u64,
}

impl IoUringStats {
    /// Record a successful read completion
    pub fn record_read_complete(&mut self, bytes: u64, latency_ns: u64) {
        self.reads_completed += 1;
        self.bytes_read += bytes;
        self.total_read_latency_ns += latency_ns;
        self.avg_read_latency_us =
            (self.total_read_latency_ns / self.reads_completed.max(1)) / 1000;
    }

    /// Record a successful write completion
    pub fn record_write_complete(&mut self, bytes: u64, latency_ns: u64) {
        self.writes_completed += 1;
        self.bytes_written += bytes;
        self.total_write_latency_ns += latency_ns;
        self.avg_write_latency_us =
            (self.total_write_latency_ns / self.writes_completed.max(1)) / 1000;
    }

    /// Record a read error
    pub fn record_read_error(&mut self) {
        self.read_errors += 1;
    }

    /// Record a write error
    pub fn record_write_error(&mut self) {
        self.write_errors += 1;
    }

    /// Get pending read count
    pub fn pending_reads(&self) -> u64 {
        self.reads_submitted
            .saturating_sub(self.reads_completed + self.read_errors)
    }

    /// Get pending write count
    pub fn pending_writes(&self) -> u64 {
        self.writes_submitted
            .saturating_sub(self.writes_completed + self.write_errors)
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// io_uring file handle
pub struct IoUringFile {
    /// File path
    pub(crate) path: PathBuf,
    /// File size
    pub(crate) size: u64,
    /// Whether the file is open
    pub(crate) is_open: bool,
}

impl IoUringFile {
    /// Create a new io_uring file
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            size: 0,
            is_open: false,
        }
    }

    /// Open the file
    pub fn open(&mut self) -> Result<(), Status> {
        self.is_open = true;
        Ok(())
    }

    /// Close the file
    pub fn close(&mut self) {
        self.is_open = false;
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Check if the file is open
    pub fn is_open(&self) -> bool {
        self.is_open
    }

    /// Get the file size
    pub fn file_size(&self) -> u64 {
        self.size
    }
}

/// Operation type for io_uring
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOperation {
    /// Read operation
    Read,
    /// Write operation
    Write,
    /// Fsync operation
    Fsync,
    /// Fallocate operation
    Fallocate,
}

/// Features supported by io_uring on this system
#[derive(Debug, Clone, Default)]
pub struct IoUringFeatures {
    /// SQPOLL mode support
    pub sqpoll: bool,
    /// Fixed buffer support
    pub fixed_buffers: bool,
    /// Registered file support
    pub registered_files: bool,
    /// IO drain support
    pub io_drain: bool,
}

impl std::fmt::Display for IoUringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoUringError::NotAvailable => write!(f, "io_uring is not available on this system"),
            IoUringError::NotInitialized => write!(f, "io_uring device not initialized"),
            IoUringError::SubmissionQueueFull => write!(f, "io_uring submission queue is full"),
            IoUringError::InvalidArgument => write!(f, "invalid argument"),
            IoUringError::IoError(msg) => write!(f, "I/O error: {msg}"),
            IoUringError::NotImplemented => write!(f, "io_uring not implemented (mock mode)"),
        }
    }
}

impl std::error::Error for IoUringError {}

impl From<IoUringError> for io::Error {
    fn from(value: IoUringError) -> Self {
        io::Error::other(value)
    }
}
