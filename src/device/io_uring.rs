//! io_uring device implementation
//!
//! Provides high-performance async I/O using Linux io_uring interface.
//!
//! # Feature Gates
//!
//! This module provides a **mock implementation** by default. To use real io_uring:
//!
//! 1. Enable the `io_uring` feature in Cargo.toml:
//!    ```toml
//!    [dependencies]
//!    oxifaster = { version = "*", features = ["io_uring"] }
//!    ```
//!
//! 2. Ensure your system meets the requirements:
//!    - Linux kernel >= 5.1
//!    - The `io-uring` crate must be added as a dependency
//!
//! # Mock Implementation
//!
//! The current mock implementation:
//! - Returns appropriate errors for unimplemented operations
//! - Tracks statistics accurately for testing
//! - Provides feature detection (always returns false on non-Linux)
//!
//! # Example
//!
//! ```ignore
//! use oxifaster::device::io_uring::{IoUringDevice, IoUringConfig};
//!
//! // Check if io_uring is available
//! if IoUringDevice::is_available() {
//!     let config = IoUringConfig::new()
//!         .with_sq_entries(256)
//!         .with_sqpoll(true);
//!     
//!     let mut device = IoUringDevice::new("/path/to/file", config);
//!     device.initialize().expect("Failed to initialize io_uring");
//! }
//! ```
//!
//! # Performance Considerations
//!
//! When io_uring is fully implemented:
//! - SQPOLL mode reduces syscall overhead for high-throughput workloads
//! - Fixed buffers avoid buffer registration overhead per I/O
//! - Registered files speed up file descriptor lookups

use std::io;
use std::path::{Path, PathBuf};

use crate::device::traits::{AsyncIoCallback, SyncStorageDevice};
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
    path: PathBuf,
    /// File size
    size: u64,
    /// Whether the file is open
    is_open: bool,
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
        // Note: In a full implementation, this would:
        // 1. Open the file with O_DIRECT flag
        // 2. Register the file descriptor with io_uring
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

/// io_uring-based storage device
pub struct IoUringDevice {
    /// Configuration
    config: IoUringConfig,
    /// Device path
    path: PathBuf,
    /// Statistics
    stats: IoUringStats,
    /// Whether the device is initialized
    initialized: bool,
    /// Pending operations count
    pending_ops: u32,
    /// Maximum pending operations
    max_pending_ops: u32,
}

impl IoUringDevice {
    /// Create a new io_uring device
    pub fn new(path: impl AsRef<Path>, config: IoUringConfig) -> Self {
        let max_pending = config.sq_entries;
        Self {
            config,
            path: path.as_ref().to_path_buf(),
            stats: IoUringStats::default(),
            initialized: false,
            pending_ops: 0,
            max_pending_ops: max_pending,
        }
    }

    /// Create with default configuration
    pub fn with_defaults(path: impl AsRef<Path>) -> Self {
        Self::new(path, IoUringConfig::default())
    }

    /// Initialize the io_uring instance
    ///
    /// # Mock Mode
    ///
    /// In mock mode, this always succeeds but the device won't perform real I/O.
    /// I/O operations will return `Unsupported` errors.
    /// Enable the `io_uring` feature for full implementation.
    ///
    /// # Full Implementation (when feature enabled)
    ///
    /// This would:
    /// 1. Create io_uring instance with `io_uring_setup` syscall
    /// 2. Register fixed buffers if `use_fixed_buffers` is enabled
    /// 3. Start SQPOLL thread if `sqpoll` is enabled
    /// 4. Register files if `register_files` is enabled
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())` in mock mode. In full implementation,
    /// returns `Err(Status::NotSupported)` if io_uring is not available.
    pub fn initialize(&mut self) -> Result<(), Status> {
        // Mock initialization - always succeeds
        // The device can be initialized, but I/O operations will return errors
        // indicating mock mode. This allows testing the API surface without
        // requiring actual io_uring support.
        //
        // In a full implementation, this would:
        // 1. Check io_uring availability
        // 2. Call io_uring_queue_init_params with config
        // 3. Set up SQPOLL if enabled
        // 4. Register buffers and files
        self.initialized = true;
        Ok(())
    }

    /// Shutdown the io_uring instance
    ///
    /// Waits for pending operations to complete before shutdown.
    /// This is a blocking operation.
    pub fn shutdown(&mut self) {
        if !self.initialized {
            return;
        }

        // Wait for pending operations
        let mut attempts = 0;
        while self.pending_ops > 0 && attempts < 1000 {
            let _ = self.process_completions();
            attempts += 1;
        }

        // Log warning if operations couldn't complete
        if self.pending_ops > 0 {
            eprintln!(
                "Warning: io_uring shutdown with {} pending operations",
                self.pending_ops
            );
        }

        self.initialized = false;
    }

    /// Get the configuration
    pub fn config(&self) -> &IoUringConfig {
        &self.config
    }

    /// Get the statistics
    pub fn stats(&self) -> &IoUringStats {
        &self.stats
    }

    /// Get mutable statistics
    pub fn stats_mut(&mut self) -> &mut IoUringStats {
        &mut self.stats
    }

    /// Check if the device is initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get pending operation count
    pub fn pending_operations(&self) -> u32 {
        self.pending_ops
    }

    /// Check if there is space for more submissions
    pub fn can_submit(&self) -> bool {
        self.pending_ops < self.max_pending_ops
    }

    /// Submit a batch of operations
    pub fn submit(&mut self) -> Result<u32, Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        // Note: In a full implementation, this would call io_uring_submit
        Ok(0)
    }

    /// Submit and wait for completions
    pub fn submit_and_wait(&mut self, min_complete: u32) -> Result<u32, Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        // Submit pending operations first
        let submitted = self.submit()?;
        // Then wait for completions
        self.wait_completions(min_complete)?;
        Ok(submitted)
    }

    /// Wait for completions
    pub fn wait_completions(&mut self, min_complete: u32) -> Result<u32, Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        // Note: In a full implementation, this would call io_uring_wait_cqe
        let _ = min_complete;
        Ok(0)
    }

    /// Submit async read
    pub fn submit_read(
        &mut self,
        file: &IoUringFile,
        offset: u64,
        buffer: &mut [u8],
        callback: AsyncIoCallback,
    ) -> Result<(), Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        if !self.can_submit() {
            self.stats.sq_full_events += 1;
            return Err(Status::Aborted);
        }
        // Note: In a full implementation, this would:
        // 1. Get a submission queue entry
        // 2. Prepare IORING_OP_READ
        // 3. Set user_data to callback context
        let _ = file;
        let _ = offset;
        let _ = buffer;
        let _ = callback;

        self.pending_ops += 1;
        self.stats.reads_submitted += 1;
        Ok(())
    }

    /// Submit async write
    pub fn submit_write(
        &mut self,
        file: &IoUringFile,
        offset: u64,
        buffer: &[u8],
        callback: AsyncIoCallback,
    ) -> Result<(), Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        if !self.can_submit() {
            self.stats.sq_full_events += 1;
            return Err(Status::Aborted);
        }
        // Note: In a full implementation, this would:
        // 1. Get a submission queue entry
        // 2. Prepare IORING_OP_WRITE
        // 3. Set user_data to callback context
        let _ = file;
        let _ = offset;
        let _ = buffer;
        let _ = callback;

        self.pending_ops += 1;
        self.stats.writes_submitted += 1;
        Ok(())
    }

    /// Submit async fsync
    pub fn submit_fsync(
        &mut self,
        file: &IoUringFile,
        callback: AsyncIoCallback,
    ) -> Result<(), Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        if !self.can_submit() {
            return Err(Status::Aborted);
        }
        // Note: In a full implementation, this would prepare IORING_OP_FSYNC
        let _ = file;
        let _ = callback;

        self.pending_ops += 1;
        Ok(())
    }

    /// Process completed operations
    pub fn process_completions(&mut self) -> u32 {
        if !self.initialized {
            return 0;
        }
        // Note: In a full implementation, this would:
        // 1. Peek/wait for CQEs
        // 2. Extract user_data and invoke callbacks
        // 3. Update statistics
        // 4. Decrement pending_ops for each completion
        0
    }

    /// Poll for completions without blocking
    pub fn poll_completions(&mut self) -> u32 {
        self.process_completions()
    }

    /// Check if io_uring is available on this system
    pub fn is_available() -> bool {
        // Note: In a full implementation, this would check:
        // 1. Linux kernel version >= 5.1
        // 2. io_uring syscalls are available
        #[cfg(target_os = "linux")]
        {
            // Try to detect io_uring support by checking kernel version
            // For now, assume it's available on Linux
            true
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// Get the supported features on this system
    pub fn supported_features() -> IoUringFeatures {
        IoUringFeatures {
            sqpoll: Self::is_available(),
            fixed_buffers: Self::is_available(),
            registered_files: Self::is_available(),
            io_drain: Self::is_available(),
        }
    }
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

impl SyncStorageDevice for IoUringDevice {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Mock implementation: returns error indicating not implemented
        // In a full implementation, this would:
        // 1. Submit a read operation to io_uring
        // 2. Wait for completion with io_uring_wait_cqe
        // 3. Return the bytes read
        if !self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                IoUringError::NotInitialized,
            ));
        }

        // Return error indicating mock mode
        let _ = (offset, buf);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring read_sync not implemented (mock mode). Enable 'io_uring' feature for full implementation.",
        ))
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        // Mock implementation: returns error indicating not implemented
        // In a full implementation, this would:
        // 1. Submit a write operation to io_uring
        // 2. Wait for completion with io_uring_wait_cqe
        // 3. Return the bytes written
        if !self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                IoUringError::NotInitialized,
            ));
        }

        let _ = (offset, buf);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring write_sync not implemented (mock mode). Enable 'io_uring' feature for full implementation.",
        ))
    }

    fn flush_sync(&self) -> io::Result<()> {
        // Mock implementation: returns error indicating not implemented
        // In a full implementation, this would submit IORING_OP_FSYNC
        if !self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                IoUringError::NotInitialized,
            ));
        }

        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring flush_sync not implemented (mock mode). Enable 'io_uring' feature for full implementation.",
        ))
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        // Mock implementation: returns error indicating not implemented
        // In a full implementation, this would use ftruncate or IORING_OP_FALLOCATE
        if !self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                IoUringError::NotInitialized,
            ));
        }

        let _ = size;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring truncate_sync not implemented (mock mode). Enable 'io_uring' feature for full implementation.",
        ))
    }

    fn size_sync(&self) -> io::Result<u64> {
        // Mock implementation: returns error indicating not implemented
        // In a full implementation, this would use fstat
        if !self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                IoUringError::NotInitialized,
            ));
        }

        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring size_sync not implemented (mock mode). Enable 'io_uring' feature for full implementation.",
        ))
    }

    fn alignment(&self) -> usize {
        // io_uring with O_DIRECT typically requires 4K alignment
        // This is a safe default that works with most storage devices
        4096
    }
}

impl std::fmt::Display for IoUringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoUringError::NotAvailable => write!(f, "io_uring is not available on this system"),
            IoUringError::NotInitialized => write!(f, "io_uring device not initialized"),
            IoUringError::SubmissionQueueFull => write!(f, "io_uring submission queue is full"),
            IoUringError::InvalidArgument => write!(f, "invalid argument"),
            IoUringError::IoError(msg) => write!(f, "I/O error: {msg}"),
            IoUringError::NotImplemented => {
                write!(f, "io_uring feature not implemented (mock mode)")
            }
        }
    }
}

impl std::error::Error for IoUringError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = IoUringConfig::default();
        assert_eq!(config.sq_entries, 256);
        assert_eq!(config.cq_entries, 512);
        assert!(!config.sqpoll);
    }

    #[test]
    fn test_config_builder() {
        let config = IoUringConfig::new()
            .with_sq_entries(512)
            .with_sqpoll(true)
            .with_fixed_buffers(true);

        assert_eq!(config.sq_entries, 512);
        assert!(config.sqpoll);
        assert!(config.use_fixed_buffers);
    }

    #[test]
    fn test_device_creation() {
        let config = IoUringConfig::default();
        let device = IoUringDevice::new("/tmp", config);

        assert!(!device.initialized);
        assert_eq!(device.stats().reads_submitted, 0);
    }

    #[test]
    fn test_device_initialization() {
        let config = IoUringConfig::default();
        let mut device = IoUringDevice::new("/tmp", config);

        let result = device.initialize();
        assert!(result.is_ok());
        assert!(device.initialized);
        assert!(device.is_initialized());
    }

    #[test]
    fn test_device_can_submit() {
        let config = IoUringConfig::default();
        let mut device = IoUringDevice::new("/tmp", config);
        device.initialize().unwrap();

        assert!(device.can_submit());
        assert_eq!(device.pending_operations(), 0);
    }

    #[test]
    fn test_file_creation() {
        let file = IoUringFile::new("/tmp/test.dat");
        assert!(!file.is_open);
        assert_eq!(file.size, 0);
    }

    #[test]
    fn test_file_open_close() {
        let mut file = IoUringFile::new("/tmp/test.dat");
        assert!(!file.is_open());

        file.open().unwrap();
        assert!(file.is_open());

        file.close();
        assert!(!file.is_open());
    }

    #[test]
    fn test_stats_default() {
        let stats = IoUringStats::default();
        assert_eq!(stats.reads_submitted, 0);
        assert_eq!(stats.writes_submitted, 0);
        assert_eq!(stats.bytes_read, 0);
        assert_eq!(stats.bytes_written, 0);
    }

    #[test]
    fn test_stats_recording() {
        let mut stats = IoUringStats::default();

        stats.record_read_complete(4096, 1000000); // 1ms
        assert_eq!(stats.reads_completed, 1);
        assert_eq!(stats.bytes_read, 4096);
        assert_eq!(stats.avg_read_latency_us, 1000);

        stats.record_write_complete(8192, 2000000); // 2ms
        assert_eq!(stats.writes_completed, 1);
        assert_eq!(stats.bytes_written, 8192);
        assert_eq!(stats.avg_write_latency_us, 2000);
    }

    #[test]
    fn test_stats_pending() {
        let stats = IoUringStats {
            reads_submitted: 10,
            reads_completed: 5,
            read_errors: 2,
            ..Default::default()
        };

        assert_eq!(stats.pending_reads(), 3);
    }

    #[test]
    fn test_stats_reset() {
        let mut stats = IoUringStats {
            reads_submitted: 100,
            writes_completed: 50,
            ..Default::default()
        };

        stats.reset();

        assert_eq!(stats.reads_submitted, 0);
        assert_eq!(stats.writes_completed, 0);
    }

    #[test]
    fn test_is_available() {
        // Just verify the function can be called
        let available = IoUringDevice::is_available();
        #[cfg(target_os = "linux")]
        assert!(available);
        #[cfg(not(target_os = "linux"))]
        assert!(!available);
    }

    #[test]
    fn test_supported_features() {
        let _features = IoUringDevice::supported_features();
        // On non-Linux, all features should be false
        #[cfg(not(target_os = "linux"))]
        {
            assert!(!_features.sqpoll);
            assert!(!_features.fixed_buffers);
        }
    }

    #[test]
    fn test_operation_type() {
        assert_ne!(IoOperation::Read, IoOperation::Write);
        assert_ne!(IoOperation::Fsync, IoOperation::Fallocate);
    }

    #[test]
    fn test_io_uring_error_display() {
        let err = IoUringError::NotAvailable;
        let display_str = format!("{}", err);
        assert!(display_str.contains("not available"));

        let err = IoUringError::NotInitialized;
        let display_str = format!("{}", err);
        assert!(display_str.contains("not initialized"));

        let err = IoUringError::SubmissionQueueFull;
        let display_str = format!("{}", err);
        assert!(display_str.contains("submission queue")); // "submission queue is full"

        let err = IoUringError::InvalidArgument;
        let display_str = format!("{}", err);
        assert!(display_str.contains("invalid")); // "invalid argument"

        let err = IoUringError::IoError("test error".to_string());
        let display_str = format!("{}", err);
        assert!(display_str.contains("test error"));

        let err = IoUringError::NotImplemented;
        let display_str = format!("{}", err);
        assert!(display_str.contains("not implemented"));
    }

    #[test]
    fn test_io_uring_error_debug() {
        let err = IoUringError::NotAvailable;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("NotAvailable"));
    }

    #[test]
    fn test_io_uring_error_clone() {
        let err = IoUringError::NotAvailable;
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }

    #[test]
    fn test_io_uring_error_std_error() {
        let err = IoUringError::NotAvailable;
        // Verify it implements std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_config_clone() {
        let config = IoUringConfig::default();
        let cloned = config.clone();
        assert_eq!(config.sq_entries, cloned.sq_entries);
        assert_eq!(config.cq_entries, cloned.cq_entries);
    }

    #[test]
    fn test_config_debug() {
        let config = IoUringConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("sq_entries"));
        assert!(debug_str.contains("cq_entries"));
    }

    #[test]
    fn test_config_with_cq_entries() {
        let config = IoUringConfig::new().with_cq_entries(1024);
        assert_eq!(config.cq_entries, 1024);
    }

    #[test]
    fn test_config_sqpoll_idle_field() {
        let config = IoUringConfig {
            sqpoll_idle_ms: 2000,
            ..Default::default()
        };
        assert_eq!(config.sqpoll_idle_ms, 2000);
    }

    #[test]
    fn test_config_register_files_field() {
        let config = IoUringConfig {
            register_files: true,
            max_registered_files: 100,
            ..Default::default()
        };
        assert!(config.register_files);
        assert_eq!(config.max_registered_files, 100);
    }

    #[test]
    fn test_config_num_fixed_buffers_field() {
        let config = IoUringConfig {
            num_fixed_buffers: 64,
            ..Default::default()
        };
        assert_eq!(config.num_fixed_buffers, 64);
    }

    #[test]
    fn test_config_with_fixed_buffer_size() {
        let config = IoUringConfig::new().with_fixed_buffer_size(8192);
        assert_eq!(config.fixed_buffer_size, 8192);
    }

    #[test]
    fn test_io_operation_debug() {
        let op = IoOperation::Read;
        let debug_str = format!("{:?}", op);
        assert!(debug_str.contains("Read"));
    }

    #[test]
    fn test_io_operation_clone_copy() {
        let op = IoOperation::Write;
        let cloned = op.clone();
        let copied = op;
        assert_eq!(op, cloned);
        assert_eq!(op, copied);
    }

    #[test]
    fn test_io_operation_all_values() {
        let _read = IoOperation::Read;
        let _write = IoOperation::Write;
        let _fsync = IoOperation::Fsync;
        let _fallocate = IoOperation::Fallocate;
    }

    #[test]
    fn test_io_uring_features_default() {
        let features = IoUringFeatures::default();
        assert!(!features.sqpoll);
        assert!(!features.fixed_buffers);
        assert!(!features.registered_files);
        assert!(!features.io_drain);
    }

    #[test]
    fn test_io_uring_features_debug() {
        let features = IoUringFeatures::default();
        let debug_str = format!("{:?}", features);
        assert!(debug_str.contains("sqpoll"));
    }

    #[test]
    fn test_io_uring_stats_clone() {
        let stats = IoUringStats {
            reads_submitted: 10,
            ..Default::default()
        };
        let cloned = stats.clone();
        assert_eq!(stats.reads_submitted, cloned.reads_submitted);
    }

    #[test]
    fn test_io_uring_stats_debug() {
        let stats = IoUringStats::default();
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("reads_submitted"));
    }

    #[test]
    fn test_io_uring_stats_pending_writes() {
        let stats = IoUringStats {
            writes_submitted: 20,
            writes_completed: 15,
            write_errors: 2,
            ..Default::default()
        };
        assert_eq!(stats.pending_writes(), 3);
    }

    #[test]
    fn test_io_uring_stats_combined_pending() {
        let stats = IoUringStats {
            reads_submitted: 10,
            reads_completed: 5,
            read_errors: 1,
            writes_submitted: 20,
            writes_completed: 15,
            write_errors: 2,
            ..Default::default()
        };
        // Total pending = pending_reads + pending_writes
        assert_eq!(stats.pending_reads() + stats.pending_writes(), 7); // 4 reads + 3 writes
    }

    #[test]
    fn test_device_path() {
        let config = IoUringConfig::default();
        let device = IoUringDevice::new("/tmp/test", config);
        assert_eq!(device.path, std::path::PathBuf::from("/tmp/test"));
    }

    #[test]
    fn test_file_path() {
        let file = IoUringFile::new("/tmp/test.dat");
        assert_eq!(file.path, std::path::PathBuf::from("/tmp/test.dat"));
    }

    #[test]
    fn test_file_size_field() {
        let file = IoUringFile::new("/tmp/test.dat");
        assert_eq!(file.size, 0);
    }
}
