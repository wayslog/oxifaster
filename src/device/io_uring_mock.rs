//! io_uring mock 实现（默认启用）
//!
//! 说明：该实现用于在非 Linux 或未开启 `feature="io_uring"` 时保持 API 兼容。

use std::io;
use std::path::PathBuf;

use crate::device::traits::SyncStorageDevice;
use crate::status::Status;

use super::io_uring_common::{IoUringConfig, IoUringError, IoUringFeatures, IoUringStats};

/// io_uring-based storage device（mock）
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
    pub fn new(path: impl AsRef<std::path::Path>, config: IoUringConfig) -> Self {
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
    pub fn with_defaults(path: impl AsRef<std::path::Path>) -> Self {
        Self::new(path, IoUringConfig::default())
    }

    /// Initialize the io_uring instance（mock：总是成功）
    pub fn initialize(&mut self) -> Result<(), Status> {
        self.initialized = true;
        Ok(())
    }

    /// Shutdown the io_uring instance（mock）
    pub fn shutdown(&mut self) {
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

    /// Submit a batch of operations（mock）
    pub fn submit(&mut self) -> Result<u32, Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        Ok(0)
    }

    /// Submit and wait for completions（mock）
    pub fn submit_and_wait(&mut self, min_complete: u32) -> Result<u32, Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        let _ = min_complete;
        Ok(0)
    }

    /// Wait for completions（mock）
    pub fn wait_completions(&mut self, min_complete: u32) -> Result<u32, Status> {
        if !self.initialized {
            return Err(Status::InvalidArgument);
        }
        let _ = min_complete;
        Ok(0)
    }

    /// Process completed operations（mock）
    pub fn process_completions(&mut self) -> u32 {
        if !self.initialized {
            return 0;
        }
        0
    }

    /// Poll for completions without blocking（mock）
    pub fn poll_completions(&mut self) -> u32 {
        self.process_completions()
    }

    /// Check if io_uring is available on this system（mock：默认视为不可用）
    pub fn is_available() -> bool {
        false
    }

    /// Get the supported features on this system（mock：全部 false）
    pub fn supported_features() -> IoUringFeatures {
        IoUringFeatures::default()
    }

    /// Path (for debugging)
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl SyncStorageDevice for IoUringDevice {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if !self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                IoUringError::NotInitialized,
            ));
        }

        let _ = (offset, buf);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring read_sync not implemented (mock mode). Enable 'io_uring' feature for full implementation.",
        ))
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
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
        4096
    }
}

#[cfg(test)]
mod tests {
    use super::super::io_uring_common::IoUringFile;
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
}
