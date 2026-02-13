//! Portable fallback backend for `IoUringDevice`.
//!
//! This module is compiled when either:
//! - the target is not Linux, or
//! - `feature = "io_uring"` is not enabled.
//!
//! It provides a real, thread-safe file-backed implementation so non-Linux platforms (e.g. macOS)
//! can still use `IoUringDevice` for correctness testing and development.

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::device::traits::SyncStorageDevice;
use crate::status::Status;

use super::io_uring_common::{
    IoUringConfig, IoUringError, IoUringFeatures, IoUringStats, checked_offset,
};

struct FallbackState {
    file: std::fs::File,
}

/// `IoUringDevice` fallback implementation (non-io_uring).
pub struct IoUringDevice {
    /// Configuration
    config: IoUringConfig,
    /// Device path
    path: PathBuf,
    /// Statistics
    stats: IoUringStats,
    state: Mutex<Option<FallbackState>>,
}

impl IoUringDevice {
    /// Create a new io_uring device
    pub fn new(path: impl AsRef<Path>, config: IoUringConfig) -> Self {
        Self {
            config,
            path: path.as_ref().to_path_buf(),
            stats: IoUringStats::default(),
            state: Mutex::new(None),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(path: impl AsRef<Path>) -> Self {
        Self::new(path, IoUringConfig::default())
    }

    /// Eagerly initialize (open/create) the backing file.
    pub fn initialize(&mut self) -> Result<(), Status> {
        self.ensure_initialized_inner()
            .map(|_| ())
            .map_err(map_io_err_to_status)
    }

    /// Shutdown (close the backing file).
    pub fn shutdown(&mut self) {
        *self.state.lock() = None;
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
        self.state.lock().is_some()
    }

    /// Get pending operation count (always 0 for the synchronous fallback).
    pub fn pending_operations(&self) -> u32 {
        0
    }

    /// Check if there is space for more submissions (always true for the synchronous fallback).
    pub fn can_submit(&self) -> bool {
        self.is_initialized() || self.ensure_initialized_inner().is_ok()
    }

    /// Submit a batch of operations (no-op for the synchronous fallback).
    pub fn submit(&mut self) -> Result<u32, Status> {
        Ok(0)
    }

    /// Submit and wait for completions (no-op for the synchronous fallback).
    pub fn submit_and_wait(&mut self, min_complete: u32) -> Result<u32, Status> {
        let _ = min_complete;
        Ok(0)
    }

    /// Wait for completions (no-op for the synchronous fallback).
    pub fn wait_completions(&mut self, min_complete: u32) -> Result<u32, Status> {
        let _ = min_complete;
        Ok(0)
    }

    /// Process completed operations (always 0 for the synchronous fallback).
    pub fn process_completions(&mut self) -> u32 {
        0
    }

    /// Poll for completions without blocking (always 0 for the synchronous fallback).
    pub fn poll_completions(&mut self) -> u32 {
        self.process_completions()
    }

    /// Check if io_uring is available on this system (always false for the fallback).
    pub fn is_available() -> bool {
        false
    }

    /// Get the supported features on this system (all false for the fallback).
    pub fn supported_features() -> IoUringFeatures {
        IoUringFeatures::default()
    }

    /// Path (for debugging)
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn ensure_initialized_inner(&self) -> io::Result<()> {
        let mut guard = self.state.lock();
        if guard.is_some() {
            return Ok(());
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)?;

        *guard = Some(FallbackState { file });
        Ok(())
    }

    fn with_file<T>(&self, f: impl FnOnce(&std::fs::File) -> io::Result<T>) -> io::Result<T> {
        self.ensure_initialized_inner()?;
        let guard = self.state.lock();
        let state = guard
            .as_ref()
            .ok_or_else(|| io::Error::other(IoUringError::NotInitialized))?;
        f(&state.file)
    }
}

impl SyncStorageDevice for IoUringDevice {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let offset = checked_offset(offset)?;
        self.with_file(|file| file.read_at(buf, offset))
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let offset = checked_offset(offset)?;
        self.with_file(|file| file.write_at(buf, offset))
    }

    fn flush_sync(&self) -> io::Result<()> {
        self.with_file(|file| file.sync_all())
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        self.with_file(|file| file.set_len(size))
    }

    fn size_sync(&self) -> io::Result<u64> {
        self.with_file(|file| file.metadata().map(|m| m.len()))
    }

    fn alignment(&self) -> usize {
        4096
    }
}

fn map_io_err_to_status(_e: io::Error) -> Status {
    Status::IoError
}

#[cfg(test)]
mod tests {
    use super::super::io_uring_common::IoUringFile;
    use super::*;
    use tempfile::tempdir;

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

        assert!(!device.is_initialized());
        assert_eq!(device.stats().reads_submitted, 0);
    }

    #[test]
    fn test_device_initialization() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fallback_init.dat");

        let config = IoUringConfig::default();
        let mut device = IoUringDevice::new(&path, config);

        let result = device.initialize();
        assert!(result.is_ok());
        assert!(device.is_initialized());
    }

    #[test]
    fn test_device_can_submit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fallback_submit.dat");

        let config = IoUringConfig::default();
        let mut device = IoUringDevice::new(&path, config);
        device.initialize().unwrap();

        assert!(device.can_submit());
        assert_eq!(device.pending_operations(), 0);
    }

    #[test]
    fn test_fallback_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fallback_rw.dat");

        let mut dev = IoUringDevice::with_defaults(&path);
        dev.initialize().unwrap();

        let data = b"hello fallback";
        let n = dev.write_sync(0, data).unwrap();
        assert_eq!(n, data.len());
        dev.flush_sync().unwrap();

        let mut buf = vec![0u8; data.len()];
        let n = dev.read_sync(0, &mut buf).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
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
