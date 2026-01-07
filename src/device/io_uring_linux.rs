//! Linux io_uring 完整实现（需要 `feature = "io_uring"`）

#![cfg(all(target_os = "linux", feature = "io_uring"))]

use std::fs::OpenOptions;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;

use crate::device::traits::SyncStorageDevice;
use crate::status::Status;

use super::io_uring_common::{IoUringConfig, IoUringError, IoUringFeatures, IoUringStats};

struct LinuxState {
    ring: IoUring,
    file: std::fs::File,
}

/// io_uring-based storage device（Linux 实现）
pub struct IoUringDevice {
    config: IoUringConfig,
    path: PathBuf,
    stats: IoUringStats,
    initialized: bool,
    state: Mutex<Option<LinuxState>>,
}

impl IoUringDevice {
    /// Create a new io_uring device（延迟初始化）
    pub fn new(path: impl AsRef<Path>, config: IoUringConfig) -> Self {
        Self {
            config,
            path: path.as_ref().to_path_buf(),
            stats: IoUringStats::default(),
            initialized: false,
            state: Mutex::new(None),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(path: impl AsRef<Path>) -> Self {
        Self::new(path, IoUringConfig::default())
    }

    /// Initialize io_uring + file（可重复调用）
    pub fn initialize(&mut self) -> Result<(), Status> {
        match self.ensure_initialized_inner() {
            Ok(_) => {
                self.initialized = true;
                Ok(())
            }
            Err(e) => Err(map_io_err_to_status(&e)),
        }
    }

    /// Shutdown（释放 ring/file）
    pub fn shutdown(&mut self) {
        *self.state.lock() = None;
        self.initialized = false;
    }

    /// Get the configuration
    pub fn config(&self) -> &IoUringConfig {
        &self.config
    }

    /// Get the statistics（当前实现为静态快照：SyncStorageDevice 的 I/O 不更新该统计）
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

    /// Check if io_uring is available on this system
    pub fn is_available() -> bool {
        IoUring::new(2).is_ok()
    }

    /// Get the supported features on this system（保守：以 is_available 为准）
    pub fn supported_features() -> IoUringFeatures {
        let ok = Self::is_available();
        IoUringFeatures {
            sqpoll: ok,
            fixed_buffers: ok,
            registered_files: ok,
            io_drain: ok,
        }
    }

    /// Path (for debugging)
    pub fn path(&self) -> &Path {
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
            .open(&self.path)?;

        let entries = self.config.sq_entries.max(2);
        let ring = IoUring::new(entries).map_err(|e| io::Error::other(e.to_string()))?;

        *guard = Some(LinuxState { ring, file });
        Ok(())
    }

    fn with_state<T>(&self, f: impl FnOnce(&mut LinuxState) -> io::Result<T>) -> io::Result<T> {
        self.ensure_initialized_inner()?;
        let mut guard = self.state.lock();
        let state = guard
            .as_mut()
            .ok_or_else(|| io::Error::other(IoUringError::NotInitialized))?;
        f(state)
    }

    fn submit_and_wait_one(state: &mut LinuxState) -> io::Result<i32> {
        state
            .ring
            .submit_and_wait(1)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut cq = state.ring.completion();
        let cqe = cq.next().ok_or_else(|| io::Error::other("missing cqe"))?;
        Ok(cqe.result())
    }
}

impl SyncStorageDevice for IoUringDevice {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.with_state(|state| {
            let start = Instant::now();

            let fd = types::Fd(state.file.as_raw_fd());
            let entry = opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(0);

            unsafe {
                state
                    .ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other(IoUringError::SubmissionQueueFull))?;
            }

            let res = Self::submit_and_wait_one(state)?;
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }

            let _elapsed = start.elapsed();
            Ok(res as usize)
        })
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.with_state(|state| {
            let start = Instant::now();

            let fd = types::Fd(state.file.as_raw_fd());
            let entry = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(0);

            unsafe {
                state
                    .ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other(IoUringError::SubmissionQueueFull))?;
            }

            let res = Self::submit_and_wait_one(state)?;
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }

            let _elapsed = start.elapsed();
            Ok(res as usize)
        })
    }

    fn flush_sync(&self) -> io::Result<()> {
        self.with_state(|state| {
            let fd = types::Fd(state.file.as_raw_fd());
            let entry = opcode::Fsync::new(fd).build().user_data(0);

            unsafe {
                state
                    .ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other(IoUringError::SubmissionQueueFull))?;
            }

            let res = Self::submit_and_wait_one(state)?;
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }
            Ok(())
        })
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        self.with_state(|state| {
            state.file.set_len(size)?;
            Ok(())
        })
    }

    fn size_sync(&self) -> io::Result<u64> {
        self.with_state(|state| state.file.metadata().map(|m| m.len()))
    }

    fn alignment(&self) -> usize {
        4096
    }
}

fn map_io_err_to_status(_e: &io::Error) -> Status {
    // 这里先做保守映射：设备初始化失败属于环境/配置问题
    Status::NotSupported
}

#[cfg(all(test, target_os = "linux", feature = "io_uring"))]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_linux_io_uring_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("io_uring.dat");

        let mut dev = IoUringDevice::with_defaults(&path);
        dev.initialize().unwrap();

        let data = b"hello io_uring";
        let n = dev.write_sync(0, data).unwrap();
        assert_eq!(n, data.len());
        dev.flush_sync().unwrap();

        let mut buf = vec![0u8; data.len()];
        let n = dev.read_sync(0, &mut buf).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_offset_overflow_check() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("io_uring_overflow.dat");

        let mut dev = IoUringDevice::with_defaults(&path);
        dev.initialize().unwrap();

        // 测试超过 i64::MAX 的 offset 应该返回错误，而非产生未定义行为
        let huge_offset = u64::MAX;
        let mut buf = [0u8; 16];

        let result = dev.read_sync(huge_offset, &mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let result = dev.write_sync(huge_offset, &buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_checked_offset_boundary() {
        // i64::MAX 是合法的
        assert!(checked_offset(i64::MAX as u64).is_ok());

        // i64::MAX + 1 应该失败
        assert!(checked_offset(i64::MAX as u64 + 1).is_err());

        // u64::MAX 应该失败
        assert!(checked_offset(u64::MAX).is_err());
    }
}
