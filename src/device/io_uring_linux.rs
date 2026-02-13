//! Linux `io_uring` backend implementation (requires `feature = "io_uring"`).

#![cfg(all(target_os = "linux", feature = "io_uring"))]

use std::fs::OpenOptions;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use io_uring::{IoUring, opcode, types};
use libc::iovec;
use parking_lot::Mutex;

use crate::device::traits::SyncStorageDevice;
use crate::status::Status;

use super::io_uring_common::{
    IoUringConfig, IoUringError, IoUringFeatures, IoUringStats, checked_offset,
};

struct LinuxState {
    ring: IoUring,
    file: std::fs::File,
    fixed_buffers: Option<FixedBuffers>,
    use_registered_file: bool,
    /// Best-effort count of submitted operations that have not been observed via CQE polling.
    ///
    /// This is used to make `pending_operations()` and the submit/wait helpers behave sensibly.
    inflight: u32,
}

struct FixedBuffers {
    buffers: Vec<Vec<u8>>,
}

impl FixedBuffers {
    fn buffer_size(&self) -> usize {
        self.buffers.first().map(|b| b.len()).unwrap_or_default()
    }

    fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffers[0]
    }

    fn try_register(ring: &IoUring, buffer_size: usize, num_buffers: usize) -> io::Result<Self> {
        let buffer_size = buffer_size.max(1);
        let num_buffers = num_buffers.max(1);

        let mut buffers = Vec::with_capacity(num_buffers);
        for _ in 0..num_buffers {
            buffers.push(vec![0u8; buffer_size]);
        }

        let iovecs: Vec<iovec> = buffers
            .iter_mut()
            .map(|buf| iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            })
            .collect();

        // Safety: `buffers` are heap-allocated and stored inside `LinuxState` for the lifetime of
        // the ring (or until explicitly unregistered).
        unsafe {
            ring.submitter().register_buffers(&iovecs)?;
        }

        Ok(Self { buffers })
    }
}

/// `io_uring`-based storage device (Linux backend).
pub struct IoUringDevice {
    config: IoUringConfig,
    path: PathBuf,
    stats: IoUringStats,
    initialized: bool,
    state: Mutex<Option<LinuxState>>,
}

impl IoUringDevice {
    /// Create a new io_uring device (lazy initialization).
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

    /// Initialize io_uring + file (idempotent).
    pub fn initialize(&mut self) -> Result<(), Status> {
        match self.ensure_initialized_inner() {
            Ok(_) => {
                self.initialized = true;
                Ok(())
            }
            Err(e) => Err(map_io_err_to_status(&e)),
        }
    }

    /// Shutdown (drop ring and file handles).
    pub fn shutdown(&mut self) {
        *self.state.lock() = None;
        self.initialized = false;
    }

    /// Get the configuration
    pub fn config(&self) -> &IoUringConfig {
        &self.config
    }

    /// Get the statistics.
    ///
    /// Note: currently a static snapshot; `SyncStorageDevice` operations do not update it.
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

    /// Get supported features on this system (best-effort).
    pub fn supported_features() -> IoUringFeatures {
        let Ok(ring) = IoUring::new(2) else {
            return IoUringFeatures::default();
        };

        let mut probe = io_uring::Probe::new();
        let _ = ring.submitter().register_probe(&mut probe);

        IoUringFeatures {
            sqpoll: true,
            fixed_buffers: probe.is_supported(opcode::ReadFixed::CODE)
                && probe.is_supported(opcode::WriteFixed::CODE),
            registered_files: true,
            io_drain: true,
        }
    }

    /// Path (for debugging)
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Submit queued operations to the kernel.
    ///
    /// If the ring is not initialized yet, this returns `Ok(0)` (there is nothing to submit).
    pub fn submit(&mut self) -> Result<u32, Status> {
        let mut guard = self.state.lock();
        let Some(state) = guard.as_mut() else {
            return Ok(0);
        };

        let submitted = state
            .ring
            .submit()
            .map_err(|e| map_ring_io_err_to_status(&e))?;
        state.inflight = state.inflight.saturating_add(submitted as u32);
        Ok(submitted as u32)
    }

    /// Submit queued operations and wait for at least `min_complete` completions.
    pub fn submit_and_wait(&mut self, min_complete: u32) -> Result<u32, Status> {
        let mut guard = self.state.lock();
        let Some(state) = guard.as_mut() else {
            return Ok(0);
        };

        let want = min_complete as usize;
        let submitted = state
            .ring
            .submit_and_wait(want)
            .map_err(|e| map_ring_io_err_to_status(&e))?;
        state.inflight = state.inflight.saturating_add(submitted as u32);
        Ok(submitted as u32)
    }

    /// Wait for at least `min_complete` completions.
    ///
    /// This does not consume CQEs; it only ensures they are available.
    pub fn wait_completions(&mut self, min_complete: u32) -> Result<u32, Status> {
        let mut guard = self.state.lock();
        let Some(state) = guard.as_mut() else {
            return Ok(0);
        };

        if min_complete == 0 {
            return Ok(available_completions(state));
        }

        let submitted = state
            .ring
            .submit_and_wait(min_complete as usize)
            .map_err(|e| map_ring_io_err_to_status(&e))?;
        state.inflight = state.inflight.saturating_add(submitted as u32);
        Ok(available_completions(state))
    }

    /// Process completed operations.
    ///
    /// This drains all currently available CQEs and returns the number drained.
    pub fn process_completions(&mut self) -> u32 {
        let mut guard = self.state.lock();
        let Some(state) = guard.as_mut() else {
            return 0;
        };

        let drained = drain_completions(state);
        state.inflight = state.inflight.saturating_sub(drained);
        drained
    }

    /// Poll for completions without blocking.
    ///
    /// This is equivalent to [`Self::process_completions`].
    pub fn poll_completions(&mut self) -> u32 {
        self.process_completions()
    }

    /// Get pending operation count.
    ///
    /// This includes both queued SQEs and submitted-but-not-yet-consumed CQEs (best-effort).
    pub fn pending_operations(&self) -> u32 {
        let mut guard = self.state.lock();
        let Some(state) = guard.as_mut() else {
            return 0;
        };
        state.inflight.saturating_add(queued_submissions(state))
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

        let entries = self.config.sq_entries.max(2);

        let mut builder = IoUring::builder();
        if self.config.cq_entries > entries {
            builder.setup_cqsize(self.config.cq_entries);
        }
        if self.config.sqpoll {
            builder.setup_sqpoll(self.config.sqpoll_idle_ms);
        }

        let ring = builder
            .build(entries)
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Best-effort fixed buffer registration: when it fails (e.g. low memlock limit), fall back
        // to normal read/write opcodes.
        let fixed_buffers = if self.config.use_fixed_buffers {
            FixedBuffers::try_register(
                &ring,
                self.config.fixed_buffer_size,
                self.config.num_fixed_buffers,
            )
            .ok()
        } else {
            None
        };

        // SQPOLL required registered files prior to Linux 5.11.
        // If the kernel reports `IORING_FEAT_SQPOLL_NONFIXED`, fixed files are no longer required.
        let need_registered_files = self.config.register_files
            || (self.config.sqpoll && !ring.params().is_feature_sqpoll_nonfixed());
        let use_registered_file = if need_registered_files {
            ring.submitter().register_files(&[file.as_raw_fd()])?;
            true
        } else {
            false
        };

        *guard = Some(LinuxState {
            ring,
            file,
            fixed_buffers,
            use_registered_file,
            inflight: 0,
        });
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
        let submitted = state.ring.submit_and_wait(1)?;
        state.inflight = state.inflight.saturating_add(submitted as u32);
        let mut cq = state.ring.completion();
        cq.sync();
        let cqe = cq.next().ok_or_else(|| io::Error::other("missing cqe"))?;
        state.inflight = state.inflight.saturating_sub(1);
        Ok(cqe.result())
    }
}

fn map_ring_io_err_to_status(e: &io::Error) -> Status {
    match e.raw_os_error() {
        Some(libc::ENOSYS) => Status::NotSupported,
        Some(libc::EINVAL) => Status::InvalidArgument,
        _ => Status::IoError,
    }
}

fn queued_submissions(state: &mut LinuxState) -> u32 {
    let mut sq = state.ring.submission();
    sq.sync();
    sq.len() as u32
}

fn available_completions(state: &mut LinuxState) -> u32 {
    let mut cq = state.ring.completion();
    cq.sync();
    cq.len() as u32
}

fn drain_completions(state: &mut LinuxState) -> u32 {
    let mut cq = state.ring.completion();
    cq.sync();

    let mut drained: u32 = 0;
    while cq.next().is_some() {
        drained = drained.saturating_add(1);
    }
    drained
}

impl SyncStorageDevice for IoUringDevice {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let offset = checked_offset(offset)?;
        self.with_state(|state| {
            let start = Instant::now();

            let entry = if state.use_registered_file {
                let fd = types::Fixed(0);
                match state.fixed_buffers.as_mut() {
                    Some(fixed) if buf.len() <= fixed.buffer_size() => opcode::ReadFixed::new(
                        fd,
                        fixed.buffer_mut().as_mut_ptr(),
                        buf.len() as u32,
                        0,
                    )
                    .offset(offset)
                    .build()
                    .user_data(0),
                    _ => opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
                        .offset(offset)
                        .build()
                        .user_data(0),
                }
            } else {
                let fd = types::Fd(state.file.as_raw_fd());
                match state.fixed_buffers.as_mut() {
                    Some(fixed) if buf.len() <= fixed.buffer_size() => opcode::ReadFixed::new(
                        fd,
                        fixed.buffer_mut().as_mut_ptr(),
                        buf.len() as u32,
                        0,
                    )
                    .offset(offset)
                    .build()
                    .user_data(0),
                    _ => opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
                        .offset(offset)
                        .build()
                        .user_data(0),
                }
            };

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

            if let Some(fixed) = state.fixed_buffers.as_mut() {
                if (res as usize) <= buf.len() && buf.len() <= fixed.buffer_size() {
                    buf[..res as usize].copy_from_slice(&fixed.buffer_mut()[..res as usize]);
                }
            }

            let _elapsed = start.elapsed();
            Ok(res as usize)
        })
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let offset = checked_offset(offset)?;
        self.with_state(|state| {
            let start = Instant::now();

            let entry = if state.use_registered_file {
                let fd = types::Fixed(0);
                match state.fixed_buffers.as_mut() {
                    Some(fixed) if buf.len() <= fixed.buffer_size() => {
                        let fixed_buf = fixed.buffer_mut();
                        fixed_buf[..buf.len()].copy_from_slice(buf);
                        opcode::WriteFixed::new(fd, fixed_buf.as_ptr(), buf.len() as u32, 0)
                            .offset(offset)
                            .build()
                            .user_data(0)
                    }
                    _ => opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                        .offset(offset)
                        .build()
                        .user_data(0),
                }
            } else {
                let fd = types::Fd(state.file.as_raw_fd());
                match state.fixed_buffers.as_mut() {
                    Some(fixed) if buf.len() <= fixed.buffer_size() => {
                        let fixed_buf = fixed.buffer_mut();
                        fixed_buf[..buf.len()].copy_from_slice(buf);
                        opcode::WriteFixed::new(fd, fixed_buf.as_ptr(), buf.len() as u32, 0)
                            .offset(offset)
                            .build()
                            .user_data(0)
                    }
                    _ => opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                        .offset(offset)
                        .build()
                        .user_data(0),
                }
            };

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
            let entry = if state.use_registered_file {
                let fd = types::Fixed(0);
                opcode::Fsync::new(fd).build().user_data(0)
            } else {
                let fd = types::Fd(state.file.as_raw_fd());
                opcode::Fsync::new(fd).build().user_data(0)
            };

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
    // Conservative mapping: initialization failures are typically environment/config issues.
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

        // Offsets beyond i64::MAX should return an error instead of triggering platform behavior.
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
        // i64::MAX is allowed.
        assert!(checked_offset(i64::MAX as u64).is_ok());

        // i64::MAX + 1 should fail.
        assert!(checked_offset(i64::MAX as u64 + 1).is_err());

        // u64::MAX should fail.
        assert!(checked_offset(u64::MAX).is_err());
    }

    #[test]
    fn test_submit_wait_and_drain_nop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("io_uring_nop.dat");

        let mut dev = IoUringDevice::with_defaults(&path);
        dev.initialize().unwrap();

        // Queue a NOP SQE so the submit/wait APIs have a real completion to work with.
        dev.with_state(|state| {
            let entry = opcode::Nop::new().build().user_data(1);
            unsafe {
                state
                    .ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other(IoUringError::SubmissionQueueFull))?;
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(dev.pending_operations(), 1);
        assert_eq!(dev.submit().unwrap(), 1);
        assert_eq!(dev.pending_operations(), 1);

        let available = dev.wait_completions(1).unwrap();
        assert!(available >= 1);

        assert_eq!(dev.process_completions(), 1);
        assert_eq!(dev.pending_operations(), 0);
    }
}
