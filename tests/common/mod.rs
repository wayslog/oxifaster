//! Shared test utilities for crash consistency and fault injection tests.

#![allow(dead_code)]

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use oxifaster::device::SyncStorageDevice;

/// A fault-injection wrapper around any `SyncStorageDevice`.
///
/// Allows deterministic injection of:
/// - write errors at a specific operation count
/// - flush errors on demand
/// - partial (truncated) writes
pub struct FaultInjectionDevice<D> {
    inner: D,
    /// Total number of write_sync calls observed so far.
    write_count: AtomicU64,
    /// When non-zero, the Nth write (1-based) will return an I/O error.
    fail_write_at: AtomicU64,
    /// When true, the next flush_sync call will return an error.
    fail_next_flush: AtomicBool,
    /// When non-zero, the next write will be silently truncated to at most
    /// this many bytes (simulating a partial / torn write).
    partial_write_max: AtomicUsize,
}

impl<D: SyncStorageDevice> FaultInjectionDevice<D> {
    /// Wrap an existing device for fault injection.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            write_count: AtomicU64::new(0),
            fail_write_at: AtomicU64::new(0),
            fail_next_flush: AtomicBool::new(false),
            partial_write_max: AtomicUsize::new(0),
        }
    }

    /// Make the Nth write (1-based) return `io::ErrorKind::Other`.
    pub fn inject_write_error_at(&self, operation_n: u64) {
        self.fail_write_at.store(operation_n, Ordering::SeqCst);
    }

    /// Make the next `flush_sync` call return an error.
    pub fn inject_flush_error(&self) {
        self.fail_next_flush.store(true, Ordering::SeqCst);
    }

    /// Make the next write silently truncate data to at most `max_bytes`.
    pub fn inject_partial_write(&self, max_bytes: usize) {
        self.partial_write_max.store(max_bytes, Ordering::SeqCst);
    }

    /// Return the total number of write_sync calls observed.
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::SeqCst)
    }
}

impl<D: SyncStorageDevice> SyncStorageDevice for FaultInjectionDevice<D> {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_sync(offset, buf)
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let n = self.write_count.fetch_add(1, Ordering::SeqCst) + 1;

        let target = self.fail_write_at.load(Ordering::SeqCst);
        if target != 0 && n == target {
            return Err(io::Error::other(format!(
                "injected write error at operation {n}"
            )));
        }

        let max = self.partial_write_max.swap(0, Ordering::SeqCst);
        if max > 0 && max < buf.len() {
            return self.inner.write_sync(offset, &buf[..max]);
        }

        self.inner.write_sync(offset, buf)
    }

    fn flush_sync(&self) -> io::Result<()> {
        if self.fail_next_flush.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected flush error"));
        }
        self.inner.flush_sync()
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        self.inner.truncate_sync(size)
    }

    fn size_sync(&self) -> io::Result<u64> {
        self.inner.size_sync()
    }

    fn alignment(&self) -> usize {
        self.inner.alignment()
    }

    fn sector_size(&self) -> usize {
        self.inner.sector_size()
    }
}

// ---------------------------------------------------------------------------
// File corruption utilities
// ---------------------------------------------------------------------------

/// Flip one bit at the given byte offset in a file.
pub fn corrupt_byte_at(path: &Path, offset: u64) -> io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0x01; // flip lowest bit
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    Ok(())
}

/// Truncate a file to `new_len` bytes.
pub fn truncate_to(path: &Path, new_len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(new_len)?;
    file.sync_all()?;
    Ok(())
}

/// Delete a file at the given path.
pub fn delete_file(path: &Path) -> io::Result<()> {
    fs::remove_file(path)
}
