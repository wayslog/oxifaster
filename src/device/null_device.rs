//! Null storage device for testing
//!
//! This module provides a null device that discards all writes and returns
//! zeros for all reads. Useful for testing and benchmarking in-memory operations.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::device::SyncStorageDevice;

/// Null disk device
///
/// A storage device that discards all writes and returns zeros for reads.
/// Useful for testing and in-memory-only operation modes.
pub struct NullDisk {
    /// Logical size of the device
    size: AtomicU64,
}

impl NullDisk {
    /// Create a new null disk
    pub fn new() -> Self {
        Self {
            size: AtomicU64::new(0),
        }
    }

    /// Create a null disk with a specified initial size
    pub fn with_size(size: u64) -> Self {
        Self {
            size: AtomicU64::new(size),
        }
    }
}

impl Default for NullDisk {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncStorageDevice for NullDisk {
    fn read_sync(&self, _offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Fill with zeros
        buf.fill(0);
        Ok(buf.len())
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        // Update size if needed
        let new_end = offset + buf.len() as u64;
        loop {
            let current = self.size.load(Ordering::Acquire);
            if new_end <= current {
                break;
            }
            if self
                .size
                .compare_exchange(current, new_end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        // Discard the data
        Ok(buf.len())
    }

    fn flush_sync(&self) -> io::Result<()> {
        Ok(())
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        self.size.store(size, Ordering::Release);
        Ok(())
    }

    fn size_sync(&self) -> io::Result<u64> {
        Ok(self.size.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_disk_read() {
        let disk = NullDisk::new();
        let mut buf = [1u8; 100];

        let result = disk.read_sync(0, &mut buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_null_disk_write() {
        let disk = NullDisk::new();
        let buf = [42u8; 100];

        let result = disk.write_sync(0, &buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);

        // Size should be updated
        assert_eq!(disk.size_sync().unwrap(), 100);
    }

    #[test]
    fn test_null_disk_size() {
        let disk = NullDisk::with_size(1024);
        assert_eq!(disk.size_sync().unwrap(), 1024);

        disk.truncate_sync(512).unwrap();
        assert_eq!(disk.size_sync().unwrap(), 512);
    }
}
