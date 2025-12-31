//! Storage device traits for FASTER
//!
//! This module defines the traits for storage devices used by FASTER.

use std::future::Future;
use std::io;
use std::pin::Pin;

/// Callback type for async I/O completion
pub type AsyncIoCallback = Box<dyn FnOnce(io::Result<usize>) + Send + 'static>;

/// Context for I/O operations
#[derive(Debug)]
pub struct IoContext {
    /// User-provided callback data
    pub callback_data: u64,
    /// Number of bytes to transfer
    pub size: u64,
    /// Offset in the file
    pub offset: u64,
}

impl IoContext {
    /// Create a new I/O context
    pub fn new(callback_data: u64, size: u64, offset: u64) -> Self {
        Self {
            callback_data,
            size,
            offset,
        }
    }
}

/// Async storage device trait
///
/// This trait defines the interface for storage devices used by FASTER.
/// Implementations include file system storage and null storage (for testing).
pub trait StorageDevice: Send + Sync + 'static {
    /// Read data from the device
    ///
    /// Reads `buf.len()` bytes from `offset` into `buf`.
    fn read(&self, offset: u64, buf: &mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>>;

    /// Write data to the device
    ///
    /// Writes `buf` to `offset`.
    fn write(&self, offset: u64, buf: &[u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>>;

    /// Flush any buffered writes to stable storage
    fn flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Truncate the device to the specified size
    fn truncate(&self, size: u64) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Get the current size of the device
    fn size(&self) -> io::Result<u64>;

    /// Get the alignment requirement for I/O operations
    fn alignment(&self) -> usize {
        512 // Default sector alignment
    }

    /// Get the sector size
    fn sector_size(&self) -> usize {
        512
    }

    /// Check if the device supports direct I/O
    fn supports_direct_io(&self) -> bool {
        false
    }
}

/// Synchronous storage device trait (for simpler implementations)
pub trait SyncStorageDevice: Send + Sync + 'static {
    /// Read data synchronously
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Write data synchronously
    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize>;

    /// Flush synchronously
    fn flush_sync(&self) -> io::Result<()>;

    /// Truncate synchronously
    fn truncate_sync(&self, size: u64) -> io::Result<()>;

    /// Get the current size
    fn size_sync(&self) -> io::Result<u64>;

    /// Get alignment requirement
    fn alignment(&self) -> usize {
        512
    }

    /// Get sector size
    fn sector_size(&self) -> usize {
        512
    }
}

/// Implement async trait for sync devices
impl<T: SyncStorageDevice> StorageDevice for T {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>> {
        let result = self.read_sync(offset, buf);
        Box::pin(async move { result })
    }

    fn write(&self, offset: u64, buf: &[u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>> {
        let result = self.write_sync(offset, buf);
        Box::pin(async move { result })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        let result = self.flush_sync();
        Box::pin(async move { result })
    }

    fn truncate(&self, size: u64) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        let result = self.truncate_sync(size);
        Box::pin(async move { result })
    }

    fn size(&self) -> io::Result<u64> {
        self.size_sync()
    }

    fn alignment(&self) -> usize {
        SyncStorageDevice::alignment(self)
    }

    fn sector_size(&self) -> usize {
        SyncStorageDevice::sector_size(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_context() {
        let ctx = IoContext::new(42, 1024, 0);
        assert_eq!(ctx.callback_data, 42);
        assert_eq!(ctx.size, 1024);
        assert_eq!(ctx.offset, 0);
    }
}

