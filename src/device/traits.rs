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
    fn read(
        &self,
        offset: u64,
        buf: &mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>>;

    /// Write data to the device
    ///
    /// Writes `buf` to `offset`.
    fn write(
        &self,
        offset: u64,
        buf: &[u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>>;

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
    fn read(
        &self,
        offset: u64,
        buf: &mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>> {
        let result = self.read_sync(offset, buf);
        Box::pin(async move { result })
    }

    fn write(
        &self,
        offset: u64,
        buf: &[u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + '_>> {
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
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_io_context() {
        let ctx = IoContext::new(42, 1024, 0);
        assert_eq!(ctx.callback_data, 42);
        assert_eq!(ctx.size, 1024);
        assert_eq!(ctx.offset, 0);
    }

    #[test]
    fn test_io_context_debug() {
        let ctx = IoContext::new(100, 512, 4096);
        let debug_str = format!("{:?}", ctx);
        assert!(debug_str.contains("callback_data"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("size"));
        assert!(debug_str.contains("512"));
        assert!(debug_str.contains("offset"));
        assert!(debug_str.contains("4096"));
    }

    #[test]
    fn test_io_context_various_values() {
        // Test with zero values
        let ctx = IoContext::new(0, 0, 0);
        assert_eq!(ctx.callback_data, 0);
        assert_eq!(ctx.size, 0);
        assert_eq!(ctx.offset, 0);

        // Test with max values
        let ctx = IoContext::new(u64::MAX, u64::MAX, u64::MAX);
        assert_eq!(ctx.callback_data, u64::MAX);
        assert_eq!(ctx.size, u64::MAX);
        assert_eq!(ctx.offset, u64::MAX);
    }

    // Mock sync storage device for testing trait default implementations
    struct MockSyncDevice {
        data: std::sync::Mutex<Vec<u8>>,
        size: AtomicU64,
    }

    impl MockSyncDevice {
        fn new() -> Self {
            Self {
                data: std::sync::Mutex::new(vec![0u8; 4096]),
                size: AtomicU64::new(4096),
            }
        }
    }

    impl SyncStorageDevice for MockSyncDevice {
        fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            let data = self.data.lock().unwrap();
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(0);
            }
            let end = std::cmp::min(offset + buf.len(), data.len());
            let len = end - offset;
            buf[..len].copy_from_slice(&data[offset..end]);
            Ok(len)
        }

        fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
            let mut data = self.data.lock().unwrap();
            let offset = offset as usize;
            if offset + buf.len() > data.len() {
                data.resize(offset + buf.len(), 0);
            }
            data[offset..offset + buf.len()].copy_from_slice(buf);
            self.size.store(data.len() as u64, Ordering::SeqCst);
            Ok(buf.len())
        }

        fn flush_sync(&self) -> io::Result<()> {
            Ok(())
        }

        fn truncate_sync(&self, size: u64) -> io::Result<()> {
            let mut data = self.data.lock().unwrap();
            data.resize(size as usize, 0);
            self.size.store(size, Ordering::SeqCst);
            Ok(())
        }

        fn size_sync(&self) -> io::Result<u64> {
            Ok(self.size.load(Ordering::SeqCst))
        }
    }

    #[test]
    fn test_sync_storage_device_default_alignment() {
        let device = MockSyncDevice::new();
        assert_eq!(SyncStorageDevice::alignment(&device), 512);
    }

    #[test]
    fn test_sync_storage_device_default_sector_size() {
        let device = MockSyncDevice::new();
        assert_eq!(SyncStorageDevice::sector_size(&device), 512);
    }

    #[test]
    fn test_storage_device_impl_for_sync() {
        let device = MockSyncDevice::new();

        // Test async read/write through sync implementation
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Write data
            let data = b"Hello, World!";
            let written = StorageDevice::write(&device, 0, data).await.unwrap();
            assert_eq!(written, data.len());

            // Read data back
            let mut buf = vec![0u8; data.len()];
            let read = StorageDevice::read(&device, 0, &mut buf).await.unwrap();
            assert_eq!(read, data.len());
            assert_eq!(&buf, data);
        });
    }

    #[test]
    fn test_storage_device_flush() {
        let device = MockSyncDevice::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = StorageDevice::flush(&device).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_storage_device_truncate() {
        let device = MockSyncDevice::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Truncate to smaller size
            StorageDevice::truncate(&device, 1024).await.unwrap();
            let size = StorageDevice::size(&device).unwrap();
            assert_eq!(size, 1024);

            // Truncate to zero
            StorageDevice::truncate(&device, 0).await.unwrap();
            let size = StorageDevice::size(&device).unwrap();
            assert_eq!(size, 0);
        });
    }

    #[test]
    fn test_storage_device_size() {
        let device = MockSyncDevice::new();
        let size = StorageDevice::size(&device).unwrap();
        assert_eq!(size, 4096);
    }

    #[test]
    fn test_storage_device_alignment() {
        let device = MockSyncDevice::new();
        assert_eq!(StorageDevice::alignment(&device), 512);
    }

    #[test]
    fn test_storage_device_sector_size() {
        let device = MockSyncDevice::new();
        assert_eq!(StorageDevice::sector_size(&device), 512);
    }

    #[test]
    fn test_storage_device_supports_direct_io() {
        struct DirectIoDevice;
        impl SyncStorageDevice for DirectIoDevice {
            fn read_sync(&self, _: u64, _: &mut [u8]) -> io::Result<usize> {
                Ok(0)
            }
            fn write_sync(&self, _: u64, _: &[u8]) -> io::Result<usize> {
                Ok(0)
            }
            fn flush_sync(&self) -> io::Result<()> {
                Ok(())
            }
            fn truncate_sync(&self, _: u64) -> io::Result<()> {
                Ok(())
            }
            fn size_sync(&self) -> io::Result<u64> {
                Ok(0)
            }
        }

        let device = DirectIoDevice;
        // Default implementation returns false
        assert!(!StorageDevice::supports_direct_io(&device));
    }
}
