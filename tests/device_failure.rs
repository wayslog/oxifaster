//! Device failure tests using the FaultInjectionDevice.
//!
//! These tests verify that I/O errors from the storage device are properly
//! propagated rather than silently ignored.

mod common;

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use oxifaster::device::{StorageDevice, SyncStorageDevice};

use common::FaultInjectionDevice;

/// A simple in-memory device for testing that actually stores data.
struct MemDevice {
    data: std::sync::Mutex<Vec<u8>>,
    size: AtomicU64,
}

impl MemDevice {
    fn new(initial_size: usize) -> Self {
        Self {
            data: std::sync::Mutex::new(vec![0u8; initial_size]),
            size: AtomicU64::new(initial_size as u64),
        }
    }
}

impl SyncStorageDevice for MemDevice {
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
fn test_write_error_during_flush() {
    let inner = MemDevice::new(4096);
    let device = FaultInjectionDevice::new(inner);

    // First write should succeed.
    let buf = [0xABu8; 512];
    let n = device.write_sync(0, &buf).unwrap();
    assert_eq!(n, 512);
    assert_eq!(device.write_count(), 1);

    // Inject error at the second write.
    device.inject_write_error_at(2);

    let result = device.write_sync(512, &buf);
    assert!(
        result.is_err(),
        "Second write should fail with injected error"
    );
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(
        format!("{err}").contains("injected write error"),
        "Error message should mention injection"
    );
}

#[test]
fn test_flush_error_propagated() {
    let inner = MemDevice::new(4096);
    let device = FaultInjectionDevice::new(inner);

    // Normal flush should succeed.
    device.flush_sync().unwrap();

    // Inject flush error.
    device.inject_flush_error();

    let result = device.flush_sync();
    assert!(
        result.is_err(),
        "flush_sync should return the injected error"
    );
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(
        format!("{err}").contains("injected flush error"),
        "Error message should mention injection"
    );

    // Subsequent flush should succeed (one-shot injection).
    device.flush_sync().unwrap();
}

#[test]
fn test_partial_write_truncates_data() {
    let inner = MemDevice::new(4096);
    let device = FaultInjectionDevice::new(inner);

    // Inject a partial write: only 64 bytes out of 512.
    device.inject_partial_write(64);

    let buf = [0xCDu8; 512];
    let n = device.write_sync(0, &buf).unwrap();
    assert_eq!(n, 64, "Partial write should only write 64 bytes");

    // Next write should be unaffected (one-shot injection).
    let n = device.write_sync(0, &buf).unwrap();
    assert_eq!(n, 512, "Subsequent write should be normal");
}

#[test]
fn test_fault_injection_device_passthrough() {
    let inner = MemDevice::new(4096);
    let device = FaultInjectionDevice::new(inner);

    // Write then read back through the fault injection layer.
    let write_buf = b"hello device";
    let written = device.write_sync(100, write_buf).unwrap();
    assert_eq!(written, write_buf.len());

    let mut read_buf = vec![0u8; write_buf.len()];
    let read = device.read_sync(100, &mut read_buf).unwrap();
    assert_eq!(read, write_buf.len());
    assert_eq!(&read_buf, write_buf);

    // size / truncate pass through
    let size = device.size_sync().unwrap();
    assert!(size >= 100 + write_buf.len() as u64);

    device.truncate_sync(50).unwrap();
    assert_eq!(device.size_sync().unwrap(), 50);
}

#[test]
fn test_fault_injection_device_async_trait() {
    // Verify that the blanket StorageDevice impl works through
    // the FaultInjectionDevice (since it implements SyncStorageDevice).
    let inner = MemDevice::new(4096);
    let device = FaultInjectionDevice::new(inner);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let buf = [0xFFu8; 256];
        let written = StorageDevice::write(&device, 0, &buf).await.unwrap();
        assert_eq!(written, 256);

        let mut read_buf = vec![0u8; 256];
        let read = StorageDevice::read(&device, 0, &mut read_buf)
            .await
            .unwrap();
        assert_eq!(read, 256);
        assert_eq!(read_buf, vec![0xFFu8; 256]);

        StorageDevice::flush(&device).await.unwrap();
    });
}

#[test]
fn test_write_error_count_increments() {
    let inner = MemDevice::new(4096);
    let device = FaultInjectionDevice::new(inner);

    assert_eq!(device.write_count(), 0);

    let buf = [0u8; 64];
    for i in 1..=5 {
        device.write_sync(0, &buf).unwrap();
        assert_eq!(device.write_count(), i);
    }
}
