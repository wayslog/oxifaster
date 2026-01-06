//! Storage device abstraction for FASTER
//!
//! This module provides traits and implementations for storage devices
//! used by FASTER for persisting data to disk.

mod file_device;
mod io_uring;
mod null_device;
mod traits;

pub use file_device::{FileSystemDisk, FileSystemFile, SegmentedFile};
pub use io_uring::{
    IoOperation, IoUringConfig, IoUringDevice, IoUringFeatures, IoUringFile, IoUringStats,
};
pub use null_device::NullDisk;
pub use traits::{AsyncIoCallback, IoContext, StorageDevice, SyncStorageDevice};
