//! Storage device abstraction for FASTER
//!
//! This module provides traits and implementations for storage devices
//! used by FASTER for persisting data to disk.

mod traits;
mod file_device;
mod io_uring;
mod null_device;

pub use traits::{StorageDevice, SyncStorageDevice, AsyncIoCallback, IoContext};
pub use file_device::{FileSystemDisk, FileSystemFile, SegmentedFile};
pub use io_uring::{IoUringConfig, IoUringDevice, IoUringFile, IoUringStats, IoUringFeatures, IoOperation};
pub use null_device::NullDisk;

