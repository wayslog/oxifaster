//! Storage device abstraction for FASTER
//!
//! This module provides traits and implementations for storage devices
//! used by FASTER for persisting data to disk.
//!
//! # Overview
//!
//! FASTER uses a device abstraction layer to support multiple storage backends.
//! All devices implement the [`StorageDevice`] trait, which provides async
//! read/write operations.
//!
//! # Available Devices
//!
//! - [`NullDisk`]: In-memory storage for testing (no persistence)
//! - [`FileSystemDisk`]: Standard file system storage
//! - [`IoUringDevice`]: Linux io_uring-based async I/O (mock implementation)
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::device::{NullDisk, FileSystemDisk, StorageDevice};
//!
//! // In-memory testing device
//! let test_device = NullDisk::new();
//!
//! // File-based persistent storage
//! let file_device = FileSystemDisk::single_file("/path/to/data.db")?;
//!
//! // Segmented storage (multiple files)
//! let segmented = FileSystemDisk::segmented("/path/to/data/", 1 << 30)?; // 1GB segments
//! ```
//!
//! # Custom Devices
//!
//! To implement a custom storage device, implement either [`StorageDevice`]
//! (for async devices) or [`SyncStorageDevice`] (for synchronous devices,
//! which automatically get an async wrapper).

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
