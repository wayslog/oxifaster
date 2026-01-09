//! `IoUringDevice` backend selection.
//!
//! - On **Linux + `feature = "io_uring"`**, this module exposes the real `io_uring` backend.
//! - Otherwise, it exposes a portable file-backed fallback implementation.

#[path = "io_uring_common.rs"]
mod io_uring_common;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[path = "io_uring_linux.rs"]
mod io_uring_linux;

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
#[path = "io_uring_mock.rs"]
mod io_uring_mock;

pub use io_uring_common::{IoOperation, IoUringConfig, IoUringFeatures, IoUringFile, IoUringStats};

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring_linux::IoUringDevice;

#[cfg(not(all(target_os = "linux", feature = "io_uring")))]
pub use io_uring_mock::IoUringDevice;
