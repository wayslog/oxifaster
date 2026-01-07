//! io_uring 设备实现
//!
//! - 默认情况下（非 Linux 或未启用 `feature = "io_uring"`）提供 **mock 实现**，用于保持 API 兼容与编译通过。
//! - 在 **Linux + `feature = "io_uring"`** 下提供真实 io_uring 后端实现。

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
