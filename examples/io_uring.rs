//! `IoUringDevice` example.
//!
//! Demonstrates using `IoUringDevice` as the storage device for `FasterKv`.
//!
//! Run:
//! - Linux + real io_uring backend: `cargo run --example io_uring --features io_uring`
//! - Other platforms / feature disabled: falls back to a portable file-backed implementation

use std::sync::Arc;

use oxifaster::device::IoUringDevice;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

fn main() {
    println!("=== oxifaster IoUringDevice example ===\n");

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    println!("Running on Linux + feature=io_uring: using the real io_uring backend\n");

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    println!(
        "Running on non-Linux or without feature=io_uring: using the portable fallback backend\n"
    );

    let dir = tempdir().expect("failed to create temp directory");
    let data_path = dir.path().join("oxifaster_io_uring.dat");

    // 1. Configuration.
    let config = FasterKvConfig::default();

    // 2. Create the device (falls back when io_uring isn't available).
    let mut device = IoUringDevice::with_defaults(&data_path);
    device.initialize().expect("device initialization failed");

    // 3. Create the store and perform basic operations.
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session();

    let upsert_status = session.upsert(1, 100);
    assert_eq!(upsert_status, Status::Ok);

    let read = session.read(&1).expect("read failed");
    println!("read key=1: {read:?}");

    let delete_status = session.delete(&1);
    println!("delete key=1: {delete_status:?}");

    println!("\ndata file: {}", data_path.display());
    println!("=== done ===");
}
