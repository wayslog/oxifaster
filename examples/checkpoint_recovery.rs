//! Checkpoint and recovery example (public API).
//!
//! This example:
//! - writes a few records
//! - creates a full checkpoint
//! - recovers a new store from the checkpoint
//! - continues a recovered session and validates data
//! - optionally prints Prometheus text exposition (feature = "prometheus")

use std::sync::Arc;

use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

#[cfg(feature = "prometheus")]
use oxifaster::stats::prometheus::PrometheusRenderer;

fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
    match r {
        Ok(v) => v,
        Err(status) => panic!("{ctx} failed: {status:?}"),
    }
}

#[cfg(feature = "prometheus")]
fn print_prometheus(store: &FasterKv<u64, u64, FileSystemDisk>) {
    let snapshot = store.stats_snapshot();
    let text = PrometheusRenderer::new().render_snapshot(&snapshot);
    println!("\n--- Prometheus metrics (store snapshot) ---\n{text}");
    assert!(text.contains("oxifaster_operations_total"));
}

#[cfg(not(feature = "prometheus"))]
fn print_prometheus(_store: &FasterKv<u64, u64, FileSystemDisk>) {}

fn main() -> std::io::Result<()> {
    let dir = tempdir()?;
    let data_path = dir.path().join("oxifaster_store.dat");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir)?;

    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };

    let device = FileSystemDisk::single_file(&data_path)?;
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config.clone(), device));

    // Write data + exercise the basic API surface.
    let mut session = expect_ok("start_session", store.start_session());
    session.upsert(1, 10);
    session.upsert(2, 20);
    assert_eq!(expect_ok("read(1)", session.read(&1)), Some(10));

    let status = session.rmw(2, |v| {
        *v += 1;
        true
    });
    assert_eq!(status, Status::Ok, "rmw(2) failed: {status:?}");
    assert_eq!(expect_ok("read(2)", session.read(&2)), Some(21));

    // Create a checkpoint.
    let token = store.checkpoint(&checkpoint_dir)?;
    let snapshot = store.stats_snapshot();
    assert!(snapshot.checkpoints_started >= 1);
    assert!(snapshot.checkpoints_completed >= 1);

    // Recover and validate.
    let recovered_device = FileSystemDisk::single_file(&data_path)?;
    let recovered = Arc::new(FasterKv::<u64, u64, _>::recover(
        &checkpoint_dir,
        token,
        config,
        recovered_device,
    )?);
    assert!(recovered.stats_snapshot().recoveries_started >= 1);

    // Continue a recovered session.
    let recovered_sessions = recovered.get_recovered_sessions();
    assert!(
        !recovered_sessions.is_empty(),
        "expected at least one recovered session state"
    );
    let mut continued = expect_ok(
        "continue_session",
        recovered.continue_session(recovered_sessions[0].clone()),
    );
    assert_eq!(
        expect_ok("read(1) after recover", continued.read(&1)),
        Some(10)
    );
    assert_eq!(
        expect_ok("read(2) after recover", continued.read(&2)),
        Some(21)
    );

    print_prometheus(recovered.as_ref());
    Ok(())
}
