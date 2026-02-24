//! Incremental checkpoint example (public API).
//!
//! This example:
//! - creates a base snapshot checkpoint
//! - mutates data and creates an incremental checkpoint (delta log)
//! - validates checkpoint metadata and chain
//! - validates delta log artifacts are persisted
//! - optionally prints Prometheus text exposition (feature = "prometheus")

use std::sync::Arc;

use oxifaster::checkpoint::CheckpointInfo;
use oxifaster::checkpoint::{delta_log_path, delta_metadata_path};
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
    let text = PrometheusRenderer::new().render_snapshot(&store.stats_snapshot());
    println!("\n--- Prometheus metrics (store snapshot) ---\n{text}");
    assert!(text.contains("oxifaster_checkpoints_started_total"));
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
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config.clone(), device).unwrap());

    // Seed initial data.
    let mut session = expect_ok("start_session", store.start_session());
    session.upsert(1, 10);
    session.upsert(2, 20);
    session.refresh();

    // Establish a snapshot base checkpoint.
    let base_token = store.checkpoint_full_snapshot(&checkpoint_dir)?;
    let base_dir = checkpoint_dir.join(base_token.to_string());
    assert!(
        base_dir.join("log.snapshot").exists(),
        "expected base snapshot to contain log.snapshot"
    );

    // Mutate data after base.
    session.upsert(2, 21);
    session.upsert(3, 30);
    assert_eq!(expect_ok("read(2)", session.read(&2)), Some(21));
    session.refresh();

    // Create an incremental checkpoint.
    let inc_token = store.checkpoint_incremental(&checkpoint_dir)?;
    assert!(
        base_dir.join("log.snapshot").exists(),
        "expected base snapshot log.snapshot to still exist after incremental checkpoint"
    );

    // Validate metadata indicates incremental checkpoint and links to the base snapshot.
    let (_index_meta, log_meta) =
        FasterKv::<u64, u64, FileSystemDisk>::get_checkpoint_info(&checkpoint_dir, inc_token)?;
    assert!(log_meta.is_incremental, "expected incremental checkpoint");
    assert_eq!(
        log_meta.prev_snapshot_token,
        Some(base_token),
        "expected incremental checkpoint to point to base snapshot"
    );

    // Validate the incremental checkpoint can be loaded via the public recovery metadata API.
    let mut inc_info = CheckpointInfo::new(inc_token, checkpoint_dir.join(inc_token.to_string()));
    inc_info.load()?;
    assert!(
        inc_info.is_valid(),
        "expected checkpoint metadata to be valid"
    );
    assert!(inc_info.is_incremental(), "expected incremental checkpoint");
    assert_eq!(inc_info.base_snapshot_token(), Some(base_token));

    // Validate incremental artifacts were persisted.
    let inc_dir = checkpoint_dir.join(inc_token.to_string());
    assert!(inc_dir.join("index.meta").exists());
    assert!(inc_dir.join("index.dat").exists());
    assert!(inc_dir.join("log.meta").exists());
    assert!(delta_log_path(&inc_dir, 0).exists());
    assert!(delta_metadata_path(&inc_dir).exists());

    print_prometheus(&store);
    Ok(())
}
