//! Operational helpers example (public API).
//!
//! This example demonstrates:
//! - creating a valid checkpoint
//! - introducing an invalid checkpoint directory
//! - running `ops::checkpoint_self_check` with repair enabled
//! - recovering the latest valid checkpoint via `ops::recover_latest`
//! - optionally rendering the self-check report as Prometheus text exposition

use std::sync::Arc;

use oxifaster::device::FileSystemDisk;
use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check, recover_latest};
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;
use uuid::Uuid;

#[cfg(feature = "prometheus")]
use oxifaster::stats::prometheus::PrometheusRenderer;

fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
    match r {
        Ok(v) => v,
        Err(status) => panic!("{ctx} failed: {status:?}"),
    }
}

#[cfg(feature = "prometheus")]
fn render_report(report: &oxifaster::ops::CheckpointSelfCheckReport) -> String {
    PrometheusRenderer::new().render_checkpoint_self_check_report(report)
}

#[cfg(not(feature = "prometheus"))]
fn render_report(_report: &oxifaster::ops::CheckpointSelfCheckReport) -> String {
    String::new()
}

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

    // Create a valid checkpoint.
    let store = Arc::new(FasterKv::<u64, u64, _>::new(
        config.clone(),
        FileSystemDisk::single_file(&data_path)?,
    ));
    {
        let mut session = expect_ok("start_session", store.start_session());
        for i in 0u64..128 {
            session.upsert(i, i * 10);
        }
    }
    let _token = store.checkpoint(&checkpoint_dir)?;

    // Introduce an invalid checkpoint directory (missing metadata files).
    let invalid_token = Uuid::new_v4();
    std::fs::create_dir_all(checkpoint_dir.join(invalid_token.to_string()))?;

    // Run self-check with repair enabled.
    let options = CheckpointSelfCheckOptions {
        repair: true,
        dry_run: false,
        remove_invalid: true,
    };
    let report = checkpoint_self_check(&checkpoint_dir, options)?;
    assert!(report.total >= 1);
    assert!(
        report.invalid >= 1,
        "expected at least one invalid checkpoint"
    );
    assert!(
        report.removed.contains(&invalid_token),
        "expected invalid checkpoint to be removed during repair"
    );

    let metrics = render_report(&report);
    if !metrics.is_empty() {
        println!("\n--- Prometheus metrics (checkpoint self-check) ---\n{metrics}");
        assert!(metrics.contains("oxifaster_checkpoint_self_check_total"));
    }

    // Recover the latest valid checkpoint using the ops helper.
    let (recovered_token, recovered_store) = recover_latest::<u64, u64, _>(
        &checkpoint_dir,
        config,
        FileSystemDisk::single_file(&data_path)?,
    )?;
    println!("Recovered token: {recovered_token}");

    let recovered_store = Arc::new(recovered_store);
    let mut session = expect_ok("start_session(recovered)", recovered_store.start_session());
    assert_eq!(expect_ok("read(7)", session.read(&7)), Some(70));
    Ok(())
}
