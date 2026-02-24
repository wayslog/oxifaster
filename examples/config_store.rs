#![allow(clippy::collapsible_if)]

use std::sync::Arc;

use oxifaster::config::OxifasterConfig;
use oxifaster::device::FileSystemDisk;
#[cfg(feature = "prometheus")]
use oxifaster::stats::prometheus::PrometheusRenderer;
use oxifaster::store::FasterKv;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A small, deterministic override to ensure the config loader path is exercised.
    //
    // Note: `OxifasterConfig::load_from_env` rejects unknown `OXIFASTER__...` keys, so keep
    // overrides minimal and valid.
    unsafe { std::env::set_var("OXIFASTER__store__table_size", "1024") };

    let config = OxifasterConfig::load_from_env()?;
    let store_config = config.to_faster_kv_config();
    let compaction_config = config.to_compaction_config();
    let cache_config = config.to_read_cache_config();

    let device = match config.open_device()? {
        Some(device) => device,
        None => FileSystemDisk::single_file("oxifaster.db")?,
    };

    let store = match cache_config {
        Some(cache) => Arc::new(FasterKv::with_read_cache(store_config, device, cache).unwrap()),
        None => Arc::new(
            FasterKv::with_compaction_config(store_config, device, compaction_config).unwrap(),
        ),
    };

    let mut session = store
        .start_session()
        .map_err(|status| std::io::Error::other(format!("start session failed: {status:?}")))?;
    session.upsert(1u64, 10u64);
    let v = session
        .read(&1u64)
        .map_err(|status| std::io::Error::other(format!("read failed: {status:?}")))?;
    assert_eq!(v, Some(10u64));

    #[cfg(feature = "prometheus")]
    {
        let snapshot = store.stats_snapshot();
        let text = PrometheusRenderer::new().render_snapshot(&snapshot);
        println!("\n--- Prometheus metrics (store stats) ---\n{text}");
        assert!(text.contains("oxifaster_operations_total"));
    }

    Ok(())
}
