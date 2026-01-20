use std::sync::Arc;

use oxifaster::config::OxifasterConfig;
use oxifaster::device::FileSystemDisk;
use oxifaster::store::FasterKv;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = OxifasterConfig::load_from_env()?;
    let store_config = config.to_faster_kv_config();
    let compaction_config = config.to_compaction_config();
    let cache_config = config.to_read_cache_config();

    let device = match config.open_device()? {
        Some(device) => device,
        None => FileSystemDisk::single_file("oxifaster.db")?,
    };

    let store = match cache_config {
        Some(cache) => Arc::new(FasterKv::with_read_cache(store_config, device, cache)),
        None => Arc::new(FasterKv::with_compaction_config(
            store_config,
            device,
            compaction_config,
        )),
    };

    let mut session = store
        .start_session()
        .map_err(|status| std::io::Error::other(format!("start session failed: {status:?}")))?;
    session.upsert(1u64, 10u64);
    let _ = session
        .read(&1u64)
        .map_err(|status| std::io::Error::other(format!("read failed: {status:?}")))?;

    Ok(())
}
