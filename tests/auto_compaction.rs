//! Auto-compaction integration tests.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use oxifaster::compaction::{AutoCompactionConfig, AutoCompactionState};
use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn create_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 12,       // 4 KB pages
        mutable_fraction: 0.9,
    };
    Arc::new(FasterKv::new(config, NullDisk::new()).unwrap())
}

#[test]
fn test_auto_compaction_handle_stop_joins_worker() {
    let store = create_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_millis(10))
        .with_min_log_size(u64::MAX);

    let mut handle = store.start_auto_compaction(config);
    assert!(handle.is_running());

    handle.stop();
    assert!(!handle.is_running());
    assert_eq!(handle.state(), AutoCompactionState::Stopped);
}

#[test]
fn test_auto_compaction_worker_exits_when_store_dropped() {
    let store = create_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_millis(10))
        .with_min_log_size(u64::MAX);

    let handle = store.start_auto_compaction(config);
    drop(store);

    for _ in 0..200 {
        if !handle.is_running() && handle.state() == AutoCompactionState::Stopped {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    assert!(!handle.is_running());
    assert_eq!(handle.state(), AutoCompactionState::Stopped);
}

#[test]
fn test_multiple_auto_compaction_workers_independent_lifecycle() {
    let store = create_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_millis(10))
        .with_min_log_size(u64::MAX);

    let mut handle1 = store.start_auto_compaction(config.clone());
    let handle2 = store.start_auto_compaction(config);

    assert!(handle1.is_running());
    assert!(handle2.is_running());

    handle1.stop();
    assert!(!handle1.is_running());
    assert!(handle2.is_running());
}
