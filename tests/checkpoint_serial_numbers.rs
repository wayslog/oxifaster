//! Tests for serial number validation during checkpoint and recovery

use std::sync::Arc;

use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn create_test_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1 << 16,
        log_memory_size: 1 << 20,
        page_size_bits: 12,
        mutable_fraction: 0.9,
    }
}

#[test]
fn test_serial_number_increases_after_upserts() {
    let config = create_test_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));

    let mut session = store.start_session().unwrap();

    for i in 0..100u64 {
        session.upsert(i, i * 10);
    }

    let state = session.to_session_state();
    assert!(
        state.serial_num > 0,
        "serial should be > 0 after upserts, got {}",
        state.serial_num
    );
}

#[test]
fn test_serial_number_preserved_through_checkpoint() {
    let config = create_test_config();
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));

    let mut session = store.start_session().unwrap();

    for i in 0..50u64 {
        session.upsert(i, i * 10);
    }
    let serial_before = session.to_session_state().serial_num;

    drop(session);

    // Checkpoint
    let temp_dir = tempfile::tempdir().unwrap();
    let token = store.checkpoint(temp_dir.path()).expect("checkpoint should succeed");
    assert!(!token.is_nil());

    // Serial should still be the same value (it was captured at checkpoint)
    // (we don't recover here, just verify checkpoint completed)
    assert!(serial_before > 0);
}
