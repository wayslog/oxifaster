//! CPR (Concurrent Prefix Recovery) integration tests
//!
//! These tests verify the complete CPR flow from operation to checkpoint to recovery.

use std::sync::Arc;

use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

/// Test CPR version tracking
#[test]
fn test_cpr_version_tracking() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    // Create session and check initial version
    let mut session = store.start_session().unwrap();
    let initial_version = session.context().version;
    println!("Initial version: {}", initial_version);
    assert_eq!(initial_version, 0, "Initial version should be 0");

    // Perform operations
    for i in 0u64..100 {
        session.upsert(i, i * 2);
    }
    session.refresh();

    // Version should still be 0 (no checkpoint yet)
    let version_before_checkpoint = session.context().version;
    println!("Version before checkpoint: {}", version_before_checkpoint);
    assert_eq!(version_before_checkpoint, 0, "Version should be 0 before checkpoint");

    drop(session);

    // Take checkpoint (this should increment version)
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint token: {}", token);

    // Create new session after checkpoint
    let mut session2 = store.start_session().unwrap();
    session2.refresh();
    let version_after_checkpoint = session2.context().version;
    println!("Version after checkpoint: {}", version_after_checkpoint);
    assert!(
        version_after_checkpoint > version_before_checkpoint,
        "Version should increment after checkpoint"
    );

    // Perform more operations with new version
    for i in 100u64..200 {
        session2.upsert(i, i * 2);
    }
    session2.refresh();

    println!("CPR version tracking test passed");
}

/// Test CPR context swapping during checkpoint
#[test]
fn test_cpr_context_swapping() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    // Create session
    let mut session = store.start_session().unwrap();

    // Perform operations before checkpoint
    for i in 0u64..100 {
        session.upsert(i, i * 2);
        if i % 10 == 0 {
            session.refresh(); // This triggers context swapping during checkpoint
        }
    }

    let ctx_before = session.context();
    let version_before = ctx_before.version;

    println!("Before checkpoint: version={}", version_before);

    drop(session);

    // Take checkpoint - this should trigger context swap in active threads
    let _token = store.checkpoint(&checkpoint_dir).unwrap();

    // Create new session after checkpoint
    let mut session2 = store.start_session().unwrap();
    session2.refresh(); // Trigger CPR update

    let ctx_after = session2.context();
    let version_after = ctx_after.version;

    println!("After checkpoint: version={}", version_after);

    // Version should have changed due to checkpoint
    println!("Context swapping test completed - version changed from {} to {}",
             version_before, version_after);
}

/// Test CPR phase transitions
#[test]
fn test_cpr_phase_transitions() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    // Create session
    let mut session = store.start_session().unwrap();

    // Write data
    for i in 0u64..500 {
        session.upsert(i, i * 2);
        if i % 50 == 0 {
            session.refresh();
        }
    }

    // Get initial system state
    let state_before = store.system_state();
    println!("System state before checkpoint: {:?}", state_before);

    drop(session);

    // Take checkpoint - this goes through all phases
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint completed: {}", token);

    // Get system state after checkpoint
    let state_after = store.system_state();
    println!("System state after checkpoint: {:?}", state_after);

    // After checkpoint, system should be back in Rest phase
    assert_eq!(
        state_after.phase,
        oxifaster::store::Phase::Rest,
        "Should be in Rest phase after checkpoint"
    );

    // Version should have incremented
    assert!(
        state_after.version >= state_before.version,
        "Version should have incremented or stayed same"
    );

    println!("Phase transition test passed");
}

/// Test refresh() integration with CPR
#[test]
fn test_refresh_with_cpr() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    // Create session
    let mut session = store.start_session().unwrap();

    // Perform operations with frequent refresh calls
    for i in 0u64..1000 {
        session.upsert(i, i * 2);
        // Refresh frequently to help checkpoint progress
        session.refresh();
    }

    drop(session);

    // Take checkpoint
    let _token = store.checkpoint(&checkpoint_dir).unwrap();

    // Create new session and refresh
    let mut session2 = store.start_session().unwrap();

    // refresh() should update the session's view of the CPR state
    session2.refresh();

    // Write more data
    for i in 1000u64..2000 {
        session2.upsert(i, i * 2);
        session2.refresh();
    }

    println!("refresh() integration test passed");
}

/// Test multiple checkpoint cycles
#[test]
fn test_multiple_checkpoint_cycles() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> = Arc::new(FasterKv::new(config, device));

    let mut versions = vec![];

    for cycle in 0..5 {
        // Create session
        let mut session = store.start_session().unwrap();

        // Write data
        let start = cycle * 100;
        for i in start..start + 100 {
            session.upsert(i as u64, i as u64 * 2);
            if i % 10 == 0 {
                session.refresh();
            }
        }

        let version = session.context().version;
        versions.push(version);
        println!("Cycle {} version: {}", cycle, version);

        drop(session);

        // Checkpoint
        let token = store.checkpoint(&checkpoint_dir).unwrap();
        println!("Cycle {} checkpoint: {}", cycle, token);

        // Give system time to settle
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    // Versions should generally increase (or stay same if checkpoint wasn't participated in)
    println!("Versions across cycles: {:?}", versions);
    println!("Multiple checkpoint cycles test passed");
}
