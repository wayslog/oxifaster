//! CPR (Concurrent Prefix Recovery) integration tests
//!
//! These tests verify the complete CPR flow from operation to checkpoint to recovery.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig, Phase};
use tempfile::tempdir;

fn create_store(path: &std::path::Path) -> Arc<FasterKv<u64, u64, FileSystemDisk>> {
    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(path).unwrap();
    Arc::new(FasterKv::new(config, device).unwrap())
}

fn checkpoint_wait_timeout() -> Duration {
    std::env::var("OXIFASTER_TEST_CHECKPOINT_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(30))
}

/// Test CPR version tracking
#[test]
fn test_cpr_version_tracking() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store = create_store(&db_path);

    let mut session = store.start_session().unwrap();
    let initial_version = session.context().version;
    assert_eq!(initial_version, 0, "Initial version should be 0");

    for i in 0u64..100 {
        assert_eq!(session.upsert(i, i * 2), Status::Ok);
    }
    session.refresh();

    let version_before_checkpoint = session.context().version;
    assert_eq!(
        version_before_checkpoint, 0,
        "Version should be 0 before checkpoint"
    );

    drop(session);

    let token = store.checkpoint(&checkpoint_dir).unwrap();
    assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
        &checkpoint_dir,
        token
    ));

    let mut session2 = store.start_session().unwrap();
    session2.refresh();
    let version_after_checkpoint = session2.context().version;
    assert_eq!(version_after_checkpoint, version_before_checkpoint + 1);

    for i in 100u64..200 {
        assert_eq!(session2.upsert(i, i * 2), Status::Ok);
    }
    session2.refresh();
    assert_eq!(session2.read(&150), Ok(Some(300)));
}

/// Test CPR context swapping during checkpoint
#[test]
fn test_cpr_context_swapping() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store = create_store(&db_path);
    let mut session = store.start_session().unwrap();

    for i in 0u64..128 {
        assert_eq!(session.upsert(i, i * 2), Status::Ok);
        if i % 16 == 0 {
            session.refresh();
        }
    }

    let version_before = session.context().version;
    let checkpoint_base = checkpoint_dir.clone();

    let (tx, rx) = std::sync::mpsc::channel();
    let checkpoint_store = store.clone();
    std::thread::spawn(move || {
        let result = checkpoint_store.checkpoint(&checkpoint_base);
        tx.send(result).unwrap();
    });

    let start = Instant::now();
    let timeout = checkpoint_wait_timeout();
    let mut wrote_during_checkpoint = 0u64;
    let token = loop {
        session.refresh();

        // Keep a few in-flight writes while checkpoint is running, but avoid
        // high-frequency write pressure that can make slow coverage jobs flaky.
        if wrote_during_checkpoint < 8 {
            let key = 10_000 + wrote_during_checkpoint;
            assert_eq!(session.upsert(key, key * 2), Status::Ok);
            wrote_during_checkpoint += 1;
        }

        let elapsed = start.elapsed();
        assert!(
            elapsed < timeout,
            "checkpoint did not complete in time (timeout={:?}, current={:?})",
            timeout,
            store.system_state().phase
        );

        let remaining = timeout - elapsed;
        let wait = remaining.min(Duration::from_millis(25));
        match rx.recv_timeout(wait) {
            Ok(result) => break result.unwrap(),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("checkpoint thread exited unexpectedly");
            }
        }
    };

    session.refresh();
    assert_eq!(session.context().version, version_before + 1);
    assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
        &checkpoint_dir,
        token
    ));
    assert!(wrote_during_checkpoint > 0);
    let last_key = 9_999 + wrote_during_checkpoint;
    assert_eq!(session.read(&last_key), Ok(Some(last_key * 2)));
}

/// Test CPR phase transitions
#[test]
fn test_cpr_phase_transitions() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store = create_store(&db_path);

    let mut session = store.start_session().unwrap();

    for i in 0u64..200 {
        assert_eq!(session.upsert(i, i * 2), Status::Ok);
        if i % 50 == 0 {
            session.refresh();
        }
    }

    let state_before = store.system_state();
    drop(session);

    let token = store.checkpoint(&checkpoint_dir).unwrap();
    let state_after = store.system_state();

    assert_eq!(state_after.phase, Phase::Rest);
    assert!(
        state_after.version > state_before.version,
        "Version should increment after checkpoint"
    );
    assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
        &checkpoint_dir,
        token
    ));
}

/// Test refresh() integration with CPR
#[test]
fn test_refresh_with_cpr() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store = create_store(&db_path);

    let mut session = store.start_session().unwrap();
    let version_before_checkpoint = session.context().version;

    for i in 0u64..256 {
        assert_eq!(session.upsert(i, i * 2), Status::Ok);
        session.refresh();
    }

    drop(session);
    store.checkpoint(&checkpoint_dir).unwrap();

    let mut session2 = store.start_session().unwrap();
    let version_before_refresh = session2.context().version;
    session2.refresh();
    let version_after_refresh = session2.context().version;

    assert!(version_after_refresh >= version_before_refresh);
    assert_eq!(version_after_refresh, version_before_checkpoint + 1);

    for i in 256u64..512 {
        assert_eq!(session2.upsert(i, i * 2), Status::Ok);
        session2.refresh();
    }

    assert_eq!(session2.read(&300), Ok(Some(600)));
}

/// Test multiple checkpoint cycles
#[test]
fn test_multiple_checkpoint_cycles() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let store = create_store(&db_path);

    let mut versions = vec![];
    let mut tokens = HashSet::new();

    for cycle in 0..4 {
        let mut session = store.start_session().unwrap();

        let start = cycle * 100;
        for i in start..start + 80 {
            assert_eq!(session.upsert(i as u64, i as u64 * 2), Status::Ok);
            if i % 10 == 0 {
                session.refresh();
            }
        }

        let version = session.context().version;
        versions.push(version);
        drop(session);

        let token = store.checkpoint(&checkpoint_dir).unwrap();
        assert!(tokens.insert(token), "checkpoint token should be unique");
        assert!(FasterKv::<u64, u64, FileSystemDisk>::checkpoint_exists(
            &checkpoint_dir,
            token
        ));

        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    for window in versions.windows(2) {
        assert!(
            window[1] >= window[0],
            "session versions should be monotonic: {:?}",
            versions
        );
    }
    assert!(versions.last().copied().unwrap_or_default() > 0);
}
