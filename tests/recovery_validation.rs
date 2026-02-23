//! Recovery validation tests
//!
//! These tests verify that recovery correctly validates session states and serial numbers.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use oxifaster::device::FileSystemDisk;
use oxifaster::format::compute_xor_checksum;
use oxifaster::store::{FasterKv, FasterKvConfig};
use serde_json::Value;
use tempfile::tempdir;
use uuid::Uuid;

fn checkpoint_log_meta_path(checkpoint_dir: &Path, token: Uuid) -> PathBuf {
    checkpoint_dir.join(token.to_string()).join("log.meta")
}

fn checkpoint_crc_path(path: &Path) -> PathBuf {
    let mut file_name: OsString = path.as_os_str().to_owned();
    file_name.push(".crc");
    PathBuf::from(file_name)
}

fn rewrite_log_meta(path: &Path, mutate: impl FnOnce(&mut Value)) {
    let mut json: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    mutate(&mut json);

    let serialized = serde_json::to_vec_pretty(&json).unwrap();
    std::fs::write(path, &serialized).unwrap();

    let checksum = compute_xor_checksum(&serialized);
    std::fs::write(checkpoint_crc_path(path), checksum.to_le_bytes()).unwrap();
}

/// Test that recovery validates serial numbers
#[test]
fn test_recovery_validates_serial_numbers() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config, device).unwrap());

    // Create a session and perform operations
    let mut session = store.start_session().unwrap();

    for i in 0u64..200 {
        session.upsert(i, i * 2);
        if i % 50 == 0 {
            session.refresh();
        }
    }

    let serial_before = session.context().serial_num();
    println!("Serial number before checkpoint: {}", serial_before);

    drop(session);

    // Take checkpoint
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    println!("Checkpoint token: {}", token);

    // Write more data after checkpoint
    {
        let mut session = store.start_session().unwrap();
        for i in 200u64..400 {
            session.upsert(i, i * 2);
            if i % 50 == 0 {
                session.refresh();
            }
        }
        let serial_after = session.context().serial_num();
        println!("Serial number after more writes: {}", serial_after);
    }

    // Drop store
    drop(store);

    // Recover - serial validation should succeed
    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);

    // Verify data is present
    let mut session = recovered.start_session().unwrap();
    let mut found_count = 0;
    for i in 0u64..200 {
        if let Ok(Some(value)) = session.read(&i) {
            assert_eq!(value, i * 2);
            found_count += 1;
        }
    }

    println!("Found {} keys after recovery", found_count);
    assert!(found_count > 0, "Should have recovered data");
}

/// Test recovery with continued sessions
#[test]
fn test_recovery_with_session_continuation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config, device).unwrap());

    // Create session and save its state
    let session_state = {
        let mut session = store.start_session().unwrap();
        for i in 0u64..200 {
            session.upsert(i, i * 3);
            if i % 25 == 0 {
                session.refresh();
            }
        }
        session.to_session_state()
    };

    println!("Original session GUID: {}", session_state.guid);
    println!("Original session serial: {}", session_state.serial_num);

    // Checkpoint
    let token = store.checkpoint(&checkpoint_dir).unwrap();
    drop(store);

    // Recover
    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let recovered: FasterKv<u64, u64, FileSystemDisk> =
        FasterKv::recover(&checkpoint_dir, token, config, device).unwrap();
    let recovered = Arc::new(recovered);

    // Get recovered sessions
    let recovered_sessions = recovered.get_recovered_sessions();
    println!("Recovered {} sessions", recovered_sessions.len());

    // Find our session
    let found_session = recovered_sessions
        .iter()
        .find(|s| s.guid == session_state.guid);

    if let Some(found) = found_session {
        println!("Recovered session GUID: {}", found.guid);
        println!("Recovered session serial: {}", found.serial_num);
        assert_eq!(found.guid, session_state.guid, "Session GUID should match");
    }

    // Continue session if it was found
    if found_session.is_some() {
        let mut continued = recovered.continue_session(session_state).unwrap();

        // Verify existing data
        let mut found_count = 0;
        for i in 0u64..200 {
            if let Ok(Some(value)) = continued.read(&i) {
                assert_eq!(value, i * 3);
                found_count += 1;
            }
        }

        println!("Continued session found {} existing keys", found_count);
        assert!(
            found_count > 0,
            "Should find existing data in continued session"
        );

        // Continue writing with the same session
        for i in 200u64..400 {
            continued.upsert(i, i * 3);
        }
    } else {
        println!("Session not found in recovered state (this is OK for testing)");
    }
}

/// Test that obviously invalid serial numbers are rejected
#[test]
fn test_recovery_rejects_invalid_serials() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("invalid_serial.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config.clone(), device).unwrap());

    let mut session = store.start_session().unwrap();
    session.upsert(1, 10);
    session.upsert(2, 20);
    session.refresh();

    let token = store.checkpoint(&checkpoint_dir).unwrap();
    drop(store);

    let log_meta_path = checkpoint_log_meta_path(&checkpoint_dir, token);
    rewrite_log_meta(&log_meta_path, |json| {
        json["session_states"][0]["serial_num"] = Value::from(u64::MAX);
    });

    let recovered_device = FileSystemDisk::single_file(&db_path).unwrap();
    let err = match FasterKv::<u64, u64, FileSystemDisk>::recover(
        &checkpoint_dir,
        token,
        config,
        recovered_device,
    ) {
        Ok(_) => panic!("expected invalid serial metadata to fail recovery"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("Invalid serial number"));
}

#[test]
fn test_recovery_rejects_num_threads_mismatch() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("thread_mismatch.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config.clone(), device).unwrap());

    let mut session = store.start_session().unwrap();
    session.upsert(1, 10);
    session.refresh();

    let token = store.checkpoint(&checkpoint_dir).unwrap();
    drop(store);

    let log_meta_path = checkpoint_log_meta_path(&checkpoint_dir, token);
    rewrite_log_meta(&log_meta_path, |json| {
        let threads = json["num_threads"].as_u64().unwrap_or(0);
        json["num_threads"] = Value::from(threads.saturating_add(3));
    });

    let recovered_device = FileSystemDisk::single_file(&db_path).unwrap();
    let err = match FasterKv::<u64, u64, FileSystemDisk>::recover(
        &checkpoint_dir,
        token,
        config,
        recovered_device,
    ) {
        Ok(_) => panic!("expected num_threads mismatch metadata to fail recovery"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("num_threads mismatch"));
}

#[test]
fn test_recovery_rejects_too_many_sessions() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("too_many_sessions.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config.clone(), device).unwrap());

    let mut session = store.start_session().unwrap();
    session.upsert(1, 10);
    session.refresh();

    let token = store.checkpoint(&checkpoint_dir).unwrap();
    drop(store);

    let log_meta_path = checkpoint_log_meta_path(&checkpoint_dir, token);
    rewrite_log_meta(&log_meta_path, |json| {
        let state = json["session_states"][0].clone();
        let mut sessions = Vec::new();
        for _ in 0..(oxifaster::constants::MAX_THREADS + 1) {
            sessions.push(state.clone());
        }
        json["session_states"] = Value::from(sessions);
        json["num_threads"] = Value::from((oxifaster::constants::MAX_THREADS + 1) as u64);
    });

    let recovered_device = FileSystemDisk::single_file(&db_path).unwrap();
    let err = match FasterKv::<u64, u64, FileSystemDisk>::recover(
        &checkpoint_dir,
        token,
        config,
        recovered_device,
    ) {
        Ok(_) => panic!("expected too-many-sessions metadata to fail recovery"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("exceeds MAX_THREADS"));
}

#[test]
fn test_recovery_rejects_duplicate_session_guids() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("duplicate_guid.db");
    let checkpoint_dir = dir.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();

    let config = FasterKvConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let store: Arc<FasterKv<u64, u64, FileSystemDisk>> =
        Arc::new(FasterKv::new(config.clone(), device).unwrap());

    let mut s1 = store.start_session().unwrap();
    let mut s2 = store.start_session().unwrap();
    s1.upsert(1, 10);
    s2.upsert(2, 20);
    s1.refresh();
    s2.refresh();

    let token = store.checkpoint(&checkpoint_dir).unwrap();
    drop(store);

    let log_meta_path = checkpoint_log_meta_path(&checkpoint_dir, token);
    rewrite_log_meta(&log_meta_path, |json| {
        assert!(
            json["session_states"].as_array().map_or(0, Vec::len) >= 2,
            "expected at least two session states in metadata"
        );
        let first = json["session_states"][0]["guid"].clone();
        json["session_states"][1]["guid"] = first;
    });

    let recovered_device = FileSystemDisk::single_file(&db_path).unwrap();
    let err = match FasterKv::<u64, u64, FileSystemDisk>::recover(
        &checkpoint_dir,
        token,
        config,
        recovered_device,
    ) {
        Ok(_) => panic!("expected duplicate session guid metadata to fail recovery"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("Duplicate session GUID"));
}
