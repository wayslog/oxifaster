//! Tests for checkpoint metadata checksum validation

use std::fs;

#[test]
fn test_metadata_checksum_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    let mut meta = oxifaster::checkpoint::LogMetadata::new();
    meta.version = 42;
    meta.write_to_file(&path).unwrap();

    let loaded = oxifaster::checkpoint::LogMetadata::read_from_file(&path).unwrap();
    assert_eq!(loaded.version, 42);
}

#[test]
fn test_metadata_corrupted_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    let mut meta = oxifaster::checkpoint::LogMetadata::new();
    meta.version = 99;
    meta.write_to_file(&path).unwrap();

    // Corrupt a byte in the JSON payload (after the 8-byte header)
    let mut data = fs::read(&path).unwrap();
    assert!(data.len() > 12, "file too small");
    data[12] ^= 0xFF;
    fs::write(&path, &data).unwrap();

    let result = oxifaster::checkpoint::LogMetadata::read_from_file(&path);
    assert!(result.is_err(), "corrupted metadata should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Checksum mismatch"),
        "error should mention checksum: {}",
        err
    );
}

#[test]
fn test_metadata_backward_compatible_loading() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    // Write plain JSON (old format, no checksum magic)
    let json = serde_json::json!({
        "token": "00000000-0000-0000-0000-000000000001",
        "version": 7,
        "num_threads": 0,
        "begin_address": 0,
        "final_address": 100,
        "flushed_until_address": 100,
        "use_object_log": false,
        "session_states": []
    });
    fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();

    let loaded = oxifaster::checkpoint::LogMetadata::read_from_file(&path).unwrap();
    assert_eq!(loaded.version, 7);
}

#[test]
fn test_index_metadata_checksum_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.meta");

    let mut meta = oxifaster::checkpoint::IndexMetadata::new();
    meta.version = 5;
    meta.write_to_file(&path).unwrap();

    let loaded = oxifaster::checkpoint::IndexMetadata::read_from_file(&path).unwrap();
    assert_eq!(loaded.version, 5);
}
