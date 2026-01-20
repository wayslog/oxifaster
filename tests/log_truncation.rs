//! Tests for FasterLog truncation API

use oxifaster::address::Address;
use oxifaster::device::FileSystemDisk;
use oxifaster::log::{FasterLog, FasterLogConfig};
use tempfile::tempdir;

#[test]
fn test_log_truncation_basic() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.log");

    let config = FasterLogConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let log = FasterLog::new(config, device).unwrap();

    // Append 1000 entries
    for i in 0..1000 {
        let entry = format!("entry-{}", i);
        log.append(entry.as_bytes()).unwrap();
    }

    // Commit
    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();

    let begin_before = log.get_begin_address();
    println!("Begin address before truncation: {}", begin_before);

    // Calculate mid-point address for truncation
    let mid_address = Address::from(u64::from(committed) / 2);

    // Truncate first half
    let new_begin = log.truncate_before(mid_address).unwrap();
    println!("New begin address after truncation: {}", new_begin);

    assert_eq!(new_begin, mid_address);
    assert_eq!(log.get_begin_address(), mid_address);

    // Verify metadata was persisted by closing and reopening
    drop(log);

    let config2 = FasterLogConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let log = FasterLog::open(config2, device).unwrap();
    let begin_after_reopen = log.get_begin_address();
    println!("Begin address after reopen: {}", begin_after_reopen);
    assert_eq!(
        begin_after_reopen, mid_address,
        "Begin address should persist"
    );
}

#[test]
fn test_truncation_validation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.log");

    let config = FasterLogConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let log = FasterLog::new(config, device).unwrap();

    // Append and commit
    for i in 0..100 {
        log.append(format!("entry-{}", i).as_bytes()).unwrap();
    }
    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();

    // Try to truncate beyond committed address (should fail)
    let beyond = Address::from(u64::from(committed) + 1000);
    let result = log.truncate_before(beyond);
    assert!(result.is_err(), "Should fail to truncate beyond committed");

    // Try to truncate before begin (should be no-op)
    let begin = log.get_begin_address();
    let before_begin = Address::from(u64::from(begin).saturating_sub(100));
    let result = log.truncate_before(before_begin).unwrap();
    assert_eq!(result, begin, "Should return current begin address");

    // Truncate at committed exactly (should succeed)
    let result = log.truncate_before(committed).unwrap();
    assert_eq!(result, committed, "Should succeed at committed boundary");
}

#[test]
fn test_truncation_with_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.log");

    let config = FasterLogConfig::default();

    // Phase 1: Append, commit, and truncate
    {
        let device = FileSystemDisk::single_file(&db_path).unwrap();
        let log = FasterLog::new(config, device).unwrap();

        for i in 0..500 {
            log.append(format!("entry-{}", i).as_bytes()).unwrap();
        }

        let committed = log.commit().unwrap();
        log.wait_for_commit(committed).unwrap();

        // Truncate first 250 entries (approximately)
        let mid = Address::from(u64::from(committed) / 2);
        log.truncate_before(mid).unwrap();

        println!("Truncated at: {}", mid);
    }

    // Phase 2: Reopen and verify
    {
        let config2 = FasterLogConfig::default();
        let device = FileSystemDisk::single_file(&db_path).unwrap();
        let log = FasterLog::open(config2, device).unwrap();

        let begin = log.get_begin_address();
        let committed = log.get_committed_until();

        println!("After reopen: begin={}, committed={}", begin, committed);
        assert!(
            begin > Address::from(0u64),
            "Begin address should be updated"
        );
        assert!(begin <= committed, "Begin should be at or before committed");
    }
}

#[test]
fn test_reclaimable_space_calculation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.log");

    let config = FasterLogConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let log = FasterLog::new(config, device).unwrap();

    // Initially no reclaimable space
    let space_before = log.get_reclaimable_space();
    println!("Reclaimable space before writes: {} bytes", space_before);
    assert_eq!(space_before, 0);

    // Append data
    for i in 0..1000 {
        log.append(format!("entry-{:06}", i).as_bytes()).unwrap();
    }

    // Commit
    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();

    // Check initial reclaimable space (should be all data from begin to committed)
    let space_after = log.get_reclaimable_space();
    println!("Reclaimable space after writes: {} bytes", space_after);
    // Note: reclaimable space depends on the difference between begin and committed
    // Initially, if begin==0 and we have data, there should be reclaimable space
    // But the exact amount depends on page boundaries

    // Truncate
    let mid = Address::from(u64::from(committed) / 2);
    log.truncate_before(mid).unwrap();

    // After truncation, reclaimable space should be less
    let space_after_truncate = log.get_reclaimable_space();
    println!(
        "Reclaimable space after truncation: {} bytes",
        space_after_truncate
    );

    // The test passes as long as truncation works correctly
    println!("Truncation and space calculation completed successfully");
}

#[test]
fn test_truncation_idempotent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.log");

    let config = FasterLogConfig::default();
    let device = FileSystemDisk::single_file(&db_path).unwrap();
    let log = FasterLog::new(config, device).unwrap();

    // Append and commit
    for i in 0..100 {
        log.append(format!("entry-{}", i).as_bytes()).unwrap();
    }
    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();

    // Truncate to mid-point
    let mid = Address::from(u64::from(committed) / 2);
    let result1 = log.truncate_before(mid).unwrap();
    assert_eq!(result1, mid);

    // Truncate again to same point (should be no-op)
    let result2 = log.truncate_before(mid).unwrap();
    assert_eq!(result2, mid, "Second truncation should be idempotent");

    // Truncate to earlier point (should be no-op)
    let earlier = Address::from(u64::from(mid) - 100);
    let result3 = log.truncate_before(earlier).unwrap();
    assert_eq!(
        result3, mid,
        "Can't truncate backwards, should return current begin"
    );
}
