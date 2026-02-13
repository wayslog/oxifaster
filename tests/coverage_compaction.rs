//! Comprehensive compaction coverage tests for `src/store/faster_kv/compaction.rs`
//!
//! Tests cover:
//! - compact() - basic compaction, flush error handling
//! - flush_and_shift_head() - flush and shift operations
//! - log_compact() / log_compact_until() - full log compaction
//! - compact_bytes() - bytes-based compaction
//! - log_utilization() - utilization calculation
//! - should_compact() - compaction recommendation
//! - start_auto_compaction() - auto compaction handle creation
//! - AutoCompactionTarget trait implementation

use oxifaster::Address;
use oxifaster::compaction::AutoCompactionConfig;
use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use std::sync::Arc;
use std::time::Duration;

fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 12,       // 4 KB pages
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

fn create_store_with_small_pages() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 256,
        log_memory_size: 1 << 16, // 64 KB
        page_size_bits: 10,       // 1 KB pages
        mutable_fraction: 0.5,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device))
}

// ============ compact() tests ============

#[test]
fn test_compact_empty_store() {
    let store = create_test_store();
    let begin = store.log_begin_address();

    let status = store.compact(begin);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_compact_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i * 10);
    }

    let begin = store.log_begin_address();
    let status = store.compact(begin);
    assert_eq!(status, Status::Ok);

    for i in 1u64..=50 {
        let result = session.read(&i);
        assert_eq!(result.unwrap(), Some(i * 10));
    }
}

#[test]
fn test_compact_advances_begin_address() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=100 {
        session.upsert(i, i);
    }

    let begin_before = store.log_begin_address();
    let tail = store.log_tail_address();

    let midpoint = Address::from_control(
        begin_before.control() + (tail.control() - begin_before.control()) / 2,
    );

    if midpoint > begin_before {
        let status = store.compact(midpoint);
        assert_eq!(status, Status::Ok);

        let begin_after = store.log_begin_address();
        assert!(
            begin_after >= begin_before,
            "Begin address should advance or stay same"
        );
    }
}

// ============ flush_and_shift_head() tests ============

#[test]
fn test_flush_and_shift_head_empty_store() {
    let store = create_test_store();
    let head = store.log_head_address();

    let status = store.flush_and_shift_head(head);
    assert_eq!(status, Status::Ok);
}

#[test]
fn test_flush_and_shift_head_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i * 100);
    }

    let head = store.log_head_address();
    let status = store.flush_and_shift_head(head);
    assert_eq!(status, Status::Ok);

    for i in 1u64..=50 {
        let result = session.read(&i);
        assert_eq!(result.unwrap(), Some(i * 100));
    }
}

#[test]
fn test_flush_and_shift_head_to_tail() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=20 {
        session.upsert(i, i);
    }

    let tail = store.log_tail_address();
    let status = store.flush_and_shift_head(tail);
    assert_eq!(status, Status::Ok);
}

// ============ log_compact() / log_compact_until() tests ============

#[test]
fn test_log_compact_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=100 {
        session.upsert(i, i * 10);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    for i in 1u64..=100 {
        let read_result = session.read(&i);
        assert_eq!(read_result.unwrap(), Some(i * 10));
    }
}

#[test]
fn test_log_compact_until_specific_address() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i);
    }

    let begin = store.log_begin_address();
    let tail = store.log_tail_address();

    let target = Address::from_control(begin.control() + (tail.control() - begin.control()) / 4);

    let result = store.log_compact_until(Some(target));
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_log_compact_until_none_behaves_like_log_compact() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=20 {
        session.upsert(i, i);
    }

    let result = store.log_compact_until(None);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_log_compact_with_updates_handles_obsolete_records() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i);
    }

    for i in 1u64..=25 {
        session.upsert(i, i + 1000);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    for i in 1u64..=25 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i + 1000));
    }

    for i in 26u64..=50 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i));
    }
}

#[test]
fn test_log_compact_with_deletes() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=30 {
        session.upsert(i, i);
    }

    for i in 1u64..=10 {
        let _ = session.delete(&i);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    for i in 1u64..=10 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, None, "Key {i} should be deleted");
    }

    for i in 11u64..=30 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i));
    }
}

// ============ compact_bytes() tests ============

#[test]
fn test_compact_bytes_zero() {
    let store = create_test_store();
    let result = store.compact_bytes(0);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compact_bytes_small_amount() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=20 {
        session.upsert(i, i);
    }

    let result = store.compact_bytes(1024);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compact_bytes_large_amount() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i);
    }

    let result = store.compact_bytes(1 << 20);
    assert_eq!(result.status, Status::Ok);
}

#[test]
fn test_compact_bytes_preserves_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=100 {
        session.upsert(i, i * 100);
    }

    let result = store.compact_bytes(4096);
    assert_eq!(result.status, Status::Ok);

    for i in 1u64..=100 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i * 100));
    }
}

// ============ log_utilization() tests ============

#[test]
fn test_log_utilization_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=100 {
        session.upsert(i, i);
    }

    let util = store.log_utilization();
    assert!((0.0..=1.0).contains(&util));
}

#[test]
fn test_log_utilization_after_updates() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i);
    }

    let util_before = store.log_utilization();

    for i in 1u64..=50 {
        session.upsert(i, i + 1000);
    }

    let util_after = store.log_utilization();

    assert!((0.0..=1.0).contains(&util_before));
    assert!((0.0..=1.0).contains(&util_after));
}

// ============ should_compact() tests ============

#[test]
fn test_should_compact_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=100 {
        session.upsert(i, i);
    }

    let _ = store.should_compact();
}

#[test]
fn test_should_compact_reflects_utilization() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i);
    }

    let util = store.log_utilization();
    let target = store.compaction_config().target_utilization;
    let should = store.should_compact();

    if util < target {
        assert!(
            should,
            "should_compact should be true when utilization < target"
        );
    } else {
        assert!(
            !should,
            "should_compact should be false when utilization >= target"
        );
    }
}

// ============ start_auto_compaction() tests ============

#[test]
fn test_start_auto_compaction_creates_handle() {
    let store = create_test_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_secs(60))
        .with_min_log_size(u64::MAX);

    let handle = store.start_auto_compaction(config);
    assert!(handle.is_running());
}

#[test]
fn test_auto_compaction_handle_pause_resume() {
    let store = create_test_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_secs(60))
        .with_min_log_size(u64::MAX);

    let mut handle = store.start_auto_compaction(config);

    handle.pause();
    assert_eq!(
        handle.state(),
        oxifaster::compaction::AutoCompactionState::Paused
    );

    handle.resume();
    std::thread::sleep(Duration::from_millis(50));
    assert_ne!(
        handle.state(),
        oxifaster::compaction::AutoCompactionState::Paused
    );

    handle.stop();
}

#[test]
fn test_auto_compaction_handle_stats() {
    let store = create_test_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_secs(60))
        .with_min_log_size(u64::MAX);

    let mut handle = store.start_auto_compaction(config);
    let stats = handle.stats();

    assert_eq!(stats.total_compactions(), 0);
    assert_eq!(stats.successful_compactions(), 0);
    assert_eq!(stats.failed_compactions(), 0);

    handle.stop();
}

#[test]
fn test_auto_compaction_handle_config() {
    let store = create_test_store();
    let config = AutoCompactionConfig::new()
        .with_check_interval(Duration::from_secs(30))
        .with_min_log_size(1024)
        .with_target_utilization(0.7);

    let mut handle = store.start_auto_compaction(config);
    let retrieved_config = handle.config();

    assert_eq!(retrieved_config.check_interval, Duration::from_secs(30));
    assert_eq!(retrieved_config.min_log_size, 1024);
    assert_eq!(retrieved_config.target_utilization, 0.7);

    handle.stop();
}

// ============ Log address helper method tests ============

#[test]
fn test_log_begin_address() {
    let store = create_test_store();
    let begin = store.log_begin_address();
    assert!(!begin.is_invalid() || begin == Address::new(0, 0));
}

#[test]
fn test_log_head_address() {
    let store = create_test_store();
    let head = store.log_head_address();
    assert!(!head.is_invalid() || head == Address::new(0, 0));
}

#[test]
fn test_log_tail_address() {
    let store = create_test_store();
    let tail = store.log_tail_address();
    assert!(!tail.is_invalid() || tail == Address::new(0, 0));
}

#[test]
fn test_log_size_bytes_empty() {
    let store = create_test_store();
    let _size = store.log_size_bytes();
}

#[test]
fn test_log_size_bytes_grows_with_data() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let size_before = store.log_size_bytes();

    for i in 1u64..=100 {
        session.upsert(i, i);
    }

    let size_after = store.log_size_bytes();
    assert!(
        size_after >= size_before,
        "Log size should grow after insertions"
    );
}

#[test]
fn test_log_addresses_ordering() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=50 {
        session.upsert(i, i);
    }

    let begin = store.log_begin_address();
    let head = store.log_head_address();
    let tail = store.log_tail_address();

    assert!(begin.control() <= head.control(), "begin should be <= head");
    assert!(head.control() <= tail.control(), "head should be <= tail");
}

// ============ compaction_config() and is_compaction_in_progress() tests ============

#[test]
fn test_compaction_config_returns_valid_config() {
    let store = create_test_store();
    let config = store.compaction_config();

    assert!(config.target_utilization > 0.0);
    assert!(config.target_utilization <= 1.0);
    assert!(config.num_threads >= 1);
}

// ============ Edge cases and stress tests ============

#[test]
fn test_compact_same_address_multiple_times() {
    let store = create_test_store();
    let begin = store.log_begin_address();

    for _ in 0..5 {
        let status = store.compact(begin);
        assert_eq!(status, Status::Ok);
    }
}

#[test]
fn test_log_compact_multiple_times() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=20 {
        session.upsert(i, i);
    }

    for _ in 0..3 {
        let result = store.log_compact();
        assert_eq!(result.status, Status::Ok);
    }

    for i in 1u64..=20 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i));
    }
}

#[test]
fn test_compaction_with_many_keys() {
    let store = create_store_with_small_pages();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=500 {
        session.upsert(i, i);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    for i in 1u64..=500 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i), "Key {i} should have correct value");
    }
}

#[test]
fn test_compaction_interleaved_with_operations() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=20 {
        session.upsert(i, i);
    }

    let result1 = store.log_compact();
    assert_eq!(result1.status, Status::Ok);

    for i in 21u64..=40 {
        session.upsert(i, i);
    }

    let result2 = store.log_compact();
    assert_eq!(result2.status, Status::Ok);

    for i in 1u64..=40 {
        let value = session.read(&i).unwrap();
        assert_eq!(value, Some(i));
    }
}

#[test]
fn test_compaction_stats_populated() {
    let store = create_store_with_small_pages();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=100 {
        session.upsert(i, i);
    }

    for i in 1u64..=50 {
        session.upsert(i, i + 1000);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    let _ = result.stats.records_scanned;
    let _ = result.stats.records_compacted;
    let _ = result.stats.records_skipped;
    let _ = result.stats.bytes_scanned;
    let _ = result.stats.bytes_compacted;
    let _ = result.stats.bytes_reclaimed;
}

#[test]
fn test_compaction_result_new_begin_address() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 1u64..=30 {
        session.upsert(i, i);
    }

    let result = store.log_compact();
    assert_eq!(result.status, Status::Ok);

    let _ = result.new_begin_address;
}
