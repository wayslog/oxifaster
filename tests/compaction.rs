//! Compaction 集成测试
//!
//! 测试日志压缩的各项功能，包括配置、压缩器、并发压缩等。

use std::sync::Arc;
use std::thread;

use oxifaster::compaction::{
    CompactionConfig, CompactionResult, CompactionStats, Compactor, ConcurrentCompactionConfig,
    ConcurrentCompactionContext, ConcurrentCompactor,
};
use oxifaster::device::NullDisk;
use oxifaster::scan::ScanRange;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::Address;

// ============ Helper Functions ============

fn create_store_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 14,       // 16 KB pages
        mutable_fraction: 0.9,
    }
}

fn create_store_with_compaction() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = create_store_config();
    let device = NullDisk::new();
    let compaction_config = CompactionConfig::new()
        .with_target_utilization(0.5)
        .with_min_compact_bytes(0);
    Arc::new(FasterKv::with_compaction_config(
        config,
        device,
        compaction_config,
    ))
}

// ============ CompactionConfig Tests ============

#[test]
fn test_compaction_config_default() {
    let config = CompactionConfig::default();

    assert_eq!(config.target_utilization, 0.5);
    assert_eq!(config.min_compact_bytes, 1 << 20);
    assert_eq!(config.max_compact_bytes, 1 << 30);
    assert_eq!(config.num_threads, 1);
    assert!(config.compact_tombstones);
}

#[test]
fn test_compaction_config_builder() {
    let config = CompactionConfig::new()
        .with_target_utilization(0.7)
        .with_min_compact_bytes(1 << 22)
        .with_max_compact_bytes(1 << 28)
        .with_num_threads(4);

    assert_eq!(config.target_utilization, 0.7);
    assert_eq!(config.min_compact_bytes, 1 << 22);
    assert_eq!(config.max_compact_bytes, 1 << 28);
    assert_eq!(config.num_threads, 4);
}

#[test]
fn test_compaction_config_clamp_utilization() {
    let config1 = CompactionConfig::new().with_target_utilization(1.5);
    assert_eq!(config1.target_utilization, 1.0);

    let config2 = CompactionConfig::new().with_target_utilization(-0.5);
    assert_eq!(config2.target_utilization, 0.0);
}

#[test]
fn test_compaction_config_min_threads() {
    let config = CompactionConfig::new().with_num_threads(0);
    assert_eq!(config.num_threads, 1); // Should be clamped to 1
}

// ============ CompactionStats Tests ============

#[test]
fn test_compaction_stats_default() {
    let stats = CompactionStats::default();

    assert_eq!(stats.records_scanned, 0);
    assert_eq!(stats.records_compacted, 0);
    assert_eq!(stats.bytes_reclaimed, 0);
}

#[test]
fn test_compaction_stats_live_ratio() {
    let stats = CompactionStats {
        records_scanned: 100,
        records_compacted: 60,
        ..Default::default()
    };

    assert_eq!(stats.live_ratio(), 0.6);
}

#[test]
fn test_compaction_stats_live_ratio_zero() {
    let stats = CompactionStats::default();
    assert_eq!(stats.live_ratio(), 0.0);
}

#[test]
fn test_compaction_stats_compaction_ratio() {
    let stats = CompactionStats {
        bytes_scanned: 10000,
        bytes_reclaimed: 4000,
        ..Default::default()
    };

    assert_eq!(stats.compaction_ratio(), 0.4);
}

#[test]
fn test_compaction_stats_compaction_ratio_zero() {
    let stats = CompactionStats::default();
    assert_eq!(stats.compaction_ratio(), 0.0);
}

// ============ CompactionResult Tests ============

#[test]
fn test_compaction_result_success() {
    let stats = CompactionStats {
        records_compacted: 100,
        bytes_reclaimed: 5000,
        ..Default::default()
    };
    let new_addr = Address::new(5, 0);

    let result = CompactionResult::success(new_addr, stats.clone());

    assert_eq!(result.status, Status::Ok);
    assert_eq!(result.new_begin_address, new_addr);
    assert_eq!(result.stats.records_compacted, 100);
}

#[test]
fn test_compaction_result_failure() {
    let result = CompactionResult::failure(Status::Aborted);

    assert_eq!(result.status, Status::Aborted);
    assert!(result.new_begin_address.is_invalid());
}

// ============ Compactor Tests ============

#[test]
fn test_compactor_create() {
    let compactor = Compactor::new();

    assert!(!compactor.is_in_progress());
    assert_eq!(compactor.config().target_utilization, 0.5);
}

#[test]
fn test_compactor_with_config() {
    let config = CompactionConfig::new().with_target_utilization(0.7);
    let compactor = Compactor::with_config(config);

    assert_eq!(compactor.config().target_utilization, 0.7);
}

#[test]
fn test_compactor_try_start() {
    let compactor = Compactor::new();

    // First start should succeed
    assert!(compactor.try_start().is_ok());
    assert!(compactor.is_in_progress());

    // Second start should fail
    assert!(compactor.try_start().is_err());

    // After complete, should be able to start again
    compactor.complete();
    assert!(!compactor.is_in_progress());
    assert!(compactor.try_start().is_ok());
}

#[test]
fn test_compactor_calculate_scan_range() {
    let compactor = Compactor::with_config(CompactionConfig::new().with_min_compact_bytes(0));

    let begin = Address::new(0, 0);
    let head = Address::new(10, 0);

    let range = compactor.calculate_scan_range(begin, head, None);
    assert!(range.is_some());

    let range = range.unwrap();
    assert_eq!(range.begin, begin);
    assert_eq!(range.end, head);
}

#[test]
fn test_compactor_calculate_scan_range_with_target() {
    let compactor = Compactor::with_config(CompactionConfig::new().with_min_compact_bytes(0));

    let begin = Address::new(0, 0);
    let head = Address::new(10, 0);
    let target = Address::new(5, 0);

    let range = compactor.calculate_scan_range(begin, head, Some(target));
    assert!(range.is_some());

    let range = range.unwrap();
    assert_eq!(range.begin, begin);
    assert_eq!(range.end, target);
}

#[test]
fn test_compactor_calculate_scan_range_empty() {
    let compactor = Compactor::with_config(CompactionConfig::new().with_min_compact_bytes(0));

    let addr = Address::new(5, 0);

    // begin >= head means nothing to compact
    let range = compactor.calculate_scan_range(addr, addr, None);
    assert!(range.is_none());
}

#[test]
fn test_compactor_calculate_scan_range_max_size() {
    let compactor = Compactor::with_config(
        CompactionConfig::new()
            .with_min_compact_bytes(0)
            .with_max_compact_bytes(100),
    );

    let begin = Address::new(0, 0);
    let head = Address::new(10, 0);

    let range = compactor.calculate_scan_range(begin, head, None);
    assert!(range.is_some());

    let range = range.unwrap();
    // Range should be limited to max_compact_bytes
    assert!(range.end.control() - range.begin.control() <= 100);
}

#[test]
fn test_compactor_should_compact_record() {
    let compactor = Compactor::new();
    let addr1 = Address::new(1, 100);
    let addr2 = Address::new(2, 200);

    // Latest version, not tombstone -> should compact
    assert!(compactor.should_compact_record(addr1, addr1, false));

    // Latest version, is tombstone -> should compact (default config)
    assert!(compactor.should_compact_record(addr1, addr1, true));

    // Not latest version -> should skip
    assert!(!compactor.should_compact_record(addr1, addr2, false));
}

#[test]
fn test_compactor_should_skip_tombstones() {
    let config = CompactionConfig {
        compact_tombstones: false,
        ..Default::default()
    };
    let compactor = Compactor::with_config(config);

    let addr = Address::new(1, 100);

    // Tombstone should be skipped
    assert!(!compactor.should_compact_record(addr, addr, true));

    // Non-tombstone should still be compacted
    assert!(compactor.should_compact_record(addr, addr, false));
}

// ============ ConcurrentCompactionConfig Tests ============

#[test]
fn test_concurrent_config_default() {
    let config = ConcurrentCompactionConfig::default();

    assert_eq!(config.num_threads, 4);
    assert_eq!(config.pages_per_chunk, 16);
    assert!(!config.to_other_store);
}

#[test]
fn test_concurrent_config_builder() {
    let config = ConcurrentCompactionConfig::new(8)
        .with_pages_per_chunk(32)
        .with_page_size(1 << 20)
        .with_to_other_store(true);

    assert_eq!(config.num_threads, 8);
    assert_eq!(config.pages_per_chunk, 32);
    assert_eq!(config.page_size, 1 << 20);
    assert!(config.to_other_store);
}

#[test]
fn test_concurrent_config_min_threads() {
    let config = ConcurrentCompactionConfig::new(0);
    assert_eq!(config.num_threads, 1);
}

#[test]
fn test_concurrent_config_min_pages() {
    let config = ConcurrentCompactionConfig::new(4).with_pages_per_chunk(0);
    assert_eq!(config.pages_per_chunk, 1);
}

#[test]
fn test_concurrent_config_from_compaction_config() {
    let base_config = CompactionConfig::new().with_num_threads(6);
    let concurrent_config = ConcurrentCompactionConfig::from_compaction_config(&base_config);

    assert_eq!(concurrent_config.num_threads, 6);
    assert_eq!(
        concurrent_config.compact_tombstones,
        base_config.compact_tombstones
    );
}

// ============ ConcurrentCompactor Tests ============

#[test]
fn test_concurrent_compactor_create() {
    let config = ConcurrentCompactionConfig::new(4);
    let compactor = ConcurrentCompactor::new(config);

    assert!(!compactor.is_in_progress());
    assert_eq!(compactor.config().num_threads, 4);
}

#[test]
fn test_concurrent_compactor_try_start() {
    let config = ConcurrentCompactionConfig::new(4);
    let compactor = ConcurrentCompactor::new(config);

    // First start should succeed
    assert!(compactor.try_start().is_ok());
    assert!(compactor.is_in_progress());

    // Second start should fail
    assert!(compactor.try_start().is_err());

    // After complete, should be able to start again
    compactor.complete();
    assert!(!compactor.is_in_progress());
    assert!(compactor.try_start().is_ok());
}

// ============ ConcurrentCompactionContext Tests ============

#[test]
fn test_concurrent_context_create() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(10, 0));
    let _context = ConcurrentCompactionContext::new(range, 4, 1 << 14, 8, false);
    // Context created successfully - fields are private
}

#[test]
fn test_concurrent_context_get_next_chunk() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let page_size = 1 << 14; // 16 KB
    let context = ConcurrentCompactionContext::new(range, 2, page_size, 1, false);

    // Should be able to get chunks until exhausted
    let mut chunks = 0;
    while let Some(_chunk) = context.get_next_chunk() {
        chunks += 1;
        if chunks > 100 {
            // Safety limit
            break;
        }
    }

    assert!(chunks > 0);
}

// ============ ScanRange Tests ============

#[test]
fn test_scan_range_create() {
    let begin = Address::new(0, 100);
    let end = Address::new(5, 0);
    let range = ScanRange::new(begin, end);

    assert_eq!(range.begin, begin);
    assert_eq!(range.end, end);
}

#[test]
fn test_scan_range_is_empty() {
    let addr = Address::new(5, 0);

    // Same begin and end means empty
    let empty_range = ScanRange::new(addr, addr);
    assert!(empty_range.is_empty());

    // begin > end also means empty
    let invalid_range = ScanRange::new(Address::new(10, 0), Address::new(5, 0));
    assert!(invalid_range.is_empty());

    // Normal range is not empty
    let normal_range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    assert!(!normal_range.is_empty());
}

// ============ FasterKv Integration Tests ============

#[test]
fn test_store_with_compaction_config() {
    let store = create_store_with_compaction();

    // Store should be functional
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }
    }

    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            let result = session.read(&i);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(i * 10));
        }
    }
}

#[test]
fn test_store_with_updates_creates_versions() {
    let store = create_store_with_compaction();

    {
        let mut session = store.start_session().unwrap();

        // Insert initial values
        for i in 1u64..=50 {
            session.upsert(i, i * 10);
        }

        // Update values (creates new versions)
        for i in 1u64..=50 {
            session.upsert(i, i * 100);
        }

        // Verify latest values
        for i in 1u64..=50 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), Some(i * 100));
        }
    }
}

#[test]
fn test_store_with_deletes() {
    let store = create_store_with_compaction();

    {
        let mut session = store.start_session().unwrap();

        // Insert values
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }

        // Delete half
        for i in 1u64..=50 {
            session.delete(&i);
        }

        // Verify deletions
        for i in 1u64..=50 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), None);
        }

        // Verify remaining
        for i in 51u64..=100 {
            let result = session.read(&i);
            assert_eq!(result.unwrap(), Some(i * 10));
        }
    }
}

// ============ Concurrent Tests ============

#[test]
fn test_compactor_concurrent_try_start() {
    let compactor = Arc::new(Compactor::new());
    let num_threads = 4;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let compactor = compactor.clone();
            thread::spawn(move || compactor.try_start().is_ok())
        })
        .collect();

    let successes: usize = handles
        .into_iter()
        .map(|h| if h.join().unwrap() { 1 } else { 0 })
        .sum();

    // Only one thread should succeed
    assert_eq!(successes, 1);
}

#[test]
fn test_concurrent_compactor_concurrent_try_start() {
    let config = ConcurrentCompactionConfig::new(4);
    let compactor = Arc::new(ConcurrentCompactor::new(config));
    let num_threads = 4;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let compactor = compactor.clone();
            thread::spawn(move || compactor.try_start().is_ok())
        })
        .collect();

    let successes: usize = handles
        .into_iter()
        .map(|h| if h.join().unwrap() { 1 } else { 0 })
        .sum();

    // Only one thread should succeed
    assert_eq!(successes, 1);
}

// ============ Edge Cases ============

#[test]
fn test_compaction_with_zero_utilization() {
    let config = CompactionConfig::new().with_target_utilization(0.0);
    let compactor = Compactor::with_config(config);

    assert_eq!(compactor.config().target_utilization, 0.0);
}

#[test]
fn test_compaction_with_full_utilization() {
    let config = CompactionConfig::new().with_target_utilization(1.0);
    let compactor = Compactor::with_config(config);

    assert_eq!(compactor.config().target_utilization, 1.0);
}

#[test]
fn test_scan_range_with_invalid_addresses() {
    let invalid = Address::INVALID;
    let valid = Address::new(5, 0);

    let range = ScanRange::new(invalid, valid);
    // Should handle gracefully
    assert!(range.begin.is_invalid());
}
