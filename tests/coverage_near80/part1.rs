#[test]
fn cov_index_grow_with_rehash_empty() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024).unwrap();
    index.initialize(&config);

    // Grow with a rehash function that never finds records
    let result = index.grow_with_rehash(|_addr| None);
    assert!(result.success);
    assert_eq!(result.old_size, 1024);
    assert_eq!(result.new_size, 2048);
    assert_eq!(result.entries_migrated, 0);
}

#[test]
fn cov_index_grow_external_not_in_progress() {
    let index = MemHashIndex::new();
    // migrate_chunk_external should return false when grow is not in progress
    let result = index.migrate_chunk_external(0, |_| None);
    assert!(!result);
}

#[test]
fn cov_index_complete_grow_not_in_progress() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024).unwrap();
    index.initialize(&config);

    // complete_grow when no grow is in progress should fail
    let result = index.complete_grow();
    assert!(!result.success);
}

#[test]
fn cov_index_complete_grow_chunks_remaining() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024).unwrap();
    index.initialize(&config);

    // Start grow but don't migrate any chunks
    let _ = index.start_grow();

    // complete_grow with chunks remaining should fail
    let result = index.complete_grow();
    assert!(!result.success);
}

#[test]
fn cov_index_grow_custom_growth_factor() {
    let grow_config = GrowConfig::new().with_growth_factor(4);
    let mut index = MemHashIndex::with_grow_config(grow_config);
    let config = MemHashIndexConfig::new(256).unwrap();
    index.initialize(&config);

    let result = index.grow_with_rehash(|_| None);
    assert!(result.success);
    assert_eq!(result.old_size, 256);
    assert_eq!(result.new_size, 1024); // 256 * 4
}

#[test]
#[allow(deprecated)]
fn cov_index_deprecated_grow() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024).unwrap();
    index.initialize(&config);

    // Test the deprecated grow() method
    let result = index.grow();
    // Since no rehash function is provided, all entries will fail rehashing
    // but the growth operation itself succeeds for empty index
    assert!(result.success);
}

// ===========================================================================
// 2. Delta log iterator tests (src/delta_log/iterator.rs)
// Test more edge cases in the iterator
// ===========================================================================

// Note: delta_log tests are covered in coverage_epoch_delta.rs

// ===========================================================================
// 3. FasterLog tests (src/log/faster_log.rs)
// ===========================================================================

#[test]
fn cov_faster_log_append_empty_data_error() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Appending empty data should fail
    let result = log.append(&[]);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), oxifaster::Status::InvalidArgument);
}

#[test]
fn cov_faster_log_append_too_large() {
    let config = FasterLogConfig {
        page_size: 512, // Minimum valid page size (must be multiple of alignment 512)
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Appending data larger than page size minus header should fail
    // LogEntryHeader is 16 bytes, so max payload is 512 - 16 = 496 bytes
    let large_data = vec![0u8; 500];
    let result = log.append(&large_data);
    assert!(result.is_err());
}

#[test]
fn cov_faster_log_append_batch() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    let entries: Vec<&[u8]> = vec![b"entry1", b"entry2", b"entry3"];
    let addresses = log.append_batch(&entries).unwrap();

    assert_eq!(addresses.len(), 3);
}

#[test]
fn cov_faster_log_read_entry_invalid_address() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Read from an address that doesn't exist
    let result = log.read_entry(Address::new(1000, 0));
    assert!(result.is_none());
}

#[test]
fn cov_faster_log_scan() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Append some data
    log.append(b"test data 1").unwrap();
    log.append(b"test data 2").unwrap();

    let begin = log.get_begin_address();
    let end = log.get_tail_address();

    let iter = log.scan(begin, end);
    assert_eq!(iter.current_address(), begin);
}

#[test]
fn cov_faster_log_scan_all() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    log.append(b"test").unwrap();

    let iter = log.scan_all();
    let _ = iter.current_address();
}

#[test]
fn cov_faster_log_truncate_before() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    log.append(b"test data").unwrap();
    let committed = log.commit().unwrap();

    // Wait for commit
    let _ = log.wait_for_commit(committed);

    // truncate_before with address beyond committed should fail
    let future_addr = Address::new(1000, 0);
    let result = log.truncate_before(future_addr);
    assert!(result.is_err());
}

#[test]
fn cov_faster_log_truncate_before_already_truncated() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    let begin = log.get_begin_address();
    // Truncate before current begin should return current begin
    let result = log.truncate_before(begin);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), begin);
}

#[test]
fn cov_faster_log_truncate_until() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    log.append(b"test data").unwrap();
    let committed = log.commit().unwrap();
    let _ = log.wait_for_commit(committed);

    // truncate_until at current begin should be Ok
    let begin = log.get_begin_address();
    let status = log.truncate_until(begin);
    assert_eq!(status, oxifaster::Status::Ok);
}

#[test]
fn cov_faster_log_reclaimable_space() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Empty log should have no reclaimable space
    let space = log.get_reclaimable_space();
    assert_eq!(space, 0);
}

#[test]
fn cov_faster_log_last_error() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Trigger an error by appending empty data
    let _ = log.append(&[]);

    // Check last error
    let error = log.last_error();
    assert!(error.is_some());
}

#[test]
fn cov_faster_log_is_closed() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    assert!(!log.is_closed());

    log.close();

    assert!(log.is_closed());
}

#[test]
fn cov_faster_log_get_stats() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    let stats = log.get_stats();
    assert_eq!(stats.page_size, 4096);
    assert_eq!(stats.buffer_pages, 8);
}

#[test]
fn cov_faster_log_self_check() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };

    let result = FasterLog::self_check(config, NullDisk::new());
    // NullDisk doesn't have persistent metadata
    assert!(result.is_ok());
}

#[test]
fn cov_faster_log_self_check_with_options() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };

    let options = FasterLogSelfCheckOptions {
        repair: false,
        dry_run: true,
        create_if_missing: true,
    };

    let result = FasterLog::self_check_with_options(config, NullDisk::new(), options);
    assert!(result.is_ok());
}

#[test]
fn cov_faster_log_open_with_options() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };

    let options = FasterLogOpenOptions::default()
        .with_self_check(true)
        .with_create_if_missing(true);

    let result = FasterLog::open_with_options(config, NullDisk::new(), options);
    assert!(result.is_ok());
}

// ===========================================================================
// 4. FasterKv tests (src/store/faster_kv.rs)
// ===========================================================================

#[test]
fn cov_faster_kv_head_address() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    // Insert some data
    let mut session = store.start_session().unwrap();
    session.upsert(1u64, 100u64);

    // Get head address (internal method tested through session)
    let log_stats = store.log_stats();
    let _ = log_stats.head_address;
}

#[test]
fn cov_faster_kv_conditional_insert_exists() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    // First insert
    let status = session.conditional_insert(42u64, 100u64);
    assert_eq!(status, oxifaster::Status::Ok);

    // Second conditional insert should fail
    let status2 = session.conditional_insert(42u64, 200u64);
    assert_eq!(status2, oxifaster::Status::Aborted);
}

#[test]
fn cov_faster_kv_rmw_not_found() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    // RMW on non-existent key should return NotFound
    let status = session.rmw(42u64, |v| {
        *v += 1;
        true
    });
    assert_eq!(status, oxifaster::Status::NotFound);
}

#[test]
fn cov_faster_kv_rmw_abort() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 100u64);

    // RMW that returns false should abort
    let status = session.rmw(42u64, |_| false);
    assert_eq!(status, oxifaster::Status::Aborted);
}

#[test]
fn cov_faster_kv_checkpoint_durability_settings() {
    let config = FasterKvConfig::default();
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    // Test log checkpoint backend settings
    let backend = store.log_checkpoint_backend();
    store.set_log_checkpoint_backend(backend);

    // Test checkpoint durability settings
    let durability = store.checkpoint_durability();
    store.set_checkpoint_durability(durability);
}

#[test]
fn cov_faster_kv_read_not_found() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 16, // Small log to force disk
        page_size_bits: 12,
        mutable_fraction: 0.5,
    };
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    // Read non-existent key returns None
    let result = session.read(&42u64);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ===========================================================================
// 5. HybridLog tests (src/allocator/hybrid_log.rs)
// ===========================================================================

// HybridLog is internal but tested through FasterKv operations

#[test]
fn cov_hybrid_log_through_store() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::new(config, NullDisk::new()).unwrap());

    let mut session = store.start_session().unwrap();

    // Insert enough data to trigger page transitions
    for i in 0..1000u64 {
        session.upsert(i, i * 10);
    }

    let log_stats = store.log_stats();
    assert!(log_stats.tail_address > log_stats.begin_address);
}

// ===========================================================================
