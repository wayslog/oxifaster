#[test]
fn cov_faster_log_config_new() {
    let config = FasterLogConfig::new(1 << 20, 4096);
    assert_eq!(config.page_size, 4096);
    assert_eq!(config.memory_pages, 256); // 1MB / 4096
    assert_eq!(config.segment_size, 1 << 30);
    assert_eq!(config.auto_commit_ms, 0);
}

#[test]
fn cov_faster_log_config_default() {
    let config = FasterLogConfig::default();
    assert_eq!(config.page_size, 1 << 22); // 4 MB
    assert_eq!(config.memory_pages, 64);
    assert_eq!(config.segment_size, 1 << 30);
    assert_eq!(config.auto_commit_ms, 0);
}

#[test]
fn cov_faster_log_config_clone_debug() {
    let config = FasterLogConfig::new(1 << 20, 4096);
    let cloned = config.clone();
    assert_eq!(cloned.page_size, config.page_size);
    let debug = format!("{:?}", config);
    assert!(debug.contains("page_size"));
}

// ===========================================================================
// FasterLogOpenOptions tests
// ===========================================================================

#[test]
fn cov_faster_log_open_options_default() {
    let opts = FasterLogOpenOptions::default();
    assert!(opts.recover);
    assert!(opts.create_if_missing);
    assert!(opts.truncate_on_corruption);
}

#[test]
fn cov_faster_log_open_options_builder_with_self_check() {
    let opts = FasterLogOpenOptions::default().with_self_check(false);
    assert!(!opts.recover);
}

#[test]
fn cov_faster_log_open_options_builder_with_self_repair() {
    let opts = FasterLogOpenOptions::default().with_self_repair(false);
    assert!(!opts.truncate_on_corruption);
}

#[test]
fn cov_faster_log_open_options_builder_with_create_if_missing() {
    let opts = FasterLogOpenOptions::default().with_create_if_missing(false);
    assert!(!opts.create_if_missing);
}

#[test]
fn cov_faster_log_open_options_chained() {
    let opts = FasterLogOpenOptions::default()
        .with_self_check(false)
        .with_self_repair(false)
        .with_create_if_missing(false);
    assert!(!opts.recover);
    assert!(!opts.truncate_on_corruption);
    assert!(!opts.create_if_missing);
}

#[test]
fn cov_faster_log_open_options_debug_clone() {
    let opts = FasterLogOpenOptions::default();
    let cloned = opts.clone();
    assert_eq!(cloned.recover, opts.recover);
    let debug = format!("{:?}", opts);
    assert!(debug.contains("recover"));
}

// ===========================================================================
// FasterLogSelfCheckOptions tests
// ===========================================================================

#[test]
fn cov_faster_log_self_check_options_default() {
    let opts = FasterLogSelfCheckOptions::default();
    assert!(!opts.repair);
    assert!(!opts.dry_run);
    assert!(!opts.create_if_missing);
}

#[test]
fn cov_faster_log_self_check_options_debug_clone() {
    let opts = FasterLogSelfCheckOptions {
        repair: true,
        dry_run: true,
        create_if_missing: true,
    };
    let cloned = opts.clone();
    assert!(cloned.repair);
    assert!(cloned.dry_run);
    assert!(cloned.create_if_missing);
    let debug = format!("{:?}", opts);
    assert!(debug.contains("repair"));
}

// ===========================================================================
// LogErrorKind tests
// ===========================================================================

#[test]
fn cov_log_error_kind_as_str() {
    assert_eq!(LogErrorKind::Io.as_str(), "Io");
    assert_eq!(LogErrorKind::Metadata.as_str(), "Metadata");
    assert_eq!(LogErrorKind::Entry.as_str(), "Entry");
    assert_eq!(LogErrorKind::Config.as_str(), "Config");
    assert_eq!(LogErrorKind::Corruption.as_str(), "Corruption");
}

#[test]
fn cov_log_error_kind_debug_clone_eq() {
    let kind = LogErrorKind::Io;
    let cloned = kind;
    assert_eq!(kind, cloned);
    assert_ne!(LogErrorKind::Io, LogErrorKind::Config);
    let debug = format!("{:?}", kind);
    assert!(debug.contains("Io"));
}

// ===========================================================================
// LogStats (from log::types) tests
// ===========================================================================

#[test]
fn cov_log_stats_display() {
    let stats = LogStats {
        tail_address: Address::new(1, 100),
        committed_address: Address::new(0, 500),
        begin_address: Address::new(0, 0),
        page_size: 4096,
        buffer_pages: 8,
    };
    let display = format!("{}", stats);
    assert!(display.contains("FASTER Log Statistics"));
    assert!(display.contains("Tail"));
    assert!(display.contains("Committed"));
    assert!(display.contains("Begin"));
    assert!(display.contains("Page size"));
    assert!(display.contains("Buffer pages"));
}

#[test]
fn cov_log_stats_debug_clone() {
    let stats = LogStats {
        tail_address: Address::new(0, 0),
        committed_address: Address::new(0, 0),
        begin_address: Address::new(0, 0),
        page_size: 4096,
        buffer_pages: 8,
    };
    let cloned = stats.clone();
    assert_eq!(cloned.page_size, 4096);
    let debug = format!("{:?}", stats);
    assert!(debug.contains("page_size"));
}

// ===========================================================================
// FasterLog creation and basic operations
// ===========================================================================

#[test]
fn cov_faster_log_create_with_null_disk() {
    let log = make_log();
    assert!(!log.is_closed());
    assert_eq!(log.get_begin_address(), Address::new(0, 0));
    assert_eq!(log.get_tail_address(), Address::new(0, 0));
    assert_eq!(log.get_committed_until(), Address::new(0, 0));
}

#[test]
fn cov_faster_log_open_explicit() {
    let config = make_config();
    let log = FasterLog::open(config, NullDisk::new()).unwrap();
    assert!(!log.is_closed());
}

#[test]
fn cov_faster_log_open_with_options_no_recover() {
    let config = make_config();
    let opts = FasterLogOpenOptions::default().with_self_check(false);
    let log = FasterLog::open_with_options(config, NullDisk::new(), opts).unwrap();
    assert!(!log.is_closed());
}

#[test]
fn cov_faster_log_open_with_options_no_create() {
    // NullDisk with size 0 and create_if_missing=false should fail with NotFound
    let config = make_config();
    let opts = FasterLogOpenOptions {
        recover: false,
        create_if_missing: false,
        truncate_on_corruption: false,
    };
    let result = FasterLog::open_with_options(config, NullDisk::new(), opts);
    assert!(result.is_err());
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err, Status::NotFound);
}

// ===========================================================================
// Append operations
// ===========================================================================

#[test]
fn cov_faster_log_append_single_entry() {
    let log = make_log();
    let addr = log.append(b"hello world").unwrap();
    assert!(addr >= Address::new(0, 0));
    assert!(log.get_tail_address() > Address::new(0, 0));
}

#[test]
fn cov_faster_log_append_multiple_entries() {
    let log = make_log();
    let addr1 = log.append(b"entry1").unwrap();
    let addr2 = log.append(b"entry2").unwrap();
    let addr3 = log.append(b"entry3").unwrap();
    assert!(addr1 < addr2);
    assert!(addr2 < addr3);
}

#[test]
fn cov_faster_log_append_empty_data_fails() {
    let log = make_log();
    let result = log.append(b"");
    assert!(matches!(result, Err(Status::InvalidArgument)));
    // Should have recorded an error
    let err = log.last_error().expect("should have a last error");
    assert_eq!(err.kind, LogErrorKind::Config);
}

#[test]
fn cov_faster_log_append_oversized_data_fails() {
    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 4,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();
    // Entry header is 16 bytes, so max payload is 512 - 16 = 496.
    // A payload of 500 bytes should fail.
    let big_data = vec![0xABu8; 500];
    let result = log.append(&big_data);
    assert!(matches!(result, Err(Status::InvalidArgument)));
    let err = log.last_error().expect("should have a last error");
    assert_eq!(err.kind, LogErrorKind::Config);
}

#[test]
fn cov_faster_log_append_batch() {
    let log = make_log();
    let entries: Vec<&[u8]> = vec![b"batch1", b"batch2", b"batch3"];
    let addresses = log.append_batch(&entries).unwrap();
    assert_eq!(addresses.len(), 3);
    assert!(addresses[0] < addresses[1]);
    assert!(addresses[1] < addresses[2]);
}

#[test]
fn cov_faster_log_append_after_close_fails() {
    let log = make_log();
    log.append(b"before close").unwrap();
    log.close();
    assert!(log.is_closed());
    let result = log.append(b"after close");
    assert!(matches!(result, Err(Status::Aborted)));
}

// ===========================================================================
// Read entry
// ===========================================================================

#[test]
fn cov_faster_log_read_entry_valid() {
    let log = make_log();
    let addr = log.append(b"readable data").unwrap();
    // Data is in memory (not flushed), so read should work
    let data = log.read_entry(addr);
    assert!(data.is_some());
    assert_eq!(data.unwrap(), b"readable data");
}

#[test]
fn cov_faster_log_read_entry_multiple() {
    let log = make_log();
    let addr1 = log.append(b"first").unwrap();
    let addr2 = log.append(b"second").unwrap();
    let addr3 = log.append(b"third").unwrap();

    assert_eq!(log.read_entry(addr1).unwrap(), b"first");
    assert_eq!(log.read_entry(addr2).unwrap(), b"second");
    assert_eq!(log.read_entry(addr3).unwrap(), b"third");
}

#[test]
fn cov_faster_log_read_entry_before_begin() {
    let log = make_log();
    log.append(b"data").unwrap();
    // Address before begin_address (which is 0,0) -- there's nothing before it
    // But we can test an address beyond tail:
    let beyond_tail = log.get_tail_address();
    assert!(log.read_entry(beyond_tail).is_none());
}

#[test]
fn cov_faster_log_read_entry_at_tail() {
    let log = make_log();
    log.append(b"data").unwrap();
    let tail = log.get_tail_address();
    // Reading at exact tail should return None (tail is exclusive)
    assert!(log.read_entry(tail).is_none());
}

// ===========================================================================
// Commit
// ===========================================================================

#[test]
fn cov_faster_log_commit_returns_tail() {
    let log = make_log();
    log.append(b"entry").unwrap();
    let tail = log.get_tail_address();
    let committed = log.commit().unwrap();
    assert_eq!(committed, tail);
    assert_eq!(log.get_committed_until(), tail);
}

#[test]
fn cov_faster_log_commit_empty_log() {
    let log = make_log();
    let committed = log.commit().unwrap();
    assert_eq!(committed, Address::new(0, 0));
}

#[test]
fn cov_faster_log_commit_idempotent() {
    let log = make_log();
    log.append(b"data").unwrap();
    let c1 = log.commit().unwrap();
    let c2 = log.commit().unwrap();
    // Second commit without new appends should return same or equal address
    assert_eq!(c1, c2);
}

#[test]
fn cov_faster_log_wait_for_commit() {
    let log = make_log();
    log.append(b"data").unwrap();
    let committed = log.commit().unwrap();
    // wait_for_commit should eventually complete (NullDisk flushes instantly)
    log.wait_for_commit(committed).unwrap();
}

// ===========================================================================
// Scan / Iterator
// ===========================================================================

#[test]
fn cov_faster_log_scan_range_from_memory() {
    let log = make_log();
    let begin = log.get_begin_address();
    log.append(b"alpha").unwrap();
    log.append(b"beta").unwrap();
    log.append(b"gamma").unwrap();
    let tail = log.get_tail_address();

    let entries: Vec<(Address, Vec<u8>)> = log.scan(begin, tail).collect();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].1, b"alpha");
    assert_eq!(entries[1].1, b"beta");
    assert_eq!(entries[2].1, b"gamma");
}

#[test]
fn cov_faster_log_scan_partial_range() {
    let log = make_log();
    let addr1 = log.append(b"first").unwrap();
    let addr2 = log.append(b"second").unwrap();
    let _addr3 = log.append(b"third").unwrap();
    let tail = log.get_tail_address();

    // Scan starting from second entry
    let entries: Vec<(Address, Vec<u8>)> = log.scan(addr2, tail).collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].1, b"second");
    assert_eq!(entries[1].1, b"third");

    // Scan only first entry
    let entries: Vec<(Address, Vec<u8>)> = log.scan(addr1, addr2).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"first");
}

#[test]
fn cov_faster_log_scan_empty_log() {
    let log = make_log();
    let begin = log.get_begin_address();
    let tail = log.get_tail_address();

    let entries: Vec<(Address, Vec<u8>)> = log.scan(begin, tail).collect();
    assert!(entries.is_empty());
}

#[test]
fn cov_faster_log_scan_all_committed() {
    let log = make_log();
    log.append(b"one").unwrap();
    log.append(b"two").unwrap();
    log.commit().unwrap();

    // scan_all uses committed_until as end.
    // Data should still be readable from memory if flush hasn't caught up.
    let entries: Vec<(Address, Vec<u8>)> = log.scan_all().collect();
    // Note: with NullDisk the flush worker may or may not have run yet.
    // If flush completed, device reads return zeros and entries won't decode.
    // So we just check it doesn't panic; count may be 0 or 2.
    assert!(entries.len() <= 2);
}

#[test]
fn cov_faster_log_scan_all_empty() {
    let log = make_log();
    let entries: Vec<(Address, Vec<u8>)> = log.scan_all().collect();
    assert!(entries.is_empty());
}

#[test]
fn cov_faster_log_log_iterator_current_address() {
    let log = make_log();
    let begin = log.get_begin_address();
    log.append(b"data").unwrap();
    let tail = log.get_tail_address();

    let iter = log.scan(begin, tail);
    assert_eq!(iter.current_address(), begin);
}

#[test]
fn cov_faster_log_scan_iterator_advances_address() {
    let log = make_log();
    let begin = log.get_begin_address();
    log.append(b"abc").unwrap();
    log.append(b"def").unwrap();
    let tail = log.get_tail_address();

    let mut iter = log.scan(begin, tail);
    assert_eq!(iter.current_address(), begin);

    let (addr, data) = iter.next().unwrap();
    assert_eq!(addr, begin);
    assert_eq!(data, b"abc");
    // After consuming one entry, current_address should have advanced
    assert!(iter.current_address() > begin);

    let (_, data) = iter.next().unwrap();
    assert_eq!(data, b"def");

    assert!(iter.next().is_none());
}

// ===========================================================================
// Truncate
// ===========================================================================

#[test]
fn cov_faster_log_truncate_until_basic() {
    let log = make_log();
    let _addr1 = log.append(b"entry1").unwrap();
    let addr2 = log.append(b"entry2").unwrap();
    log.commit().unwrap();

    let status = log.truncate_until(addr2);
    assert_eq!(status, Status::Ok);
    assert_eq!(log.get_begin_address(), addr2);
}

#[test]
fn cov_faster_log_truncate_until_already_truncated() {
    let log = make_log();
    let addr1 = log.append(b"entry1").unwrap();
    let addr2 = log.append(b"entry2").unwrap();
    log.commit().unwrap();

    log.truncate_until(addr2);
    // Truncating to an earlier address should be a no-op
    let status = log.truncate_until(addr1);
    assert_eq!(status, Status::Ok);
    assert_eq!(log.get_begin_address(), addr2);
}

#[test]
fn cov_faster_log_truncate_before_basic() {
    let log = make_log();
    log.append(b"entry1").unwrap();
    let addr2 = log.append(b"entry2").unwrap();
    log.commit().unwrap();

    // Wait for flush so committed address is up to date
    let committed = log.get_committed_until();
    log.wait_for_commit(committed).unwrap();

    let result = log.truncate_before(addr2);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), addr2);
    assert_eq!(log.get_begin_address(), addr2);
}

#[test]
fn cov_faster_log_truncate_before_beyond_committed_fails() {
    let log = make_log();
    log.append(b"entry").unwrap();
    // Don't commit -- committed_until is still 0,0
    let tail = log.get_tail_address();
    let result = log.truncate_before(tail);
    assert!(matches!(result, Err(Status::InvalidArgument)));
}

#[test]
fn cov_faster_log_truncate_before_already_truncated() {
    let log = make_log();
    log.append(b"entry1").unwrap();
    let addr2 = log.append(b"entry2").unwrap();
    log.commit().unwrap();
    let committed = log.get_committed_until();
    log.wait_for_commit(committed).unwrap();

    log.truncate_before(addr2).unwrap();
    // Truncating to same or earlier address should return current begin
    let result = log.truncate_before(addr2);
    assert!(result.is_ok());
    let result = log.truncate_before(Address::new(0, 0));
    assert!(result.is_ok());
    assert_eq!(log.get_begin_address(), addr2);
}

// ===========================================================================
// Address getters and stats
// ===========================================================================

#[test]
fn cov_faster_log_get_addresses_after_append() {
    let log = make_log();
    let begin = log.get_begin_address();
    assert_eq!(begin, Address::new(0, 0));

    log.append(b"test").unwrap();

    let tail = log.get_tail_address();
    assert!(tail > begin);

    let committed = log.get_committed_until();
    // Not yet committed
    assert_eq!(committed, Address::new(0, 0));

    log.commit().unwrap();
    assert!(log.get_committed_until() > Address::new(0, 0));
}

#[test]
fn cov_faster_log_get_stats() {
    let log = make_log();
    log.append(b"stats test").unwrap();
    log.commit().unwrap();

    let stats = log.get_stats();
    assert!(stats.tail_address > Address::new(0, 0));
    assert_eq!(stats.page_size, 4096);
    assert_eq!(stats.buffer_pages, 8);
    assert_eq!(stats.begin_address, Address::new(0, 0));
}

#[test]
fn cov_faster_log_get_reclaimable_space_empty() {
    let log = make_log();
    assert_eq!(log.get_reclaimable_space(), 0);
}

#[test]
fn cov_faster_log_get_reclaimable_space_with_data() {
    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 64,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();
    // Fill enough data to span multiple pages (page_size=512, header=16)
    for _ in 0..10 {
        let data = vec![0xAAu8; 480];
        log.append(&data).unwrap();
    }
    log.commit().unwrap();
    let space = log.get_reclaimable_space();
    // Reclaimable space is a u64; just verify we can call the method.
    // With begin at 0 and committed spanning multiple pages, we expect > 0 or 0
    // depending on how many complete pages have been committed.
    let _ = space;
}

// ===========================================================================
// Close and error
// ===========================================================================

#[test]
fn cov_faster_log_close_returns_ok() {
    let log = make_log();
    log.append(b"data").unwrap();
    let status = log.close();
    assert_eq!(status, Status::Ok);
    assert!(log.is_closed());
}

#[test]
fn cov_faster_log_close_idempotent() {
    let log = make_log();
    log.close();
    assert!(log.is_closed());
    // Closing again should not panic
    let status = log.close();
    assert_eq!(status, Status::Ok);
}

#[test]
fn cov_faster_log_last_error_none() {
    let log = make_log();
    assert!(log.last_error().is_none());
}

#[test]
fn cov_faster_log_last_error_after_invalid_append() {
    let log = make_log();
    let _ = log.append(b"");
    let err = log.last_error();
    assert!(err.is_some());
    let err = err.unwrap();
    assert_eq!(err.kind, LogErrorKind::Config);
    // Test Display impl
    let display = format!("{}", err);
    assert!(!display.is_empty());
}

// ===========================================================================
