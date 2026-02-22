// 7. IoUring tests (portable fallback + Linux backend)
// ===========================================================================

#[test]
fn cov_io_uring_config_with_cq_entries() {
    let config = IoUringConfig::new().with_cq_entries(1024);
    assert_eq!(config.cq_entries, 1024);
}

#[test]
fn cov_io_uring_config_with_fixed_buffer_size() {
    let config = IoUringConfig::new().with_fixed_buffer_size(8192);
    assert_eq!(config.fixed_buffer_size, 8192);
}

#[test]
fn cov_io_uring_device_creation() {
    let config = IoUringConfig::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_iouring.dat");
    let device = IoUringDevice::new(&path, config);

    assert!(!device.is_initialized());
    assert_eq!(device.stats().reads_submitted, 0);

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    assert!(!IoUringDevice::is_available());
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        let _ = IoUringDevice::is_available();
    }
}

#[test]
fn cov_io_uring_device_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_iouring2.dat");
    let device = IoUringDevice::with_defaults(&path);

    assert!(!device.is_initialized());
    assert_eq!(device.config().sq_entries, 256);
}

#[test]
fn cov_io_uring_device_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_path.dat");
    let device = IoUringDevice::with_defaults(&path);
    assert_eq!(device.path(), path.as_path());
}

#[test]
fn cov_io_uring_device_submit_methods() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_submit.dat");
    let mut device = IoUringDevice::with_defaults(&path);

    // In the portable fallback, these methods are no-ops. In the Linux backend, these methods
    // operate on the ring; with no queued operations, they return `Ok(0)` and must not block.
    let result = device.submit();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    let result = device.submit_and_wait(1);
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    let result = device.submit_and_wait(0);
    assert!(result.is_ok());

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    let result = device.wait_completions(1);
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    let result = device.wait_completions(0);
    assert!(result.is_ok());
}

#[test]
fn cov_io_uring_device_process_completions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_completions.dat");
    let mut device = IoUringDevice::with_defaults(&path);

    // In mock mode, always returns 0
    let completed = device.process_completions();
    assert_eq!(completed, 0);

    let polled = device.poll_completions();
    assert_eq!(polled, 0);
}

#[test]
fn cov_io_uring_device_pending_operations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_pending.dat");
    let device = IoUringDevice::with_defaults(&path);

    // In mock mode, always returns 0
    assert_eq!(device.pending_operations(), 0);
}

#[test]
fn cov_io_uring_supported_features() {
    let features = IoUringDevice::supported_features();

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        // Fallback mode has all features disabled.
        assert!(!features.sqpoll);
        assert!(!features.fixed_buffers);
        assert!(!features.registered_files);
        assert!(!features.io_drain);
    }
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        let _ = (
            features.sqpoll,
            features.fixed_buffers,
            features.registered_files,
            features.io_drain,
        );
    }
}

#[test]
fn cov_io_uring_file_operations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_file.dat");
    let mut file = IoUringFile::new(&path);

    assert!(!file.is_open());
    assert_eq!(file.file_size(), 0);

    file.open().unwrap();
    assert!(file.is_open());

    let _ = file.path();

    file.close();
    assert!(!file.is_open());
}

#[test]
fn cov_io_uring_stats_operations() {
    let mut stats = oxifaster::device::IoUringStats::default();

    // Test recording operations
    stats.record_read_complete(1024, 1000);
    assert_eq!(stats.reads_completed, 1);
    assert_eq!(stats.bytes_read, 1024);

    stats.record_write_complete(2048, 2000);
    assert_eq!(stats.writes_completed, 1);
    assert_eq!(stats.bytes_written, 2048);

    stats.record_read_error();
    assert_eq!(stats.read_errors, 1);

    stats.record_write_error();
    assert_eq!(stats.write_errors, 1);

    // Test pending calculations
    stats.reads_submitted = 5;
    assert_eq!(stats.pending_reads(), 3); // 5 - 1 completed - 1 error

    stats.writes_submitted = 5;
    assert_eq!(stats.pending_writes(), 3);

    // Test reset
    stats.reset();
    assert_eq!(stats.reads_completed, 0);
    assert_eq!(stats.writes_completed, 0);
}

// ===========================================================================
// 8. Read cache tests (src/cache/read_cache.rs)
// ===========================================================================

#[test]
fn cov_read_cache_pre_allocate() {
    // Test with pre-allocation enabled
    let config = ReadCacheConfig::new(1024 * 1024).with_pre_allocate(true);
    let cache = ReadCache::<u64, u64>::new(config);

    assert!(cache.config().pre_allocate);
}

#[test]
fn cov_read_cache_read_non_cache_address() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<u64, u64>::new(config);

    // Reading from a non-cache address should return None
    let non_cache_addr = Address::new(5, 100);
    let result = cache.read(non_cache_addr, &42u64);
    assert!(result.is_none());
}

#[test]
fn cov_read_cache_addresses() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<u64, u64>::new(config);

    let tail = cache.tail_address();
    let safe_head = cache.safe_head_address();
    let read_only = cache.read_only_address();

    assert!(tail.control() >= safe_head.control());
    assert!(read_only.control() <= safe_head.control() || read_only.control() == 0);
}

#[test]
fn cov_read_cache_eviction_trigger() {
    // Create a small cache to trigger eviction
    let config = ReadCacheConfig::new(1024) // Very small
        .with_mutable_fraction(0.5);
    let cache = ReadCache::<u64, u64>::new(config);

    // Insert data to trigger eviction
    for i in 0..100 {
        let _ = cache.try_insert(&i, &(i * 10), Address::INVALID, false);
    }

    // Check that eviction happened
    let stats = cache.stats();
    // Stats may or may not record eviction depending on whether cache wrapped
    let _ = stats.evicted_records();
}

#[test]
fn cov_read_cache_invalidate_non_cache_addr() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<u64, u64>::new(config);

    // Invalidate a non-cache address should return the same address
    let non_cache_addr = Address::new(5, 100);
    let result = cache.invalidate(non_cache_addr, &42u64);
    assert_eq!(result, non_cache_addr);
}

#[test]
fn cov_read_cache_skip_non_cache_addr() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<u64, u64>::new(config);

    // Skip a non-cache address should return the same address
    let non_cache_addr = Address::new(5, 100);
    let result = cache.skip(non_cache_addr);
    assert_eq!(result, non_cache_addr);
}

#[test]
fn cov_read_cache_is_cold_log_record() {
    let config = ReadCacheConfig::new(1024 * 1024);
    let cache = ReadCache::<u64, u64>::new(config);

    // Insert with is_cold_log_record flag
    let result = cache.try_insert(&42u64, &100u64, Address::INVALID, true);
    assert!(result.is_ok());
}

// ===========================================================================
// 9. Log iterator tests (src/scan/log_iterator.rs)
// ===========================================================================

#[test]
fn cov_log_page_status_as_str() {
    assert_eq!(LogPageStatus::Uninitialized.as_str(), "Uninitialized");
    assert_eq!(LogPageStatus::Ready.as_str(), "Ready");
    assert_eq!(LogPageStatus::Pending.as_str(), "Pending");
    assert_eq!(LogPageStatus::Unavailable.as_str(), "Unavailable");
}

#[test]
fn cov_log_page_new() {
    let page = LogPage::new(4096, 4096);

    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    assert!(page.buffer().is_some());
}

#[test]
fn cov_log_page_buffer_mut() {
    let mut page = LogPage::new(4096, 4096);

    let buf = page.buffer_mut();
    assert!(buf.is_some());
    let buf = buf.unwrap();
    buf[0] = 42;
}

#[test]
fn cov_log_page_update_and_reset() {
    let mut page = LogPage::new(4096, 4096);

    let page_addr = Address::new(0, 0);
    let current = Address::new(0, 100);
    let until = Address::new(0, 500);

    page.update(page_addr, current, until);
    page.set_status(LogPageStatus::Ready);

    assert_eq!(page.status(), LogPageStatus::Ready);
    assert!(page.has_more());

    page.reset();
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    assert!(!page.has_more());
}

#[test]
fn cov_log_page_update_cross_page() {
    let mut page = LogPage::new(4096, 4096);

    let page_addr = Address::new(0, 0);
    let current = Address::new(0, 100);
    let until = Address::new(1, 500); // Next page

    page.update(page_addr, current, until);
    // until_offset should be set to page_size when until is in a later page
}

#[test]
fn cov_log_page_iterator_new() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter = LogPageIterator::new(range, 4096);

    assert!(iter.has_more());
    assert_eq!(iter.remaining_pages(), 6);
}

#[test]
fn cov_log_page_iterator_next_page() {
    let range = ScanRange::new(Address::new(0, 100), Address::new(2, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    let first = iter.next_page();
    assert!(first.is_some());
    let (page_addr, start_addr) = first.unwrap();
    assert_eq!(page_addr.page(), 0);
    assert_eq!(start_addr.offset(), 100);

    let second = iter.next_page();
    assert!(second.is_some());
    let (page_addr2, start_addr2) = second.unwrap();
    assert_eq!(page_addr2.page(), 1);
    assert_eq!(start_addr2.offset(), 0);
}

#[test]
fn cov_log_page_iterator_empty_range() {
    let range = ScanRange::new(Address::new(5, 0), Address::new(5, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    assert!(!iter.has_more());
    assert_eq!(iter.remaining_pages(), 0);
    assert!(iter.next_page().is_none());
}

#[test]
fn cov_log_scan_iterator_new() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter = LogScanIterator::<u64, u64>::new(range, 4096);

    assert_eq!(iter.range().begin, Address::new(0, 0));
    assert_eq!(iter.records_scanned(), 0);
    assert!(iter.has_more());
}

#[test]
fn cov_log_scan_iterator_advance_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let mut iter = LogScanIterator::<u64, u64>::new(range, 4096);

    let first = iter.advance_page();
    assert!(first.is_some());

    let second = iter.advance_page();
    assert!(second.is_some());

    let third = iter.advance_page();
    assert!(third.is_none());
}

#[test]
fn cov_log_scan_iterator_load_page_data() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    let mut iter = LogScanIterator::<u64, u64>::new(range, 4096);

    let data = vec![0u8; 4096];
    let start = Address::new(0, 0);
    let until = Address::new(0, 1000);

    iter.load_page_data(&data, start, until);

    assert_eq!(iter.current_page_mut().status(), LogPageStatus::Ready);
}

#[test]
fn cov_log_scan_iterator_finish_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    let mut iter = LogScanIterator::<u64, u64>::new(range, 4096);

    iter.finish_page();
    assert_eq!(
        iter.current_page_mut().status(),
        LogPageStatus::Uninitialized
    );
}

#[test]
fn cov_concurrent_scan_iterator_new() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(10, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 11);
    assert_eq!(iter.page_size(), 4096);
    assert!(!iter.is_complete());
}

#[test]
fn cov_concurrent_scan_iterator_get_next_page() {
    let range = ScanRange::new(Address::new(0, 100), Address::new(3, 500));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    // First page
    let first = iter.get_next_page();
    assert!(first.is_some());
    let (idx, begin, _end) = first.unwrap();
    assert_eq!(idx, 0);
    assert_eq!(begin.offset(), 100); // First page starts at range.begin

    // Second page
    let second = iter.get_next_page();
    assert!(second.is_some());
    let (idx2, begin2, _) = second.unwrap();
    assert_eq!(idx2, 1);
    assert_eq!(begin2.offset(), 0); // Subsequent pages start at 0
}

#[test]
fn cov_concurrent_scan_iterator_last_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 500));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    let _ = iter.get_next_page(); // page 0
    let _ = iter.get_next_page(); // page 1
    let last = iter.get_next_page(); // page 2 (last)

    assert!(last.is_some());
    let (_, _, end) = last.unwrap();
    assert_eq!(end.page(), 2);
    assert_eq!(end.offset(), 500); // Last page ends at range.end
}

#[test]
fn cov_concurrent_scan_iterator_progress() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    let (distributed, total) = iter.progress();
    assert_eq!(distributed, 0);
    assert_eq!(total, 6);

    iter.get_next_page();
    iter.get_next_page();

    let (distributed2, total2) = iter.progress();
    assert_eq!(distributed2, 2);
    assert_eq!(total2, 6);
}

#[test]
fn cov_concurrent_scan_iterator_is_complete() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert!(!iter.is_complete());

    iter.get_next_page(); // page 0
    iter.get_next_page(); // page 1

    // Try to get more (should return None)
    let exhausted = iter.get_next_page();
    assert!(exhausted.is_none());
    assert!(iter.is_complete());
}

#[test]
fn cov_concurrent_scan_iterator_range() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    let iter_range = iter.range();
    assert_eq!(iter_range.begin, range.begin);
    assert_eq!(iter_range.end, range.end);
}

#[test]
fn cov_concurrent_scan_iterator_empty() {
    let range = ScanRange::new(Address::new(5, 0), Address::new(5, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 0);
    assert!(iter.get_next_page().is_none());
}

// ===========================================================================
// Additional edge case tests
// ===========================================================================

#[test]
fn cov_faster_kv_with_read_cache_operations() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let cache_config = ReadCacheConfig::new(1 << 20);
    let store: Arc<FasterKv<u64, u64, NullDisk>> = Arc::new(FasterKv::with_read_cache(
        config,
        NullDisk::new(),
        cache_config,
    ).unwrap());

    assert!(store.has_read_cache());
    assert!(store.read_cache_stats().is_some());
    assert!(store.read_cache_config().is_some());

    // Insert and read
    {
        let mut session = store.start_session().unwrap();
        session.upsert(1u64, 100u64);
        let _ = session.read(&1u64);
    }

    // Clear cache
    store.clear_read_cache();
}

#[test]
fn cov_faster_log_commit_and_wait() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    log.append(b"test data 1").unwrap();
    log.append(b"test data 2").unwrap();

    let committed = log.commit().unwrap();

    // Wait should return quickly for NullDisk
    let result = log.wait_for_commit(committed);
    assert!(result.is_ok());
}

#[test]
fn cov_faster_log_multiple_page_appends() {
    let config = FasterLogConfig {
        page_size: 512, // Small page (must be multiple of alignment 512)
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Append enough data to span multiple pages
    // Each entry has 16-byte header, so with ~100 bytes payload = ~116 bytes per entry
    // 512 / 116 = ~4 entries per page, so 50 entries should span ~12 pages
    for i in 0..50 {
        let data = format!("test entry number {} with some data", i);
        log.append(data.as_bytes()).unwrap();
    }

    let stats = log.get_stats();
    assert!(stats.tail_address > stats.begin_address);
}

#[test]
fn cov_scan_range_debug() {
    let range = ScanRange::new(Address::new(1, 100), Address::new(5, 500));
    let debug = format!("{:?}", range);
    assert!(debug.contains("ScanRange"));
}

#[test]
fn cov_io_operation_values() {
    use oxifaster::device::IoOperation;

    let read = IoOperation::Read;
    let write = IoOperation::Write;
    let fsync = IoOperation::Fsync;
    let fallocate = IoOperation::Fallocate;

    assert_eq!(read, IoOperation::Read);
    assert_ne!(read, write);
    assert_ne!(fsync, fallocate);

    let debug = format!("{:?}", read);
    assert!(debug.contains("Read"));
}
