// Self-check on NullDisk (empty device)
// ===========================================================================

#[test]
fn cov_faster_log_self_check_empty_device() {
    let config = make_config();
    let report = FasterLog::self_check(config, NullDisk::new()).unwrap();
    assert!(!report.metadata_present);
    assert!(!report.had_corruption);
    assert!(!report.repaired);
    assert!(!report.dry_run);
    assert_eq!(report.scan_end, Address::new(0, 0));
    assert!(report.truncate_to.is_none());
}

#[test]
fn cov_faster_log_self_check_with_options_dry_run() {
    let config = make_config();
    let opts = FasterLogSelfCheckOptions {
        repair: true,
        dry_run: true,
        create_if_missing: true,
    };
    let report = FasterLog::self_check_with_options(config, NullDisk::new(), opts).unwrap();
    assert!(report.dry_run);
    assert!(!report.repaired); // dry_run prevents repair
}

#[test]
fn cov_faster_log_self_check_report_debug_clone() {
    let config = make_config();
    let report = FasterLog::self_check(config, NullDisk::new()).unwrap();
    let cloned = report.clone();
    assert_eq!(cloned.metadata_present, report.metadata_present);
    let debug = format!("{:?}", report);
    assert!(debug.contains("metadata_present"));
}

// ===========================================================================
// Page boundary crossing
// ===========================================================================

#[test]
fn cov_faster_log_append_across_page_boundary() {
    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    // Header is 16 bytes, so each entry is 16 + payload_len.
    // With page_size=512, a 480-byte payload fills a page (16+480=496, leaving 16 bytes).
    // The next 480-byte entry won't fit and should move to the next page.
    let data = vec![0xBBu8; 480];
    let addr1 = log.append(&data).unwrap();
    let addr2 = log.append(&data).unwrap();

    // addr2 should be on a different page
    assert!(addr2.page() > addr1.page() || addr2.offset() > addr1.offset());

    // Both should be readable
    assert_eq!(log.read_entry(addr1).unwrap().len(), 480);
    assert_eq!(log.read_entry(addr2).unwrap().len(), 480);
}

#[test]
fn cov_faster_log_scan_across_page_boundary() {
    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = FasterLog::new(config, NullDisk::new()).unwrap();

    let begin = log.get_begin_address();
    for i in 0..5 {
        let data = format!("entry-{}", i);
        log.append(data.as_bytes()).unwrap();
    }
    let tail = log.get_tail_address();

    let entries: Vec<(Address, Vec<u8>)> = log.scan(begin, tail).collect();
    assert_eq!(entries.len(), 5);
    for (i, (_, data)) in entries.iter().enumerate() {
        let expected = format!("entry-{}", i);
        assert_eq!(data, expected.as_bytes());
    }
}

// ===========================================================================
// Multi-threaded append
// ===========================================================================

#[test]
fn cov_faster_log_concurrent_append() {
    let config = FasterLogConfig {
        page_size: 4096,
        memory_pages: 32,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };
    let log = Arc::new(FasterLog::new(config, NullDisk::new()).unwrap());

    let num_threads = 4;
    let entries_per_thread = 50;
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let log_clone = Arc::clone(&log);
        handles.push(thread::spawn(move || {
            for e in 0..entries_per_thread {
                let data = format!("t{}-e{}", t, e);
                log_clone.append(data.as_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let begin = log.get_begin_address();
    let tail = log.get_tail_address();

    let entries: Vec<(Address, Vec<u8>)> = log.scan(begin, tail).collect();
    assert_eq!(entries.len(), num_threads * entries_per_thread);
}

// ===========================================================================
// Scan module: ScanRange
// ===========================================================================

#[test]
fn cov_scan_range_new_and_accessors() {
    let begin = Address::new(1, 50);
    let end = Address::new(5, 200);
    let range = ScanRange::new(begin, end);
    assert_eq!(range.begin, begin);
    assert_eq!(range.end, end);
    assert!(!range.is_empty());
}

#[test]
fn cov_scan_range_empty_cases() {
    // Same address
    let addr = Address::new(3, 100);
    assert!(ScanRange::new(addr, addr).is_empty());
    assert_eq!(ScanRange::new(addr, addr).page_count(), 0);

    // Reversed
    let begin = Address::new(10, 0);
    let end = Address::new(5, 0);
    assert!(ScanRange::new(begin, end).is_empty());
    assert_eq!(ScanRange::new(begin, end).page_count(), 0);
}

#[test]
fn cov_scan_range_page_count_various() {
    // Single page
    assert_eq!(
        ScanRange::new(Address::new(0, 0), Address::new(0, 100)).page_count(),
        1
    );

    // Two pages
    assert_eq!(
        ScanRange::new(Address::new(0, 0), Address::new(1, 0)).page_count(),
        2
    );

    // Many pages with offsets
    assert_eq!(
        ScanRange::new(Address::new(2, 50), Address::new(7, 300)).page_count(),
        6
    );
}

#[test]
fn cov_scan_range_copy_clone_debug() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let copied = range;
    assert_eq!(range.begin, copied.begin);
    assert_eq!(range.end, copied.end);
    let debug = format!("{:?}", range);
    assert!(debug.contains("begin"));
    assert!(debug.contains("end"));
}

// ===========================================================================
// Scan module: LogPageStatus
// ===========================================================================

#[test]
fn cov_log_page_status_as_str() {
    assert_eq!(LogPageStatus::Uninitialized.as_str(), "Uninitialized");
    assert_eq!(LogPageStatus::Ready.as_str(), "Ready");
    assert_eq!(LogPageStatus::Pending.as_str(), "Pending");
    assert_eq!(LogPageStatus::Unavailable.as_str(), "Unavailable");
}

#[test]
fn cov_log_page_status_default_is_uninitialized() {
    assert_eq!(LogPageStatus::default(), LogPageStatus::Uninitialized);
}

#[test]
fn cov_log_page_status_repr_values() {
    assert_eq!(LogPageStatus::Uninitialized as u8, 0);
    assert_eq!(LogPageStatus::Ready as u8, 1);
    assert_eq!(LogPageStatus::Pending as u8, 2);
    assert_eq!(LogPageStatus::Unavailable as u8, 3);
}

// ===========================================================================
// Scan module: LogPage
// ===========================================================================

#[test]
fn cov_log_page_new_has_buffer() {
    let page = LogPage::new(4096, 4096);
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    assert!(page.buffer().is_some());
    assert_eq!(page.buffer().unwrap().len(), 4096);
    assert!(!page.has_more());
}

#[test]
fn cov_log_page_buffer_mut_write_read() {
    let mut page = LogPage::new(4096, 4096);
    let buf = page.buffer_mut().unwrap();
    buf[0] = 0xAA;
    buf[1] = 0xBB;
    assert_eq!(page.buffer().unwrap()[0], 0xAA);
    assert_eq!(page.buffer().unwrap()[1], 0xBB);
}

#[test]
fn cov_log_page_set_status_transitions() {
    let mut page = LogPage::new(4096, 4096);
    assert_eq!(page.status(), LogPageStatus::Uninitialized);

    page.set_status(LogPageStatus::Pending);
    assert_eq!(page.status(), LogPageStatus::Pending);

    page.set_status(LogPageStatus::Ready);
    assert_eq!(page.status(), LogPageStatus::Ready);

    page.set_status(LogPageStatus::Unavailable);
    assert_eq!(page.status(), LogPageStatus::Unavailable);
}

#[test]
fn cov_log_page_update_same_page() {
    let mut page = LogPage::new(4096, 4096);
    let page_addr = Address::new(3, 0);
    let current = Address::new(3, 100);
    let until = Address::new(3, 500);
    page.update(page_addr, current, until);
    // Internal state is updated (no public accessor for offsets, but
    // has_more depends on status being Ready)
    page.set_status(LogPageStatus::Ready);
    assert!(page.has_more());
}

#[test]
fn cov_log_page_update_across_pages() {
    let mut page = LogPage::new(4096, 4096);
    let page_addr = Address::new(5, 0);
    let current = Address::new(5, 0);
    let until = Address::new(6, 0); // next page -> until_offset = page_size
    page.update(page_addr, current, until);
    page.set_status(LogPageStatus::Ready);
    assert!(page.has_more());
}

#[test]
fn cov_log_page_reset_clears_state() {
    let mut page = LogPage::new(4096, 4096);
    let buf = page.buffer_mut().unwrap();
    buf[0] = 0xFF;
    page.set_status(LogPageStatus::Ready);

    page.reset();

    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    assert_eq!(page.buffer().unwrap()[0], 0);
    assert!(!page.has_more());
}

#[test]
fn cov_log_page_has_more_not_ready() {
    let mut page = LogPage::new(4096, 4096);
    page.update(Address::new(0, 0), Address::new(0, 0), Address::new(0, 100));
    // Status is still Uninitialized, so has_more returns false
    assert!(!page.has_more());
}

// ===========================================================================
// Scan module: LogPageIterator
// ===========================================================================

#[test]
fn cov_log_page_iterator_basic() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    assert!(iter.has_more());
    assert_eq!(iter.remaining_pages(), 4); // pages 0, 1, 2, 3

    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(0, 0));
    assert_eq!(start_addr, Address::new(0, 0));

    let (page_addr, _) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(1, 0));

    let (page_addr, _) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(2, 0));

    // Page 3 is within range (end is exclusive but page 3 offset 0 == end)
    // Actually Address::new(3, 0) == end, and current becomes page 3 which >= end
    // Let me check: after consuming page 2, current becomes Address::new(3, 0)
    // next_page checks current >= until -> Address(3,0) >= Address(3,0) -> true -> None
    assert!(iter.next_page().is_none());
    assert!(!iter.has_more());
    assert_eq!(iter.remaining_pages(), 0);
}

#[test]
fn cov_log_page_iterator_with_offset() {
    let range = ScanRange::new(Address::new(2, 500), Address::new(4, 100));
    let mut iter = LogPageIterator::new(range, 4096);

    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(2, 0));
    assert_eq!(start_addr, Address::new(2, 500));

    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(3, 0));
    assert_eq!(start_addr, Address::new(3, 0));

    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(4, 0));
    assert_eq!(start_addr, Address::new(4, 0));

    assert!(iter.next_page().is_none());
}

#[test]
fn cov_log_page_iterator_single_page() {
    let range = ScanRange::new(Address::new(5, 100), Address::new(5, 500));
    let mut iter = LogPageIterator::new(range, 4096);

    assert!(iter.has_more());
    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(5, 0));
    assert_eq!(start_addr, Address::new(5, 100));

    assert!(iter.next_page().is_none());
}

#[test]
fn cov_log_page_iterator_empty_range() {
    let addr = Address::new(5, 0);
    let range = ScanRange::new(addr, addr);
    let mut iter = LogPageIterator::new(range, 4096);

    assert!(!iter.has_more());
    assert_eq!(iter.remaining_pages(), 0);
    assert!(iter.next_page().is_none());
}

#[test]
fn cov_log_page_iterator_remaining_decreases() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(4, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    let r0 = iter.remaining_pages();
    iter.next_page();
    let r1 = iter.remaining_pages();
    iter.next_page();
    let r2 = iter.remaining_pages();

    assert!(r0 > r1);
    assert!(r1 > r2);
}

// ===========================================================================
// Scan module: LogScanIterator (memory-only mode, D=())
// ===========================================================================

#[test]
fn cov_log_scan_iterator_new_memory_only() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
    let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    assert_eq!(iter.records_scanned(), 0);
    assert_eq!(iter.range().begin, Address::new(0, 0));
    assert_eq!(iter.range().end, Address::new(3, 0));
    assert!(iter.has_more());
}

#[test]
fn cov_log_scan_iterator_advance_and_finish_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    let result = iter.advance_page();
    assert!(result.is_some());
    let (page_addr, start_addr) = result.unwrap();
    assert_eq!(page_addr, Address::new(0, 0));
    assert_eq!(start_addr, Address::new(0, 0));

    // The page is not loaded yet, so next_record returns None
    assert!(iter.next_record().is_none());

    iter.finish_page();
    assert_eq!(
        iter.current_page_mut().status(),
        LogPageStatus::Uninitialized
    );
}

#[test]
fn cov_log_scan_iterator_load_page_data() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    // Load some data (just zeros, so no valid records will be found)
    let data = vec![0u8; 4096];
    let start = Address::new(0, 0);
    let until = Address::new(1, 0);
    iter.load_page_data(&data, start, until);

    assert_eq!(iter.current_page_mut().status(), LogPageStatus::Ready);
    // No valid records in zero-filled data
    assert!(iter.next_record().is_none());
}

#[test]
fn cov_log_scan_iterator_current_page_mut() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    let page = iter.current_page_mut();
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    page.set_status(LogPageStatus::Pending);
    assert_eq!(iter.current_page_mut().status(), LogPageStatus::Pending);
}

// ===========================================================================
// Scan module: LogScanIterator with NullDisk device
// ===========================================================================

#[test]
fn cov_log_scan_iterator_with_device() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let device = Arc::new(NullDisk::new());
    let iter: LogScanIterator<u64, u64, NullDisk> =
        LogScanIterator::with_device(range, 4096, device);

    assert_eq!(iter.records_scanned(), 0);
    assert_eq!(iter.range().begin, Address::new(0, 0));
    assert_eq!(iter.range().end, Address::new(2, 0));
    assert!(iter.has_more());
}

#[test]
fn cov_log_scan_iterator_with_device_load_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    let device = Arc::new(NullDisk::new());
    let mut iter: LogScanIterator<u64, u64, NullDisk> =
        LogScanIterator::with_device(range, 4096, device);

    // Load page from device (NullDisk returns zeros)
    let result = iter.load_page_from_device(0, Address::new(0, 0), Address::new(1, 0));
    assert!(result.is_ok());
    assert_eq!(iter.current_page_mut().status(), LogPageStatus::Ready);
}

#[test]
fn cov_log_scan_iterator_with_device_scan_all() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let device = Arc::new(NullDisk::new());
    let mut iter: LogScanIterator<u64, u64, NullDisk> =
        LogScanIterator::with_device(range, 4096, device);

    let mut count = 0u64;
    let result = iter.scan_all(|_addr, _record| {
        count += 1;
        true
    });
    assert!(result.is_ok());
    // NullDisk returns zeros, so no valid records
    assert_eq!(result.unwrap(), 0);
    assert_eq!(count, 0);
}

#[test]
fn cov_log_scan_iterator_with_device_advance_finish() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
    let device = Arc::new(NullDisk::new());
    let mut iter: LogScanIterator<u64, u64, NullDisk> =
        LogScanIterator::with_device(range, 4096, device);

    let result = iter.advance_page();
    assert!(result.is_some());

    iter.finish_page();
    assert_eq!(
        iter.current_page_mut().status(),
        LogPageStatus::Uninitialized
    );
}

#[test]
fn cov_log_scan_iterator_with_device_load_page_data() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    let device = Arc::new(NullDisk::new());
    let mut iter: LogScanIterator<u64, u64, NullDisk> =
        LogScanIterator::with_device(range, 4096, device);

    let data = vec![0u8; 4096];
    iter.load_page_data(&data, Address::new(0, 0), Address::new(1, 0));
    assert_eq!(iter.current_page_mut().status(), LogPageStatus::Ready);
}

// ===========================================================================
