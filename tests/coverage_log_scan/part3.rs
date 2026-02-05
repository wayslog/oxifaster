// Scan module: ConcurrentLogScanIterator
// ===========================================================================

#[test]
fn cov_concurrent_scan_iterator_basic() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 6);
    assert_eq!(iter.page_size(), 4096);
    assert!(!iter.is_complete());
    assert_eq!(iter.range().begin, Address::new(0, 0));
    assert_eq!(iter.range().end, Address::new(5, 0));

    let (distributed, total) = iter.progress();
    assert_eq!(distributed, 0);
    assert_eq!(total, 6);
}

#[test]
fn cov_concurrent_scan_iterator_empty_range() {
    let addr = Address::new(3, 0);
    let range = ScanRange::new(addr, addr);
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 0);
    assert!(iter.get_next_page().is_none());
    assert!(iter.is_complete());
}

#[test]
fn cov_concurrent_scan_iterator_exhaustion() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    // Should have 3 pages: 0, 1, 2
    assert_eq!(iter.total_pages(), 3);

    let p0 = iter.get_next_page().unwrap();
    assert_eq!(p0.0, 0);
    assert_eq!(p0.1, Address::new(0, 0));
    assert_eq!(p0.2, Address::new(1, 0));

    let p1 = iter.get_next_page().unwrap();
    assert_eq!(p1.0, 1);
    assert_eq!(p1.1, Address::new(1, 0));
    assert_eq!(p1.2, Address::new(2, 0));

    let p2 = iter.get_next_page().unwrap();
    assert_eq!(p2.0, 2);
    assert_eq!(p2.1, Address::new(2, 0));
    assert_eq!(p2.2, Address::new(2, 0)); // last page ends at range.end

    assert!(iter.get_next_page().is_none());
    assert!(iter.is_complete());

    let (distributed, total) = iter.progress();
    assert_eq!(distributed, 3);
    assert_eq!(total, 3);
}

#[test]
fn cov_concurrent_scan_iterator_with_offsets() {
    let range = ScanRange::new(Address::new(1, 200), Address::new(3, 400));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 3);

    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 0);
    assert_eq!(begin, Address::new(1, 200)); // first page starts at range.begin
    assert_eq!(end, Address::new(2, 0));

    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 1);
    assert_eq!(begin, Address::new(2, 0));
    assert_eq!(end, Address::new(3, 0));

    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 2);
    assert_eq!(begin, Address::new(3, 0));
    assert_eq!(end, Address::new(3, 400)); // last page ends at range.end
}

#[test]
fn cov_concurrent_scan_iterator_multithread() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(99, 0));
    let iter = Arc::new(ConcurrentLogScanIterator::new(range, 4096));

    let num_threads = 4;
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let iter_clone = Arc::clone(&iter);
        handles.push(thread::spawn(move || {
            let mut count = 0u64;
            while iter_clone.get_next_page().is_some() {
                count += 1;
            }
            count
        }));
    }

    let total_claimed: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

    assert_eq!(total_claimed, iter.total_pages());
    assert!(iter.is_complete());
}

#[test]
fn cov_concurrent_scan_iterator_single_page() {
    let range = ScanRange::new(Address::new(7, 100), Address::new(7, 900));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 1);

    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 0);
    assert_eq!(begin, Address::new(7, 100));
    assert_eq!(end, Address::new(7, 900));

    assert!(iter.get_next_page().is_none());
}

// ===========================================================================
// FasterLog: drop behavior (implicit close)
// ===========================================================================

#[test]
fn cov_faster_log_drop_does_not_panic() {
    let log = make_log();
    log.append(b"data before drop").unwrap();
    // Dropping the log should shut down the flush worker cleanly
    drop(log);
}

// ===========================================================================
// FasterLog: large batch and various data sizes
// ===========================================================================

#[test]
fn cov_faster_log_append_various_sizes() {
    let log = make_log();
    // Test various payload sizes
    let sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4000];
    let begin = log.get_begin_address();

    for &size in &sizes {
        let data = vec![0xCCu8; size];
        let addr = log.append(&data).unwrap();
        let read_back = log.read_entry(addr).unwrap();
        assert_eq!(read_back.len(), size);
        assert!(read_back.iter().all(|&b| b == 0xCC));
    }

    let tail = log.get_tail_address();
    let entries: Vec<_> = log.scan(begin, tail).collect();
    assert_eq!(entries.len(), sizes.len());
}

// ===========================================================================
// FasterLog: scan with specific start/end addresses
// ===========================================================================

#[test]
fn cov_faster_log_scan_with_equal_start_end() {
    let log = make_log();
    log.append(b"data").unwrap();
    let addr = Address::new(0, 0);
    let entries: Vec<_> = log.scan(addr, addr).collect();
    assert!(entries.is_empty());
}

#[test]
fn cov_faster_log_scan_reversed_range() {
    let log = make_log();
    log.append(b"data").unwrap();
    let tail = log.get_tail_address();
    // Reversed range should produce nothing
    let entries: Vec<_> = log.scan(tail, Address::new(0, 0)).collect();
    assert!(entries.is_empty());
}
