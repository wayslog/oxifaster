use super::*;
use crate::codec::Utf8;
use crate::device::NullDisk;
use crate::record::RecordInfo;
use std::ptr;

#[test]
fn test_scan_range() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(10, 0));
    assert!(!range.is_empty());
    assert_eq!(range.page_count(), 11);
}

#[test]
fn test_scan_range_empty() {
    let range = ScanRange::new(Address::new(5, 0), Address::new(5, 0));
    assert!(range.is_empty());
    assert_eq!(range.page_count(), 0);
}

#[test]
fn test_log_page() {
    let page = LogPage::new(4096, 4096);
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    assert!(page.buffer().is_some());
}

#[test]
fn test_log_page_get_next_view_bounds_check() {
    let page_size = 64usize;
    let mut page = LogPage::new(page_size, 4096);
    page.update(
        Address::new(0, 0),
        Address::new(0, 0),
        Address::new(0, page_size as u32),
    );
    page.set_status(LogPageStatus::Ready);

    let buf = page.buffer_mut().unwrap();
    unsafe {
        ptr::write(
            buf.as_mut_ptr().cast::<RecordInfo>(),
            RecordInfo::new(Address::INVALID, 0, false, false, false),
        );
    }
    buf[8..12].copy_from_slice(&(1000u32.to_le_bytes()));
    buf[12..16].copy_from_slice(&(0u32.to_le_bytes()));

    assert!(page.get_next_view::<Utf8, Utf8>().is_none());
    assert!(!page.has_more());
}

#[test]
fn test_log_page_get_next_view_skips_invalid_record() {
    let page_size = 128usize;
    let mut page = LogPage::new(page_size, 4096);
    page.update(
        Address::new(0, 0),
        Address::new(0, 0),
        Address::new(0, page_size as u32),
    );
    page.set_status(LogPageStatus::Ready);

    let buf = page.buffer_mut().unwrap();

    // Record #1: invalid varlen record (key_len=1, value_len=1).
    unsafe {
        ptr::write(
            buf.as_mut_ptr().cast::<RecordInfo>(),
            RecordInfo::new(Address::INVALID, 0, true, false, false),
        );
    }
    buf[8..12].copy_from_slice(&(1u32.to_le_bytes()));
    buf[12..16].copy_from_slice(&(1u32.to_le_bytes()));
    buf[16] = b'a';
    buf[17] = b'b';

    // Record #2 starts at the next 8-byte aligned offset.
    let off2 = 24usize;
    unsafe {
        ptr::write(
            buf.as_mut_ptr().add(off2).cast::<RecordInfo>(),
            RecordInfo::new(Address::INVALID, 0, false, false, false),
        );
    }
    buf[off2 + 8..off2 + 12].copy_from_slice(&(1u32.to_le_bytes()));
    buf[off2 + 12..off2 + 16].copy_from_slice(&(1u32.to_le_bytes()));
    buf[off2 + 16] = b'k';
    buf[off2 + 17] = b'v';

    let (addr, view) = page.get_next_view::<Utf8, Utf8>().unwrap();
    assert_eq!(addr.offset(), off2 as u32);
    assert_eq!(view.key_bytes(), b"k");
    assert_eq!(view.value_bytes().unwrap(), b"v");
}

#[test]
fn test_log_page_iterator() {
    // Range from page 0 offset 100 to page 3 offset 0 (exclusive)
    // This means we scan pages 0, 1, 2
    let range = ScanRange::new(Address::new(0, 100), Address::new(3, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    // First page
    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(0, 0));
    assert_eq!(start_addr, Address::new(0, 100));

    // Second page
    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(1, 0));
    assert_eq!(start_addr, Address::new(1, 0));

    // Third page
    let (page_addr, start_addr) = iter.next_page().unwrap();
    assert_eq!(page_addr, Address::new(2, 0));
    assert_eq!(start_addr, Address::new(2, 0));

    // No more pages (page 3 is past the end)
    assert!(iter.next_page().is_none());
}

#[test]
fn test_log_scan_iterator() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    assert_eq!(iter.records_scanned(), 0);
    // range() is only available when D: SyncStorageDevice
}

#[test]
fn test_log_scan_iterator_with_device() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(2, 0));
    let device = Arc::new(NullDisk::new());
    let iter: LogScanIterator<u64, u64, NullDisk> =
        LogScanIterator::with_device(range, 4096, device);

    assert_eq!(iter.records_scanned(), 0);
    assert_eq!(iter.range().begin, Address::new(0, 0));
    assert_eq!(iter.range().end, Address::new(2, 0));
}

#[test]
fn test_double_buffered_iterator() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter: DoubleBufferedLogIterator<u64, u64, ()> = DoubleBufferedLogIterator::new(range, 4096);

    assert_eq!(iter.records_scanned(), 0);
    assert!(iter.has_more());
}

#[test]
fn test_double_buffered_iterator_with_device() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let device = Arc::new(NullDisk::new());
    let iter: DoubleBufferedLogIterator<u64, u64, NullDisk> =
        DoubleBufferedLogIterator::with_device(range, 4096, device);

    assert_eq!(iter.records_scanned(), 0);
    assert!(iter.has_more());
    assert_eq!(iter.range().begin, Address::new(0, 0));
}

#[test]
fn test_log_page_status_values() {
    assert_eq!(LogPageStatus::default(), LogPageStatus::Uninitialized);
    assert_ne!(LogPageStatus::Ready, LogPageStatus::Pending);
    assert_ne!(LogPageStatus::Unavailable, LogPageStatus::Ready);
}

#[test]
fn test_log_page_set_status() {
    let mut page = LogPage::new(4096, 4096);
    assert_eq!(page.status(), LogPageStatus::Uninitialized);

    page.set_status(LogPageStatus::Ready);
    assert_eq!(page.status(), LogPageStatus::Ready);

    page.set_status(LogPageStatus::Pending);
    assert_eq!(page.status(), LogPageStatus::Pending);

    page.set_status(LogPageStatus::Unavailable);
    assert_eq!(page.status(), LogPageStatus::Unavailable);
}

#[test]
fn test_log_page_buffer_mut() {
    let mut page = LogPage::new(4096, 4096);
    let buf = page.buffer_mut().unwrap();
    assert_eq!(buf.len(), 4096);

    buf[0] = 42;
    assert_eq!(page.buffer().unwrap()[0], 42);
}

#[test]
fn test_log_page_update() {
    let mut page = LogPage::new(4096, 4096);

    let page_addr = Address::new(5, 0);
    let current = Address::new(5, 100);
    let until = Address::new(6, 0);
    page.update(page_addr, current, until);
}

#[test]
fn test_log_page_update_same_page() {
    let mut page = LogPage::new(4096, 4096);

    let page_addr = Address::new(5, 0);
    let current = Address::new(5, 100);
    let until = Address::new(5, 500);
    page.update(page_addr, current, until);
}

#[test]
fn test_log_page_reset() {
    let mut page = LogPage::new(4096, 4096);

    page.set_status(LogPageStatus::Ready);
    if let Some(buf) = page.buffer_mut() {
        buf[0] = 42;
    }

    page.reset();

    assert_eq!(page.status(), LogPageStatus::Uninitialized);
    assert_eq!(page.buffer().unwrap()[0], 0);
}

#[test]
fn test_log_page_has_more() {
    let page = LogPage::new(4096, 4096);

    assert!(!page.has_more());
}

#[test]
fn test_log_page_iterator_has_more() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    assert!(iter.has_more());

    while iter.next_page().is_some() {}

    assert!(!iter.has_more());
}

#[test]
fn test_log_page_iterator_remaining_pages() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(3, 0));
    let mut iter = LogPageIterator::new(range, 4096);

    let initial = iter.remaining_pages();
    assert!(initial > 0);

    iter.next_page();
    let after_one = iter.remaining_pages();
    assert!(after_one < initial);

    while iter.next_page().is_some() {}

    assert_eq!(iter.remaining_pages(), 0);
}

#[test]
fn test_scan_range_debug() {
    let range = ScanRange::new(Address::new(1, 100), Address::new(5, 200));
    let debug_str = format!("{range:?}");
    assert!(debug_str.contains("begin"));
    assert!(debug_str.contains("end"));
}

#[test]
fn test_scan_range_clone_copy() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(10, 0));
    let cloned = range;
    let copied = range;

    assert_eq!(range.begin, cloned.begin);
    assert_eq!(range.end, cloned.end);
    assert_eq!(range.begin, copied.begin);
    assert_eq!(range.end, copied.end);
}

#[test]
fn test_log_scan_iterator_range() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    assert_eq!(iter.range().begin, Address::new(0, 0));
    assert_eq!(iter.range().end, Address::new(5, 0));
}

#[test]
fn test_log_scan_iterator_has_more() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    assert!(iter.has_more());
}

#[test]
fn test_log_scan_iterator_current_page_mut() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    let page = iter.current_page_mut();
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
}

#[test]
fn test_log_scan_iterator_advance_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    let result = iter.advance_page();
    assert!(result.is_some());

    let (page_addr, start_addr) = result.unwrap();
    assert_eq!(page_addr, Address::new(0, 0));
    assert_eq!(start_addr, Address::new(0, 0));
}

#[test]
fn test_log_scan_iterator_finish_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: LogScanIterator<u64, u64, ()> = LogScanIterator::new(range, 4096);

    iter.finish_page();
    assert_eq!(
        iter.current_page_mut().status(),
        LogPageStatus::Uninitialized
    );
}

#[test]
fn test_double_buffered_current_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter: DoubleBufferedLogIterator<u64, u64, ()> = DoubleBufferedLogIterator::new(range, 4096);

    let page = iter.current_page();
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
}

#[test]
fn test_double_buffered_prefetch_page() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter: DoubleBufferedLogIterator<u64, u64, ()> = DoubleBufferedLogIterator::new(range, 4096);

    let page = iter.prefetch_page();
    assert_eq!(page.status(), LogPageStatus::Uninitialized);
}

#[test]
fn test_double_buffered_prefetch_page_mut() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: DoubleBufferedLogIterator<u64, u64, ()> =
        DoubleBufferedLogIterator::new(range, 4096);

    let page = iter.prefetch_page_mut();
    page.set_status(LogPageStatus::Ready);
    assert_eq!(iter.prefetch_page().status(), LogPageStatus::Ready);
}

#[test]
fn test_double_buffered_switch_pages() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: DoubleBufferedLogIterator<u64, u64, ()> =
        DoubleBufferedLogIterator::new(range, 4096);

    iter.current_page_mut().set_status(LogPageStatus::Ready);
    iter.prefetch_page_mut().set_status(LogPageStatus::Pending);

    iter.switch_pages();

    assert_eq!(iter.current_page().status(), LogPageStatus::Pending);
}

#[test]
fn test_double_buffered_next_prefetch_address() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let mut iter: DoubleBufferedLogIterator<u64, u64, ()> =
        DoubleBufferedLogIterator::new(range, 4096);

    let result = iter.next_prefetch_address();
    assert!(result.is_some());

    let (page_addr, start_addr) = result.unwrap();
    assert_eq!(page_addr, Address::new(0, 0));
    assert_eq!(start_addr, Address::new(0, 0));
}

#[test]
fn test_log_page_status_clone_copy_debug() {
    let status = LogPageStatus::Ready;
    let cloned = status;
    let copied = status;

    assert_eq!(status, cloned);
    assert_eq!(status, copied);

    let debug_str = format!("{status:?}");
    assert!(debug_str.contains("Ready"));
}

// ============ ConcurrentLogScanIterator Tests ============

#[test]
fn test_concurrent_log_scan_iterator_new() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.range().begin, range.begin);
    assert_eq!(iter.range().end, range.end);
    assert_eq!(iter.page_size(), 4096);
    assert_eq!(iter.total_pages(), 6); // Pages 0, 1, 2, 3, 4, 5
    assert!(!iter.is_complete());
}

#[test]
fn test_concurrent_log_scan_iterator_empty_range() {
    let range = ScanRange::new(Address::new(5, 0), Address::new(5, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert!(range.is_empty());
    assert_eq!(iter.total_pages(), 0);
}

#[test]
fn test_concurrent_log_scan_iterator_get_next_page() {
    let range = ScanRange::new(Address::new(0, 100), Address::new(2, 200));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    // First page
    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 0);
    assert_eq!(begin, Address::new(0, 100)); // Start at range.begin
    assert_eq!(end, Address::new(1, 0));

    // Second page
    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 1);
    assert_eq!(begin, Address::new(1, 0));
    assert_eq!(end, Address::new(2, 0));

    // Third page (last)
    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 2);
    assert_eq!(begin, Address::new(2, 0));
    assert_eq!(end, Address::new(2, 200)); // End at range.end

    // No more pages
    assert!(iter.get_next_page().is_none());
    assert!(iter.is_complete());
}

#[test]
fn test_concurrent_log_scan_iterator_progress() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(4, 0));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    let (distributed, total) = iter.progress();
    assert_eq!(distributed, 0);
    assert_eq!(total, 5); // Pages 0, 1, 2, 3, 4

    // Claim some pages
    iter.get_next_page();
    iter.get_next_page();

    let (distributed, total) = iter.progress();
    assert_eq!(distributed, 2);
    assert_eq!(total, 5);
}

#[test]
fn test_concurrent_log_scan_iterator_single_page() {
    let range = ScanRange::new(Address::new(3, 100), Address::new(3, 500));
    let iter = ConcurrentLogScanIterator::new(range, 4096);

    assert_eq!(iter.total_pages(), 1);

    let (idx, begin, end) = iter.get_next_page().unwrap();
    assert_eq!(idx, 0);
    assert_eq!(begin, Address::new(3, 100));
    assert_eq!(end, Address::new(3, 500));

    assert!(iter.get_next_page().is_none());
}
