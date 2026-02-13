//! Log Scan 集成测试
//!
//! 测试日志扫描功能。

use oxifaster::Address;
use oxifaster::scan::{LogPageStatus, ScanRange};

// ============ ScanRange Tests ============

#[test]
fn test_scan_range_new() {
    let begin = Address::new(0, 0);
    let end = Address::new(5, 0);
    let range = ScanRange::new(begin, end);

    assert_eq!(range.begin, begin);
    assert_eq!(range.end, end);
}

#[test]
fn test_scan_range_is_empty_same_address() {
    let addr = Address::new(5, 0);
    let range = ScanRange::new(addr, addr);

    assert!(range.is_empty());
}

#[test]
fn test_scan_range_is_empty_reversed() {
    let begin = Address::new(10, 0);
    let end = Address::new(5, 0);
    let range = ScanRange::new(begin, end);

    assert!(range.is_empty());
}

#[test]
fn test_scan_range_is_not_empty() {
    let begin = Address::new(0, 0);
    let end = Address::new(5, 0);
    let range = ScanRange::new(begin, end);

    assert!(!range.is_empty());
}

#[test]
fn test_scan_range_page_count_single_page() {
    let begin = Address::new(0, 0);
    let end = Address::new(0, 1000);
    let range = ScanRange::new(begin, end);

    assert_eq!(range.page_count(), 1);
}

#[test]
fn test_scan_range_page_count_multiple_pages() {
    let begin = Address::new(0, 0);
    let end = Address::new(5, 0);
    let range = ScanRange::new(begin, end);

    assert_eq!(range.page_count(), 6); // Pages 0, 1, 2, 3, 4, 5
}

#[test]
fn test_scan_range_page_count_empty() {
    let addr = Address::new(5, 0);
    let range = ScanRange::new(addr, addr);

    assert_eq!(range.page_count(), 0);
}

#[test]
fn test_scan_range_page_count_with_offset() {
    let begin = Address::new(3, 100);
    let end = Address::new(5, 500);
    let range = ScanRange::new(begin, end);

    // Should span pages 3, 4, 5
    assert_eq!(range.page_count(), 3);
}

// ============ LogPageStatus Tests ============

#[test]
fn test_log_page_status_values() {
    assert_eq!(LogPageStatus::Uninitialized as u8, 0);
    assert_eq!(LogPageStatus::Ready as u8, 1);
    assert_eq!(LogPageStatus::Pending as u8, 2);
    assert_eq!(LogPageStatus::Unavailable as u8, 3);
}

#[test]
fn test_log_page_status_default() {
    let status = LogPageStatus::default();
    assert_eq!(status, LogPageStatus::Uninitialized);
}

#[test]
fn test_log_page_status_equality() {
    assert_eq!(LogPageStatus::Ready, LogPageStatus::Ready);
    assert_ne!(LogPageStatus::Ready, LogPageStatus::Pending);
}

#[test]
fn test_log_page_status_debug() {
    assert_eq!(
        format!("{:?}", LogPageStatus::Uninitialized),
        "Uninitialized"
    );
    assert_eq!(format!("{:?}", LogPageStatus::Ready), "Ready");
    assert_eq!(format!("{:?}", LogPageStatus::Pending), "Pending");
    assert_eq!(format!("{:?}", LogPageStatus::Unavailable), "Unavailable");
}

// ============ Edge Cases ============

#[test]
fn test_scan_range_with_invalid_addresses() {
    let begin = Address::INVALID;
    let end = Address::new(5, 0);
    let range = ScanRange::new(begin, end);

    // Should handle gracefully
    assert!(begin.is_invalid());
    assert!(!range.is_empty() || begin >= end);
}

#[test]
fn test_scan_range_large_range() {
    let begin = Address::new(0, 0);
    let end = Address::new(1000, 0);
    let range = ScanRange::new(begin, end);

    assert_eq!(range.page_count(), 1001);
    assert!(!range.is_empty());
}

#[test]
fn test_scan_range_single_byte() {
    let begin = Address::new(0, 0);
    let end = Address::new(0, 1);
    let range = ScanRange::new(begin, end);

    assert!(!range.is_empty());
    assert_eq!(range.page_count(), 1);
}

#[test]
fn test_scan_range_page_boundary() {
    // Test range that starts at exact page boundary
    let begin = Address::new(5, 0);
    let end = Address::new(10, 0);
    let range = ScanRange::new(begin, end);

    assert_eq!(range.page_count(), 6);
}

// ============ Clone and Copy Tests ============

#[test]
fn test_scan_range_clone() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let cloned = range;

    assert_eq!(range.begin, cloned.begin);
    assert_eq!(range.end, cloned.end);
}

#[test]
fn test_scan_range_copy() {
    let range = ScanRange::new(Address::new(0, 0), Address::new(5, 0));
    let copied = range;

    assert_eq!(range.begin, copied.begin);
    assert_eq!(range.end, copied.end);
}

#[test]
fn test_log_page_status_clone() {
    let status = LogPageStatus::Ready;
    let cloned = status;

    assert_eq!(status, cloned);
}

#[test]
fn test_log_page_status_copy() {
    let status = LogPageStatus::Pending;
    let copied = status;

    assert_eq!(status, copied);
}

// ============ Debug Format Tests ============

#[test]
fn test_scan_range_debug() {
    let range = ScanRange::new(Address::new(1, 100), Address::new(5, 500));
    let debug_str = format!("{range:?}");

    assert!(debug_str.contains("begin"));
    assert!(debug_str.contains("end"));
}
