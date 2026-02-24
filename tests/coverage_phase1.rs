//! Phase 1 coverage tests: address, record, ops, index

use oxifaster::address::{Address, AtomicAddress, AtomicPageOffset, PageOffset};
use oxifaster::record::RecordInfo;
use std::sync::atomic::Ordering;

// ========== Address Tests ==========

#[test]
fn test_address_display() {
    let addr = Address::new(5, 100);
    let display = format!("{}", addr);
    assert_eq!(display, "5:100");
}

#[test]
fn test_address_debug() {
    let addr = Address::new(5, 100);
    let debug = format!("{:?}", addr);
    assert!(debug.contains("Address"));
    assert!(debug.contains("page"));
}

#[test]
fn test_address_add_assign() {
    let mut addr = Address::new(0, 100);
    addr += 50;
    assert_eq!(addr.offset(), 150);
}

#[test]
fn test_address_from_u64() {
    let val: u64 = Address::new(3, 500).control();
    let addr = Address::from(val);
    assert_eq!(addr.page(), 3);
    assert_eq!(addr.offset(), 500);
    let back: u64 = addr.into();
    assert_eq!(back, val);
}

#[test]
fn test_address_default() {
    let addr = Address::default();
    assert_eq!(addr.control(), 0);
}

#[test]
fn test_address_readcache_alias() {
    let addr = Address::from_control(Address::READCACHE_BIT | 0x1234);
    assert!(addr.in_readcache());
    let cleared = addr.readcache_address();
    assert!(!cleared.in_readcache());
}

#[test]
fn test_address_is_valid() {
    assert!(Address::new(5, 100).is_valid());
    assert!(!Address::INVALID.is_valid());
}

#[test]
fn test_address_equality() {
    let a = Address::new(1, 100);
    let b = Address::new(1, 100);
    let c = Address::new(1, 200);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn test_address_partial_ord() {
    let a = Address::new(0, 100);
    let b = Address::new(0, 200);
    assert!(a.partial_cmp(&b) == Some(std::cmp::Ordering::Less));
}

// ========== AtomicAddress Tests ==========

#[test]
fn test_atomic_address_compare_exchange() {
    let atomic = AtomicAddress::new(Address::new(1, 100));
    let result = atomic.compare_exchange(
        Address::new(1, 100),
        Address::new(2, 200),
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    assert!(result.is_ok());
    assert_eq!(atomic.load(Ordering::Relaxed), Address::new(2, 200));
}

#[test]
fn test_atomic_address_compare_exchange_fail() {
    let atomic = AtomicAddress::new(Address::new(1, 100));
    let result = atomic.compare_exchange(
        Address::new(1, 200),
        Address::new(2, 200),
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    assert!(result.is_err());
}

#[test]
fn test_atomic_address_compare_exchange_weak() {
    let atomic = AtomicAddress::new(Address::new(1, 100));
    loop {
        match atomic.compare_exchange_weak(
            Address::new(1, 100),
            Address::new(2, 200),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(_) => continue,
        }
    }
    assert_eq!(atomic.load(Ordering::Relaxed), Address::new(2, 200));
}

#[test]
fn test_atomic_address_fetch_add() {
    let atomic = AtomicAddress::new(Address::new(0, 100));
    let prev = atomic.fetch_add(50, Ordering::AcqRel);
    assert_eq!(prev.offset(), 100);
    assert_eq!(atomic.load(Ordering::Relaxed).offset(), 150);
}

#[test]
fn test_atomic_address_page_offset() {
    let atomic = AtomicAddress::new(Address::new(7, 300));
    assert_eq!(atomic.page(), 7);
    assert_eq!(atomic.offset(), 300);
}

#[test]
fn test_atomic_address_default() {
    let atomic = AtomicAddress::default();
    assert_eq!(atomic.load(Ordering::Relaxed).control(), 0);
}

#[test]
fn test_atomic_address_debug() {
    let atomic = AtomicAddress::new(Address::new(3, 500));
    let debug = format!("{:?}", atomic);
    assert!(debug.contains("AtomicAddress"));
}

#[test]
fn test_atomic_address_clone() {
    let atomic = AtomicAddress::new(Address::new(3, 500));
    let cloned = atomic.clone();
    assert_eq!(
        cloned.load(Ordering::Relaxed),
        atomic.load(Ordering::Relaxed)
    );
}

// ========== PageOffset Tests ==========

#[test]
fn test_page_offset_new() {
    let po = PageOffset::new(5, 1000);
    assert_eq!(po.page(), 5);
    assert_eq!(po.offset(), 1000);
}

#[test]
fn test_page_offset_to_address() {
    let po = PageOffset::new(5, 1000);
    let addr = po.to_address();
    assert_eq!(addr.page(), 5);
    assert_eq!(addr.offset(), 1000);
}

#[test]
fn test_page_offset_to_address_overflow() {
    let po = PageOffset::new(5, Address::MAX_OFFSET as u64 + 100);
    let addr = po.to_address();
    assert_eq!(addr.page(), 5);
    assert_eq!(addr.offset(), Address::MAX_OFFSET);
}

#[test]
fn test_page_offset_from_address() {
    let addr = Address::new(10, 500);
    let po = PageOffset::from(addr);
    assert_eq!(po.page(), 10);
    assert_eq!(po.offset(), 500);
}

#[test]
fn test_page_offset_debug() {
    let po = PageOffset::new(3, 200);
    let debug = format!("{:?}", po);
    assert!(debug.contains("PageOffset"));
}

#[test]
fn test_page_offset_control() {
    let po = PageOffset::new(1, 100);
    let ctrl = po.control();
    assert_eq!(ctrl, PageOffset::new(1, 100).control());
}

// ========== AtomicPageOffset Tests ==========

#[test]
fn test_atomic_page_offset_from_address() {
    let addr = Address::new(5, 1000);
    let apo = AtomicPageOffset::from_address(addr);
    let loaded = apo.load(Ordering::Relaxed);
    assert_eq!(loaded.page(), 5);
    assert_eq!(loaded.offset(), 1000);
}

#[test]
fn test_atomic_page_offset_store_address() {
    let apo = AtomicPageOffset::default();
    apo.store_address(Address::new(3, 500), Ordering::Release);
    let loaded = apo.load(Ordering::Acquire);
    assert_eq!(loaded.page(), 3);
    assert_eq!(loaded.offset(), 500);
}

#[test]
fn test_atomic_page_offset_compare_exchange() {
    let apo = AtomicPageOffset::new(PageOffset::new(0, 0));
    let current = apo.load(Ordering::Acquire);
    let new = PageOffset::new(1, 100);
    let result = apo.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire);
    assert!(result.is_ok());
    assert_eq!(apo.load(Ordering::Relaxed).page(), 1);
}

#[test]
fn test_atomic_page_offset_new_page() {
    let apo = AtomicPageOffset::new(PageOffset::new(0, 100));
    let (advanced, won_cas) = apo.new_page(0);
    assert!(advanced);
    assert!(won_cas);
    assert_eq!(apo.load(Ordering::Relaxed).page(), 1);
    assert_eq!(apo.load(Ordering::Relaxed).offset(), 0);
}

#[test]
fn test_atomic_page_offset_new_page_already_advanced() {
    let apo = AtomicPageOffset::new(PageOffset::new(5, 100));
    let (advanced, won_cas) = apo.new_page(3);
    assert!(advanced);
    assert!(!won_cas);
}

#[test]
fn test_atomic_page_offset_debug() {
    let apo = AtomicPageOffset::new(PageOffset::new(2, 300));
    let debug = format!("{:?}", apo);
    assert!(debug.contains("AtomicPageOffset"));
}

// ========== RecordInfo Tests ==========

#[test]
fn test_record_info_new_with_all_flags() {
    let addr = Address::new(1, 100);
    let info = RecordInfo::new(addr, 42, true, true, true);
    assert_eq!(info.previous_address(), addr);
    assert_eq!(info.checkpoint_version(), 42);
    assert!(info.is_invalid());
    assert!(info.is_tombstone());
    assert!(info.is_final());
}

#[test]
fn test_record_info_new_no_flags() {
    let addr = Address::new(2, 200);
    let info = RecordInfo::new(addr, 10, false, false, false);
    assert!(!info.is_invalid());
    assert!(!info.is_tombstone());
    assert!(!info.is_final());
}

#[test]
fn test_record_info_set_invalid_toggle() {
    let info = RecordInfo::new(Address::new(0, 0), 0, false, false, false);
    info.set_invalid(true);
    assert!(info.is_invalid());
    info.set_invalid(false);
    assert!(!info.is_invalid());
}

#[test]
fn test_record_info_set_tombstone_toggle() {
    let info = RecordInfo::new(Address::new(0, 0), 0, false, false, false);
    info.set_tombstone(true);
    assert!(info.is_tombstone());
    info.set_tombstone(false);
    assert!(!info.is_tombstone());
}

#[test]
fn test_record_info_set_previous_address() {
    let info = RecordInfo::new(Address::new(1, 100), 5, false, false, false);
    info.set_previous_address(Address::new(3, 300));
    assert_eq!(info.previous_address(), Address::new(3, 300));
    assert_eq!(info.checkpoint_version(), 5);
}

#[test]
fn test_record_info_from_control() {
    let info1 = RecordInfo::new(Address::new(1, 100), 5, true, false, false);
    let control = info1.control();
    let info2 = RecordInfo::from_control(control);
    assert_eq!(info2.previous_address(), info1.previous_address());
    assert_eq!(info2.checkpoint_version(), info1.checkpoint_version());
}

#[test]
fn test_record_info_is_null() {
    let info = RecordInfo::default();
    assert!(info.is_null());
    let info2 = RecordInfo::new(Address::new(1, 100), 0, false, false, false);
    assert!(!info2.is_null());
}

#[test]
fn test_record_info_in_read_cache() {
    let addr = Address::from_control(Address::READ_CACHE_MASK | 0x100);
    let info = RecordInfo::new(addr, 0, false, false, false);
    assert!(info.in_read_cache());
}

#[test]
fn test_record_info_load_store() {
    let info = RecordInfo::default();
    info.store(0x12345678, Ordering::Release);
    assert_eq!(info.load(Ordering::Acquire), 0x12345678);
}

#[test]
fn test_record_info_clone() {
    let info = RecordInfo::new(Address::new(5, 500), 7, true, false, true);
    let cloned = info.clone();
    assert_eq!(cloned.previous_address(), info.previous_address());
    assert_eq!(cloned.checkpoint_version(), info.checkpoint_version());
}

#[test]
fn test_record_info_debug() {
    let info = RecordInfo::new(Address::new(1, 100), 5, true, true, true);
    let debug = format!("{:?}", info);
    assert!(debug.contains("RecordInfo"));
    assert!(debug.contains("invalid"));
    assert!(debug.contains("tombstone"));
}

// ========== Ops Tests ==========

#[test]
fn test_checkpoint_self_check_empty_dir() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    let dir = tempfile::tempdir().unwrap();
    let options = CheckpointSelfCheckOptions::default();
    let report = checkpoint_self_check(dir.path(), options).unwrap();
    assert_eq!(report.total, 0);
    assert_eq!(report.valid, 0);
    assert_eq!(report.invalid, 0);
    assert!(!report.repaired);
}

#[test]
fn test_checkpoint_self_check_nonexistent_dir() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    use std::path::Path;
    let options = CheckpointSelfCheckOptions::default();
    let report =
        checkpoint_self_check(Path::new("/tmp/nonexistent_oxifaster_test"), options).unwrap();
    assert_eq!(report.total, 0);
}

#[test]
fn test_checkpoint_self_check_with_non_uuid_dirs() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("not-a-uuid")).unwrap();
    std::fs::create_dir(dir.path().join("also-not-uuid")).unwrap();
    let options = CheckpointSelfCheckOptions::default();
    let report = checkpoint_self_check(dir.path(), options).unwrap();
    assert_eq!(report.total, 0);
}

#[test]
fn test_checkpoint_self_check_with_uuid_dir_missing_metadata() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    let dir = tempfile::tempdir().unwrap();
    let uuid = uuid::Uuid::new_v4();
    std::fs::create_dir(dir.path().join(uuid.to_string())).unwrap();
    let options = CheckpointSelfCheckOptions {
        repair: false,
        dry_run: false,
        remove_invalid: true,
    };
    let report = checkpoint_self_check(dir.path(), options).unwrap();
    assert_eq!(report.total, 1);
    assert_eq!(report.invalid, 1);
    assert_eq!(report.valid, 0);
    assert_eq!(report.issues.len(), 1);
}

#[test]
fn test_checkpoint_self_check_dry_run_repair() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    let dir = tempfile::tempdir().unwrap();
    let uuid = uuid::Uuid::new_v4();
    std::fs::create_dir(dir.path().join(uuid.to_string())).unwrap();
    let options = CheckpointSelfCheckOptions {
        repair: true,
        dry_run: true,
        remove_invalid: true,
    };
    let report = checkpoint_self_check(dir.path(), options).unwrap();
    assert_eq!(report.total, 1);
    assert_eq!(report.invalid, 1);
    assert!(report.dry_run);
    assert_eq!(report.planned_removals.len(), 1);
    assert!(report.removed.is_empty());
    assert!(dir.path().join(uuid.to_string()).exists());
}

#[test]
fn test_checkpoint_self_check_actual_repair() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    let dir = tempfile::tempdir().unwrap();
    let uuid = uuid::Uuid::new_v4();
    let cp_dir = dir.path().join(uuid.to_string());
    std::fs::create_dir(&cp_dir).unwrap();
    let options = CheckpointSelfCheckOptions {
        repair: true,
        dry_run: false,
        remove_invalid: true,
    };
    let report = checkpoint_self_check(dir.path(), options).unwrap();
    assert!(report.repaired);
    assert_eq!(report.removed.len(), 1);
    assert!(!cp_dir.exists());
}

#[test]
fn test_checkpoint_self_check_options_default() {
    use oxifaster::ops::CheckpointSelfCheckOptions;
    let opts = CheckpointSelfCheckOptions::default();
    assert!(!opts.repair);
    assert!(!opts.dry_run);
    assert!(opts.remove_invalid);
}

#[test]
fn test_checkpoint_self_check_with_files_skipped() {
    use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("somefile.txt"), "hello").unwrap();
    let options = CheckpointSelfCheckOptions::default();
    let report = checkpoint_self_check(dir.path(), options).unwrap();
    assert_eq!(report.total, 0);
}

// ========== Index (public API) Tests ==========

#[test]
fn test_mem_hash_index_config_default() {
    use oxifaster::index::MemHashIndexConfig;
    let config = MemHashIndexConfig::default();
    assert_eq!(config.table_size, 1 << 20);
}

#[test]
fn test_mem_hash_index_config_new() {
    use oxifaster::index::MemHashIndexConfig;
    let config = MemHashIndexConfig::new(4096).unwrap();
    assert_eq!(config.table_size, 4096);
}

#[test]
fn test_find_result_not_found() {
    use oxifaster::index::FindResult;
    let result = FindResult::not_found();
    assert!(!result.found());
}

#[test]
fn test_index_stats_display() {
    use oxifaster::index::IndexStats;
    let stats = IndexStats {
        table_size: 1024,
        total_entries: 7168,
        used_entries: 500,
        buckets_with_entries: 400,
        load_factor: 0.0698,
    };
    let display = format!("{}", stats);
    assert!(display.contains("Index Statistics"));
    assert!(display.contains("1024"));
}

// ========== Store public API Tests ==========

#[test]
fn test_pending_context_new() {
    use oxifaster::status::OperationType;
    use oxifaster::store::PendingContext;
    let ctx = PendingContext::new(OperationType::Read, 5);
    assert_eq!(ctx.thread_id, 5);
    assert!(ctx.address.is_invalid());
}
