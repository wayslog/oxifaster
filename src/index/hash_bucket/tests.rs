use super::*;

#[test]
fn test_hash_bucket_entry() {
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);

    assert_eq!(entry.address(), addr);
    assert!(!entry.is_unused());
    assert!(!entry.in_read_cache());
}

#[test]
fn test_index_hash_bucket_entry() {
    let addr = Address::new(5, 500);
    let entry = IndexHashBucketEntry::new(addr, 0x1234, false);

    assert_eq!(entry.address(), addr);
    assert_eq!(entry.tag(), 0x1234);
    assert!(!entry.is_tentative());
    assert!(!entry.is_unused());
}

#[test]
fn test_index_entry_tentative() {
    let addr = Address::new(1, 100);
    let entry = IndexHashBucketEntry::new(addr, 0x5678, true);

    assert!(entry.is_tentative());
}

#[test]
fn test_atomic_entry_cas() {
    let atomic = AtomicHashBucketEntry::invalid();
    let old = HashBucketEntry::INVALID;
    let new = HashBucketEntry::new(Address::new(1, 1));

    let result = atomic.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire);
    assert!(result.is_ok());

    let loaded = atomic.load(Ordering::Acquire);
    assert_eq!(loaded, new);
}

#[test]
fn test_hash_bucket_size() {
    assert_eq!(mem::size_of::<HashBucket>(), 64);
    assert_eq!(mem::align_of::<HashBucket>(), 64);
}

#[test]
fn test_overflow_entry() {
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new(addr);

    assert_eq!(entry.address().control(), 12345);
    assert!(!entry.is_unused());
}

#[test]
fn test_hash_bucket_entry_invalid() {
    let entry = HashBucketEntry::INVALID;
    assert!(entry.is_unused());
    assert_eq!(entry.control(), 0);
}

#[test]
fn test_hash_bucket_entry_from_control() {
    let control = 0x123456789ABC;
    let entry = HashBucketEntry::from_control(control);
    assert_eq!(entry.control(), control);
}

#[test]
fn test_hash_bucket_entry_debug() {
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);
    let debug_str = format!("{entry:?}");
    assert!(debug_str.contains("HashBucketEntry"));
    assert!(debug_str.contains("address"));
}

#[test]
fn test_hash_bucket_entry_eq() {
    let addr = Address::new(10, 1000);
    let entry1 = HashBucketEntry::new(addr);
    let entry2 = HashBucketEntry::new(addr);
    assert_eq!(entry1, entry2);
}

#[test]
fn test_hash_bucket_entry_default() {
    let entry = HashBucketEntry::default();
    assert!(entry.is_unused());
}

#[test]
fn test_hash_bucket_entry_clone_copy() {
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);
    let cloned = entry;
    let copied = entry;
    assert_eq!(entry, cloned);
    assert_eq!(entry, copied);
}

#[test]
fn test_index_hash_bucket_entry_invalid() {
    let entry = IndexHashBucketEntry::INVALID;
    assert!(entry.is_unused());
    assert_eq!(entry.control(), 0);
}

#[test]
fn test_index_hash_bucket_entry_from_control() {
    let control = 0xABCDEF123456;
    let entry = IndexHashBucketEntry::from_control(control);
    assert_eq!(entry.control(), control);
}

#[test]
fn test_index_hash_bucket_entry_debug() {
    let addr = Address::new(5, 500);
    let entry = IndexHashBucketEntry::new(addr, 0x1234, true);
    let debug_str = format!("{entry:?}");
    assert!(debug_str.contains("IndexHashBucketEntry"));
    assert!(debug_str.contains("address"));
    assert!(debug_str.contains("tag"));
    assert!(debug_str.contains("tentative"));
}

#[test]
fn test_index_hash_bucket_entry_eq() {
    let addr = Address::new(5, 500);
    let entry1 = IndexHashBucketEntry::new(addr, 0x1234, false);
    let entry2 = IndexHashBucketEntry::new(addr, 0x1234, false);
    assert_eq!(entry1, entry2);
}

#[test]
fn test_index_hash_bucket_entry_default() {
    let entry = IndexHashBucketEntry::default();
    assert!(entry.is_unused());
}

#[test]
fn test_index_hash_bucket_entry_clone_copy() {
    let addr = Address::new(5, 500);
    let entry = IndexHashBucketEntry::new(addr, 0x1234, false);
    let cloned = entry;
    let copied = entry;
    assert_eq!(entry, cloned);
    assert_eq!(entry, copied);
}

#[test]
fn test_index_hash_bucket_entry_with_read_cache() {
    let addr = Address::new(5, 500);
    let entry = IndexHashBucketEntry::new_with_read_cache(addr, 0x1234, false, true);
    assert!(entry.in_read_cache());

    let entry_no_cache = IndexHashBucketEntry::new_with_read_cache(addr, 0x1234, false, false);
    assert!(!entry_no_cache.in_read_cache());
}

#[test]
fn test_index_hash_bucket_entry_to_hash_bucket_entry() {
    let addr = Address::new(5, 500);
    let index_entry = IndexHashBucketEntry::new(addr, 0x1234, false);
    let hash_entry = index_entry.to_hash_bucket_entry();
    assert_eq!(hash_entry.control(), index_entry.control());
}

#[test]
fn test_index_hash_bucket_entry_from_hash_bucket_entry() {
    let addr = Address::new(5, 500);
    let hash_entry = HashBucketEntry::new(addr);
    let index_entry: IndexHashBucketEntry = hash_entry.into();
    assert_eq!(index_entry.address(), addr);
}

#[test]
fn test_atomic_hash_bucket_entry_new() {
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);
    let atomic = AtomicHashBucketEntry::new(entry);
    assert_eq!(atomic.load(Ordering::Relaxed), entry);
}

#[test]
fn test_atomic_hash_bucket_entry_invalid() {
    let atomic = AtomicHashBucketEntry::invalid();
    assert!(atomic.load(Ordering::Relaxed).is_unused());
}

#[test]
fn test_atomic_hash_bucket_entry_store() {
    let atomic = AtomicHashBucketEntry::invalid();
    let entry = HashBucketEntry::new(Address::new(10, 1000));
    atomic.store(entry, Ordering::Relaxed);
    assert_eq!(atomic.load(Ordering::Relaxed), entry);
}

#[test]
fn test_atomic_hash_bucket_entry_load_index() {
    let addr = Address::new(10, 1000);
    let entry = IndexHashBucketEntry::new(addr, 0x1234, false);
    let atomic = AtomicHashBucketEntry::new(entry.to_hash_bucket_entry());
    let loaded = atomic.load_index(Ordering::Relaxed);
    assert_eq!(loaded.address(), addr);
}

#[test]
fn test_atomic_hash_bucket_entry_store_index() {
    let atomic = AtomicHashBucketEntry::invalid();
    let addr = Address::new(10, 1000);
    let entry = IndexHashBucketEntry::new(addr, 0x1234, false);
    atomic.store_index(entry, Ordering::Relaxed);
    let loaded = atomic.load_index(Ordering::Relaxed);
    assert_eq!(loaded.address(), addr);
}

#[test]
fn test_atomic_hash_bucket_entry_compare_exchange_weak() {
    let atomic = AtomicHashBucketEntry::invalid();
    let old = HashBucketEntry::INVALID;
    let new = HashBucketEntry::new(Address::new(1, 1));

    loop {
        match atomic.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(_) => continue,
        }
    }

    let loaded = atomic.load(Ordering::Acquire);
    assert_eq!(loaded, new);
}

#[test]
fn test_atomic_hash_bucket_entry_default() {
    let atomic = AtomicHashBucketEntry::default();
    assert!(atomic.load(Ordering::Relaxed).is_unused());
}

#[test]
fn test_atomic_hash_bucket_entry_clone() {
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);
    let atomic = AtomicHashBucketEntry::new(entry);
    let cloned = atomic.clone();
    assert_eq!(
        atomic.load(Ordering::Relaxed),
        cloned.load(Ordering::Relaxed)
    );
}

#[test]
fn test_atomic_hash_bucket_entry_debug() {
    let atomic = AtomicHashBucketEntry::invalid();
    let debug_str = format!("{atomic:?}");
    assert!(debug_str.contains("AtomicHashBucketEntry"));
}

#[test]
fn test_fixed_page_address_invalid() {
    let addr = FixedPageAddress::INVALID;
    assert!(addr.is_invalid());
    assert_eq!(addr.control(), u64::MAX);
}

#[test]
fn test_fixed_page_address_new() {
    let addr = FixedPageAddress::new(12345);
    assert!(!addr.is_invalid());
    assert_eq!(addr.control(), 12345);
}

#[test]
fn test_fixed_page_address_debug() {
    let addr = FixedPageAddress::new(12345);
    let debug_str = format!("{addr:?}");
    assert!(debug_str.contains("12345"));

    let invalid = FixedPageAddress::INVALID;
    let debug_str = format!("{invalid:?}");
    assert!(debug_str.contains("INVALID"));
}

#[test]
fn test_fixed_page_address_eq() {
    let addr1 = FixedPageAddress::new(12345);
    let addr2 = FixedPageAddress::new(12345);
    assert_eq!(addr1, addr2);

    let addr3 = FixedPageAddress::new(54321);
    assert_ne!(addr1, addr3);
}

#[test]
fn test_fixed_page_address_default() {
    let addr = FixedPageAddress::default();
    assert_eq!(addr.control(), 0);
}

#[test]
fn test_fixed_page_address_clone_copy() {
    let addr = FixedPageAddress::new(12345);
    let cloned = addr;
    let copied = addr;
    assert_eq!(addr, cloned);
    assert_eq!(addr, copied);
}

#[test]
fn test_hash_bucket_overflow_entry_invalid() {
    let entry = HashBucketOverflowEntry::INVALID;
    assert!(entry.is_unused());
    assert_eq!(entry.control(), 0);
}

#[test]
fn test_hash_bucket_overflow_entry_from_control() {
    let control = 0x123456;
    let entry = HashBucketOverflowEntry::from_control(control);
    assert_eq!(entry.control(), control);
}

#[test]
fn test_hash_bucket_overflow_entry_tag_summary_bits() {
    let addr = FixedPageAddress::new(42);
    let mut entry = HashBucketOverflowEntry::new(addr);
    assert_eq!(entry.tag_summary(), 0);
    assert!(entry.may_contain_tag(7)); // zero summary means unknown

    entry = entry.with_tag_summary_bit(7);
    assert_ne!(entry.tag_summary(), 0);
    assert!(entry.may_contain_tag(7));
    assert!(!entry.may_contain_tag(8));
}

#[test]
fn test_hash_bucket_overflow_entry_new_with_tag_summary() {
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new_with_tag_summary(addr, 0x00f0);
    assert_eq!(entry.address().control(), 12345);
    assert_eq!(entry.tag_summary(), 0x00f0);
}

#[test]
fn test_hash_bucket_overflow_entry_debug() {
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new(addr);
    let debug_str = format!("{entry:?}");
    assert!(debug_str.contains("HashBucketOverflowEntry"));
}

#[test]
fn test_hash_bucket_overflow_entry_eq() {
    let addr = FixedPageAddress::new(12345);
    let entry1 = HashBucketOverflowEntry::new(addr);
    let entry2 = HashBucketOverflowEntry::new(addr);
    assert_eq!(entry1, entry2);
}

#[test]
fn test_hash_bucket_overflow_entry_default() {
    let entry = HashBucketOverflowEntry::default();
    assert!(entry.is_unused());
}

#[test]
fn test_hash_bucket_overflow_entry_clone_copy() {
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new(addr);
    let cloned = entry;
    let copied = entry;
    assert_eq!(entry, cloned);
    assert_eq!(entry, copied);
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_new() {
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new(addr);
    let atomic = AtomicHashBucketOverflowEntry::new(entry);
    assert_eq!(atomic.load(Ordering::Relaxed), entry);
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_invalid() {
    let atomic = AtomicHashBucketOverflowEntry::invalid();
    assert!(atomic.load(Ordering::Relaxed).is_unused());
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_store() {
    let atomic = AtomicHashBucketOverflowEntry::invalid();
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new(addr);
    atomic.store(entry, Ordering::Relaxed);
    assert_eq!(atomic.load(Ordering::Relaxed), entry);
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_compare_exchange() {
    let atomic = AtomicHashBucketOverflowEntry::invalid();
    let old = HashBucketOverflowEntry::INVALID;
    let addr = FixedPageAddress::new(12345);
    let new = HashBucketOverflowEntry::new(addr);

    let result = atomic.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire);
    assert!(result.is_ok());
    assert_eq!(atomic.load(Ordering::Relaxed), new);
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_set_tag_summary_bit() {
    let addr = FixedPageAddress::new(9);
    let atomic = AtomicHashBucketOverflowEntry::new(HashBucketOverflowEntry::new(addr));

    atomic.set_tag_summary_bit(3, Ordering::AcqRel);
    let entry = atomic.load(Ordering::Acquire);
    assert_eq!(entry.address().control(), 9);
    assert!(entry.may_contain_tag(3));
    assert!(!entry.may_contain_tag(4));
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_default() {
    let atomic = AtomicHashBucketOverflowEntry::default();
    assert!(atomic.load(Ordering::Relaxed).is_unused());
}

#[test]
fn test_atomic_hash_bucket_overflow_entry_clone() {
    let addr = FixedPageAddress::new(12345);
    let entry = HashBucketOverflowEntry::new(addr);
    let atomic = AtomicHashBucketOverflowEntry::new(entry);
    let cloned = atomic.clone();
    assert_eq!(
        atomic.load(Ordering::Relaxed),
        cloned.load(Ordering::Relaxed)
    );
}

#[test]
fn test_hash_bucket_new() {
    let bucket = HashBucket::new();
    for i in 0..HashBucket::NUM_ENTRIES {
        assert!(bucket.entries[i].load(Ordering::Relaxed).is_unused());
    }
    assert!(bucket.overflow_entry.load(Ordering::Relaxed).is_unused());
}

#[test]
fn test_hash_bucket_default() {
    let bucket = HashBucket::default();
    for i in 0..HashBucket::NUM_ENTRIES {
        assert!(bucket.entries[i].load(Ordering::Relaxed).is_unused());
    }
}

#[test]
fn test_hash_bucket_clone() {
    let bucket = HashBucket::new();
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);
    bucket.entries[0].store(entry, Ordering::Relaxed);

    let cloned = bucket.clone();
    assert_eq!(
        cloned.entries[0].load(Ordering::Relaxed),
        bucket.entries[0].load(Ordering::Relaxed)
    );
}

#[test]
fn test_hash_bucket_debug() {
    let bucket = HashBucket::new();
    let debug_str = format!("{bucket:?}");
    assert!(debug_str.contains("HashBucket"));
    assert!(debug_str.contains("entries"));
    assert!(debug_str.contains("overflow"));
}

#[test]
fn test_hash_bucket_num_entries() {
    assert_eq!(HashBucket::NUM_ENTRIES, 7);
}

#[test]
fn test_cold_hash_bucket_new() {
    let bucket = ColdHashBucket::new();
    for i in 0..ColdHashBucket::NUM_ENTRIES {
        assert!(bucket.entries[i].load(Ordering::Relaxed).is_unused());
    }
}

#[test]
fn test_cold_hash_bucket_default() {
    let bucket = ColdHashBucket::default();
    for i in 0..ColdHashBucket::NUM_ENTRIES {
        assert!(bucket.entries[i].load(Ordering::Relaxed).is_unused());
    }
}

#[test]
fn test_cold_hash_bucket_clone() {
    let bucket = ColdHashBucket::new();
    let addr = Address::new(10, 1000);
    let entry = HashBucketEntry::new(addr);
    bucket.entries[0].store(entry, Ordering::Relaxed);

    let cloned = bucket.clone();
    assert_eq!(
        cloned.entries[0].load(Ordering::Relaxed),
        bucket.entries[0].load(Ordering::Relaxed)
    );
}

#[test]
fn test_cold_hash_bucket_num_entries() {
    assert_eq!(ColdHashBucket::NUM_ENTRIES, 8);
}

#[test]
fn test_cold_hash_bucket_size() {
    assert_eq!(mem::size_of::<ColdHashBucket>(), 64);
    assert_eq!(mem::align_of::<ColdHashBucket>(), 64);
}
