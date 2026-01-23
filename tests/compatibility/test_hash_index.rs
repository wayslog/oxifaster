// Hash Index 兼容性测试
//
// 目标: 验证 oxifaster 的 hash index 与 C++ FASTER 的兼容性
// 参考: C++ FASTER hash_bucket.h 的 64 字节 bucket 布局

use oxifaster::address::Address;
use oxifaster::index::{AtomicHashBucketEntry, ColdHashBucket, HashBucket, IndexHashBucketEntry};
use std::mem;
use std::ptr;

#[test]
fn test_hash_bucket_size() {
    // Hot bucket: 7 entries + 1 overflow = 64 bytes
    assert_eq!(HashBucket::NUM_ENTRIES, 7);
    assert_eq!(mem::size_of::<HashBucket>(), 64);
    assert_eq!(mem::align_of::<HashBucket>(), 64);

    // Cold bucket: 8 entries = 64 bytes
    assert_eq!(ColdHashBucket::NUM_ENTRIES, 8);
    assert_eq!(mem::size_of::<ColdHashBucket>(), 64);
    assert_eq!(mem::align_of::<ColdHashBucket>(), 64);

    // Entry size
    assert_eq!(mem::size_of::<AtomicHashBucketEntry>(), 8);
    assert_eq!(mem::align_of::<AtomicHashBucketEntry>(), 8);

    // Tag width (C++ FASTER uses 14-bit tag)
    assert_eq!(IndexHashBucketEntry::TAG_BITS, 14);

    let tag_mask = (1u64 << IndexHashBucketEntry::TAG_BITS) - 1;
    let tag = tag_mask as u16;
    let entry = IndexHashBucketEntry::new(Address::from_control(0), tag, false);
    let control = entry.control();

    assert_eq!(entry.tag(), tag);
    assert_eq!((control >> 48) & tag_mask, tag as u64);
    assert_eq!((control >> 62) & 1, 0);
    assert_eq!((control >> 63) & 1, 0);
}

#[test]
fn test_hash_bucket_layout() {
    let bucket = mem::MaybeUninit::<HashBucket>::uninit();
    let base = bucket.as_ptr() as *const u8;
    let entries_ptr = unsafe { ptr::addr_of!((*bucket.as_ptr()).entries) } as *const u8;
    let overflow_ptr = unsafe { ptr::addr_of!((*bucket.as_ptr()).overflow_entry) } as *const u8;

    let entries_offset = unsafe { entries_ptr.offset_from(base) } as usize;
    let overflow_offset = unsafe { overflow_ptr.offset_from(base) } as usize;
    let entry_stride = mem::size_of::<AtomicHashBucketEntry>();

    assert_eq!(entries_offset, 0);
    assert_eq!(overflow_offset, HashBucket::NUM_ENTRIES * entry_stride);
    assert_eq!(overflow_offset, 56);

    let entries_first = entries_ptr as *const AtomicHashBucketEntry;
    let entries_second = unsafe { entries_first.add(1) } as *const u8;
    let stride = unsafe { entries_second.offset_from(entries_ptr) } as usize;
    assert_eq!(stride, entry_stride);

    let cold_bucket = mem::MaybeUninit::<ColdHashBucket>::uninit();
    let cold_base = cold_bucket.as_ptr() as *const u8;
    let cold_entries_ptr = unsafe { ptr::addr_of!((*cold_bucket.as_ptr()).entries) } as *const u8;
    let cold_entries_offset = unsafe { cold_entries_ptr.offset_from(cold_base) } as usize;

    assert_eq!(cold_entries_offset, 0);
    assert_eq!(
        mem::size_of::<ColdHashBucket>(),
        ColdHashBucket::NUM_ENTRIES * entry_stride
    );
}
