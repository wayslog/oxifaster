use super::*;
use crate::device::NullDisk;

fn create_test_allocator() -> PersistentMemoryMalloc<NullDisk> {
    let config = HybridLogConfig {
        page_size: 4096, // 4 KB pages for testing
        memory_pages: 16,
        mutable_pages: 4,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    PersistentMemoryMalloc::new(config, device)
}

#[test]
fn test_allocate_basic() {
    let allocator = create_test_allocator();

    let addr1 = allocator.allocate(100).unwrap();
    assert_eq!(addr1.page(), 0);
    assert_eq!(addr1.offset(), 0);

    let addr2 = allocator.allocate(100).unwrap();
    assert_eq!(addr2.page(), 0);
    assert_eq!(addr2.offset(), 100);
}

#[test]
fn test_allocate_page_overflow() {
    let allocator = create_test_allocator();
    let page_size = allocator.page_size();

    let addr1 = allocator.allocate((page_size - 100) as u32).unwrap();
    assert_eq!(addr1.page(), 0);

    let addr2 = allocator.allocate(200).unwrap();
    assert_eq!(addr2.page(), 1);
    assert_eq!(addr2.offset(), 0);
}

#[test]
fn test_get_addresses() {
    let allocator = create_test_allocator();

    let tail = allocator.get_tail_address();
    let read_only = allocator.get_read_only_address();
    let head = allocator.get_head_address();
    let begin = allocator.get_begin_address();

    assert_eq!(tail, Address::new(0, 0));
    assert_eq!(read_only, Address::new(0, 0));
    assert_eq!(head, Address::new(0, 0));
    assert_eq!(begin, Address::new(0, 0));
}

#[test]
fn test_shift_addresses() {
    let allocator = create_test_allocator();

    let new_addr = Address::new(5, 0);
    allocator.shift_read_only_address(new_addr);

    assert_eq!(allocator.get_read_only_address(), new_addr);
}

#[test]
fn test_log_stats() {
    let allocator = create_test_allocator();

    allocator.allocate(1000).unwrap();

    let stats = allocator.get_stats();
    assert!(stats.tail_address > Address::new(0, 0));
}

#[test]
fn test_is_mutable() {
    let allocator = create_test_allocator();

    let addr = allocator.allocate(100).unwrap();

    assert!(allocator.is_mutable(addr));

    allocator.shift_read_only_address(Address::new(1, 0));

    assert!(!allocator.is_mutable(addr));
}

#[test]
fn test_checkpoint_metadata() {
    let allocator = create_test_allocator();

    allocator.allocate(1000).unwrap();

    let token = uuid::Uuid::new_v4();
    let metadata = allocator.checkpoint_metadata(token, 1, true);

    assert_eq!(metadata.token, token);
    assert_eq!(metadata.version, 1);
    assert_eq!(metadata.begin_address, Address::new(0, 0));
    assert!(metadata.final_address > Address::new(0, 0));
    assert!(metadata.use_snapshot_file);
}

#[test]
fn test_flush_until() {
    let allocator = create_test_allocator();

    let addr = allocator.allocate(1000).unwrap();

    let flush_addr = Address::new(addr.page() + 1, 0);
    allocator.flush_until(flush_addr).unwrap();

    assert!(allocator.get_flushed_until_address() >= flush_addr);
}

#[test]
fn test_checkpoint_and_recover_empty() {
    let allocator = create_test_allocator();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    let metadata = allocator.checkpoint(temp_dir.path(), token, 1).unwrap();

    assert_eq!(metadata.token, token);
    assert_eq!(metadata.version, 1);

    let mut recovered = create_test_allocator();
    recovered.recover(temp_dir.path(), Some(&metadata)).unwrap();

    assert_eq!(recovered.get_begin_address(), allocator.get_begin_address());
}

#[test]
fn test_checkpoint_and_recover_with_data() {
    let mut allocator = create_test_allocator();

    let addr1 = allocator.allocate(100).unwrap();
    let addr2 = allocator.allocate(200).unwrap();

    unsafe {
        let ptr1 = allocator
            .get_mut(addr1)
            .expect("addr1 should be accessible");
        std::ptr::write_bytes(ptr1.as_ptr(), 0xAB, 100);

        let ptr2 = allocator
            .get_mut(addr2)
            .expect("addr2 should be accessible");
        std::ptr::write_bytes(ptr2.as_ptr(), 0xCD, 200);
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    let metadata = allocator.checkpoint(temp_dir.path(), token, 2).unwrap();

    let mut recovered = create_test_allocator();
    recovered.recover(temp_dir.path(), Some(&metadata)).unwrap();

    assert_eq!(recovered.get_begin_address(), allocator.get_begin_address());

    unsafe {
        let ptr1 = recovered
            .get(addr1)
            .expect("recovered addr1 should be accessible");
        let data1 = std::slice::from_raw_parts(ptr1.as_ptr(), 100);
        assert!(
            data1.iter().all(|&b| b == 0xAB),
            "Data at addr1 not correctly recovered"
        );

        let ptr2 = recovered
            .get(addr2)
            .expect("recovered addr2 should be accessible");
        let data2 = std::slice::from_raw_parts(ptr2.as_ptr(), 200);
        assert!(
            data2.iter().all(|&b| b == 0xCD),
            "Data at addr2 not correctly recovered"
        );
    }
}

#[test]
fn test_recover_without_preloaded_metadata() {
    let allocator = create_test_allocator();

    allocator.allocate(500).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    allocator.checkpoint(temp_dir.path(), token, 3).unwrap();

    let mut recovered = create_test_allocator();
    recovered.recover(temp_dir.path(), None).unwrap();

    assert_eq!(recovered.get_begin_address(), allocator.get_begin_address());
}

#[test]
fn test_checkpoint_preserves_stats() {
    let allocator = create_test_allocator();

    for _ in 0..10 {
        allocator.allocate(1000).unwrap();
    }

    let original_stats = allocator.get_stats();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    let metadata = allocator.checkpoint(temp_dir.path(), token, 1).unwrap();

    assert_eq!(metadata.begin_address, original_stats.begin_address);
    assert_eq!(metadata.final_address, original_stats.tail_address);
}

#[test]
fn test_recover_rejects_smaller_buffer_size() {
    let config = HybridLogConfig {
        page_size: 4096,
        memory_pages: 32,
        mutable_pages: 8,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    let allocator = PersistentMemoryMalloc::new(config, device);

    allocator.allocate(1000).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    allocator.checkpoint(temp_dir.path(), token, 1).unwrap();

    let smaller_config = HybridLogConfig {
        page_size: 4096,
        memory_pages: 16,
        mutable_pages: 4,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    let mut smaller_allocator = PersistentMemoryMalloc::new(smaller_config, device);

    let result = smaller_allocator.recover(temp_dir.path(), None);
    assert!(
        result.is_err(),
        "Recovery should fail with smaller buffer size"
    );

    if let Err(e) = result {
        assert!(
            e.to_string().contains("Buffer size mismatch"),
            "Error should mention buffer size mismatch, got: {e}"
        );
    }
}

#[test]
fn test_recover_allows_larger_buffer_size() {
    let config = HybridLogConfig {
        page_size: 4096,
        memory_pages: 16,
        mutable_pages: 4,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    let allocator = PersistentMemoryMalloc::new(config, device);

    allocator.allocate(1000).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    allocator.checkpoint(temp_dir.path(), token, 1).unwrap();

    let larger_config = HybridLogConfig {
        page_size: 4096,
        memory_pages: 32,
        mutable_pages: 8,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    let mut larger_allocator = PersistentMemoryMalloc::new(larger_config, device);

    let result = larger_allocator.recover(temp_dir.path(), None);
    assert!(
        result.is_ok(),
        "Recovery should succeed with larger buffer size"
    );
}

#[test]
fn test_recover_fails_without_snapshot() {
    let config = HybridLogConfig {
        page_size: 4096,
        memory_pages: 16,
        mutable_pages: 4,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    let allocator = PersistentMemoryMalloc::new(config, device);

    allocator.allocate(1000).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    let cp_dir = temp_dir.path().join(token.to_string());
    std::fs::create_dir_all(&cp_dir).unwrap();

    allocator.checkpoint(&cp_dir, token, 1).unwrap();

    let snapshot_path = cp_dir.join("log.snapshot");
    assert!(
        snapshot_path.exists(),
        "Snapshot should exist before deletion"
    );
    std::fs::remove_file(&snapshot_path).unwrap();

    let device = Arc::new(NullDisk::new());
    let mut recovered = PersistentMemoryMalloc::new(
        HybridLogConfig {
            page_size: 4096,
            memory_pages: 16,
            mutable_pages: 4,
            segment_size: 1 << 20,
        },
        device,
    );

    let result = recovered.recover(&cp_dir, None);
    assert!(
        result.is_err(),
        "Recovery should fail when snapshot file is missing"
    );

    if let Err(e) = result {
        assert!(
            e.to_string().contains("Log snapshot file missing")
                || e.to_string().contains("snapshot"),
            "Error should mention missing snapshot, got: {e}"
        );
    }
}

#[test]
fn test_recovered_data_is_readable() {
    let config = HybridLogConfig {
        page_size: 4096,
        memory_pages: 16,
        mutable_pages: 4,
        segment_size: 1 << 20,
    };
    let device = Arc::new(NullDisk::new());
    let mut allocator = PersistentMemoryMalloc::new(config, device);

    let addr1 = allocator.allocate(100).unwrap();
    let addr2 = allocator.allocate(200).unwrap();

    unsafe {
        let ptr1 = allocator
            .get_mut(addr1)
            .expect("addr1 should be accessible");
        std::ptr::write_bytes(ptr1.as_ptr(), 0xAA, 100);

        let ptr2 = allocator
            .get_mut(addr2)
            .expect("addr2 should be accessible");
        std::ptr::write_bytes(ptr2.as_ptr(), 0xBB, 200);
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    let cp_dir = temp_dir.path().join(token.to_string());
    std::fs::create_dir_all(&cp_dir).unwrap();
    allocator.checkpoint(&cp_dir, token, 1).unwrap();

    let device = Arc::new(NullDisk::new());
    let mut recovered = PersistentMemoryMalloc::new(
        HybridLogConfig {
            page_size: 4096,
            memory_pages: 16,
            mutable_pages: 4,
            segment_size: 1 << 20,
        },
        device,
    );
    recovered.recover(&cp_dir, None).unwrap();

    assert!(
        !recovered.is_on_disk(addr1),
        "Recovered address addr1 should not be marked as on-disk"
    );
    assert!(
        !recovered.is_on_disk(addr2),
        "Recovered address addr2 should not be marked as on-disk"
    );

    assert_eq!(
        recovered.get_head_address(),
        recovered.get_begin_address(),
        "Head address should equal begin address after recovery (all data in memory)"
    );

    unsafe {
        let ptr1 = recovered
            .get(addr1)
            .expect("recovered addr1 should be accessible");
        let data1 = std::slice::from_raw_parts(ptr1.as_ptr(), 100);
        assert!(
            data1.iter().all(|&b| b == 0xAA),
            "Data at addr1 not correctly recovered"
        );

        let ptr2 = recovered
            .get(addr2)
            .expect("recovered addr2 should be accessible");
        let data2 = std::slice::from_raw_parts(ptr2.as_ptr(), 200);
        assert!(
            data2.iter().all(|&b| b == 0xBB),
            "Data at addr2 not correctly recovered"
        );
    }
}
