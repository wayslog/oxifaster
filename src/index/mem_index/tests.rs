use super::*;

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::address::Address;
use crate::index::grow::GrowConfig;
use crate::index::{HashBucket, HashBucketOverflowEntry, KeyHash};
use crate::status::Status;

#[test]
fn test_mem_hash_index_initialize() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024);

    let result = index.initialize(&config);
    assert_eq!(result, Status::Ok);
    assert_eq!(index.size(), 1024);
}

#[test]
fn test_find_entry_not_found() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024);
    index.initialize(&config);

    let hash = KeyHash::new(12345);
    let result = index.find_entry(hash);

    assert!(!result.found());
}

#[test]
fn test_find_or_create_entry() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024);
    index.initialize(&config);

    let hash = KeyHash::new(12345);

    // First call should create
    let result = index.find_or_create_entry(hash);
    assert!(result.atomic_entry.is_some());

    // Update the entry
    if let Some(atomic_entry) = result.atomic_entry {
        let new_address = Address::new(1, 100);
        let status = index.update_entry(atomic_entry, new_address, hash.tag());
        assert_eq!(status, Status::Ok);
    }

    // Second call should find
    let result2 = index.find_entry(hash);
    assert!(result2.found());
    assert_eq!(result2.entry.address(), Address::new(1, 100));
}

#[test]
fn test_try_update_entry() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024);
    index.initialize(&config);

    let hash = KeyHash::new(54321);
    let result = index.find_or_create_entry(hash);

    if let Some(atomic_entry) = result.atomic_entry {
        // First update should succeed
        let status = index.try_update_entry(
            atomic_entry,
            result.entry.to_hash_bucket_entry(),
            Address::new(2, 200),
            hash.tag(),
            false,
        );
        assert_eq!(status, Status::Ok);

        // Second update with wrong expected should fail
        let status2 = index.try_update_entry(
            atomic_entry,
            result.entry.to_hash_bucket_entry(), // Old expected value
            Address::new(3, 300),
            hash.tag(),
            false,
        );
        assert_eq!(status2, Status::Aborted);
    }
}

#[test]
fn test_garbage_collect() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024);
    index.initialize(&config);

    // Create and populate some entries
    for i in 0..10u64 {
        let hash = KeyHash::new(i * 1000);
        let result = index.find_or_create_entry(hash);
        if let Some(atomic_entry) = result.atomic_entry {
            let addr = Address::new(0, (i * 100) as u32);
            index.update_entry(atomic_entry, addr, hash.tag());
        }
    }

    // GC with threshold at offset 500
    let threshold = Address::new(0, 500);
    let cleaned = index.garbage_collect(threshold);

    // Should have cleaned entries with offset < 500
    assert!(cleaned > 0);
}

#[test]
fn test_dump_distribution() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1024);
    index.initialize(&config);

    let stats = index.dump_distribution();
    assert_eq!(stats.table_size, 1024);
    assert_eq!(stats.used_entries, 0);
}

// ============ Checkpoint and Recovery Tests ============

#[test]
fn test_checkpoint_empty_index() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(256);
    index.initialize(&config);

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();

    let metadata = index.checkpoint(temp_dir.path(), token).unwrap();

    assert_eq!(metadata.token, token);
    assert_eq!(metadata.table_size, 256);
    assert_eq!(metadata.num_entries, 0);
}

#[test]
fn test_checkpoint_and_recover() {
    // Create and populate an index
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(256);
    index.initialize(&config);

    // Add some entries
    let test_hashes: Vec<KeyHash> = (0..10).map(|i| KeyHash::new(i * 12345)).collect();
    for (i, hash) in test_hashes.iter().enumerate() {
        let result = index.find_or_create_entry(*hash);
        if let Some(atomic_entry) = result.atomic_entry {
            let addr = Address::new((i / 5) as u32, ((i % 5) * 100) as u32);
            index.update_entry(atomic_entry, addr, hash.tag());
        }
    }

    // Checkpoint
    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    let metadata = index.checkpoint(temp_dir.path(), token).unwrap();

    assert!(metadata.num_entries > 0);

    // Create a new index and recover
    let mut recovered_index = MemHashIndex::new();
    recovered_index
        .recover(temp_dir.path(), Some(&metadata))
        .unwrap();

    // Verify all entries are present
    for hash in &test_hashes {
        let original = index.find_entry(*hash);
        let recovered = recovered_index.find_entry(*hash);

        assert_eq!(original.found(), recovered.found());
        if original.found() {
            assert_eq!(original.entry.address(), recovered.entry.address());
            assert_eq!(original.entry.tag(), recovered.entry.tag());
        }
    }
}

#[test]
fn test_checkpoint_header_matches_overflow_snapshot_under_concurrent_allocations() {
    use std::fs::File;
    use std::io::Read;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    // Use a large table to make the main-bucket serialization take long enough
    // for concurrent overflow allocations to overlap deterministically.
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1 << 15);
    index.initialize(&config);

    let index = Arc::new(index);

    // Pre-fill the target bucket with a few entries to reduce noise.
    for i in 0u64..7 {
        let hash = KeyHash::new((i + 1) << 48);
        let result = index.find_or_create_entry(hash);
        if let Some(atomic_entry) = result.atomic_entry {
            index.update_entry(atomic_entry, Address::new(0, (i + 1) as u32), hash.tag());
        }
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    let index_dat_path = temp_dir.path().join("index.dat");
    let barrier = Arc::new(Barrier::new(2));

    let index_for_checkpoint = index.clone();
    let checkpoint_dir = temp_dir.path().to_path_buf();
    let barrier_for_checkpoint = barrier.clone();
    let checkpoint_handle = thread::spawn(move || {
        barrier_for_checkpoint.wait();
        index_for_checkpoint
            .checkpoint(&checkpoint_dir, token)
            .unwrap();
    });

    let index_for_alloc = index.clone();
    let barrier_for_alloc = barrier.clone();
    let index_dat_path_for_alloc = index_dat_path.clone();
    let alloc_handle = thread::spawn(move || {
        barrier_for_alloc.wait();

        // Wait until the checkpoint thread has written past the header.
        for _ in 0..2000 {
            if let Ok(meta) = std::fs::metadata(&index_dat_path_for_alloc) {
                if meta.len() > 16 {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(1));
        }

        // Drive overflow bucket allocations while checkpoint is running.
        for i in 7u64..2000 {
            let tag = i + 1;
            let hash = KeyHash::new(tag << 48);
            let result = index_for_alloc.find_or_create_entry(hash);
            if let Some(atomic_entry) = result.atomic_entry {
                index_for_alloc.update_entry(atomic_entry, Address::new(1, tag as u32), hash.tag());
            }
        }
    });

    checkpoint_handle.join().unwrap();
    alloc_handle.join().unwrap();

    let file_len = std::fs::metadata(&index_dat_path).unwrap().len();

    let mut f = File::open(&index_dat_path).unwrap();
    let mut header = [0u8; 16];
    f.read_exact(&mut header).unwrap();
    let table_size = u64::from_le_bytes(header[0..8].try_into().unwrap());
    let overflow_count = u64::from_le_bytes(header[8..16].try_into().unwrap());

    let bucket_bytes = (HashBucket::NUM_ENTRIES as u64 + 1) * 8;
    let expected_len = 16 + bucket_bytes * (table_size + overflow_count);
    assert_eq!(file_len, expected_len);
}

#[test]
fn test_recover_without_preloaded_metadata() {
    // Create and populate an index
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(128);
    index.initialize(&config);

    let hash = KeyHash::new(99999);
    let result = index.find_or_create_entry(hash);
    if let Some(atomic_entry) = result.atomic_entry {
        index.update_entry(atomic_entry, Address::new(5, 500), hash.tag());
    }

    // Checkpoint
    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    index.checkpoint(temp_dir.path(), token).unwrap();

    // Recover without providing metadata (should load from file)
    let mut recovered_index = MemHashIndex::new();
    recovered_index.recover(temp_dir.path(), None).unwrap();

    // Verify
    let recovered_result = recovered_index.find_entry(hash);
    assert!(recovered_result.found());
    assert_eq!(recovered_result.entry.address(), Address::new(5, 500));
}

#[test]
fn test_verify_recovery() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(128);
    index.initialize(&config);

    let hash = KeyHash::new(77777);
    let result = index.find_or_create_entry(hash);
    if let Some(atomic_entry) = result.atomic_entry {
        index.update_entry(atomic_entry, Address::new(3, 300), hash.tag());
    }

    // Checkpoint and recover
    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    index.checkpoint(temp_dir.path(), token).unwrap();

    let mut recovered_index = MemHashIndex::new();
    recovered_index.recover(temp_dir.path(), None).unwrap();

    // Verify should pass
    recovered_index.verify_recovery().unwrap();
}

#[test]
fn test_checkpoint_preserves_stats() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(512);
    index.initialize(&config);

    // Add entries
    for i in 0..20u64 {
        let hash = KeyHash::new(i * 7919); // Prime multiplier for spread
        let result = index.find_or_create_entry(hash);
        if let Some(atomic_entry) = result.atomic_entry {
            index.update_entry(atomic_entry, Address::new(i as u32, 0), hash.tag());
        }
    }

    let original_stats = index.dump_distribution();

    // Checkpoint and recover
    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    index.checkpoint(temp_dir.path(), token).unwrap();

    let mut recovered_index = MemHashIndex::new();
    recovered_index.recover(temp_dir.path(), None).unwrap();

    let recovered_stats = recovered_index.dump_distribution();

    assert_eq!(original_stats.table_size, recovered_stats.table_size);
    assert_eq!(original_stats.used_entries, recovered_stats.used_entries);
}

// ============ Growth Tests ============

#[test]
fn test_grow_config() {
    let config = GrowConfig::new()
        .with_max_load_factor(0.8)
        .with_growth_factor(4)
        .with_auto_grow(true);

    assert_eq!(config.growth_factor, 4);
    assert!(config.auto_grow);
}

#[test]
fn test_grow_empty_index() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(256);
    index.initialize(&config);
    index.set_grow_config(GrowConfig::new().with_growth_factor(2));

    // Grow with a rehash callback that returns None (no entries to rehash)
    let result = index.grow_with_rehash(|_addr| None);
    assert!(result.success);
    assert_eq!(result.old_size, 256);
    assert_eq!(result.new_size, 512);
    assert_eq!(index.size(), 512);
    assert_eq!(index.version(), 1);
}

#[test]
fn test_grow_with_entries() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::RwLock;

    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(256);
    index.initialize(&config);
    index.set_grow_config(GrowConfig::new().with_growth_factor(2));

    // Store address -> hash mapping for rehash callback
    let hash_map: Arc<RwLock<HashMap<u64, KeyHash>>> = Arc::new(RwLock::new(HashMap::new()));

    // Add some entries
    // Note: Start from i=1 and use non-zero addresses to avoid the edge case
    // where address=0 and tag=0 creates an entry that is_unused() returns true for.
    let test_hashes: Vec<KeyHash> = (1..=20).map(|i| KeyHash::new(i * 7919)).collect();
    for (i, hash) in test_hashes.iter().enumerate() {
        let result = index.find_or_create_entry(*hash);
        if let Some(atomic_entry) = result.atomic_entry {
            // Use non-zero page to avoid Address(0) which would create is_unused() entry
            let addr = Address::new(1, ((i + 1) * 100) as u32);
            index.update_entry(atomic_entry, addr, hash.tag());
            // Store the hash for later rehashing
            hash_map.write().unwrap().insert(addr.control(), *hash);
        }
    }

    let original_stats = index.dump_distribution();
    assert!(original_stats.used_entries > 0);

    // Grow the index with proper rehash callback
    let hash_map_ref = hash_map.clone();
    let result =
        index.grow_with_rehash(|addr| hash_map_ref.read().unwrap().get(&addr.control()).copied());

    assert_eq!(result.old_size, 256);
    assert_eq!(result.new_size, 512);
    assert!(result.entries_migrated > 0, "Should have migrated entries");
    assert_eq!(
        result.rehash_failures, 0,
        "All entries should be rehashed successfully"
    );

    // Verify all entries can still be found after grow
    for hash in test_hashes.iter() {
        let find_result = index.find_entry(*hash);
        assert!(
            find_result.found(),
            "Entry should still be findable after grow"
        );
    }

    let new_stats = index.dump_distribution();
    assert!(new_stats.used_entries > 0);
    assert_eq!(new_stats.table_size, 512);
}

#[test]
fn test_should_grow() {
    let grow_config = GrowConfig::new()
        .with_max_load_factor(0.01) // Very low to trigger grow
        .with_auto_grow(true);

    let mut index = MemHashIndex::with_grow_config(grow_config);
    let config = MemHashIndexConfig::new(256);
    index.initialize(&config);

    // Initially should not grow (no entries)
    assert!(!index.should_grow());

    // Add many entries to increase load factor
    for i in 0..100u64 {
        let hash = KeyHash::new(i * 12345);
        let result = index.find_or_create_entry(hash);
        if let Some(atomic_entry) = result.atomic_entry {
            index.update_entry(atomic_entry, Address::new(i as u32, 0), hash.tag());
        }
    }

    // Now should grow
    assert!(index.should_grow());
}

#[test]
fn test_grow_in_progress_prevention() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(256);
    index.initialize(&config);

    // Start grow
    assert!(index.start_grow().is_ok());
    assert!(index.is_grow_in_progress());

    // Second start should fail
    assert!(index.start_grow().is_err());

    // Complete the grow
    let result = index.grow_state_mut();
    while result.get_next_chunk().is_some() {
        result.complete_chunk();
    }
    let _ = index.complete_grow();

    // Now should be able to start again
    assert!(!index.is_grow_in_progress());
}

// ============ Overflow Bucket Tests ============

#[test]
fn test_overflow_buckets_insert_and_find() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1); // All hashes map to a single bucket.
    index.initialize(&config);

    let num = 32u64;
    let hashes: Vec<KeyHash> = (1..=num).map(|i| KeyHash::new(i << 48)).collect();

    for (i, hash) in hashes.iter().enumerate() {
        let result = index.find_or_create_entry(*hash);
        assert!(result.atomic_entry.is_some());
        let atomic_entry = result.atomic_entry.unwrap();
        let addr = Address::new(1, ((i as u32) + 1) * 64);
        index.update_entry(atomic_entry, addr, hash.tag());
    }

    for (i, hash) in hashes.iter().enumerate() {
        let found = index.find_entry(*hash);
        assert!(found.found());
        let expected = Address::new(1, ((i as u32) + 1) * 64);
        assert_eq!(found.entry.address(), expected);
    }

    let stats = index.dump_distribution();
    assert_eq!(stats.used_entries, num);
    assert!(stats.total_entries >= num);
}

#[test]
fn test_overflow_link_summary_updates_on_reused_overflow_slot() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1); // Force all keys into one bucket chain.
    index.initialize(&config);

    let target_low4 = 5u16;
    let mut hashes = Vec::new();
    let mut hash_by_tag = HashMap::new();
    for tag in 1u16..(1u16 << KeyHash::TAG_BITS) {
        if (tag & 0xF) == target_low4 {
            continue;
        }
        let hash = KeyHash::new((tag as u64) << 48);
        hashes.push(hash);
        hash_by_tag.insert(tag, hash);
        if hashes.len() == 40 {
            break;
        }
    }

    for (i, hash) in hashes.iter().enumerate() {
        let result = index.find_or_create_entry(*hash);
        let atomic_entry = result.atomic_entry.unwrap();
        index.update_entry(
            atomic_entry,
            Address::new(7, (i as u32 + 1) * 32),
            hash.tag(),
        );
    }

    let version = index.version() as usize;
    let base_bucket = index.tables[version].bucket_at(0);
    let first_overflow = base_bucket.overflow_entry.load(Ordering::Acquire);
    assert!(!first_overflow.is_unused());

    let first_overflow_ptr = index.overflow_pools[version]
        .bucket_ptr(first_overflow.address())
        .unwrap();
    let first_overflow_bucket = unsafe { &*first_overflow_ptr };

    let mut tag_to_delete = None;
    for slot in &first_overflow_bucket.entries {
        let entry = slot.load_index(Ordering::Acquire);
        if !entry.is_unused() {
            tag_to_delete = Some(entry.tag());
            break;
        }
    }
    let tag_to_delete = tag_to_delete.expect("first overflow bucket should not be empty");
    let hash_to_delete = hash_by_tag[&tag_to_delete];

    let found = index.find_entry(hash_to_delete);
    assert!(found.found());
    let delete_status = index.try_update_entry(
        found.atomic_entry.unwrap(),
        found.entry.to_hash_bucket_entry(),
        Address::INVALID,
        hash_to_delete.tag(),
        false,
    );
    assert_eq!(delete_status, Status::Ok);

    let target_tag = 0x0155u16; // low 4 bits are 0x5.
    assert_eq!(target_tag & 0xF, target_low4);
    assert!(!hash_by_tag.contains_key(&target_tag));
    let target_hash = KeyHash::new((target_tag as u64) << 48);
    let target_bit = HashBucketOverflowEntry::tag_summary_bit(target_tag);

    let summary_before = base_bucket
        .overflow_entry
        .load(Ordering::Acquire)
        .tag_summary();
    assert_eq!(summary_before & target_bit, 0);

    let target_entry = index.find_or_create_entry(target_hash);
    let target_slot = target_entry.atomic_entry.unwrap();
    index.update_entry(target_slot, Address::new(8, 4096), target_hash.tag());

    let summary_after = base_bucket
        .overflow_entry
        .load(Ordering::Acquire)
        .tag_summary();
    assert_ne!(summary_after & target_bit, 0);

    let target_found = index.find_entry(target_hash);
    assert!(target_found.found());
    assert_eq!(target_found.entry.address(), Address::new(8, 4096));
}

#[test]
fn test_overflow_buckets_checkpoint_and_recover() {
    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1);
    index.initialize(&config);

    let num = 20u64;
    let hashes: Vec<KeyHash> = (1..=num).map(|i| KeyHash::new(i << 48)).collect();

    for (i, hash) in hashes.iter().enumerate() {
        let result = index.find_or_create_entry(*hash);
        let atomic_entry = result.atomic_entry.unwrap();
        let addr = Address::new(2, ((i as u32) + 1) * 128);
        index.update_entry(atomic_entry, addr, hash.tag());
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let token = uuid::Uuid::new_v4();
    let metadata = index.checkpoint(temp_dir.path(), token).unwrap();

    let mut recovered = MemHashIndex::new();
    recovered.recover(temp_dir.path(), Some(&metadata)).unwrap();

    for (i, hash) in hashes.iter().enumerate() {
        let found = recovered.find_entry(*hash);
        assert!(found.found());
        let expected = Address::new(2, ((i as u32) + 1) * 128);
        assert_eq!(found.entry.address(), expected);
    }
}

#[test]
fn test_grow_with_overflow_entries_preserves_all_keys() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::RwLock;

    let mut index = MemHashIndex::new();
    let config = MemHashIndexConfig::new(1);
    index.initialize(&config);
    index.set_grow_config(GrowConfig::new().with_growth_factor(2));

    let hash_map: Arc<RwLock<HashMap<u64, KeyHash>>> = Arc::new(RwLock::new(HashMap::new()));

    let num = 32u64;
    let hashes: Vec<KeyHash> = (1..=num).map(|i| KeyHash::new(i << 48)).collect();
    for (i, hash) in hashes.iter().enumerate() {
        let result = index.find_or_create_entry(*hash);
        let atomic_entry = result.atomic_entry.unwrap();
        let addr = Address::new(3, ((i as u32) + 1) * 64);
        index.update_entry(atomic_entry, addr, hash.tag());
        hash_map.write().unwrap().insert(addr.control(), *hash);
    }

    let hm = hash_map.clone();
    let result = index.grow_with_rehash(|addr| hm.read().unwrap().get(&addr.control()).copied());
    assert!(result.success);

    for hash in hashes {
        assert!(index.find_entry(hash).found());
    }
}
