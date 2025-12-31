//! In-memory hash index implementation for FASTER
//!
//! This module provides the main hash index used by FasterKV to locate records
//! in the hybrid log.

use std::sync::atomic::Ordering;

use crate::address::Address;
use crate::constants::CACHE_LINE_BYTES;
use crate::index::{
    AtomicHashBucketEntry, HashBucket, HashBucketEntry,
    IndexHashBucketEntry, InternalHashTable, KeyHash,
};
use crate::status::Status;
use crate::utility::is_power_of_two;

/// Result of a find operation
#[derive(Debug)]
pub struct FindResult {
    /// The entry found (or INVALID if not found)
    pub entry: IndexHashBucketEntry,
    /// Pointer to the atomic entry in the bucket
    pub atomic_entry: Option<*const AtomicHashBucketEntry>,
}

impl FindResult {
    /// Create a not-found result
    pub fn not_found() -> Self {
        Self {
            entry: IndexHashBucketEntry::INVALID,
            atomic_entry: None,
        }
    }

    /// Check if an entry was found
    pub fn found(&self) -> bool {
        self.atomic_entry.is_some() && !self.entry.is_unused()
    }
}

/// Configuration for the memory hash index
#[derive(Debug, Clone)]
pub struct MemHashIndexConfig {
    /// Size of the hash table (must be power of 2)
    pub table_size: u64,
}

impl MemHashIndexConfig {
    /// Create a new configuration
    pub fn new(table_size: u64) -> Self {
        Self { table_size }
    }
}

impl Default for MemHashIndexConfig {
    fn default() -> Self {
        Self {
            table_size: 1 << 20, // 1M buckets
        }
    }
}

/// In-memory hash index
///
/// Provides fast lookup from key hash to record address in the hybrid log.
/// Uses a two-version scheme to support concurrent index growth.
pub struct MemHashIndex {
    /// Hash tables (two versions for growth)
    tables: [InternalHashTable; 2],
    /// Current version (0 or 1)
    version: u8,
}

impl MemHashIndex {
    /// Create a new uninitialized hash index
    pub fn new() -> Self {
        Self {
            tables: [InternalHashTable::new(), InternalHashTable::new()],
            version: 0,
        }
    }

    /// Initialize the hash index with the given configuration
    pub fn initialize(&mut self, config: &MemHashIndexConfig) -> Status {
        if !is_power_of_two(config.table_size) {
            return Status::Corruption;
        }
        if config.table_size > i32::MAX as u64 {
            return Status::Corruption;
        }

        self.version = 0;
        self.tables[0].initialize(config.table_size, CACHE_LINE_BYTES)
    }

    /// Get the current table size
    #[inline]
    pub fn size(&self) -> u64 {
        self.tables[self.version as usize].size()
    }

    /// Get the new table size (during growth)
    #[inline]
    pub fn new_size(&self) -> u64 {
        self.tables[1 - self.version as usize].size()
    }

    /// Get the current version
    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Find an entry in the hash index
    ///
    /// Returns the entry and a pointer to the atomic entry location.
    pub fn find_entry(&self, hash: KeyHash) -> FindResult {
        let version = self.version as usize;
        let mut bucket = self.tables[version].bucket(hash);
        let tag = hash.tag();

        loop {
            // Search through the bucket
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                
                if entry.is_unused() {
                    continue;
                }
                
                if entry.tag() == tag && !entry.is_tentative() {
                    return FindResult {
                        entry,
                        atomic_entry: Some(&bucket.entries[i] as *const _),
                    };
                }
            }

            // Check overflow bucket
            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                return FindResult::not_found();
            }

            // Move to next bucket in chain
            // Note: In a full implementation, this would dereference the overflow
            // pointer from the overflow bucket allocator. For now, we just return
            // not found since we don't have the allocator here.
            return FindResult::not_found();
        }
    }

    /// Find or create an entry in the hash index
    ///
    /// If the entry doesn't exist, creates a new tentative entry that the caller
    /// should finalize by CAS-ing in the actual address.
    pub fn find_or_create_entry(&self, hash: KeyHash) -> FindResult {
        let version = self.version as usize;
        let tag = hash.tag();

        loop {
            let bucket = self.tables[version].bucket(hash);
            let mut free_entry: Option<*const AtomicHashBucketEntry> = None;

            // Search through the bucket
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                
                if entry.is_unused() {
                    if free_entry.is_none() {
                        free_entry = Some(&bucket.entries[i] as *const _);
                    }
                    continue;
                }
                
                if entry.tag() == tag && !entry.is_tentative() {
                    return FindResult {
                        entry,
                        atomic_entry: Some(&bucket.entries[i] as *const _),
                    };
                }
            }

            // If we found a free slot, try to install a tentative entry
            if let Some(atomic_entry) = free_entry {
                let tentative_entry = IndexHashBucketEntry::new(Address::INVALID, tag, true);
                let expected = HashBucketEntry::INVALID;
                
                // SAFETY: atomic_entry points to valid bucket entry
                let atomic_ref = unsafe { &*atomic_entry };
                
                match atomic_ref.compare_exchange(
                    expected,
                    tentative_entry.to_hash_bucket_entry(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Check for conflicts
                        if self.has_conflicting_entry(hash, bucket, atomic_entry) {
                            // Back off - clear the tentative entry
                            atomic_ref.store(HashBucketEntry::INVALID, Ordering::Release);
                            continue;
                        }

                        // Success - return the non-tentative version
                        let final_entry = IndexHashBucketEntry::new(Address::INVALID, tag, false);
                        atomic_ref.store_index(final_entry, Ordering::Release);
                        
                        return FindResult {
                            entry: final_entry,
                            atomic_entry: Some(atomic_entry),
                        };
                    }
                    Err(_) => {
                        // Someone else got there first, retry
                        continue;
                    }
                }
            }

            // No free slot found - in a full implementation, we would allocate
            // an overflow bucket here. For now, just retry.
            // This would require access to the overflow bucket allocator.
            return FindResult::not_found();
        }
    }

    /// Try to update an entry atomically
    pub fn try_update_entry(
        &self,
        atomic_entry: *const AtomicHashBucketEntry,
        expected: HashBucketEntry,
        new_address: Address,
        tag: u16,
        read_cache: bool,
    ) -> Status {
        let new_entry = if new_address == Address::INVALID {
            HashBucketEntry::INVALID
        } else {
            IndexHashBucketEntry::new_with_read_cache(new_address, tag, false, read_cache)
                .to_hash_bucket_entry()
        };

        // SAFETY: atomic_entry points to valid bucket entry
        let atomic_ref = unsafe { &*atomic_entry };
        
        match atomic_ref.compare_exchange(
            expected,
            new_entry,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Status::Ok,
            Err(_) => Status::Aborted,
        }
    }

    /// Update an entry unconditionally
    pub fn update_entry(
        &self,
        atomic_entry: *const AtomicHashBucketEntry,
        new_address: Address,
        tag: u16,
    ) -> Status {
        let new_entry = IndexHashBucketEntry::new(new_address, tag, false);

        // SAFETY: atomic_entry points to valid bucket entry
        let atomic_ref = unsafe { &*atomic_entry };
        atomic_ref.store_index(new_entry, Ordering::Release);
        
        Status::Ok
    }

    /// Check if there's a conflicting entry with the same tag
    fn has_conflicting_entry(
        &self,
        hash: KeyHash,
        bucket: &HashBucket,
        our_entry: *const AtomicHashBucketEntry,
    ) -> bool {
        let tag = hash.tag();
        
        for i in 0..HashBucket::NUM_ENTRIES {
            let entry_ptr = &bucket.entries[i] as *const _;
            if entry_ptr == our_entry {
                continue;
            }
            
            let entry = bucket.entries[i].load_index(Ordering::Acquire);
            if !entry.is_unused() && entry.tag() == tag {
                return true;
            }
        }
        
        false
    }

    /// Garbage collect entries pointing to addresses before the given address
    pub fn garbage_collect(&self, new_begin_address: Address) -> u64 {
        let version = self.version as usize;
        let table_size = self.tables[version].size();
        let mut cleaned = 0u64;

        for idx in 0..table_size {
            let bucket = self.tables[version].bucket_at(idx);
            
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                
                if entry.is_unused() {
                    continue;
                }
                
                let address = entry.address();
                if address < new_begin_address && address != Address::INVALID {
                    // Try to delete the entry
                    let expected = entry.to_hash_bucket_entry();
                    if bucket.entries[i]
                        .compare_exchange(
                            expected,
                            HashBucketEntry::INVALID,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        cleaned += 1;
                    }
                }
            }
        }
        
        cleaned
    }

    /// Clear all tentative entries (used during recovery)
    pub fn clear_tentative_entries(&self) {
        let version = self.version as usize;
        let table_size = self.tables[version].size();

        for idx in 0..table_size {
            let bucket = self.tables[version].bucket_at(idx);
            
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                
                if entry.is_tentative() {
                    bucket.entries[i].store(HashBucketEntry::INVALID, Ordering::Release);
                }
            }
        }
    }

    /// Dump distribution statistics
    pub fn dump_distribution(&self) -> IndexStats {
        let version = self.version as usize;
        let table_size = self.tables[version].size();
        
        let mut total_entries = 0u64;
        let mut used_entries = 0u64;
        let mut buckets_with_entries = 0u64;

        for idx in 0..table_size {
            let bucket = self.tables[version].bucket_at(idx);
            let mut bucket_used = 0;
            
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Relaxed);
                total_entries += 1;
                if !entry.is_unused() {
                    used_entries += 1;
                    bucket_used += 1;
                }
            }
            
            if bucket_used > 0 {
                buckets_with_entries += 1;
            }
        }

        IndexStats {
            table_size,
            total_entries,
            used_entries,
            buckets_with_entries,
            load_factor: used_entries as f64 / total_entries as f64,
        }
    }
}

impl Default for MemHashIndex {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: MemHashIndex uses atomic operations for all concurrent access
unsafe impl Send for MemHashIndex {}
unsafe impl Sync for MemHashIndex {}

/// Statistics about the hash index
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Total table size
    pub table_size: u64,
    /// Total entry slots
    pub total_entries: u64,
    /// Number of used entries
    pub used_entries: u64,
    /// Number of buckets with at least one entry
    pub buckets_with_entries: u64,
    /// Load factor (used/total)
    pub load_factor: f64,
}

impl std::fmt::Display for IndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Index Statistics:")?;
        writeln!(f, "  Table size: {}", self.table_size)?;
        writeln!(f, "  Total entries: {}", self.total_entries)?;
        writeln!(f, "  Used entries: {}", self.used_entries)?;
        writeln!(f, "  Buckets with entries: {}", self.buckets_with_entries)?;
        writeln!(f, "  Load factor: {:.2}%", self.load_factor * 100.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

