use std::sync::atomic::Ordering;

use crate::address::Address;
use crate::constants::CACHE_LINE_BYTES;
use crate::index::{
    AtomicHashBucketEntry, HashBucket, HashBucketEntry, HashBucketOverflowEntry,
    IndexHashBucketEntry, KeyHash,
};
use crate::status::Status;
use crate::utility::is_power_of_two;

use super::{FindResult, IndexStats, MemHashIndex, MemHashIndexConfig};

impl MemHashIndex {
    /// Create a new uninitialized hash index
    pub fn new() -> Self {
        Self {
            tables: [
                crate::index::InternalHashTable::new(),
                crate::index::InternalHashTable::new(),
            ],
            overflow_pools: [
                super::overflow::OverflowBucketPool::new(),
                super::overflow::OverflowBucketPool::new(),
            ],
            version: std::sync::atomic::AtomicU8::new(0),
            grow_state: crate::index::grow::GrowState::new(),
            grow_config: crate::index::grow::GrowConfig::default(),
            grow_in_progress: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create a new hash index with growth configuration
    pub fn with_grow_config(grow_config: crate::index::grow::GrowConfig) -> Self {
        Self {
            tables: [
                crate::index::InternalHashTable::new(),
                crate::index::InternalHashTable::new(),
            ],
            overflow_pools: [
                super::overflow::OverflowBucketPool::new(),
                super::overflow::OverflowBucketPool::new(),
            ],
            version: std::sync::atomic::AtomicU8::new(0),
            grow_state: crate::index::grow::GrowState::new(),
            grow_config,
            grow_in_progress: std::sync::atomic::AtomicBool::new(false),
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

        self.version.store(0, Ordering::Release);
        self.overflow_pools[0].clear();
        self.overflow_pools[1].clear();
        self.tables[0].initialize(config.table_size, CACHE_LINE_BYTES)
    }

    /// Get the current table size
    #[inline]
    pub fn size(&self) -> u64 {
        let v = self.version.load(Ordering::Acquire);
        self.tables[v as usize].size()
    }

    /// Get the new table size (during growth)
    #[inline]
    pub fn new_size(&self) -> u64 {
        let v = self.version.load(Ordering::Acquire);
        self.tables[1 - v as usize].size()
    }

    /// Get the current version
    #[inline]
    pub fn version(&self) -> u8 {
        self.version.load(Ordering::Acquire)
    }

    /// Get the grow configuration
    pub fn grow_config(&self) -> &crate::index::grow::GrowConfig {
        &self.grow_config
    }

    /// Set the grow configuration
    pub fn set_grow_config(&mut self, config: crate::index::grow::GrowConfig) {
        self.grow_config = config;
    }

    /// Check if grow is in progress
    pub fn is_grow_in_progress(&self) -> bool {
        self.grow_in_progress.load(Ordering::Acquire)
    }

    /// Get the current load factor
    pub fn load_factor(&self) -> f64 {
        let stats = self.dump_distribution();
        stats.load_factor
    }

    /// Check if growth should be triggered based on configuration
    pub fn should_grow(&self) -> bool {
        if self.grow_in_progress.load(Ordering::Acquire) {
            return false;
        }
        self.grow_config.should_grow(self.load_factor())
    }

    /// Get the grow state (for external progress tracking)
    pub fn grow_state(&self) -> &crate::index::grow::GrowState {
        &self.grow_state
    }

    /// Get a mutable reference to grow state
    pub fn grow_state_mut(&mut self) -> &mut crate::index::grow::GrowState {
        &mut self.grow_state
    }

    /// Find an entry in the hash index
    ///
    /// Returns the entry and a pointer to the atomic entry location.
    pub fn find_entry(&self, hash: KeyHash) -> FindResult {
        let version = self.version.load(Ordering::Acquire) as usize;
        let bucket = self.tables[version].bucket(hash);
        let tag = hash.tag();

        self.find_entry_in_bucket_chain(version, bucket, tag)
    }

    /// Find or create an entry in the hash index
    ///
    /// If the entry doesn't exist, creates a new tentative entry that the caller
    /// should finalize by CAS-ing in the actual address.
    pub fn find_or_create_entry(&self, hash: KeyHash) -> FindResult {
        let version = self.version.load(Ordering::Acquire) as usize;
        let tag = hash.tag();

        loop {
            let bucket = self.tables[version].bucket(hash);

            // 先在整个 bucket 链中查找（包括 overflow 链）
            if let Some(found) = self.find_existing_in_bucket_chain(version, bucket, tag) {
                return found;
            }

            // 查找可用的空位（包括 overflow 链）
            let mut free_entry = self.find_free_entry_in_bucket_chain(version, bucket);

            // 如果整个链都满了，则追加一个新的 overflow bucket，并使用它的第一个 entry。
            if free_entry.is_none() {
                free_entry = self.append_overflow_bucket_and_get_free_entry(version, bucket);
            }

            // 尝试安装 tentative entry
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
                        if self.has_conflicting_entry_in_chain(tag, version, bucket, atomic_entry) {
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
                    Err(_) => continue,
                }
            }
        }
    }

    /// Try to update an entry atomically
    pub(crate) fn try_update_entry(
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

        match atomic_ref.compare_exchange(expected, new_entry, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Status::Ok,
            Err(_) => Status::Aborted,
        }
    }

    /// Update an entry unconditionally
    pub(crate) fn update_entry(
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
    fn has_conflicting_entry_in_chain(
        &self,
        tag: u16,
        version: usize,
        base_bucket: &HashBucket,
        our_entry: *const AtomicHashBucketEntry,
    ) -> bool {
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

        loop {
            // SAFETY: bucket_ptr 指向 valid HashBucket，且 HashBucket 内部字段都是原子类型，可并发只读/写。
            let bucket = unsafe { &*bucket_ptr };
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry_ptr = &bucket.entries[i] as *const _;
                if entry_ptr == our_entry {
                    continue;
                }

                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                if !entry.is_unused() && !entry.is_tentative() && entry.tag() == tag {
                    return true;
                }
            }

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                return false;
            }

            let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
            match next_ptr {
                Some(p) => bucket_ptr = p,
                None => return false,
            }
        }
    }

    fn find_entry_in_bucket_chain(
        &self,
        version: usize,
        base_bucket: &HashBucket,
        tag: u16,
    ) -> FindResult {
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;
        loop {
            // SAFETY: bucket_ptr 指向有效 bucket；entries/overflow_entry 都是原子字段，读是安全的。
            let bucket = unsafe { &*bucket_ptr };

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

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                return FindResult::not_found();
            }
            let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
            match next_ptr {
                Some(p) => bucket_ptr = p,
                None => return FindResult::not_found(),
            }
        }
    }

    fn find_existing_in_bucket_chain(
        &self,
        version: usize,
        base_bucket: &HashBucket,
        tag: u16,
    ) -> Option<FindResult> {
        let found = self.find_entry_in_bucket_chain(version, base_bucket, tag);
        if found.found() {
            Some(found)
        } else {
            None
        }
    }

    fn find_free_entry_in_bucket_chain(
        &self,
        version: usize,
        base_bucket: &HashBucket,
    ) -> Option<*const AtomicHashBucketEntry> {
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

        loop {
            // SAFETY: 同 find_entry_in_bucket_chain
            let bucket = unsafe { &*bucket_ptr };

            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                if entry.is_unused() {
                    return Some(&bucket.entries[i] as *const _);
                }
            }

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                return None;
            }
            let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
            match next_ptr {
                Some(p) => bucket_ptr = p,
                None => return None,
            }
        }
    }

    fn append_overflow_bucket_and_get_free_entry(
        &self,
        version: usize,
        base_bucket: &HashBucket,
    ) -> Option<*const AtomicHashBucketEntry> {
        // 找到链尾（overflow_entry 未使用的 bucket）并追加一个新的 overflow bucket。
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

        loop {
            // SAFETY: 同上
            let bucket = unsafe { &*bucket_ptr };
            let overflow = bucket.overflow_entry.load(Ordering::Acquire);

            if overflow.is_unused() {
                // 分配新 bucket，并 CAS 安装到 overflow_entry。
                let new_addr = self.overflow_pools[version].allocate();
                let new_overflow = HashBucketOverflowEntry::new(new_addr);
                let expected = HashBucketOverflowEntry::INVALID;

                match bucket.overflow_entry.compare_exchange(
                    expected,
                    new_overflow,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let new_ptr = self.overflow_pools[version].bucket_ptr(new_addr)?;
                        // SAFETY: 新 bucket 刚创建，entries 均为 INVALID
                        let new_bucket = unsafe { &*new_ptr };
                        return Some(&new_bucket.entries[0] as *const _);
                    }
                    Err(actual) => {
                        // CAS 失败：说明有其他线程先一步安装了 overflow；归还本次新分配的 bucket 以便复用。
                        self.overflow_pools[version].deallocate(new_addr);

                        // 有其他线程抢先安装了 overflow，继续沿链前进。
                        if actual.is_unused() {
                            continue;
                        }
                        let next_ptr = self.overflow_pools[version].bucket_ptr(actual.address());
                        match next_ptr {
                            Some(p) => bucket_ptr = p,
                            None => return None,
                        }
                    }
                }
            } else {
                // 继续遍历
                let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => return None,
                }
            }
        }
    }

    /// Try to update the address of an entry atomically by hash
    ///
    /// This is useful during compaction when we need to update the index
    /// to point to a record's new location.
    pub fn try_update_address(
        &self,
        hash: KeyHash,
        old_address: Address,
        new_address: Address,
    ) -> Status {
        let result = self.find_entry(hash);

        if !result.found() {
            return Status::NotFound;
        }

        if result.entry.address() != old_address {
            return Status::NotFound;
        }

        if let Some(atomic_entry) = result.atomic_entry {
            let expected = result.entry.to_hash_bucket_entry();
            let new_entry = IndexHashBucketEntry::new(new_address, hash.tag(), false);

            // SAFETY: atomic_entry points to a valid bucket entry
            let atomic_ref = unsafe { &*atomic_entry };

            match atomic_ref.compare_exchange(
                expected,
                new_entry.to_hash_bucket_entry(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => Status::Ok,
                Err(_) => Status::Aborted,
            }
        } else {
            Status::NotFound
        }
    }

    /// Garbage collect entries pointing to addresses before the given address
    pub fn garbage_collect(&self, new_begin_address: Address) -> u64 {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();
        let mut cleaned = 0u64;

        for idx in 0..table_size {
            let base_bucket = self.tables[version].bucket_at(idx);
            let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

            loop {
                // SAFETY: bucket_ptr 指向有效 bucket，且 entries/overflow 都是原子字段。
                let bucket = unsafe { &*bucket_ptr };

                for i in 0..HashBucket::NUM_ENTRIES {
                    let entry = bucket.entries[i].load_index(Ordering::Acquire);

                    if entry.is_unused() {
                        continue;
                    }

                    let address = entry.address();
                    if address < new_begin_address && address != Address::INVALID {
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

                let overflow = bucket.overflow_entry.load(Ordering::Acquire);
                if overflow.is_unused() {
                    break;
                }
                let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => break,
                }
            }
        }

        cleaned
    }

    /// Clear all tentative entries (used during recovery)
    pub fn clear_tentative_entries(&self) {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        for idx in 0..table_size {
            let base_bucket = self.tables[version].bucket_at(idx);
            let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

            loop {
                // SAFETY: bucket_ptr 指向有效 bucket。
                let bucket = unsafe { &*bucket_ptr };

                for i in 0..HashBucket::NUM_ENTRIES {
                    let entry = bucket.entries[i].load_index(Ordering::Acquire);

                    if entry.is_tentative() {
                        bucket.entries[i].store(HashBucketEntry::INVALID, Ordering::Release);
                    }
                }

                let overflow = bucket.overflow_entry.load(Ordering::Acquire);
                if overflow.is_unused() {
                    break;
                }
                let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => break,
                }
            }
        }
    }

    /// Dump distribution statistics
    pub fn dump_distribution(&self) -> IndexStats {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        let mut total_entries = 0u64;
        let mut used_entries = 0u64;
        let mut buckets_with_entries = 0u64;

        for idx in 0..table_size {
            let base_bucket = self.tables[version].bucket_at(idx);
            let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

            loop {
                // SAFETY: bucket_ptr 指向有效 bucket。
                let bucket = unsafe { &*bucket_ptr };
                let mut bucket_used = 0u64;

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

                let overflow = bucket.overflow_entry.load(Ordering::Relaxed);
                if overflow.is_unused() {
                    break;
                }
                let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => break,
                }
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
