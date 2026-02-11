use std::sync::atomic::Ordering;
#[cfg(feature = "index-profile")]
use std::time::Instant;

use crate::address::Address;
use crate::constants::CACHE_LINE_BYTES;
use crate::index::{
    AtomicHashBucketEntry, HashBucket, HashBucketEntry, HashBucketOverflowEntry,
    IndexHashBucketEntry, KeyHash,
};
use crate::status::Status;
use crate::utility::is_power_of_two;

use super::{FindResult, IndexStats, MemHashIndex, MemHashIndexConfig};

struct InsertScanResult {
    found: Option<FindResult>,
    free_entry: Option<*const AtomicHashBucketEntry>,
    secondary_free_entry: Option<*const AtomicHashBucketEntry>,
    tail: *const HashBucket,
    retry: bool,
}

impl MemHashIndex {
    #[inline]
    fn preferred_entry_index(tag: u16) -> usize {
        (tag as usize) % HashBucket::NUM_ENTRIES
    }

    #[inline]
    fn probe_stride(tag: u16) -> usize {
        // Keep probe 0 on the preferred lane, then rotate remaining probes by tag-derived stride
        // to reduce slot herding under heavy contention.
        ((tag as usize / HashBucket::NUM_ENTRIES) % (HashBucket::NUM_ENTRIES - 1)) + 1
    }

    #[inline]
    fn probe_entry_index(preferred_idx: usize, probe: usize, stride: usize) -> usize {
        debug_assert!(preferred_idx < HashBucket::NUM_ENTRIES);
        debug_assert!(probe < HashBucket::NUM_ENTRIES);
        debug_assert!((1..HashBucket::NUM_ENTRIES).contains(&stride));
        (preferred_idx + probe * stride) % HashBucket::NUM_ENTRIES
    }

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

        #[cfg(feature = "index-profile")]
        crate::index::profile::INDEX_INSERT_PROFILE.record_find_or_create_call();

        #[cfg(feature = "index-profile")]
        let mut first_try = true;
        loop {
            #[cfg(feature = "index-profile")]
            {
                if !first_try {
                    crate::index::profile::INDEX_INSERT_PROFILE.record_retry();
                }
                first_try = false;
            }

            let bucket = self.tables[version].bucket(hash);

            // Single traversal: find an existing matching entry or record a free slot for insert.
            #[cfg(feature = "index-profile")]
            let scan_start = Instant::now();
            let scan = self.scan_bucket_chain_for_insert(version, bucket, tag);
            #[cfg(feature = "index-profile")]
            crate::index::profile::INDEX_INSERT_PROFILE.record_scan(scan_start.elapsed());
            if let Some(found) = scan.found {
                return found;
            }
            if scan.retry {
                continue;
            }

            // Prefer a free slot found during the scan; only append an overflow bucket if the
            // chain is full.
            let mut free_entry = scan.free_entry;
            let secondary_free_entry = scan.secondary_free_entry;
            if free_entry.is_none() {
                #[cfg(feature = "index-profile")]
                let overflow_start = Instant::now();
                free_entry =
                    self.append_overflow_bucket_at_tail_and_get_free_entry(version, scan.tail, tag);
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE
                    .record_append_overflow(overflow_start.elapsed());
            }

            // Try to install a tentative entry.
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
                        #[cfg(feature = "index-profile")]
                        crate::index::profile::INDEX_INSERT_PROFILE.record_cas_attempt(true);

                        // Success - return the non-tentative version
                        let final_entry = IndexHashBucketEntry::new(Address::INVALID, tag, false);
                        atomic_ref.store_index(final_entry, Ordering::Release);

                        return FindResult {
                            entry: final_entry,
                            atomic_entry: Some(atomic_entry),
                        };
                    }
                    Err(_) => {
                        #[cfg(feature = "index-profile")]
                        crate::index::profile::INDEX_INSERT_PROFILE.record_cas_attempt(false);

                        if let Some(secondary) = secondary_free_entry {
                            // SAFETY: `secondary` points to another valid entry observed in the
                            // same scan pass.
                            let secondary_ref = unsafe { &*secondary };
                            match secondary_ref.compare_exchange(
                                expected,
                                tentative_entry.to_hash_bucket_entry(),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            ) {
                                Ok(_) => {
                                    #[cfg(feature = "index-profile")]
                                    crate::index::profile::INDEX_INSERT_PROFILE
                                        .record_cas_attempt(true);

                                    let final_entry =
                                        IndexHashBucketEntry::new(Address::INVALID, tag, false);
                                    secondary_ref.store_index(final_entry, Ordering::Release);

                                    return FindResult {
                                        entry: final_entry,
                                        atomic_entry: Some(secondary),
                                    };
                                }
                                Err(_) => {
                                    #[cfg(feature = "index-profile")]
                                    crate::index::profile::INDEX_INSERT_PROFILE
                                        .record_cas_attempt(false);
                                }
                            }
                        }

                        continue;
                    }
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

    // Tag-conflict detection is handled by `scan_bucket_chain_for_insert` (including tentative
    // entries) before attempting to install a new entry.

    fn scan_bucket_chain_for_insert(
        &self,
        version: usize,
        base_bucket: &HashBucket,
        tag: u16,
    ) -> InsertScanResult {
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;
        let mut free_entry: Option<*const AtomicHashBucketEntry> = None;
        let mut secondary_free_entry: Option<*const AtomicHashBucketEntry> = None;
        let preferred_idx = Self::preferred_entry_index(tag);
        let probe_stride = Self::probe_stride(tag);
        #[cfg(feature = "index-profile")]
        let mut scan_chain_depth = 1u64;

        #[cfg(feature = "index-profile")]
        let mut scan_slots = 0u64;
        #[cfg(feature = "index-profile")]
        let mut scan_tag_matches = 0u64;
        #[cfg(feature = "index-profile")]
        let mut scan_preferred_tag_matches = 0u64;

        // Scan base bucket first (common case: no overflow).
        // SAFETY: `bucket_ptr` points to a valid bucket; entries/overflow_entry are atomic.
        let bucket = unsafe { &*bucket_ptr };

        for probe in 0..HashBucket::NUM_ENTRIES {
            let i = Self::probe_entry_index(preferred_idx, probe, probe_stride);
            #[cfg(feature = "index-profile")]
            {
                scan_slots += 1;
            }

            let entry = bucket.entries[i].load_index(Ordering::Relaxed);
            if entry.is_unused() {
                let slot_ptr = &bucket.entries[i] as *const _;
                if free_entry.is_none() {
                    free_entry = Some(slot_ptr);
                } else if secondary_free_entry.is_none() {
                    secondary_free_entry = Some(slot_ptr);
                }
                continue;
            }
            if entry.tag() != tag {
                continue;
            }

            #[cfg(feature = "index-profile")]
            {
                scan_tag_matches += 1;
                if i == preferred_idx {
                    scan_preferred_tag_matches += 1;
                }
            }

            let entry = bucket.entries[i].load_index(Ordering::Acquire);
            if entry.is_unused() || entry.tag() != tag {
                continue;
            }
            if entry.is_tentative() {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                    scan_slots,
                    scan_tag_matches,
                    scan_preferred_tag_matches,
                    scan_chain_depth,
                );
                return InsertScanResult {
                    found: None,
                    free_entry: None,
                    secondary_free_entry: None,
                    tail: bucket_ptr,
                    retry: true,
                };
            }

            #[cfg(feature = "index-profile")]
            crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                scan_slots,
                scan_tag_matches,
                scan_preferred_tag_matches,
                scan_chain_depth,
            );
            return InsertScanResult {
                found: Some(FindResult {
                    entry,
                    atomic_entry: Some(&bucket.entries[i] as *const _),
                }),
                free_entry,
                secondary_free_entry,
                tail: bucket_ptr,
                retry: false,
            };
        }

        let overflow = bucket.overflow_entry.load(Ordering::Acquire);
        if overflow.is_unused() {
            #[cfg(feature = "index-profile")]
            crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                scan_slots,
                scan_tag_matches,
                scan_preferred_tag_matches,
                scan_chain_depth,
            );
            return InsertScanResult {
                found: None,
                free_entry,
                secondary_free_entry,
                tail: bucket_ptr,
                retry: false,
            };
        }

        // Slow-path: traverse overflow chain with a single read lock for pointer lookups.
        let overflow_buckets = self.overflow_pools[version].buckets_read();
        let next_ptr = self.overflow_pools[version]
            .bucket_ptr_in(overflow_buckets.as_slice(), overflow.address());
        match next_ptr {
            Some(p) => {
                bucket_ptr = p;
                #[cfg(feature = "index-profile")]
                {
                    scan_chain_depth += 1;
                }
            }
            None => {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                    scan_slots,
                    scan_tag_matches,
                    scan_preferred_tag_matches,
                    scan_chain_depth,
                );
                return InsertScanResult {
                    found: None,
                    free_entry,
                    secondary_free_entry,
                    tail: bucket_ptr,
                    retry: false,
                };
            }
        }

        loop {
            // SAFETY: `bucket_ptr` points to a valid overflow bucket owned by the pool.
            let bucket = unsafe { &*bucket_ptr };

            for probe in 0..HashBucket::NUM_ENTRIES {
                let i = MemHashIndex::probe_entry_index(preferred_idx, probe, probe_stride);
                #[cfg(feature = "index-profile")]
                {
                    scan_slots += 1;
                }

                let entry = bucket.entries[i].load_index(Ordering::Relaxed);
                if entry.is_unused() {
                    let slot_ptr = &bucket.entries[i] as *const _;
                    if free_entry.is_none() {
                        free_entry = Some(slot_ptr);
                    } else if secondary_free_entry.is_none() {
                        secondary_free_entry = Some(slot_ptr);
                    }
                    continue;
                }
                if entry.tag() != tag {
                    continue;
                }

                #[cfg(feature = "index-profile")]
                {
                    scan_tag_matches += 1;
                    if i == preferred_idx {
                        scan_preferred_tag_matches += 1;
                    }
                }

                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                if entry.is_unused() || entry.tag() != tag {
                    continue;
                }
                if entry.is_tentative() {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                        scan_slots,
                        scan_tag_matches,
                        scan_preferred_tag_matches,
                        scan_chain_depth,
                    );
                    return InsertScanResult {
                        found: None,
                        free_entry: None,
                        secondary_free_entry: None,
                        tail: bucket_ptr,
                        retry: true,
                    };
                }

                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                    scan_slots,
                    scan_tag_matches,
                    scan_preferred_tag_matches,
                    scan_chain_depth,
                );

                return InsertScanResult {
                    found: Some(FindResult {
                        entry,
                        atomic_entry: Some(&bucket.entries[i] as *const _),
                    }),
                    free_entry,
                    secondary_free_entry,
                    tail: bucket_ptr,
                    retry: false,
                };
            }

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                    scan_slots,
                    scan_tag_matches,
                    scan_preferred_tag_matches,
                    scan_chain_depth,
                );
                return InsertScanResult {
                    found: None,
                    free_entry,
                    secondary_free_entry,
                    tail: bucket_ptr,
                    retry: false,
                };
            }

            let next_ptr = self.overflow_pools[version]
                .bucket_ptr_in(overflow_buckets.as_slice(), overflow.address());
            match next_ptr {
                Some(p) => {
                    bucket_ptr = p;
                    #[cfg(feature = "index-profile")]
                    {
                        scan_chain_depth += 1;
                    }
                }
                None => {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_scan_observations(
                        scan_slots,
                        scan_tag_matches,
                        scan_preferred_tag_matches,
                        scan_chain_depth,
                    );
                    return InsertScanResult {
                        found: None,
                        free_entry,
                        secondary_free_entry,
                        tail: bucket_ptr,
                        retry: false,
                    };
                }
            }
        }
    }

    fn append_overflow_bucket_at_tail_and_get_free_entry(
        &self,
        version: usize,
        tail_bucket: *const HashBucket,
        tag: u16,
    ) -> Option<*const AtomicHashBucketEntry> {
        #[inline]
        fn pick_free_entry_in_bucket(
            bucket: &HashBucket,
            preferred_idx: usize,
            probe_stride: usize,
        ) -> Option<*const AtomicHashBucketEntry> {
            // Fast path: try the preferred slot first to spread concurrent inserts across lanes.
            let preferred = bucket.entries[preferred_idx].load_index(Ordering::Relaxed);
            if preferred.is_unused() {
                return Some(&bucket.entries[preferred_idx] as *const _);
            }

            // Fallback: continue probing in the same lane-aware order used by insert scans.
            for probe in 1..HashBucket::NUM_ENTRIES {
                let i = MemHashIndex::probe_entry_index(preferred_idx, probe, probe_stride);
                let entry = bucket.entries[i].load_index(Ordering::Relaxed);
                if entry.is_unused() {
                    return Some(&bucket.entries[i] as *const _);
                }
            }
            None
        }

        let preferred_idx = Self::preferred_entry_index(tag);
        let probe_stride = Self::probe_stride(tag);

        // SAFETY: `tail_bucket` originates from an earlier traversal and points to a valid bucket.
        // Its fields are atomic, so concurrent mutation via atomic ops is allowed.
        let tail = unsafe { &*tail_bucket };

        let overflow = tail.overflow_entry.load(Ordering::Acquire);
        if !overflow.is_unused() {
            // Another thread already linked a bucket off our tail. That bucket is likely to have
            // free space (it was just allocated), so avoid rescanning from the head.
            let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address())?;
            // SAFETY: `next_ptr` points to a valid bucket managed by the overflow pool.
            let next_bucket = unsafe { &*next_ptr };

            if let Some(free) = pick_free_entry_in_bucket(next_bucket, preferred_idx, probe_stride)
            {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_append_chain_depth(1);
                return Some(free);
            }

            // Extremely rare: the next bucket was already filled; fall back to a deeper search.
            if let Some(free) = self.find_free_entry_in_bucket_chain(
                version,
                next_bucket,
                preferred_idx,
                probe_stride,
            ) {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_append_chain_depth(1);
                return Some(free);
            }

            return self.append_overflow_bucket_and_get_free_entry(version, next_bucket, tag, 2);
        }

        let (new_addr, new_ptr) = self.overflow_pools[version].allocate_with_ptr();
        let new_overflow = HashBucketOverflowEntry::new(new_addr);
        let expected = HashBucketOverflowEntry::INVALID;

        match tail.overflow_entry.compare_exchange(
            expected,
            new_overflow,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                #[cfg(feature = "index-profile")]
                {
                    crate::index::profile::INDEX_INSERT_PROFILE.record_append_link_attempt(true);
                    crate::index::profile::INDEX_INSERT_PROFILE.record_append_chain_depth(1);
                }

                // SAFETY: The bucket was just allocated/reset and is now linked into the chain.
                let new_bucket = unsafe { &*new_ptr };
                pick_free_entry_in_bucket(new_bucket, preferred_idx, probe_stride)
            }
            Err(actual) => {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_append_link_attempt(false);

                if actual.is_unused() {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE
                        .record_append_link_race_deallocate();
                    self.overflow_pools[version].deallocate_with_ptr(new_addr, new_ptr);
                    return None;
                }

                let next_ptr = self.overflow_pools[version].bucket_ptr(actual.address())?;
                // SAFETY: `next_ptr` points to a valid bucket managed by the overflow pool.
                let next_bucket = unsafe { &*next_ptr };

                if let Some(free) =
                    pick_free_entry_in_bucket(next_bucket, preferred_idx, probe_stride)
                {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE
                        .record_append_link_race_deallocate();
                    self.overflow_pools[version].deallocate_with_ptr(new_addr, new_ptr);

                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_append_chain_depth(2);
                    return Some(free);
                }

                // Tail-link CAS lost and the winner bucket is full. Try to consume an existing
                // deeper free slot before appending.
                if let Some(free) = self.find_free_entry_in_bucket_chain(
                    version,
                    next_bucket,
                    preferred_idx,
                    probe_stride,
                ) {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE
                        .record_append_link_race_deallocate();
                    self.overflow_pools[version].deallocate_with_ptr(new_addr, new_ptr);

                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_append_chain_depth(2);
                    return Some(free);
                }

                self.append_overflow_bucket_and_get_free_entry_with_preallocated(
                    version,
                    next_bucket,
                    tag,
                    2,
                    Some((new_addr, new_ptr)),
                )
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
        let preferred_idx = Self::preferred_entry_index(tag);

        #[cfg(feature = "index-profile")]
        let mut lookup_base_slots = 0u64;
        #[cfg(feature = "index-profile")]
        let mut lookup_overflow_slots = 0u64;
        #[cfg(feature = "index-profile")]
        let mut lookup_tag_matches = 0u64;
        #[cfg(feature = "index-profile")]
        let mut lookup_preferred_tag_matches = 0u64;

        // Scan base bucket first (common case: no overflow).
        // SAFETY: `bucket_ptr` points to a valid bucket; entries/overflow_entry are atomic.
        let bucket = unsafe { &*bucket_ptr };

        #[cfg(feature = "index-profile")]
        {
            lookup_base_slots += 1;
        }

        // Preferred lane fast path.
        // Use Relaxed for the probe and only pay Acquire when we have a viable candidate.
        let entry = bucket.entries[preferred_idx].load_index(Ordering::Relaxed);
        if !entry.is_unused() && entry.tag() == tag && !entry.is_tentative() {
            #[cfg(feature = "index-profile")]
            {
                lookup_tag_matches += 1;
                lookup_preferred_tag_matches += 1;
            }

            let entry = bucket.entries[preferred_idx].load_index(Ordering::Acquire);
            if !entry.is_unused() && entry.tag() == tag && !entry.is_tentative() {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                    lookup_base_slots,
                    lookup_overflow_slots,
                    lookup_tag_matches,
                    lookup_preferred_tag_matches,
                );

                return FindResult {
                    entry,
                    atomic_entry: Some(&bucket.entries[preferred_idx] as *const _),
                };
            }
        }

        for i in 0..HashBucket::NUM_ENTRIES {
            if i == preferred_idx {
                continue;
            }

            #[cfg(feature = "index-profile")]
            {
                lookup_base_slots += 1;
            }

            // Avoid paying an Acquire barrier on every slot when scanning.
            // We only need Acquire when we have a candidate match and might return the entry
            // (the Acquire pairs with writers' Release updates to publish record bytes).
            let entry = bucket.entries[i].load_index(Ordering::Relaxed);
            if entry.is_unused() {
                continue;
            }
            if entry.tag() == tag && !entry.is_tentative() {
                #[cfg(feature = "index-profile")]
                {
                    lookup_tag_matches += 1;
                }

                let entry = bucket.entries[i].load_index(Ordering::Acquire);
                if entry.is_unused() || entry.is_tentative() || entry.tag() != tag {
                    continue;
                }

                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                    lookup_base_slots,
                    lookup_overflow_slots,
                    lookup_tag_matches,
                    lookup_preferred_tag_matches,
                );

                return FindResult {
                    entry,
                    atomic_entry: Some(&bucket.entries[i] as *const _),
                };
            }
        }

        let overflow = bucket.overflow_entry.load(Ordering::Acquire);
        if overflow.is_unused() {
            #[cfg(feature = "index-profile")]
            crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                lookup_base_slots,
                lookup_overflow_slots,
                lookup_tag_matches,
                lookup_preferred_tag_matches,
            );
            return FindResult::not_found();
        }

        // Slow-path: traverse overflow chain with a single read lock for pointer lookups.
        let overflow_buckets = self.overflow_pools[version].buckets_read();
        let next_ptr = self.overflow_pools[version]
            .bucket_ptr_in(overflow_buckets.as_slice(), overflow.address());
        match next_ptr {
            Some(p) => bucket_ptr = p,
            None => {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                    lookup_base_slots,
                    lookup_overflow_slots,
                    lookup_tag_matches,
                    lookup_preferred_tag_matches,
                );
                return FindResult::not_found();
            }
        }

        loop {
            // SAFETY: `bucket_ptr` points to a valid overflow bucket owned by the pool.
            let bucket = unsafe { &*bucket_ptr };

            #[cfg(feature = "index-profile")]
            {
                lookup_overflow_slots += 1;
            }

            let entry = bucket.entries[preferred_idx].load_index(Ordering::Relaxed);
            if !entry.is_unused() && entry.tag() == tag && !entry.is_tentative() {
                #[cfg(feature = "index-profile")]
                {
                    lookup_tag_matches += 1;
                    lookup_preferred_tag_matches += 1;
                }

                let entry = bucket.entries[preferred_idx].load_index(Ordering::Acquire);
                if !entry.is_unused() && entry.tag() == tag && !entry.is_tentative() {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                        lookup_base_slots,
                        lookup_overflow_slots,
                        lookup_tag_matches,
                        lookup_preferred_tag_matches,
                    );

                    return FindResult {
                        entry,
                        atomic_entry: Some(&bucket.entries[preferred_idx] as *const _),
                    };
                }
            }

            for i in 0..HashBucket::NUM_ENTRIES {
                if i == preferred_idx {
                    continue;
                }

                #[cfg(feature = "index-profile")]
                {
                    lookup_overflow_slots += 1;
                }

                let entry = bucket.entries[i].load_index(Ordering::Relaxed);
                if entry.is_unused() {
                    continue;
                }
                if entry.tag() == tag && !entry.is_tentative() {
                    #[cfg(feature = "index-profile")]
                    {
                        lookup_tag_matches += 1;
                    }

                    let entry = bucket.entries[i].load_index(Ordering::Acquire);
                    if entry.is_unused() || entry.is_tentative() || entry.tag() != tag {
                        continue;
                    }

                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                        lookup_base_slots,
                        lookup_overflow_slots,
                        lookup_tag_matches,
                        lookup_preferred_tag_matches,
                    );

                    return FindResult {
                        entry,
                        atomic_entry: Some(&bucket.entries[i] as *const _),
                    };
                }
            }

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                #[cfg(feature = "index-profile")]
                crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                    lookup_base_slots,
                    lookup_overflow_slots,
                    lookup_tag_matches,
                    lookup_preferred_tag_matches,
                );
                return FindResult::not_found();
            }
            let next_ptr = self.overflow_pools[version]
                .bucket_ptr_in(overflow_buckets.as_slice(), overflow.address());
            match next_ptr {
                Some(p) => bucket_ptr = p,
                None => {
                    #[cfg(feature = "index-profile")]
                    crate::index::profile::INDEX_INSERT_PROFILE.record_lookup_observations(
                        lookup_base_slots,
                        lookup_overflow_slots,
                        lookup_tag_matches,
                        lookup_preferred_tag_matches,
                    );
                    return FindResult::not_found();
                }
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
        preferred_idx: usize,
        probe_stride: usize,
    ) -> Option<*const AtomicHashBucketEntry> {
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

        // Check base bucket first (common case: no overflow).
        // SAFETY: Same rationale as `find_entry_in_bucket_chain`.
        let bucket = unsafe { &*bucket_ptr };

        for probe in 0..HashBucket::NUM_ENTRIES {
            let i = Self::probe_entry_index(preferred_idx, probe, probe_stride);
            let entry = bucket.entries[i].load_index(Ordering::Relaxed);
            if entry.is_unused() {
                return Some(&bucket.entries[i] as *const _);
            }
        }

        let overflow = bucket.overflow_entry.load(Ordering::Acquire);
        if overflow.is_unused() {
            return None;
        }

        // Slow-path: traverse overflow chain with a single read lock for pointer lookups.
        let overflow_buckets = self.overflow_pools[version].buckets_read();
        let next_ptr = self.overflow_pools[version]
            .bucket_ptr_in(overflow_buckets.as_slice(), overflow.address());
        match next_ptr {
            Some(p) => bucket_ptr = p,
            None => return None,
        }

        loop {
            // SAFETY: Same rationale as above.
            let bucket = unsafe { &*bucket_ptr };

            for probe in 0..HashBucket::NUM_ENTRIES {
                let i = Self::probe_entry_index(preferred_idx, probe, probe_stride);
                let entry = bucket.entries[i].load_index(Ordering::Relaxed);
                if entry.is_unused() {
                    return Some(&bucket.entries[i] as *const _);
                }
            }

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                return None;
            }

            let next_ptr = self.overflow_pools[version]
                .bucket_ptr_in(overflow_buckets.as_slice(), overflow.address());
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
        tag: u16,
        initial_chain_depth: u64,
    ) -> Option<*const AtomicHashBucketEntry> {
        self.append_overflow_bucket_and_get_free_entry_with_preallocated(
            version,
            base_bucket,
            tag,
            initial_chain_depth,
            None,
        )
    }

    fn append_overflow_bucket_and_get_free_entry_with_preallocated(
        &self,
        version: usize,
        base_bucket: &HashBucket,
        tag: u16,
        initial_chain_depth: u64,
        preallocated: Option<(
            crate::index::hash_bucket::FixedPageAddress,
            *const HashBucket,
        )>,
    ) -> Option<*const AtomicHashBucketEntry> {
        // Find the chain tail (a bucket with an unused overflow entry) and append a new overflow
        // bucket.
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;
        let preferred_idx = Self::preferred_entry_index(tag);
        let mut pending_bucket = preallocated;
        #[cfg(not(feature = "index-profile"))]
        let _ = initial_chain_depth;
        #[cfg(feature = "index-profile")]
        let mut chain_depth = initial_chain_depth;

        loop {
            // SAFETY: Same rationale as above.
            let bucket = unsafe { &*bucket_ptr };
            let overflow = bucket.overflow_entry.load(Ordering::Acquire);

            if overflow.is_unused() {
                // Reuse a previously allocated bucket after a race; otherwise allocate a fresh one.
                let (new_addr, new_ptr) = match pending_bucket.take() {
                    Some(bucket) => bucket,
                    None => self.overflow_pools[version].allocate_with_ptr(),
                };
                let new_overflow = HashBucketOverflowEntry::new(new_addr);
                let expected = HashBucketOverflowEntry::INVALID;

                match bucket.overflow_entry.compare_exchange(
                    expected,
                    new_overflow,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        #[cfg(feature = "index-profile")]
                        {
                            crate::index::profile::INDEX_INSERT_PROFILE
                                .record_append_link_attempt(true);
                            crate::index::profile::INDEX_INSERT_PROFILE
                                .record_append_chain_depth(chain_depth);
                        }

                        // SAFETY: The bucket was just allocated/reset and is now linked.
                        let new_bucket = unsafe { &*new_ptr };
                        return Some(&new_bucket.entries[preferred_idx] as *const _);
                    }
                    Err(actual) => {
                        #[cfg(feature = "index-profile")]
                        crate::index::profile::INDEX_INSERT_PROFILE
                            .record_append_link_attempt(false);

                        // CAS failed: keep the allocated bucket and continue from the winner
                        // bucket to avoid repeated deallocate and allocate churn.
                        pending_bucket = Some((new_addr, new_ptr));

                        // Another thread installed an overflow bucket; keep traversing.
                        if actual.is_unused() {
                            continue;
                        }
                        let next_ptr = self.overflow_pools[version].bucket_ptr(actual.address());
                        match next_ptr {
                            Some(p) => {
                                bucket_ptr = p;
                                #[cfg(feature = "index-profile")]
                                {
                                    chain_depth += 1;
                                }
                            }
                            None => {
                                if let Some((addr, ptr)) = pending_bucket.take() {
                                    #[cfg(feature = "index-profile")]
                                    crate::index::profile::INDEX_INSERT_PROFILE
                                        .record_append_link_race_deallocate();
                                    self.overflow_pools[version].deallocate_with_ptr(addr, ptr);
                                }
                                return None;
                            }
                        }
                    }
                }
            } else {
                // Keep traversing.
                let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => {
                        bucket_ptr = p;
                        #[cfg(feature = "index-profile")]
                        {
                            chain_depth += 1;
                        }
                    }
                    None => {
                        if let Some((addr, ptr)) = pending_bucket.take() {
                            #[cfg(feature = "index-profile")]
                            crate::index::profile::INDEX_INSERT_PROFILE
                                .record_append_link_race_deallocate();
                            self.overflow_pools[version].deallocate_with_ptr(addr, ptr);
                        }
                        return None;
                    }
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
                // SAFETY: `bucket_ptr` points to a valid bucket; entries/overflow are atomic.
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
                // SAFETY: `bucket_ptr` points to a valid bucket.
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
                // SAFETY: `bucket_ptr` points to a valid bucket.
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
