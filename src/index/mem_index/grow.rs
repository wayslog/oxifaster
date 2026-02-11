use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::address::Address;
use crate::constants::CACHE_LINE_BYTES;
use crate::index::grow::{calculate_num_chunks, get_chunk_bounds, GrowResult};
use crate::index::{
    HashBucket, HashBucketEntry, HashBucketOverflowEntry, IndexHashBucketEntry, KeyHash,
};
use crate::status::Status;

use super::MemHashIndex;

/// Result of migrating a single chunk during index growth
#[derive(Debug, Default)]
struct ChunkMigrationResult {
    /// Number of entries successfully migrated
    migrated: u64,
    /// Number of overflow buckets encountered (for statistics).
    overflow_buckets_seen: u64,
    /// Number of entries that couldn't be rehashed (rehash callback returned None)
    rehash_failures: u64,
}

impl MemHashIndex {
    /// Start a grow operation
    ///
    /// This allocates a new hash table with `growth_factor` times the current size
    /// and initializes the grow state for chunk-based migration.
    ///
    /// # Returns
    /// - `Ok(new_size)` if grow was started successfully
    /// - `Err(Status::Aborted)` if grow is already in progress
    pub fn start_grow(&mut self) -> Result<u64, Status> {
        if self.grow_in_progress.swap(true, Ordering::AcqRel) {
            return Err(Status::Aborted);
        }

        let current_version = self.version.load(Ordering::Acquire);
        let old_size = self.tables[current_version as usize].size();
        let new_size = old_size * self.grow_config.growth_factor;

        // Initialize the new table
        let new_version = 1 - current_version;
        let status = self.tables[new_version as usize].initialize(new_size, CACHE_LINE_BYTES);
        if status != Status::Ok {
            self.grow_in_progress.store(false, Ordering::Release);
            return Err(status);
        }
        self.overflow_pools[new_version as usize].clear();

        // Initialize grow state
        let num_chunks = calculate_num_chunks(old_size);
        self.grow_state.initialize(current_version, num_chunks);

        Ok(new_size)
    }

    /// Grow the hash index to a new size using a rehash callback.
    pub fn grow_with_rehash<F>(&mut self, rehash_fn: F) -> GrowResult
    where
        F: Fn(Address) -> Option<KeyHash>,
    {
        let start_time = Instant::now();

        let new_size = match self.start_grow() {
            Ok(size) => size,
            Err(status) => return GrowResult::failure(status),
        };

        let old_version = self.grow_state.old_version();
        let new_version = self.grow_state.new_version();
        let old_size = self.tables[old_version as usize].size();
        let mut entries_migrated = 0u64;
        let mut rehash_failures = 0u64;

        while let Some(chunk_idx) = self.grow_state.get_next_chunk() {
            let result = self.migrate_chunk_with_rehash(
                chunk_idx,
                old_version,
                new_version,
                new_size,
                &rehash_fn,
            );
            entries_migrated += result.migrated;
            rehash_failures += result.rehash_failures;
            self.grow_state.complete_chunk();
        }

        self.version.store(new_version, Ordering::Release);
        self.grow_state.reset();
        self.grow_in_progress.store(false, Ordering::Release);

        let duration = start_time.elapsed();
        GrowResult::with_data_loss_tracking(
            old_size,
            new_size,
            entries_migrated,
            duration.as_millis() as u64,
            0,
            rehash_failures,
        )
    }

    /// Grow the hash index to a new size (deprecated - use grow_with_rehash).
    #[deprecated(
        since = "0.1.0",
        note = "Use grow_with_rehash() with a proper rehash callback for correct bucket placement"
    )]
    pub fn grow(&mut self) -> GrowResult {
        self.grow_with_rehash(|_addr| None)
    }

    /// Migrate a single chunk during external/chunked growth.
    pub fn migrate_chunk_external<F>(&self, chunk_idx: u64, rehash_fn: F) -> bool
    where
        F: Fn(Address) -> Option<KeyHash>,
    {
        if !self.grow_in_progress.load(Ordering::Acquire) {
            return false;
        }

        let old_version = self.grow_state.old_version();
        let new_version = self.grow_state.new_version();
        let new_size = self.tables[new_version as usize].size();

        let result = self.migrate_chunk_with_rehash(
            chunk_idx,
            old_version,
            new_version,
            new_size,
            &rehash_fn,
        );

        self.grow_state
            .record_chunk_result(result.migrated, 0, result.rehash_failures);

        self.grow_state.complete_chunk()
    }

    fn migrate_chunk_with_rehash<F>(
        &self,
        chunk_idx: u64,
        old_version: u8,
        new_version: u8,
        new_size: u64,
        rehash_fn: &F,
    ) -> ChunkMigrationResult
    where
        F: Fn(Address) -> Option<KeyHash>,
    {
        let old_size = self.tables[old_version as usize].size();
        let (start_bucket, end_bucket) = get_chunk_bounds(chunk_idx, old_size);

        let mut migrated = 0u64;
        let mut overflow_buckets_seen = 0u64;
        let mut rehash_failures = 0u64;

        for bucket_idx in start_bucket..end_bucket {
            let base_bucket = self.tables[old_version as usize].bucket_at(bucket_idx);
            let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

            loop {
                // SAFETY: `bucket_ptr` points to a valid bucket; entries/overflow are atomic.
                let old_bucket = unsafe { &*bucket_ptr };

                for i in 0..HashBucket::NUM_ENTRIES {
                    let entry = old_bucket.entries[i].load_index(Ordering::Acquire);

                    if entry.is_unused() || entry.is_tentative() {
                        continue;
                    }

                    let address = entry.address();
                    let hash = match rehash_fn(address) {
                        Some(h) => h,
                        None => {
                            rehash_failures += 1;
                            continue;
                        }
                    };

                    let new_bucket_idx = hash.hash_table_index(new_size) as u64;
                    let new_entry = IndexHashBucketEntry::new(address, hash.tag(), false);

                    if self.insert_into_new_table_with_overflow(
                        new_entry,
                        new_bucket_idx,
                        new_version,
                    ) {
                        migrated += 1;
                    }
                }

                let overflow = old_bucket.overflow_entry.load(Ordering::Acquire);
                if overflow.is_unused() {
                    break;
                }
                overflow_buckets_seen += 1;
                let next_ptr =
                    self.overflow_pools[old_version as usize].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => break,
                }
            }
        }

        ChunkMigrationResult {
            migrated,
            overflow_buckets_seen,
            rehash_failures,
        }
    }

    fn insert_into_new_table_with_overflow(
        &self,
        entry: IndexHashBucketEntry,
        bucket_idx: u64,
        new_version: u8,
    ) -> bool {
        let base_bucket = self.tables[new_version as usize].bucket_at(bucket_idx);
        let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

        loop {
            // SAFETY: `bucket_ptr` points to a valid bucket.
            let bucket = unsafe { &*bucket_ptr };

            for i in 0..HashBucket::NUM_ENTRIES {
                let current = bucket.entries[i].load_index(Ordering::Acquire);

                if current.is_unused() {
                    match bucket.entries[i].compare_exchange(
                        HashBucketEntry::INVALID,
                        entry.to_hash_bucket_entry(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return true,
                        Err(_) => continue,
                    }
                }
            }

            let overflow = bucket.overflow_entry.load(Ordering::Acquire);
            if overflow.is_unused() {
                // Append an overflow bucket.
                let (new_addr, new_ptr) =
                    self.overflow_pools[new_version as usize].allocate_with_ptr();
                let new_overflow = HashBucketOverflowEntry::new(new_addr);
                let expected = HashBucketOverflowEntry::INVALID;

                match bucket.overflow_entry.compare_exchange(
                    expected,
                    new_overflow,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        bucket_ptr = new_ptr;
                        continue;
                    }
                    Err(actual) => {
                        // Another thread installed an overflow bucket first. Return ours to the
                        // pool for reuse.
                        self.overflow_pools[new_version as usize]
                            .deallocate_with_ptr(new_addr, new_ptr);
                        if actual.is_unused() {
                            continue;
                        }
                        let next_ptr =
                            self.overflow_pools[new_version as usize].bucket_ptr(actual.address());
                        match next_ptr {
                            Some(p) => {
                                bucket_ptr = p;
                                continue;
                            }
                            None => return false,
                        }
                    }
                }
            } else {
                let next_ptr =
                    self.overflow_pools[new_version as usize].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => return false,
                }
            }
        }
    }

    /// Complete a grow operation that was started with `start_grow()`.
    pub fn complete_grow(&mut self) -> GrowResult {
        if !self.grow_in_progress.load(Ordering::Acquire) {
            return GrowResult::failure(Status::Aborted);
        }

        if self.grow_state.remaining_chunks() > 0 {
            return GrowResult::failure(Status::Pending);
        }

        let old_version = self.grow_state.old_version();
        let new_version = self.grow_state.new_version();
        let old_size = self.tables[old_version as usize].size();
        let new_size = self.tables[new_version as usize].size();

        let entries_migrated = self.grow_state.get_entries_migrated();
        let overflow_buckets_skipped = self.grow_state.get_overflow_buckets_skipped();
        let rehash_failures = self.grow_state.get_rehash_failures();

        self.version.store(new_version, Ordering::Release);
        self.grow_state.reset();
        self.grow_in_progress.store(false, Ordering::Release);

        GrowResult::with_data_loss_tracking(
            old_size,
            new_size,
            entries_migrated,
            0,
            overflow_buckets_skipped,
            rehash_failures,
        )
    }
}
