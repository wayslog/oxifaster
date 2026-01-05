//! In-memory hash index implementation for FASTER
//!
//! This module provides the main hash index used by FasterKV to locate records
//! in the hybrid log.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Instant;

use crate::address::Address;
use crate::checkpoint::IndexMetadata;
use crate::constants::CACHE_LINE_BYTES;
use crate::index::grow::{calculate_num_chunks, get_chunk_bounds, GrowConfig, GrowResult, GrowState};
use crate::index::{
    AtomicHashBucketEntry, HashBucket, HashBucketEntry, HashBucketOverflowEntry,
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

/// Result of migrating a single chunk during index growth
#[derive(Debug, Default)]
struct ChunkMigrationResult {
    /// Number of entries successfully migrated
    migrated: u64,
    /// Number of overflow buckets encountered that couldn't be followed
    overflow_buckets_skipped: u64,
    /// Number of entries that couldn't be rehashed (rehash callback returned None)
    rehash_failures: u64,
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
    version: AtomicU8,
    /// Grow state for tracking growth operations
    grow_state: GrowState,
    /// Grow configuration
    grow_config: GrowConfig,
    /// Whether a grow operation is in progress
    grow_in_progress: AtomicBool,
}

impl MemHashIndex {
    /// Create a new uninitialized hash index
    pub fn new() -> Self {
        Self {
            tables: [InternalHashTable::new(), InternalHashTable::new()],
            version: AtomicU8::new(0),
            grow_state: GrowState::new(),
            grow_config: GrowConfig::default(),
            grow_in_progress: AtomicBool::new(false),
        }
    }

    /// Create a new hash index with growth configuration
    pub fn with_grow_config(grow_config: GrowConfig) -> Self {
        Self {
            tables: [InternalHashTable::new(), InternalHashTable::new()],
            version: AtomicU8::new(0),
            grow_state: GrowState::new(),
            grow_config,
            grow_in_progress: AtomicBool::new(false),
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
    pub fn grow_config(&self) -> &GrowConfig {
        &self.grow_config
    }

    /// Set the grow configuration
    pub fn set_grow_config(&mut self, config: GrowConfig) {
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

    /// Start a grow operation
    ///
    /// This allocates a new hash table with `growth_factor` times the current size
    /// and initializes the grow state for chunk-based migration.
    ///
    /// # Returns
    /// - `Ok(new_size)` if grow was started successfully
    /// - `Err(Status::Aborted)` if grow is already in progress
    pub fn start_grow(&mut self) -> Result<u64, Status> {
        // Check if already in progress
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

        // Initialize grow state
        let num_chunks = calculate_num_chunks(old_size);
        self.grow_state.initialize(current_version, num_chunks);

        Ok(new_size)
    }

    /// Grow the hash index to a new size using a rehash callback.
    ///
    /// This is a blocking operation that migrates all entries from the old
    /// table to the new table. During migration, each entry's key is rehashed
    /// using the provided callback to determine the correct new bucket.
    ///
    /// # Arguments
    /// * `rehash_fn` - A callback that takes an Address and returns the KeyHash
    ///   for the record at that address. This is required because the hash index
    ///   only stores partial hash information (the tag), which is insufficient
    ///   to determine the correct new bucket after the table size changes.
    ///
    /// # Returns
    /// `GrowResult` with details about the grow operation.
    /// 
    /// # Warning
    /// If `GrowResult::overflow_buckets_skipped > 0`, some entries stored in
    /// overflow buckets were NOT migrated and are lost.
    pub fn grow_with_rehash<F>(&mut self, rehash_fn: F) -> GrowResult
    where
        F: Fn(Address) -> Option<KeyHash>,
    {
        let start_time = Instant::now();

        // Start the grow operation
        let new_size = match self.start_grow() {
            Ok(size) => size,
            Err(status) => return GrowResult::failure(status),
        };

        let old_version = self.grow_state.old_version();
        let new_version = self.grow_state.new_version();
        let old_size = self.tables[old_version as usize].size();
        let mut entries_migrated = 0u64;
        let mut total_overflow_buckets_skipped = 0u64;
        let mut rehash_failures = 0u64;

        // Process all chunks
        while let Some(chunk_idx) = self.grow_state.get_next_chunk() {
            let result = self.migrate_chunk_with_rehash(
                chunk_idx,
                old_version,
                new_version,
                new_size,
                &rehash_fn,
            );
            entries_migrated += result.migrated;
            total_overflow_buckets_skipped += result.overflow_buckets_skipped;
            rehash_failures += result.rehash_failures;
            self.grow_state.complete_chunk();
        }

        // Switch to new version
        self.version.store(new_version, Ordering::Release);

        // Reset grow state
        self.grow_state.reset();
        self.grow_in_progress.store(false, Ordering::Release);

        let duration = start_time.elapsed();

        // Return result with both overflow and rehash failure tracking
        // Success is false if either overflow buckets were skipped or rehash failures occurred
        GrowResult::with_data_loss_tracking(
            old_size,
            new_size,
            entries_migrated,
            duration.as_millis() as u64,
            total_overflow_buckets_skipped,
            rehash_failures,
        )
    }

    /// Grow the hash index to a new size (deprecated - use grow_with_rehash).
    ///
    /// # Warning
    /// This method uses an approximation for bucket placement that may result
    /// in entries being placed in incorrect buckets. After grow completes,
    /// some lookups may fail to find existing keys.
    ///
    /// Use `grow_with_rehash` with a proper rehash callback for correct behavior.
    #[deprecated(
        since = "0.1.0",
        note = "Use grow_with_rehash() with a proper rehash callback for correct bucket placement"
    )]
    pub fn grow(&mut self) -> GrowResult {
        // Fallback: use a simple heuristic that may not be correct
        // This exists for backward compatibility but should not be used
        self.grow_with_rehash(|_addr| {
            // Cannot rehash without access to keys - return None to skip
            None
        })
    }

    /// Migrate a single chunk during external/chunked growth.
    ///
    /// This is for callers using the chunked growth path:
    /// 1. Call `start_grow()` to begin
    /// 2. Get chunks via `grow_state().get_next_chunk()`
    /// 3. Call `migrate_chunk_external()` for each chunk
    /// 4. Call `complete_grow()` when done
    ///
    /// The migration results are automatically recorded in grow_state and will
    /// be included in the `GrowResult` returned by `complete_grow()`.
    ///
    /// # Arguments
    /// * `chunk_idx` - The chunk index to migrate (from `get_next_chunk()`)
    /// * `rehash_fn` - Callback to get the hash for a record at an address
    ///
    /// # Returns
    /// `true` if this was the last chunk, `false` otherwise
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

        // Record results in grow_state for complete_grow to report
        self.grow_state.record_chunk_result(
            result.migrated,
            result.overflow_buckets_skipped,
            result.rehash_failures,
        );

        self.grow_state.complete_chunk()
    }

    /// Migrate a single chunk of entries from old table to new table using rehash
    ///
    /// This function migrates all entries from the primary bucket slots.
    /// For each entry, it calls the rehash callback to get the full hash,
    /// then uses that hash to compute the correct new bucket index.
    /// 
    /// # Warning
    /// Currently, overflow bucket chains are NOT followed because there is no
    /// overflow bucket allocator implementation. If overflow buckets exist
    /// (indicated by `overflow_buckets_skipped > 0` in the result), those
    /// entries will be lost after the grow completes.
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
        let mut overflow_buckets_skipped = 0u64;
        let mut rehash_failures = 0u64;

        for bucket_idx in start_bucket..end_bucket {
            let old_bucket = self.tables[old_version as usize].bucket_at(bucket_idx);

            // Migrate primary bucket entries
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = old_bucket.entries[i].load_index(Ordering::Acquire);

                if entry.is_unused() || entry.is_tentative() {
                    continue;
                }

                // Get the full hash by reading and rehashing the key at this address
                // NOTE: We call rehash_fn only once and reuse the result to avoid
                // issues with non-deterministic callbacks or callbacks with side effects
                let address = entry.address();
                let hash = match rehash_fn(address) {
                    Some(h) => h,
                    None => {
                        // Cannot rehash - skip this entry (data loss)
                        rehash_failures += 1;
                        continue;
                    }
                };

                // Use the full hash to compute the correct new bucket index
                // This is the key fix: we use hash.hash_table_index(new_size)
                // instead of trying to infer from the stored tag
                let new_bucket_idx = hash.hash_table_index(new_size) as u64;

                // Create a new entry with the rehashed tag for consistency
                let new_entry = IndexHashBucketEntry::new(address, hash.tag(), false);

                // Try to insert into new table
                if self.insert_into_new_table(new_entry, new_bucket_idx, new_version) {
                    migrated += 1;
                }
            }

            // Check for overflow buckets that we cannot migrate
            // WARNING: Without an overflow bucket allocator, we cannot follow
            // overflow chains. Any entries in overflow buckets will be lost.
            let overflow = old_bucket.overflow_entry.load(Ordering::Acquire);
            if !overflow.is_unused() {
                // Count the overflow bucket - entries here will be lost
                overflow_buckets_skipped += 1;
                
                // TODO: Once overflow bucket allocator is implemented, follow
                // the chain and migrate all entries using rehash_fn for each
            }
        }

        ChunkMigrationResult {
            migrated,
            overflow_buckets_skipped,
            rehash_failures,
        }
    }

    /// Insert an entry into the new table during growth
    fn insert_into_new_table(
        &self,
        entry: IndexHashBucketEntry,
        bucket_idx: u64,
        new_version: u8,
    ) -> bool {
        let new_bucket = self.tables[new_version as usize].bucket_at(bucket_idx);

        // Find a free slot in the new bucket
        for i in 0..HashBucket::NUM_ENTRIES {
            let current = new_bucket.entries[i].load_index(Ordering::Acquire);

            if current.is_unused() {
                // Try to claim this slot
                match new_bucket.entries[i].compare_exchange(
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

        // No free slot found - in a full implementation, we would allocate
        // an overflow bucket here
        false
    }

    /// Complete a grow operation that was started with `start_grow()`
    ///
    /// This should be called after all chunks have been processed via
    /// `migrate_chunk()`. This is for external callers that want to
    /// control the migration process (e.g., for concurrent compaction).
    ///
    /// # Warning
    /// If the returned `GrowResult` has `overflow_buckets_skipped > 0` or
    /// `rehash_failures > 0`, some entries may have been lost during migration.
    /// Check `has_data_loss_warning()` on the result.
    pub fn complete_grow(&mut self) -> GrowResult {
        if !self.grow_in_progress.load(Ordering::Acquire) {
            return GrowResult::failure(Status::Aborted);
        }

        // Check if all chunks are done
        if self.grow_state.remaining_chunks() > 0 {
            return GrowResult::failure(Status::Pending);
        }

        let old_version = self.grow_state.old_version();
        let new_version = self.grow_state.new_version();
        let old_size = self.tables[old_version as usize].size();
        let new_size = self.tables[new_version as usize].size();

        // Get accumulated data loss stats from chunk migrations
        let entries_migrated = self.grow_state.get_entries_migrated();
        let overflow_buckets_skipped = self.grow_state.get_overflow_buckets_skipped();
        let rehash_failures = self.grow_state.get_rehash_failures();

        // Switch to new version
        self.version.store(new_version, Ordering::Release);

        // Reset grow state
        self.grow_state.reset();
        self.grow_in_progress.store(false, Ordering::Release);

        // Return result with data loss tracking, consistent with grow_with_rehash
        GrowResult::with_data_loss_tracking(
            old_size,
            new_size,
            entries_migrated,
            0, // duration not tracked for chunked growth
            overflow_buckets_skipped,
            rehash_failures,
        )
    }

    /// Get the grow state (for external progress tracking)
    pub fn grow_state(&self) -> &GrowState {
        &self.grow_state
    }

    /// Get a mutable reference to grow state
    pub fn grow_state_mut(&mut self) -> &mut GrowState {
        &mut self.grow_state
    }

    /// Find an entry in the hash index
    ///
    /// Returns the entry and a pointer to the atomic entry location.
    pub fn find_entry(&self, hash: KeyHash) -> FindResult {
        let version = self.version.load(Ordering::Acquire) as usize;
        let bucket = self.tables[version].bucket(hash);
        let tag = hash.tag();

        // Search through the bucket.
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

        // Check overflow bucket.
        // Note: In a full implementation, this would dereference the overflow pointer from the
        // overflow bucket allocator. For now, we return not found since we don't have the
        // allocator here.
        let overflow = bucket.overflow_entry.load(Ordering::Acquire);
        if overflow.is_unused() {
            return FindResult::not_found();
        }

        FindResult::not_found()
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

    /// Try to update the address of an entry atomically by hash
    ///
    /// This is useful during compaction when we need to update the index
    /// to point to a record's new location.
    ///
    /// # Arguments
    /// * `hash` - The key hash to look up
    /// * `old_address` - The expected current address
    /// * `new_address` - The new address to set
    ///
    /// # Returns
    /// `Status::Ok` if the update succeeded, `Status::NotFound` if the entry
    /// wasn't found or the address didn't match.
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

        // Check if the current address matches what we expect
        if result.entry.address() != old_address {
            return Status::NotFound;
        }

        // Try to atomically update the entry
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
                Err(_) => Status::Aborted, // Concurrent modification
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
        let version = self.version.load(Ordering::Acquire) as usize;
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
        let version = self.version.load(Ordering::Acquire) as usize;
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

    // ============ Checkpoint and Recovery Methods ============

    /// Create a checkpoint of the hash index to disk
    ///
    /// This writes the hash table data to a file and returns metadata about the checkpoint.
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory where checkpoint files will be written
    /// * `token` - Unique token identifying this checkpoint
    ///
    /// # Returns
    /// IndexMetadata containing information about the saved checkpoint
    pub fn checkpoint(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
    ) -> io::Result<IndexMetadata> {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        if !self.tables[version].is_initialized() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Hash index not initialized",
            ));
        }

        // Count entries
        let stats = self.dump_distribution();

        // Create metadata
        let mut metadata = IndexMetadata::with_token(token);
        metadata.version = self.version.load(Ordering::Acquire) as u32;
        metadata.table_size = table_size;
        metadata.num_buckets = stats.buckets_with_entries;
        metadata.num_entries = stats.used_entries;
        // num_ht_bytes: table_size * entries_per_bucket * 8 bytes per entry
        metadata.num_ht_bytes = table_size * crate::index::HashBucket::NUM_ENTRIES as u64 * 8;

        // Write index data file
        let data_path = checkpoint_dir.join("index.dat");
        self.write_index_data(&data_path)?;

        // Write metadata file
        let meta_path = checkpoint_dir.join("index.meta");
        metadata.write_to_file(&meta_path)?;

        Ok(metadata)
    }

    /// Write the raw hash table data to a file
    fn write_index_data(&self, path: &Path) -> io::Result<()> {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(1 << 20, file); // 1 MB buffer

        // Write header: table_size (8 bytes)
        writer.write_all(&table_size.to_le_bytes())?;

        // Write each bucket
        for idx in 0..table_size {
            let bucket = self.tables[version].bucket_at(idx);

            // Write each entry in the bucket (8 bytes each, 7 entries per bucket)
            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load(Ordering::Relaxed);
                writer.write_all(&entry.control().to_le_bytes())?;
            }

            // Write overflow entry (8 bytes)
            let overflow = bucket.overflow_entry.load(Ordering::Relaxed);
            writer.write_all(&overflow.control().to_le_bytes())?;
        }

        writer.flush()?;
        Ok(())
    }

    /// Recover the hash index from a checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory containing checkpoint files
    /// * `metadata` - Optional pre-loaded metadata; if None, reads from file
    ///
    /// # Returns
    /// Ok(()) on success, or an error
    pub fn recover(
        &mut self,
        checkpoint_dir: &Path,
        metadata: Option<&IndexMetadata>,
    ) -> io::Result<()> {
        // Load metadata if not provided
        let meta_path = checkpoint_dir.join("index.meta");
        let loaded_metadata;
        let metadata = match metadata {
            Some(m) => m,
            None => {
                loaded_metadata = IndexMetadata::read_from_file(&meta_path)?;
                &loaded_metadata
            }
        };

        // Initialize the hash table with the correct size
        let config = MemHashIndexConfig::new(metadata.table_size);
        let status = self.initialize(&config);
        if status != Status::Ok {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to initialize hash index: {:?}", status),
            ));
        }

        // Read the index data
        let data_path = checkpoint_dir.join("index.dat");
        self.read_index_data(&data_path)?;

        // Clear any tentative entries that might have been partially written
        self.clear_tentative_entries();

        Ok(())
    }

    /// Read the raw hash table data from a file
    fn read_index_data(&mut self, path: &Path) -> io::Result<()> {
        let version = self.version.load(Ordering::Acquire) as usize;

        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(1 << 20, file); // 1 MB buffer

        // Read header: table_size (8 bytes)
        let mut size_buf = [0u8; 8];
        reader.read_exact(&mut size_buf)?;
        let file_table_size = u64::from_le_bytes(size_buf);

        if file_table_size != self.tables[version].size() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Table size mismatch: file has {}, index has {}",
                    file_table_size,
                    self.tables[version].size()
                ),
            ));
        }

        // Read each bucket
        let mut entry_buf = [0u8; 8];
        for idx in 0..file_table_size {
            let bucket = self.tables[version].bucket_at(idx);

            // Read each entry in the bucket
            for i in 0..HashBucket::NUM_ENTRIES {
                reader.read_exact(&mut entry_buf)?;
                let control = u64::from_le_bytes(entry_buf);
                bucket.entries[i].store(HashBucketEntry::from_control(control), Ordering::Release);
            }

            // Read overflow entry
            reader.read_exact(&mut entry_buf)?;
            let overflow_control = u64::from_le_bytes(entry_buf);
            bucket.overflow_entry.store(
                HashBucketOverflowEntry::from_control(overflow_control),
                Ordering::Release,
            );
        }

        Ok(())
    }

    /// Verify that the recovered index is consistent
    pub fn verify_recovery(&self) -> io::Result<()> {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        let mut errors = 0u64;

        for idx in 0..table_size {
            let bucket = self.tables[version].bucket_at(idx);

            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load_index(Ordering::Relaxed);

                // Check for obviously corrupted entries
                if entry.is_tentative() {
                    errors += 1;
                }
            }
        }

        if errors > 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Found {} corrupted entries during recovery verification",
                    errors
                ),
            ))
        } else {
            Ok(())
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
        let result = index.grow_with_rehash(|addr| {
            hash_map_ref.read().unwrap().get(&addr.control()).copied()
        });
        
        assert_eq!(result.old_size, 256);
        assert_eq!(result.new_size, 512);
        assert!(result.entries_migrated > 0, "Should have migrated entries");
        assert_eq!(result.rehash_failures, 0, "All entries should be rehashed successfully");

        // Verify all entries can still be found after grow
        for hash in test_hashes.iter() {
            let find_result = index.find_entry(*hash);
            assert!(find_result.found(), "Entry should still be findable after grow");
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
}
