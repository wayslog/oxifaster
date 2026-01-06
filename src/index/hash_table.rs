//! Internal hash table implementation for FASTER
//!
//! This module provides the core hash table used by the hash index.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::constants::{CACHE_LINE_BYTES, NUM_MERGE_CHUNKS};
use crate::index::{HashBucket, KeyHash};
use crate::status::Status;
use crate::utility::is_power_of_two;

/// Internal hash table - a sized array of HashBuckets
pub struct InternalHashTable {
    /// Pointer to the bucket array
    buckets: Option<NonNull<HashBucket>>,
    /// Number of buckets
    size: u64,
    /// Pending checkpoint writes
    pending_checkpoint_writes: AtomicU64,
    /// Pending recovery reads
    pending_recover_reads: AtomicU64,
    /// Checkpoint in progress flag
    checkpoint_pending: AtomicBool,
    /// Checkpoint failed flag
    checkpoint_failed: AtomicBool,
    /// Recovery in progress flag
    recover_pending: AtomicBool,
    /// Recovery failed flag
    recover_failed: AtomicBool,
}

impl InternalHashTable {
    /// Create a new uninitialized hash table
    pub fn new() -> Self {
        Self {
            buckets: None,
            size: 0,
            pending_checkpoint_writes: AtomicU64::new(0),
            pending_recover_reads: AtomicU64::new(0),
            checkpoint_pending: AtomicBool::new(false),
            checkpoint_failed: AtomicBool::new(false),
            recover_pending: AtomicBool::new(false),
            recover_failed: AtomicBool::new(false),
        }
    }

    /// Initialize the hash table with the specified size
    ///
    /// # Panics
    /// Panics if size is not a power of two or exceeds i32::MAX
    pub fn initialize(&mut self, new_size: u64, alignment: usize) -> Status {
        assert!(new_size < i32::MAX as u64, "Hash table size too large");
        assert!(
            is_power_of_two(new_size),
            "Hash table size must be power of 2"
        );
        assert!(
            is_power_of_two(alignment as u64),
            "Alignment must be power of 2"
        );
        assert!(
            alignment >= CACHE_LINE_BYTES,
            "Alignment must be >= cache line size"
        );

        if self.size != new_size {
            // Free existing buckets if any
            self.uninitialize();

            self.size = new_size;
            let layout = Layout::from_size_align(
                (new_size as usize) * std::mem::size_of::<HashBucket>(),
                alignment,
            )
            .expect("Invalid layout");

            // Allocate zeroed memory
            let ptr = unsafe { alloc_zeroed(layout) };
            if ptr.is_null() {
                return Status::OutOfMemory;
            }
            self.buckets = NonNull::new(ptr as *mut HashBucket);
        } else if let Some(ptr) = self.buckets {
            // Clear existing buckets
            unsafe {
                std::ptr::write_bytes(ptr.as_ptr(), 0, new_size as usize);
            }
        }

        // Verify no operations in progress
        debug_assert_eq!(self.pending_checkpoint_writes.load(Ordering::Relaxed), 0);
        debug_assert_eq!(self.pending_recover_reads.load(Ordering::Relaxed), 0);
        debug_assert!(!self.checkpoint_pending.load(Ordering::Relaxed));
        debug_assert!(!self.checkpoint_failed.load(Ordering::Relaxed));
        debug_assert!(!self.recover_pending.load(Ordering::Relaxed));
        debug_assert!(!self.recover_failed.load(Ordering::Relaxed));

        Status::Ok
    }

    /// Uninitialize and free the hash table
    pub fn uninitialize(&mut self) {
        if let Some(ptr) = self.buckets.take() {
            if self.size > 0 {
                let layout = Layout::from_size_align(
                    (self.size as usize) * std::mem::size_of::<HashBucket>(),
                    CACHE_LINE_BYTES,
                )
                .expect("Invalid layout");
                unsafe {
                    dealloc(ptr.as_ptr() as *mut u8, layout);
                }
            }
        }
        self.size = 0;
    }

    /// Get the bucket for the given hash
    #[inline]
    pub fn bucket(&self, hash: KeyHash) -> &HashBucket {
        debug_assert!(self.buckets.is_some());
        let index = hash.hash_table_index(self.size);
        unsafe { &*self.buckets.unwrap().as_ptr().add(index) }
    }

    /// Get a mutable bucket for the given hash
    #[inline]
    pub fn bucket_mut(&mut self, hash: KeyHash) -> &mut HashBucket {
        debug_assert!(self.buckets.is_some());
        let index = hash.hash_table_index(self.size);
        unsafe { &mut *self.buckets.unwrap().as_ptr().add(index) }
    }

    /// Get the bucket at a specific index
    #[inline]
    pub fn bucket_at(&self, index: u64) -> &HashBucket {
        debug_assert!(index < self.size);
        debug_assert!(self.buckets.is_some());
        unsafe { &*self.buckets.unwrap().as_ptr().add(index as usize) }
    }

    /// Get a mutable bucket at a specific index
    #[inline]
    pub fn bucket_at_mut(&mut self, index: u64) -> &mut HashBucket {
        debug_assert!(index < self.size);
        debug_assert!(self.buckets.is_some());
        unsafe { &mut *self.buckets.unwrap().as_ptr().add(index as usize) }
    }

    /// Get the number of buckets
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Check if the table is initialized
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.buckets.is_some()
    }

    /// Get a raw pointer to the bucket array
    pub fn as_ptr(&self) -> *const HashBucket {
        self.buckets
            .map(|p| p.as_ptr() as *const _)
            .unwrap_or(std::ptr::null())
    }

    /// Get a mutable raw pointer to the bucket array
    pub fn as_mut_ptr(&mut self) -> *mut HashBucket {
        self.buckets
            .map(|p| p.as_ptr())
            .unwrap_or(std::ptr::null_mut())
    }

    /// Start a checkpoint operation
    pub fn start_checkpoint(&self) -> Result<(), Status> {
        if self.checkpoint_pending.load(Ordering::Acquire) {
            return Err(Status::Aborted);
        }

        debug_assert_eq!(self.pending_checkpoint_writes.load(Ordering::Relaxed), 0);

        self.checkpoint_failed.store(false, Ordering::Release);
        self.checkpoint_pending.store(true, Ordering::Release);
        self.pending_checkpoint_writes
            .store(NUM_MERGE_CHUNKS as u64, Ordering::Release);

        Ok(())
    }

    /// Complete a checkpoint write
    pub fn complete_checkpoint_write(&self, success: bool) {
        if !success {
            self.checkpoint_failed.store(true, Ordering::Release);
        }

        if self
            .pending_checkpoint_writes
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.checkpoint_pending.store(false, Ordering::Release);
        }
    }

    /// Check if checkpoint is complete
    pub fn checkpoint_complete(&self, wait: bool) -> Status {
        if wait {
            while self.checkpoint_pending.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        }

        if !self.checkpoint_pending.load(Ordering::Acquire) {
            if self.checkpoint_failed.load(Ordering::Acquire) {
                Status::IoError
            } else {
                Status::Ok
            }
        } else {
            Status::Pending
        }
    }

    /// Start a recovery operation
    pub fn start_recovery(&self) -> Result<(), Status> {
        if self.recover_pending.load(Ordering::Acquire) {
            return Err(Status::Aborted);
        }

        debug_assert_eq!(self.pending_recover_reads.load(Ordering::Relaxed), 0);

        self.recover_failed.store(false, Ordering::Release);
        self.recover_pending.store(true, Ordering::Release);
        self.pending_recover_reads
            .store(NUM_MERGE_CHUNKS as u64, Ordering::Release);

        Ok(())
    }

    /// Complete a recovery read
    pub fn complete_recovery_read(&self, success: bool) {
        if !success {
            self.recover_failed.store(true, Ordering::Release);
        }

        if self.pending_recover_reads.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.recover_pending.store(false, Ordering::Release);
        }
    }

    /// Check if recovery is complete
    pub fn recovery_complete(&self, wait: bool) -> Status {
        if wait {
            while self.recover_pending.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        }

        if !self.recover_pending.load(Ordering::Acquire) {
            if self.recover_failed.load(Ordering::Acquire) {
                Status::IoError
            } else {
                Status::Ok
            }
        } else {
            Status::Pending
        }
    }

    /// Get checkpoint chunk size
    pub fn checkpoint_chunk_size(&self) -> u64 {
        self.size / NUM_MERGE_CHUNKS as u64
    }

    /// Get the byte offset for a checkpoint chunk
    pub fn checkpoint_chunk_offset(&self, chunk_index: u32) -> u64 {
        let chunk_size = self.checkpoint_chunk_size();
        chunk_index as u64 * chunk_size * std::mem::size_of::<HashBucket>() as u64
    }

    /// Get the byte size for a checkpoint chunk
    pub fn checkpoint_chunk_bytes(&self) -> u64 {
        self.checkpoint_chunk_size() * std::mem::size_of::<HashBucket>() as u64
    }
}

impl Default for InternalHashTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InternalHashTable {
    fn drop(&mut self) {
        self.uninitialize();
    }
}

// Safety: InternalHashTable uses atomic operations for concurrent access
unsafe impl Send for InternalHashTable {}
unsafe impl Sync for InternalHashTable {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_table_initialize() {
        let mut table = InternalHashTable::new();

        let result = table.initialize(1024, CACHE_LINE_BYTES);
        assert_eq!(result, Status::Ok);
        assert!(table.is_initialized());
        assert_eq!(table.size(), 1024);
    }

    #[test]
    fn test_hash_table_bucket_access() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        let hash = KeyHash::new(12345);
        let _bucket = table.bucket(hash);
        let _bucket_mut = table.bucket_mut(hash);
    }

    #[test]
    fn test_hash_table_uninitialize() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        table.uninitialize();
        assert!(!table.is_initialized());
        assert_eq!(table.size(), 0);
    }

    #[test]
    #[should_panic]
    fn test_hash_table_non_power_of_two() {
        let mut table = InternalHashTable::new();
        table.initialize(1000, CACHE_LINE_BYTES); // Not a power of 2
    }

    #[test]
    fn test_hash_table_default() {
        let table = InternalHashTable::default();
        assert!(!table.is_initialized());
        assert_eq!(table.size(), 0);
    }

    #[test]
    fn test_hash_table_bucket_at() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        let _bucket = table.bucket_at(0);
        let _bucket = table.bucket_at(100);
        let _bucket = table.bucket_at(1023);
    }

    #[test]
    fn test_hash_table_bucket_at_mut() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        let _bucket = table.bucket_at_mut(0);
        let _bucket = table.bucket_at_mut(100);
        let _bucket = table.bucket_at_mut(1023);
    }

    #[test]
    fn test_hash_table_as_ptr() {
        let mut table = InternalHashTable::new();

        // Before init, should be null
        assert!(table.as_ptr().is_null());

        table.initialize(1024, CACHE_LINE_BYTES);

        // After init, should not be null
        assert!(!table.as_ptr().is_null());
    }

    #[test]
    fn test_hash_table_as_mut_ptr() {
        let mut table = InternalHashTable::new();

        // Before init, should be null
        assert!(table.as_mut_ptr().is_null());

        table.initialize(1024, CACHE_LINE_BYTES);

        // After init, should not be null
        assert!(!table.as_mut_ptr().is_null());
    }

    #[test]
    fn test_hash_table_reinitialize_same_size() {
        let mut table = InternalHashTable::new();

        table.initialize(1024, CACHE_LINE_BYTES);
        let ptr1 = table.as_ptr();

        // Re-initialize with same size should clear existing
        table.initialize(1024, CACHE_LINE_BYTES);
        let ptr2 = table.as_ptr();

        // Should use the same allocation (cleared)
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn test_hash_table_reinitialize_different_size() {
        let mut table = InternalHashTable::new();

        table.initialize(1024, CACHE_LINE_BYTES);
        let ptr1 = table.as_ptr();

        // Re-initialize with different size
        table.initialize(2048, CACHE_LINE_BYTES);
        let ptr2 = table.as_ptr();

        // Should be a different allocation
        assert_ne!(ptr1, ptr2);
        assert_eq!(table.size(), 2048);
    }

    #[test]
    fn test_hash_table_checkpoint_operations() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        // Start checkpoint
        let result = table.start_checkpoint();
        assert!(result.is_ok());

        // Check status is pending
        let status = table.checkpoint_complete(false);
        assert_eq!(status, Status::Pending);

        // Complete checkpoint writes
        for _ in 0..NUM_MERGE_CHUNKS {
            table.complete_checkpoint_write(true);
        }

        // Now should be complete
        let status = table.checkpoint_complete(false);
        assert_eq!(status, Status::Ok);
    }

    #[test]
    fn test_hash_table_checkpoint_failure() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        table.start_checkpoint().unwrap();

        // Complete with failures
        for i in 0..NUM_MERGE_CHUNKS {
            table.complete_checkpoint_write(i != 0); // First one fails
        }

        let status = table.checkpoint_complete(false);
        assert_eq!(status, Status::IoError);
    }

    #[test]
    fn test_hash_table_checkpoint_already_in_progress() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        // Start first checkpoint
        let result = table.start_checkpoint();
        assert!(result.is_ok());

        // Try to start another - should fail
        let result = table.start_checkpoint();
        assert!(result.is_err());

        // Complete the first
        for _ in 0..NUM_MERGE_CHUNKS {
            table.complete_checkpoint_write(true);
        }
    }

    #[test]
    fn test_hash_table_recovery_operations() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        // Start recovery
        let result = table.start_recovery();
        assert!(result.is_ok());

        // Check status is pending
        let status = table.recovery_complete(false);
        assert_eq!(status, Status::Pending);

        // Complete recovery reads
        for _ in 0..NUM_MERGE_CHUNKS {
            table.complete_recovery_read(true);
        }

        // Now should be complete
        let status = table.recovery_complete(false);
        assert_eq!(status, Status::Ok);
    }

    #[test]
    fn test_hash_table_recovery_failure() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        table.start_recovery().unwrap();

        // Complete with failures
        for i in 0..NUM_MERGE_CHUNKS {
            table.complete_recovery_read(i != 0); // First one fails
        }

        let status = table.recovery_complete(false);
        assert_eq!(status, Status::IoError);
    }

    #[test]
    fn test_hash_table_recovery_already_in_progress() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        // Start first recovery
        let result = table.start_recovery();
        assert!(result.is_ok());

        // Try to start another - should fail
        let result = table.start_recovery();
        assert!(result.is_err());

        // Complete the first
        for _ in 0..NUM_MERGE_CHUNKS {
            table.complete_recovery_read(true);
        }
    }

    #[test]
    fn test_hash_table_checkpoint_chunk_calculations() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        let chunk_size = table.checkpoint_chunk_size();
        assert_eq!(chunk_size, 1024 / NUM_MERGE_CHUNKS as u64);

        let offset_0 = table.checkpoint_chunk_offset(0);
        assert_eq!(offset_0, 0);

        let offset_1 = table.checkpoint_chunk_offset(1);
        assert_eq!(
            offset_1,
            chunk_size * std::mem::size_of::<HashBucket>() as u64
        );

        let chunk_bytes = table.checkpoint_chunk_bytes();
        assert_eq!(
            chunk_bytes,
            chunk_size * std::mem::size_of::<HashBucket>() as u64
        );
    }

    #[test]
    fn test_hash_table_checkpoint_complete_with_wait() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        table.start_checkpoint().unwrap();

        // Complete all writes first
        for _ in 0..NUM_MERGE_CHUNKS {
            table.complete_checkpoint_write(true);
        }

        // Wait should return immediately since already complete
        let status = table.checkpoint_complete(true);
        assert_eq!(status, Status::Ok);
    }

    #[test]
    fn test_hash_table_recovery_complete_with_wait() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);

        table.start_recovery().unwrap();

        // Complete all reads first
        for _ in 0..NUM_MERGE_CHUNKS {
            table.complete_recovery_read(true);
        }

        // Wait should return immediately since already complete
        let status = table.recovery_complete(true);
        assert_eq!(status, Status::Ok);
    }

    #[test]
    fn test_hash_table_drop() {
        let mut table = InternalHashTable::new();
        table.initialize(1024, CACHE_LINE_BYTES);
        // Drop should clean up properly - no crash
    }

    #[test]
    fn test_hash_table_uninitialize_when_empty() {
        let mut table = InternalHashTable::new();
        // Should not crash
        table.uninitialize();
        assert!(!table.is_initialized());
    }
}
