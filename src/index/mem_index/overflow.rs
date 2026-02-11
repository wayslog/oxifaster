use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use parking_lot::{Mutex, RwLock};

use crate::index::hash_bucket::FixedPageAddress;
use crate::index::{HashBucket, HashBucketEntry, HashBucketOverflowEntry};

/// Overflow bucket pool (used by `MemHashIndex` only).
///
/// Design notes:
/// - `HashBucketOverflowEntry` stores a `FixedPageAddress` (48-bit); `0` means "empty".
/// - Addresses are 1-based: the N-th bucket has address N (so it is never `0`).
/// - Each bucket is allocated as a standalone `Box<HashBucket>`, so the pointer is stable.
///   Even if the backing `Vec` grows, bucket memory addresses do not move, so it is safe to use
///   the pointer after unlocking.
pub(super) struct OverflowBucketPool {
    buckets: RwLock<Vec<*mut HashBucket>>,
    free_list: Mutex<Vec<FixedPageAddress>>,
    refill_in_progress: AtomicBool,
}

impl OverflowBucketPool {
    // Refill the pool in chunks to amortize the cost of taking the `buckets` write lock under
    // concurrent inserts that create long overflow chains.
    const REFILL_BATCH_SMALL: usize = 8;
    const REFILL_BATCH_LARGE: usize = 64;
    const REFILL_THRESHOLD: usize = 32;

    pub(super) fn new() -> Self {
        Self {
            buckets: RwLock::new(Vec::new()),
            free_list: Mutex::new(Vec::new()),
            refill_in_progress: AtomicBool::new(false),
        }
    }

    /// Clear and free all overflow buckets.
    ///
    /// This requires `&mut self`, which guarantees exclusive access and avoids races with
    /// concurrent traversal that could otherwise lead to UAF.
    pub(super) fn clear(&mut self) {
        self.free_list.get_mut().clear();
        self.refill_in_progress
            .store(false, AtomicOrdering::Relaxed);
        let mut buckets = self.buckets.write();
        for ptr in buckets.drain(..) {
            // SAFETY: `ptr` originates from `Box::into_raw` and is freed exactly once, here or in
            // `Drop`, after being removed from the pool's vector.
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }

    pub(super) fn len(&self) -> usize {
        self.buckets.read().len()
    }

    pub(super) fn buckets_read(&self) -> parking_lot::RwLockReadGuard<'_, Vec<*mut HashBucket>> {
        self.buckets.read()
    }

    #[inline]
    pub(super) fn bucket_ptr_in(
        &self,
        buckets: &[*mut HashBucket],
        addr: FixedPageAddress,
    ) -> Option<*const HashBucket> {
        if addr.is_invalid() {
            return None;
        }
        let index = addr.control() as usize;
        if index == 0 {
            return None;
        }
        buckets
            .get(index - 1)
            .copied()
            .map(|p| p as *const HashBucket)
    }

    fn reset_bucket(bucket: &HashBucket) {
        for entry in &bucket.entries {
            entry.store(HashBucketEntry::INVALID, Ordering::Release);
        }
        bucket
            .overflow_entry
            .store(HashBucketOverflowEntry::INVALID, Ordering::Release);
    }

    /// Allocate an overflow bucket and return both its fixed page address and a stable pointer to
    /// the bucket.
    ///
    /// Returning the pointer avoids an immediate `bucket_ptr()` lookup (and its lock acquisition)
    /// on the hot append path.
    pub(super) fn allocate_with_ptr(&self) -> (FixedPageAddress, *const HashBucket) {
        // Fast path: grab an address from the free list.
        if let Some(addr) = self.free_list.lock().pop() {
            if let Some(ptr) = self.bucket_ptr(addr) {
                // SAFETY: `ptr` is owned by this pool and points to a valid bucket.
                let bucket = unsafe { &*ptr };
                Self::reset_bucket(bucket);
                return (addr, ptr);
            }
            // Discard invalid addresses and fall through to refill.
        }

        struct RefillGuard<'a>(&'a AtomicBool);
        impl Drop for RefillGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, AtomicOrdering::Release);
            }
        }

        // Avoid stampeding refills under contention: only one thread allocates a new batch when
        // the free list is empty. Other threads spin briefly until the refiller publishes spare
        // addresses.
        loop {
            if self
                .refill_in_progress
                .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                let _guard = RefillGuard(&self.refill_in_progress);

                // Another thread may have refilled while we were acquiring the flag.
                if let Some(addr) = self.free_list.lock().pop() {
                    if let Some(ptr) = self.bucket_ptr(addr) {
                        // SAFETY: `ptr` is owned by this pool and points to a valid bucket.
                        let bucket = unsafe { &*ptr };
                        Self::reset_bucket(bucket);
                        return (addr, ptr);
                    }
                }

                // Slow-path: allocate a batch of new buckets and push the rest into the free list.
                // This reduces contention on the `buckets` write lock when many threads need new
                // overflow buckets at the same time.
                let mut first_addr: Option<FixedPageAddress> = None;
                let mut first_ptr: *const HashBucket = std::ptr::null();
                let mut spare_addrs: Vec<FixedPageAddress> = Vec::new();

                // Compute the batch size using a short read lock, then do the actual heap
                // allocations *outside* the buckets write lock to keep the critical section small.
                let approx_len = self.buckets.read().len();
                let batch = if approx_len < Self::REFILL_THRESHOLD {
                    Self::REFILL_BATCH_SMALL
                } else {
                    Self::REFILL_BATCH_LARGE
                };
                spare_addrs.reserve(batch.saturating_sub(1));

                let mut new_ptrs: Vec<*mut HashBucket> = Vec::with_capacity(batch);
                for _ in 0..batch {
                    let bucket = Box::new(HashBucket::new());
                    new_ptrs.push(Box::into_raw(bucket));
                }

                {
                    let mut buckets = self.buckets.write();
                    let start = buckets.len();
                    buckets.reserve(new_ptrs.len());

                    for (i, ptr) in new_ptrs.into_iter().enumerate() {
                        buckets.push(ptr);

                        let addr = FixedPageAddress::new((start + i + 1) as u64);
                        if first_addr.is_none() {
                            first_addr = Some(addr);
                            first_ptr = ptr as *const HashBucket;
                        } else {
                            spare_addrs.push(addr);
                        }
                    }
                }

                if !spare_addrs.is_empty() {
                    self.free_list.lock().extend(spare_addrs);
                }

                // SAFETY: We always allocate at least one bucket.
                return (first_addr.unwrap(), first_ptr);
            }

            // Another thread is refilling; spin briefly then retry.
            for _ in 0..32 {
                if let Some(addr) = self.free_list.lock().pop() {
                    if let Some(ptr) = self.bucket_ptr(addr) {
                        // SAFETY: `ptr` is owned by this pool and points to a valid bucket.
                        let bucket = unsafe { &*ptr };
                        Self::reset_bucket(bucket);
                        return (addr, ptr);
                    }
                }
                std::hint::spin_loop();
            }

            std::thread::yield_now();
        }
    }

    fn bucket_ref(&self, addr: FixedPageAddress) -> Option<&HashBucket> {
        self.bucket_ptr(addr).map(|ptr| {
            // SAFETY: The returned pointer is owned by this pool and points to a valid `HashBucket`.
            // The bucket itself contains atomics, so concurrent mutation via atomic ops is allowed.
            unsafe { &*ptr }
        })
    }

    /// Allocate an overflow bucket and return its fixed page address (1-based, `0` is reserved).
    pub(super) fn allocate(&self) -> FixedPageAddress {
        self.allocate_with_ptr().0
    }

    /// Return an unlinked overflow bucket to the pool (e.g. after a failed CAS).
    pub(super) fn deallocate(&self, addr: FixedPageAddress) {
        if addr.is_invalid() || addr.control() == 0 {
            return;
        }
        let Some(bucket) = self.bucket_ref(addr) else {
            return;
        };

        // This method must only be called for buckets that are not linked into any hash chain
        // (e.g. after a failed CAS), so no other thread may concurrently access the bucket.
        Self::reset_bucket(bucket);
        self.free_list.lock().push(addr);
    }

    /// Return an unlinked overflow bucket to the pool using a direct pointer.
    ///
    /// This avoids an extra `bucket_ptr()` lookup when the caller already holds the pointer
    /// returned by `allocate_with_ptr()`. The caller must only use this for buckets that were not
    /// linked into any hash chain (e.g. after a failed CAS).
    pub(super) fn deallocate_with_ptr(
        &self,
        addr: FixedPageAddress,
        bucket_ptr: *const HashBucket,
    ) {
        if addr.is_invalid() || addr.control() == 0 {
            return;
        }

        // The caller guarantees the bucket is unlinked, so no other thread can concurrently
        // access it. We purposely do *not* reset the bucket here:
        // - `allocate_with_ptr()` already returns a reset bucket (or a freshly allocated bucket,
        //   which is already in the invalid state).
        // - `allocate_with_ptr()` resets buckets again when popping from `free_list`.
        // Avoiding the extra reset reduces the cost of losing the append CAS under contention.
        debug_assert_eq!(
            self.bucket_ptr(addr).map(|p| p as usize),
            Some(bucket_ptr as usize)
        );
        self.free_list.lock().push(addr);
    }

    /// Get an overflow bucket pointer by fixed page address.
    pub(super) fn bucket_ptr(&self, addr: FixedPageAddress) -> Option<*const HashBucket> {
        if addr.is_invalid() {
            return None;
        }
        let index = addr.control() as usize;
        if index == 0 {
            return None;
        }
        let buckets = self.buckets.read();
        buckets
            .get(index - 1)
            .copied()
            .map(|p| p as *const HashBucket)
    }

    /// Return a snapshot of all overflow bucket pointers (for checkpointing).
    pub(super) fn snapshot_ptrs(&self) -> Vec<*const HashBucket> {
        let buckets = self.buckets.read();
        buckets
            .iter()
            .copied()
            .map(|p| p as *const HashBucket)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn free_list_len(&self) -> usize {
        self.free_list.lock().len()
    }
}

impl Default for OverflowBucketPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for OverflowBucketPool {
    fn drop(&mut self) {
        // Drop should not block. At this point there is no concurrent access (the struct is being
        // destroyed).
        self.free_list.get_mut().clear();
        let buckets = self.buckets.get_mut();
        for ptr in buckets.drain(..) {
            // SAFETY: Same rationale as `clear()`.
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OverflowBucketPool;
    use crate::index::{HashBucketEntry, HashBucketOverflowEntry};
    use std::sync::atomic::Ordering;

    #[test]
    fn test_overflow_bucket_pool_reuses_deallocated_address() {
        let pool = OverflowBucketPool::new();

        let a1 = pool.allocate();
        let a2 = pool.allocate();
        assert_ne!(a1.control(), 0);
        assert_ne!(a2.control(), 0);
        assert_ne!(a1, a2);
        let free_before = pool.free_list_len();

        pool.deallocate(a2);
        assert_eq!(pool.free_list_len(), free_before + 1);

        let a3 = pool.allocate();
        assert_eq!(a3, a2);
        assert_eq!(pool.free_list_len(), free_before);

        let bucket = pool.bucket_ref(a3).unwrap();
        for entry in &bucket.entries {
            assert_eq!(entry.load(Ordering::Acquire), HashBucketEntry::INVALID);
        }
        assert_eq!(
            bucket.overflow_entry.load(Ordering::Acquire),
            HashBucketOverflowEntry::INVALID
        );
    }
}
