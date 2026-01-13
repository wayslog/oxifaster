use std::sync::atomic::Ordering;

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
}

impl OverflowBucketPool {
    pub(super) fn new() -> Self {
        Self {
            buckets: RwLock::new(Vec::new()),
            free_list: Mutex::new(Vec::new()),
        }
    }

    /// Clear and free all overflow buckets.
    ///
    /// This requires `&mut self`, which guarantees exclusive access and avoids races with
    /// concurrent traversal that could otherwise lead to UAF.
    pub(super) fn clear(&mut self) {
        self.free_list.get_mut().clear();
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

    fn reset_bucket(bucket: &HashBucket) {
        for entry in &bucket.entries {
            entry.store(HashBucketEntry::INVALID, Ordering::Release);
        }
        bucket
            .overflow_entry
            .store(HashBucketOverflowEntry::INVALID, Ordering::Release);
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
        if let Some(addr) = self.free_list.lock().pop() {
            if let Some(bucket) = self.bucket_ref(addr) {
                Self::reset_bucket(bucket);
                return addr;
            }
            // Discard invalid addresses and fall through to allocate a new bucket.
        }

        let mut buckets = self.buckets.write();
        let bucket = Box::new(HashBucket::new());
        buckets.push(Box::into_raw(bucket));
        FixedPageAddress::new(buckets.len() as u64)
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
        assert_eq!(pool.free_list_len(), 0);

        pool.deallocate(a2);
        assert_eq!(pool.free_list_len(), 1);

        let a3 = pool.allocate();
        assert_eq!(a3, a2);
        assert_eq!(pool.free_list_len(), 0);

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
