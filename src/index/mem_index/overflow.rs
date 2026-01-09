use std::sync::atomic::Ordering;

use parking_lot::{Mutex, RwLock};

use crate::index::hash_bucket::FixedPageAddress;
use crate::index::{HashBucket, HashBucketEntry, HashBucketOverflowEntry};

/// overflow bucket 池（仅用于 MemHashIndex）
///
/// 设计说明：
/// - `HashBucketOverflowEntry` 中存的是 `FixedPageAddress`（48-bit），0 表示空。
/// - 这里用 1-based 编号：第 N 个 bucket 的地址为 N（因此永不为 0）。
/// - bucket 本体使用 `Box<HashBucket>` 单独分配，指针稳定；
///   即使 `Vec` 发生扩容，bucket 内存地址也不变，因此可在解锁后安全使用其指针。
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

    /// 清空并释放所有 overflow bucket。
    ///
    /// 需要 `&mut self`：在安全 Rust 中这保证了调用方拥有独占访问，避免与并发遍历产生 UAF。
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

    /// 分配一个新的 overflow bucket，并返回其固定页地址（1-based，0 保留为“空”）。
    pub(super) fn allocate(&self) -> FixedPageAddress {
        if let Some(addr) = self.free_list.lock().pop() {
            if let Some(ptr) = self.bucket_ptr(addr) {
                // SAFETY: `ptr` originates from `Box::into_raw` and remains owned by the pool.
                // Resetting only performs atomic stores to fields inside the bucket.
                let bucket = unsafe { &*ptr };
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

    /// 归还一个未链接的 overflow bucket（用于 CAS 失败等场景）。
    pub(super) fn deallocate(&self, addr: FixedPageAddress) {
        if addr.is_invalid() || addr.control() == 0 {
            return;
        }
        let Some(ptr) = self.bucket_ptr(addr) else {
            return;
        };

        // SAFETY: `bucket_ptr` only returns pointers owned by this pool. This method must only be
        // called for buckets that are not linked into any hash chain (e.g. after a failed CAS),
        // so no other thread may concurrently access the returned bucket.
        let bucket = unsafe { &*ptr };
        Self::reset_bucket(bucket);
        self.free_list.lock().push(addr);
    }

    /// 根据 fixed page address 获取 overflow bucket 指针。
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

    /// 获取所有 overflow bucket 的快照（用于 checkpoint）。
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
        // Drop 时不应阻塞，且此时不会有并发访问（结构体正在析构）。
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

        let ptr = pool.bucket_ptr(a3).unwrap();
        // SAFETY: ptr 指向池内 bucket。
        let bucket = unsafe { &*ptr };
        for entry in &bucket.entries {
            assert_eq!(entry.load(Ordering::Acquire), HashBucketEntry::INVALID);
        }
        assert_eq!(
            bucket.overflow_entry.load(Ordering::Acquire),
            HashBucketOverflowEntry::INVALID
        );
    }
}
