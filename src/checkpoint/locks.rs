//! Checkpoint locks for CPR protocol
//!
//! This module provides locking primitives used during checkpoint operations
//! to protect concurrent access to records during the CPR (Concurrent Prefix Recovery) protocol.
//!
//! Based on C++ FASTER's checkpoint_locks.h implementation.
//!
//! # Overview
//!
//! During a checkpoint, records may be in one of two states:
//! - **Old version**: Records that were created before the checkpoint started
//! - **New version**: Records that were created after the checkpoint started
//!
//! The checkpoint locks ensure that:
//! - Old version records can only be locked when no new version locks are held
//! - New version records can only be locked when no old version locks are held
//!
//! This mutual exclusion prevents data races during the checkpoint process.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::index::KeyHash;

/// Cache line size for alignment
const CACHE_LINE_BYTES: usize = 64;

/// A checkpoint lock that tracks old and new version lock counts.
///
/// The lock uses a 64-bit control word split into two 32-bit counters:
/// - Lower 32 bits: old version lock count
/// - Upper 32 bits: new version lock count
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct CheckpointLock {
    /// Old version lock count (lower 32 bits of control)
    pub old_lock_count: u32,
    /// New version lock count (upper 32 bits of control)
    pub new_lock_count: u32,
}

impl CheckpointLock {
    /// Create a new checkpoint lock with zero counts
    #[inline]
    pub const fn new() -> Self {
        Self {
            old_lock_count: 0,
            new_lock_count: 0,
        }
    }

    /// Create from a control value
    #[inline]
    pub const fn from_control(control: u64) -> Self {
        Self {
            old_lock_count: control as u32,
            new_lock_count: (control >> 32) as u32,
        }
    }

    /// Convert to a control value
    #[inline]
    pub const fn to_control(&self) -> u64 {
        (self.old_lock_count as u64) | ((self.new_lock_count as u64) << 32)
    }

    /// Create with specific counts
    #[inline]
    pub const fn with_counts(old_lock_count: u32, new_lock_count: u32) -> Self {
        Self {
            old_lock_count,
            new_lock_count,
        }
    }
}

/// Atomic checkpoint lock for thread-safe operations.
///
/// This provides atomic operations for locking old and new versions of records
/// during checkpoint operations.
#[repr(align(8))]
pub struct AtomicCheckpointLock {
    control: AtomicU64,
}

impl AtomicCheckpointLock {
    /// Create a new atomic checkpoint lock
    #[inline]
    pub const fn new() -> Self {
        Self {
            control: AtomicU64::new(0),
        }
    }

    /// Try to lock the old version of a record.
    ///
    /// This will succeed only if no new version locks are currently held.
    /// Multiple old version locks can be held simultaneously.
    ///
    /// # Returns
    /// - `true` if the lock was acquired
    /// - `false` if new version locks are held (cannot acquire)
    #[inline]
    pub fn try_lock_old(&self) -> bool {
        let mut expected = CheckpointLock::from_control(self.control.load(Ordering::Acquire));

        // Only proceed if no new locks are held (mutual exclusion invariant)
        while expected.new_lock_count == 0 {
            // Preserve new_lock_count (which we know is 0) for clarity and robustness
            let desired = CheckpointLock::with_counts(
                expected.old_lock_count + 1,
                expected.new_lock_count,
            );

            match self.control.compare_exchange_weak(
                expected.to_control(),
                desired.to_control(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => {
                    expected = CheckpointLock::from_control(current);
                }
            }
        }

        false
    }

    /// Unlock the old version of a record.
    ///
    /// # Panics
    /// Debug builds will panic if the old lock count is already zero.
    #[inline]
    pub fn unlock_old(&self) {
        let decrement = CheckpointLock::with_counts(1, 0).to_control();
        let prev = self.control.fetch_sub(decrement, Ordering::AcqRel);

        debug_assert!(
            CheckpointLock::from_control(prev).old_lock_count > 0,
            "unlock_old called when old_lock_count is 0"
        );
    }

    /// Try to lock the new version of a record.
    ///
    /// This will succeed only if no old version locks are currently held.
    /// Multiple new version locks can be held simultaneously.
    ///
    /// # Returns
    /// - `true` if the lock was acquired
    /// - `false` if old version locks are held (cannot acquire)
    #[inline]
    pub fn try_lock_new(&self) -> bool {
        let mut expected = CheckpointLock::from_control(self.control.load(Ordering::Acquire));

        // Only proceed if no old locks are held (mutual exclusion invariant)
        while expected.old_lock_count == 0 {
            // Preserve old_lock_count (which we know is 0) for clarity and robustness
            let desired = CheckpointLock::with_counts(
                expected.old_lock_count,
                expected.new_lock_count + 1,
            );

            match self.control.compare_exchange_weak(
                expected.to_control(),
                desired.to_control(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => {
                    expected = CheckpointLock::from_control(current);
                }
            }
        }

        false
    }

    /// Unlock the new version of a record.
    ///
    /// # Panics
    /// Debug builds will panic if the new lock count is already zero.
    #[inline]
    pub fn unlock_new(&self) {
        let decrement = CheckpointLock::with_counts(0, 1).to_control();
        let prev = self.control.fetch_sub(decrement, Ordering::AcqRel);

        debug_assert!(
            CheckpointLock::from_control(prev).new_lock_count > 0,
            "unlock_new called when new_lock_count is 0"
        );
    }

    /// Check if any old version locks are held
    #[inline]
    pub fn old_locked(&self) -> bool {
        let lock = CheckpointLock::from_control(self.control.load(Ordering::Acquire));
        lock.old_lock_count > 0
    }

    /// Check if any new version locks are held
    #[inline]
    pub fn new_locked(&self) -> bool {
        let lock = CheckpointLock::from_control(self.control.load(Ordering::Acquire));
        lock.new_lock_count > 0
    }

    /// Check if any locks are held (old or new)
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.control.load(Ordering::Acquire) != 0
    }

    /// Get the current lock state
    #[inline]
    pub fn load(&self) -> CheckpointLock {
        CheckpointLock::from_control(self.control.load(Ordering::Acquire))
    }
}

impl Default for AtomicCheckpointLock {
    fn default() -> Self {
        Self::new()
    }
}

// Verify size matches C++ implementation
const _: () = assert!(std::mem::size_of::<AtomicCheckpointLock>() == 8);

/// A table of checkpoint locks indexed by key hash.
///
/// This provides a fixed-size array of locks that can be used to protect
/// records during checkpoint operations. The lock for a particular key
/// is determined by hashing the key and using the hash to index into the table.
pub struct CheckpointLocks {
    /// Number of locks in the table (must be power of 2)
    size: u64,
    /// The lock table
    locks: Vec<AtomicCheckpointLock>,
}

impl CheckpointLocks {
    /// Create a new empty checkpoint locks table
    pub fn new() -> Self {
        Self {
            size: 0,
            locks: Vec::new(),
        }
    }

    /// Initialize the checkpoint locks table with the given size.
    ///
    /// # Arguments
    /// * `size` - Number of locks (must be power of 2 and less than i32::MAX)
    ///
    /// # Panics
    /// Panics if size is not a power of 2 or exceeds i32::MAX.
    pub fn initialize(&mut self, size: u64) {
        assert!(size < i32::MAX as u64, "Size too large");
        assert!(
            size == 0 || size.is_power_of_two(),
            "Size must be power of 2"
        );

        self.size = size;

        if size > 0 {
            // Allocate cache-line aligned locks
            let mut locks = Vec::with_capacity(size as usize);
            for _ in 0..size {
                locks.push(AtomicCheckpointLock::new());
            }
            self.locks = locks;
        } else {
            self.locks = Vec::new();
        }
    }

    /// Create and initialize a checkpoint locks table with the given size.
    pub fn with_size(size: u64) -> Self {
        let mut locks = Self::new();
        locks.initialize(size);
        locks
    }

    /// Free the lock table.
    ///
    /// # Panics
    /// Debug builds will panic if any locks are still held.
    pub fn free(&mut self) {
        #[cfg(debug_assertions)]
        {
            for lock in &self.locks {
                debug_assert!(!lock.old_locked(), "Old lock still held during free");
                debug_assert!(!lock.new_locked(), "New lock still held during free");
            }
        }

        self.size = 0;
        self.locks = Vec::new();
    }

    /// Get the size of the lock table
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Check if the lock table is empty (size is 0)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Get the lock for a given key hash.
    ///
    /// # Panics
    /// Panics if the lock table is empty (size is 0).
    #[inline]
    pub fn get_lock(&self, hash: KeyHash) -> &AtomicCheckpointLock {
        debug_assert!(self.size > 0, "Lock table is empty");
        let index = hash.hash_table_index(self.size);
        &self.locks[index]
    }

    /// Get the lock for a given raw hash value.
    #[inline]
    pub fn get_lock_by_hash(&self, hash: u64) -> &AtomicCheckpointLock {
        self.get_lock(KeyHash::new(hash))
    }
}

impl Default for CheckpointLocks {
    fn default() -> Self {
        Self::new()
    }
}

/// Type of lock held by a CheckpointLockGuard
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// No lock held
    None,
    /// Old version lock
    Old,
    /// New version lock
    New,
}

/// RAII guard for checkpoint locks.
///
/// This guard automatically releases any held locks when dropped.
/// It can hold at most one old lock and one new lock simultaneously.
pub struct CheckpointLockGuard<'a> {
    /// The lock being guarded (None if lock table is empty)
    lock: Option<&'a AtomicCheckpointLock>,
    /// Whether we hold an old version lock
    locked_old: bool,
    /// Whether we hold a new version lock
    locked_new: bool,
}

impl<'a> CheckpointLockGuard<'a> {
    /// Create a new lock guard for the given lock table and key hash.
    ///
    /// The guard does not acquire any locks upon creation - you must
    /// call `try_lock_old()` or `try_lock_new()` to acquire locks.
    pub fn new(locks: &'a CheckpointLocks, hash: KeyHash) -> Self {
        let lock = if locks.size() > 0 {
            Some(locks.get_lock(hash))
        } else {
            None
        };

        Self {
            lock,
            locked_old: false,
            locked_new: false,
        }
    }

    /// Create a guard directly from an atomic lock reference.
    pub fn from_lock(lock: &'a AtomicCheckpointLock) -> Self {
        Self {
            lock: Some(lock),
            locked_old: false,
            locked_new: false,
        }
    }

    /// Try to acquire an old version lock.
    ///
    /// # Panics
    /// Panics if no lock is available (empty lock table) or if
    /// an old lock is already held by this guard.
    #[inline]
    pub fn try_lock_old(&mut self) -> bool {
        let lock = self.lock.expect("No lock available");
        debug_assert!(!self.locked_old, "Already holding old lock");

        self.locked_old = lock.try_lock_old();
        self.locked_old
    }

    /// Try to acquire a new version lock.
    ///
    /// # Panics
    /// Panics if no lock is available (empty lock table) or if
    /// a new lock is already held by this guard.
    #[inline]
    pub fn try_lock_new(&mut self) -> bool {
        let lock = self.lock.expect("No lock available");
        debug_assert!(!self.locked_new, "Already holding new lock");

        self.locked_new = lock.try_lock_new();
        self.locked_new
    }

    /// Check if any old version locks are held on the underlying lock.
    #[inline]
    pub fn old_locked(&self) -> bool {
        self.lock.map_or(false, |l| l.old_locked())
    }

    /// Check if any new version locks are held on the underlying lock.
    #[inline]
    pub fn new_locked(&self) -> bool {
        self.lock.map_or(false, |l| l.new_locked())
    }

    /// Check if this guard holds an old lock.
    #[inline]
    pub fn holds_old(&self) -> bool {
        self.locked_old
    }

    /// Check if this guard holds a new lock.
    #[inline]
    pub fn holds_new(&self) -> bool {
        self.locked_new
    }

    /// Check if a lock is available (lock table is not empty).
    #[inline]
    pub fn has_lock(&self) -> bool {
        self.lock.is_some()
    }

    /// Manually release the old lock if held.
    pub fn unlock_old(&mut self) {
        if self.locked_old {
            if let Some(lock) = self.lock {
                lock.unlock_old();
                self.locked_old = false;
            }
        }
    }

    /// Manually release the new lock if held.
    pub fn unlock_new(&mut self) {
        if self.locked_new {
            if let Some(lock) = self.lock {
                lock.unlock_new();
                self.locked_new = false;
            }
        }
    }
}

impl<'a> Drop for CheckpointLockGuard<'a> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock {
            if self.locked_old {
                lock.unlock_old();
            }
            if self.locked_new {
                lock.unlock_new();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_lock_control() {
        let lock = CheckpointLock::new();
        assert_eq!(lock.old_lock_count, 0);
        assert_eq!(lock.new_lock_count, 0);
        assert_eq!(lock.to_control(), 0);

        let lock = CheckpointLock::with_counts(5, 10);
        assert_eq!(lock.old_lock_count, 5);
        assert_eq!(lock.new_lock_count, 10);

        let control = lock.to_control();
        let restored = CheckpointLock::from_control(control);
        assert_eq!(restored.old_lock_count, 5);
        assert_eq!(restored.new_lock_count, 10);
    }

    #[test]
    fn test_atomic_lock_old() {
        let lock = AtomicCheckpointLock::new();

        // Should be able to lock old when no new locks held
        assert!(lock.try_lock_old());
        assert!(lock.old_locked());
        assert!(!lock.new_locked());

        // Should be able to lock old multiple times
        assert!(lock.try_lock_old());
        assert_eq!(lock.load().old_lock_count, 2);

        // Should NOT be able to lock new when old is locked
        assert!(!lock.try_lock_new());

        // Unlock old
        lock.unlock_old();
        lock.unlock_old();
        assert!(!lock.old_locked());
    }

    #[test]
    fn test_atomic_lock_new() {
        let lock = AtomicCheckpointLock::new();

        // Should be able to lock new when no old locks held
        assert!(lock.try_lock_new());
        assert!(lock.new_locked());
        assert!(!lock.old_locked());

        // Should be able to lock new multiple times
        assert!(lock.try_lock_new());
        assert_eq!(lock.load().new_lock_count, 2);

        // Should NOT be able to lock old when new is locked
        assert!(!lock.try_lock_old());

        // Unlock new
        lock.unlock_new();
        lock.unlock_new();
        assert!(!lock.new_locked());
    }

    #[test]
    fn test_checkpoint_locks_table() {
        let mut locks = CheckpointLocks::new();
        assert!(locks.is_empty());

        locks.initialize(16);
        assert_eq!(locks.size(), 16);
        assert!(!locks.is_empty());

        // Get locks for different hashes
        let hash1 = KeyHash::new(0x1234);
        let hash2 = KeyHash::new(0x5678);

        let lock1 = locks.get_lock(hash1);
        let lock2 = locks.get_lock(hash2);

        // Lock them independently
        assert!(lock1.try_lock_old());
        assert!(lock2.try_lock_new());

        lock1.unlock_old();
        lock2.unlock_new();

        locks.free();
        assert!(locks.is_empty());
    }

    #[test]
    fn test_checkpoint_lock_guard() {
        let locks = CheckpointLocks::with_size(16);
        let hash = KeyHash::new(0xABCD);

        {
            let mut guard = CheckpointLockGuard::new(&locks, hash);
            assert!(guard.has_lock());

            // Acquire old lock
            assert!(guard.try_lock_old());
            assert!(guard.holds_old());
            assert!(guard.old_locked());

            // Cannot acquire new while old is held
            assert!(!guard.try_lock_new());
        }

        // Lock should be released after guard is dropped
        let lock = locks.get_lock(hash);
        assert!(!lock.old_locked());
        assert!(!lock.new_locked());
    }

    #[test]
    fn test_checkpoint_lock_guard_new() {
        let locks = CheckpointLocks::with_size(16);
        let hash = KeyHash::new(0xEF01);

        {
            let mut guard = CheckpointLockGuard::new(&locks, hash);

            // Acquire new lock
            assert!(guard.try_lock_new());
            assert!(guard.holds_new());
            assert!(guard.new_locked());

            // Cannot acquire old while new is held
            assert!(!guard.try_lock_old());
        }

        // Lock should be released after guard is dropped
        let lock = locks.get_lock(hash);
        assert!(!lock.old_locked());
        assert!(!lock.new_locked());
    }

    #[test]
    fn test_empty_lock_table_guard() {
        let locks = CheckpointLocks::new();
        let hash = KeyHash::new(0x1234);

        let guard = CheckpointLockGuard::new(&locks, hash);
        assert!(!guard.has_lock());
        assert!(!guard.old_locked());
        assert!(!guard.new_locked());
    }

    #[test]
    fn test_manual_unlock() {
        let locks = CheckpointLocks::with_size(16);
        let hash = KeyHash::new(0x9999);

        let mut guard = CheckpointLockGuard::new(&locks, hash);
        assert!(guard.try_lock_old());
        assert!(guard.holds_old());

        // Manually unlock
        guard.unlock_old();
        assert!(!guard.holds_old());

        // Should be able to lock new now
        assert!(guard.try_lock_new());
        assert!(guard.holds_new());
    }

    #[test]
    fn test_concurrent_old_locks() {
        use std::sync::Arc;
        use std::thread;

        let locks = Arc::new(CheckpointLocks::with_size(16));
        let hash = KeyHash::new(0x1111);

        let mut handles = vec![];

        // Multiple threads trying to acquire old locks
        for _ in 0..4 {
            let locks_clone = Arc::clone(&locks);
            let handle = thread::spawn(move || {
                let lock = locks_clone.get_lock(hash);
                for _ in 0..100 {
                    while !lock.try_lock_old() {
                        std::hint::spin_loop();
                    }
                    // Hold lock briefly
                    std::thread::yield_now();
                    lock.unlock_old();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All locks should be released
        let lock = locks.get_lock(hash);
        assert!(!lock.old_locked());
        assert!(!lock.new_locked());
    }
}
