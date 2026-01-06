//! Checkpoint Locks 集成测试
//!
//! 测试 CPR 协议中使用的检查点锁。

use std::sync::Arc;
use std::thread;

use oxifaster::checkpoint::{AtomicCheckpointLock, CheckpointLock, CheckpointLockGuard, CheckpointLocks};
use oxifaster::index::KeyHash;

// ============ CheckpointLock Tests ============

#[test]
fn test_checkpoint_lock_new() {
    let lock = CheckpointLock::new();

    assert_eq!(lock.old_lock_count, 0);
    assert_eq!(lock.new_lock_count, 0);
}

#[test]
fn test_checkpoint_lock_with_counts() {
    let lock = CheckpointLock::with_counts(5, 10);

    assert_eq!(lock.old_lock_count, 5);
    assert_eq!(lock.new_lock_count, 10);
}

#[test]
fn test_checkpoint_lock_from_control() {
    // Control value with old=3, new=7
    let control = 3u64 | (7u64 << 32);
    let lock = CheckpointLock::from_control(control);

    assert_eq!(lock.old_lock_count, 3);
    assert_eq!(lock.new_lock_count, 7);
}

#[test]
fn test_checkpoint_lock_to_control() {
    let lock = CheckpointLock::with_counts(3, 7);
    let control = lock.to_control();

    let recovered = CheckpointLock::from_control(control);
    assert_eq!(recovered.old_lock_count, 3);
    assert_eq!(recovered.new_lock_count, 7);
}

#[test]
fn test_checkpoint_lock_roundtrip() {
    for old in [0u32, 1, 100, u32::MAX / 2] {
        for new in [0u32, 1, 100, u32::MAX / 2] {
            let lock = CheckpointLock::with_counts(old, new);
            let control = lock.to_control();
            let recovered = CheckpointLock::from_control(control);

            assert_eq!(recovered.old_lock_count, old);
            assert_eq!(recovered.new_lock_count, new);
        }
    }
}

// ============ AtomicCheckpointLock Tests ============

#[test]
fn test_atomic_checkpoint_lock_new() {
    let lock = AtomicCheckpointLock::new();

    // Initial state should allow both old and new locks
    assert!(lock.try_lock_old());
    lock.unlock_old();
}

#[test]
fn test_atomic_checkpoint_lock_old() {
    let lock = AtomicCheckpointLock::new();

    // Should be able to acquire old lock
    assert!(lock.try_lock_old());

    // Should be able to acquire multiple old locks
    assert!(lock.try_lock_old());
    assert!(lock.try_lock_old());

    // Unlock all
    lock.unlock_old();
    lock.unlock_old();
    lock.unlock_old();
}

#[test]
fn test_atomic_checkpoint_lock_new_version() {
    let lock = AtomicCheckpointLock::new();

    // Should be able to acquire new lock
    assert!(lock.try_lock_new());

    // Should be able to acquire multiple new locks
    assert!(lock.try_lock_new());
    assert!(lock.try_lock_new());

    // Unlock all
    lock.unlock_new();
    lock.unlock_new();
    lock.unlock_new();
}

#[test]
fn test_atomic_checkpoint_lock_mutual_exclusion() {
    let lock = AtomicCheckpointLock::new();

    // Acquire old lock
    assert!(lock.try_lock_old());

    // Should not be able to acquire new lock while old is held
    assert!(!lock.try_lock_new());

    // Release old lock
    lock.unlock_old();

    // Now should be able to acquire new lock
    assert!(lock.try_lock_new());

    // Should not be able to acquire old lock while new is held
    assert!(!lock.try_lock_old());

    // Release new lock
    lock.unlock_new();

    // Both should be available again
    assert!(lock.try_lock_old());
    lock.unlock_old();
    assert!(lock.try_lock_new());
    lock.unlock_new();
}

#[test]
fn test_atomic_checkpoint_lock_old_locked() {
    let lock = AtomicCheckpointLock::new();

    assert!(!lock.old_locked());
    
    assert!(lock.try_lock_old());
    assert!(lock.old_locked());
    
    lock.unlock_old();
    assert!(!lock.old_locked());
}

#[test]
fn test_atomic_checkpoint_lock_new_locked() {
    let lock = AtomicCheckpointLock::new();

    assert!(!lock.new_locked());
    
    assert!(lock.try_lock_new());
    assert!(lock.new_locked());
    
    lock.unlock_new();
    assert!(!lock.new_locked());
}

#[test]
fn test_atomic_checkpoint_lock_is_locked() {
    let lock = AtomicCheckpointLock::new();

    assert!(!lock.is_locked());
    
    assert!(lock.try_lock_old());
    assert!(lock.is_locked());
    
    lock.unlock_old();
    assert!(!lock.is_locked());
    
    assert!(lock.try_lock_new());
    assert!(lock.is_locked());
    
    lock.unlock_new();
    assert!(!lock.is_locked());
}

#[test]
fn test_atomic_checkpoint_lock_load() {
    let lock = AtomicCheckpointLock::new();

    let state = lock.load();
    assert_eq!(state.old_lock_count, 0);
    assert_eq!(state.new_lock_count, 0);

    assert!(lock.try_lock_old());
    let state = lock.load();
    assert_eq!(state.old_lock_count, 1);
    assert_eq!(state.new_lock_count, 0);

    lock.unlock_old();
}

// ============ CheckpointLocks Table Tests ============

#[test]
fn test_checkpoint_locks_new() {
    let locks = CheckpointLocks::new();

    assert!(locks.is_empty());
    assert_eq!(locks.size(), 0);
}

#[test]
fn test_checkpoint_locks_with_size() {
    let locks = CheckpointLocks::with_size(1024);

    assert!(!locks.is_empty());
    assert_eq!(locks.size(), 1024);
}

#[test]
fn test_checkpoint_locks_initialize() {
    let mut locks = CheckpointLocks::new();
    locks.initialize(256);

    assert_eq!(locks.size(), 256);
    assert!(!locks.is_empty());
}

#[test]
fn test_checkpoint_locks_get_lock() {
    let locks = CheckpointLocks::with_size(1024);
    let hash = KeyHash::new(12345);
    
    let lock = locks.get_lock(hash);
    assert!(lock.try_lock_old());
    lock.unlock_old();
}

#[test]
fn test_checkpoint_locks_get_lock_by_hash() {
    let locks = CheckpointLocks::with_size(1024);
    
    let lock = locks.get_lock_by_hash(12345);
    assert!(lock.try_lock_old());
    lock.unlock_old();
}

#[test]
fn test_checkpoint_locks_different_hashes() {
    let locks = CheckpointLocks::with_size(1024);

    let hash1 = KeyHash::new(1);
    let hash2 = KeyHash::new(2);

    let lock1 = locks.get_lock(hash1);
    let lock2 = locks.get_lock(hash2);

    assert!(lock1.try_lock_old());
    assert!(lock2.try_lock_old());

    lock1.unlock_old();
    lock2.unlock_old();
}

#[test]
fn test_checkpoint_locks_free() {
    let mut locks = CheckpointLocks::with_size(256);
    assert!(!locks.is_empty());

    locks.free();
    assert!(locks.is_empty());
}

// ============ CheckpointLockGuard Tests ============

#[test]
fn test_lock_guard_from_lock() {
    let lock = AtomicCheckpointLock::new();
    let guard = CheckpointLockGuard::from_lock(&lock);

    assert!(guard.has_lock());
    assert!(!guard.holds_old());
    assert!(!guard.holds_new());
}

#[test]
fn test_lock_guard_try_lock_old() {
    let lock = AtomicCheckpointLock::new();
    let mut guard = CheckpointLockGuard::from_lock(&lock);

    assert!(guard.try_lock_old());
    assert!(guard.holds_old());
    assert!(!guard.holds_new());

    guard.unlock_old();
    assert!(!guard.holds_old());
}

#[test]
fn test_lock_guard_try_lock_new() {
    let lock = AtomicCheckpointLock::new();
    let mut guard = CheckpointLockGuard::from_lock(&lock);

    assert!(guard.try_lock_new());
    assert!(guard.holds_new());
    assert!(!guard.holds_old());

    guard.unlock_new();
    assert!(!guard.holds_new());
}

#[test]
fn test_lock_guard_from_locks_table() {
    let locks = CheckpointLocks::with_size(256);
    let hash = KeyHash::new(42);
    
    let mut guard = CheckpointLockGuard::new(&locks, hash);
    
    assert!(guard.has_lock());
    assert!(guard.try_lock_old());
    guard.unlock_old();
}

#[test]
fn test_lock_guard_drop_releases_old() {
    let lock = AtomicCheckpointLock::new();
    
    {
        let mut guard = CheckpointLockGuard::from_lock(&lock);
        assert!(guard.try_lock_old());
    } // Guard dropped here, should release lock
    
    // Lock should be free now
    assert!(!lock.old_locked());
}

#[test]
fn test_lock_guard_drop_releases_new() {
    let lock = AtomicCheckpointLock::new();
    
    {
        let mut guard = CheckpointLockGuard::from_lock(&lock);
        assert!(guard.try_lock_new());
    } // Guard dropped here, should release lock
    
    // Lock should be free now
    assert!(!lock.new_locked());
}

// ============ Concurrent Tests ============

#[test]
fn test_atomic_lock_concurrent_old() {
    let lock = Arc::new(AtomicCheckpointLock::new());
    let num_threads = 4;
    let iterations = 1000;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let lock = lock.clone();
            thread::spawn(move || {
                for _ in 0..iterations {
                    while !lock.try_lock_old() {
                        thread::yield_now();
                    }
                    // Critical section
                    lock.unlock_old();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Lock should be free at the end
    assert!(lock.try_lock_old());
    lock.unlock_old();
}

#[test]
fn test_checkpoint_locks_concurrent() {
    let locks = Arc::new(CheckpointLocks::with_size(16));
    let num_threads = 4;
    let iterations = 500;

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let locks = locks.clone();
            thread::spawn(move || {
                for j in 0..iterations {
                    let hash = KeyHash::new((i * iterations + j) as u64);
                    let lock = locks.get_lock(hash);
                    while !lock.try_lock_old() {
                        thread::yield_now();
                    }
                    lock.unlock_old();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_checkpoint_locks_mixed_old_new() {
    let locks = Arc::new(CheckpointLocks::with_size(16));
    let num_threads = 8;
    let iterations = 200;

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let locks = locks.clone();
            thread::spawn(move || {
                for j in 0..iterations {
                    let hash = KeyHash::new((j % 16) as u64);
                    let lock = locks.get_lock(hash);

                    if i % 2 == 0 {
                        // Even threads try old locks
                        if lock.try_lock_old() {
                            // Small critical section
                            lock.unlock_old();
                        }
                    } else {
                        // Odd threads try new locks
                        if lock.try_lock_new() {
                            // Small critical section
                            lock.unlock_new();
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

// ============ Edge Cases ============

#[test]
fn test_checkpoint_lock_max_counts() {
    let lock = CheckpointLock::with_counts(u32::MAX, u32::MAX);

    assert_eq!(lock.old_lock_count, u32::MAX);
    assert_eq!(lock.new_lock_count, u32::MAX);

    let control = lock.to_control();
    let recovered = CheckpointLock::from_control(control);
    assert_eq!(recovered.old_lock_count, u32::MAX);
    assert_eq!(recovered.new_lock_count, u32::MAX);
}

#[test]
fn test_checkpoint_lock_zero_counts() {
    let lock = CheckpointLock::with_counts(0, 0);
    let control = lock.to_control();

    assert_eq!(control, 0);
}

#[test]
fn test_checkpoint_locks_power_of_two_sizes() {
    for shift in 0..10 {
        let size = 1u64 << shift;
        let locks = CheckpointLocks::with_size(size);
        assert_eq!(locks.size(), size);
    }
}

// ============ Stress Tests ============

#[test]
fn test_rapid_lock_unlock() {
    let lock = AtomicCheckpointLock::new();

    for _ in 0..10000 {
        assert!(lock.try_lock_old());
        lock.unlock_old();
    }
}

#[test]
fn test_rapid_lock_unlock_alternating() {
    let lock = AtomicCheckpointLock::new();

    for _ in 0..5000 {
        assert!(lock.try_lock_old());
        lock.unlock_old();
        assert!(lock.try_lock_new());
        lock.unlock_new();
    }
}
