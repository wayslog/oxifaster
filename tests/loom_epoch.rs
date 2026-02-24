//! Loom-based model-checked tests for epoch protection algorithms.
//!
//! These tests replicate the core concurrency algorithms from
//! `src/epoch/light_epoch.rs` in self-contained test-local structs that use
//! loom atomics.  This approach lets Loom's model checker exhaustively explore
//! thread interleavings WITHOUT modifying any production code.
//!
//! Run with:
//! ```bash
//! cargo test --test loom_epoch
//! ```

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::thread;

// ---------------------------------------------------------------------------
// Test-local harness: replicates the LightEpoch protect / unprotect /
// bump / safe-to-reclaim algorithm using loom atomics.
//
// We cannot use the production `LightEpoch` directly because:
//   1. It uses `OnceLock`, `thread_local!`, `parking_lot::Mutex` -- none of
//      which are instrumented by Loom.
//   2. Its table is sized at MAX_THREADS (96), making Loom state explosion
//      prohibitive.
//
// Instead we faithfully reproduce the algorithm at a small scale.
// ---------------------------------------------------------------------------

const UNPROTECTED: u64 = 0;

/// Minimal epoch table entry (mirrors `Entry` in light_epoch.rs).
struct TestEntry {
    local_current_epoch: AtomicU64,
    reentrant: AtomicU32,
}

impl TestEntry {
    fn new() -> Self {
        Self {
            local_current_epoch: AtomicU64::new(UNPROTECTED),
            reentrant: AtomicU32::new(0),
        }
    }
}

/// Minimal epoch harness (mirrors the core of `LightEpoch`).
/// Sized for 2 threads to keep Loom's state space tractable.
struct TestLightEpoch {
    table: [TestEntry; 2],
    current_epoch: AtomicU64,
    safe_to_reclaim_epoch: AtomicU64,
}

impl TestLightEpoch {
    fn new() -> Self {
        Self {
            table: [TestEntry::new(), TestEntry::new()],
            current_epoch: AtomicU64::new(1),
            safe_to_reclaim_epoch: AtomicU64::new(0),
        }
    }

    /// Protect: publish the current global epoch into the thread's slot.
    fn protect(&self, tid: usize) -> u64 {
        let epoch = self.current_epoch.load(Ordering::Acquire);
        self.table[tid]
            .local_current_epoch
            .store(epoch, Ordering::Release);
        epoch
    }

    /// Unprotect: mark the slot as not protected.
    fn unprotect(&self, tid: usize) {
        self.table[tid]
            .local_current_epoch
            .store(UNPROTECTED, Ordering::Release);
    }

    /// Reentrant protect: increment reentrancy counter; only publish epoch
    /// on the outermost call.
    fn reentrant_protect(&self, tid: usize) -> u64 {
        let entry = &self.table[tid];
        let current_count = entry.reentrant.fetch_add(1, Ordering::AcqRel);
        if current_count == 0 {
            let epoch = self.current_epoch.load(Ordering::Acquire);
            entry.local_current_epoch.store(epoch, Ordering::Release);
            epoch
        } else {
            entry.local_current_epoch.load(Ordering::Acquire)
        }
    }

    /// Reentrant unprotect: decrement counter; only clear epoch on last call.
    fn reentrant_unprotect(&self, tid: usize) {
        let entry = &self.table[tid];
        if entry.reentrant.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        entry
            .local_current_epoch
            .store(UNPROTECTED, Ordering::Release);
    }

    /// Is the thread currently protected?
    fn is_protected(&self, tid: usize) -> bool {
        self.table[tid].local_current_epoch.load(Ordering::Acquire) != UNPROTECTED
    }

    /// Bump the global epoch.
    fn bump_current_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Compute the safe-to-reclaim epoch by scanning all slots.
    fn compute_safe_to_reclaim(&self, current_epoch: u64) -> u64 {
        let mut oldest = current_epoch;
        for entry in &self.table {
            let e = entry.local_current_epoch.load(Ordering::Acquire);
            if e != UNPROTECTED && e < oldest {
                oldest = e;
            }
        }
        let safe = oldest.saturating_sub(1);
        self.safe_to_reclaim_epoch.store(safe, Ordering::Release);
        safe
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Two threads concurrently bump the epoch counter.
/// Invariant: the final epoch equals initial + number of bumps.
#[test]
fn test_loom_epoch_advance() {
    loom::model(|| {
        let epoch = Arc::new(TestLightEpoch::new());

        let e1 = Arc::clone(&epoch);
        let e2 = Arc::clone(&epoch);

        let h1 = thread::spawn(move || e1.bump_current_epoch());
        let h2 = thread::spawn(move || e2.bump_current_epoch());

        h1.join().unwrap();
        h2.join().unwrap();

        let final_val = epoch.current_epoch.load(Ordering::SeqCst);
        assert_eq!(final_val, 3); // started at 1, two bumps => 3
    });
}

/// Thread A protects and unprotects while thread B bumps the epoch.
/// Invariant: after A finishes, its slot must be UNPROTECTED.
/// While A is protected, its local epoch must be <= global epoch.
#[test]
fn test_loom_protect_unprotect() {
    loom::model(|| {
        let epoch = Arc::new(TestLightEpoch::new());

        let e1 = Arc::clone(&epoch);
        let e2 = Arc::clone(&epoch);

        // Thread 0: protect, then unprotect
        let h1 = thread::spawn(move || {
            let local = e1.protect(0);
            // While protected, local epoch <= global epoch
            let global = e1.current_epoch.load(Ordering::Acquire);
            assert!(local <= global);
            e1.unprotect(0);
        });

        // Thread 1: bump epoch
        let h2 = thread::spawn(move || {
            e2.bump_current_epoch();
        });

        h1.join().unwrap();
        h2.join().unwrap();

        // After both finish, slot 0 must be unprotected
        assert!(!epoch.is_protected(0));
        // Global epoch is 2 or more
        let ge = epoch.current_epoch.load(Ordering::Acquire);
        assert!(ge >= 2);
    });
}

/// Two threads protect at potentially different epochs.
/// safe_to_reclaim must be < min(protected epochs).
#[test]
fn test_loom_safe_to_reclaim() {
    loom::model(|| {
        let epoch = Arc::new(TestLightEpoch::new());

        let e1 = Arc::clone(&epoch);
        let e2 = Arc::clone(&epoch);

        // Thread 0 protects first
        let h0 = thread::spawn(move || {
            e1.protect(0);
        });

        // Thread 1 bumps and then protects
        let h1 = thread::spawn(move || {
            e2.bump_current_epoch();
            e2.protect(1);
        });

        h0.join().unwrap();
        h1.join().unwrap();

        let ge = epoch.current_epoch.load(Ordering::Acquire);
        let safe = epoch.compute_safe_to_reclaim(ge);

        // Both threads are protected; safe must be strictly less than each
        let l0 = epoch.table[0].local_current_epoch.load(Ordering::Acquire);
        let l1 = epoch.table[1].local_current_epoch.load(Ordering::Acquire);
        if l0 != UNPROTECTED {
            assert!(safe < l0);
        }
        if l1 != UNPROTECTED {
            assert!(safe < l1);
        }

        epoch.unprotect(0);
        epoch.unprotect(1);
    });
}

/// Reentrant protection: nested protect/unprotect from two threads.
/// Invariant: slot stays protected until the outermost unprotect.
#[test]
fn test_loom_reentrant_protect() {
    loom::model(|| {
        let epoch = Arc::new(TestLightEpoch::new());

        let e1 = Arc::clone(&epoch);
        let e2 = Arc::clone(&epoch);

        // Thread 0: nested protect/unprotect
        let h0 = thread::spawn(move || {
            e1.reentrant_protect(0);
            assert!(e1.is_protected(0));

            e1.reentrant_protect(0);
            assert!(e1.is_protected(0));

            e1.reentrant_unprotect(0);
            assert!(e1.is_protected(0)); // still nested

            e1.reentrant_unprotect(0);
            assert!(!e1.is_protected(0)); // fully unprotected
        });

        // Thread 1: bump epoch concurrently
        let h1 = thread::spawn(move || {
            e2.bump_current_epoch();
        });

        h0.join().unwrap();
        h1.join().unwrap();

        assert!(!epoch.is_protected(0));
    });
}

/// Model the thread-ID allocation pattern: two threads race to CAS-claim
/// a single slot. Exactly one must succeed.
#[test]
fn test_loom_thread_id_reuse() {
    loom::model(|| {
        // A single-slot allocator: 0 = free, non-zero = owner tag.
        let slot = Arc::new(AtomicU64::new(0));

        let s1 = Arc::clone(&slot);
        let s2 = Arc::clone(&slot);

        let h1 = thread::spawn(move || {
            s1.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        });

        let h2 = thread::spawn(move || {
            s2.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        });

        let won_1 = h1.join().unwrap();
        let won_2 = h2.join().unwrap();

        // Exactly one must win the CAS.
        assert!(won_1 ^ won_2);

        let final_val = slot.load(Ordering::Acquire);
        if won_1 {
            assert_eq!(final_val, 1);
        } else {
            assert_eq!(final_val, 2);
        }
    });
}
