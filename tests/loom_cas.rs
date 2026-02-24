//! Loom-based model-checked tests for CAS retry patterns.
//!
//! These tests replicate the CAS (compare-and-swap) retry loops used in
//! the hash index and record update paths.  Each test builds a self-contained
//! harness using loom atomics so that Loom can exhaustively explore all
//! interleavings WITHOUT touching production code.
//!
//! Run with:
//! ```bash
//! cargo test --test loom_cas
//! ```

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::thread;

// ---------------------------------------------------------------------------
// Test-local harness: hash bucket entry CAS.
//
// A simplified model of `AtomicHashBucketEntry` from
// `src/index/hash_bucket.rs`.  Each entry stores a packed u64 containing
// an address and a tag.  Concurrent threads CAS to insert / update entries.
// ---------------------------------------------------------------------------

/// Packed bucket entry: upper 14 bits = tag, lower 48 bits = address.
struct AtomicEntry(AtomicU64);

impl AtomicEntry {
    fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }

    fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    fn cas(&self, expected: u64, new: u64) -> Result<u64, u64> {
        self.0
            .compare_exchange(expected, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Two threads race to CAS-insert into an empty bucket entry.
/// Exactly one must succeed; the loser must see the winner's value.
#[test]
fn test_loom_cas_insert_race() {
    loom::model(|| {
        let entry = Arc::new(AtomicEntry::new(0)); // 0 = empty

        let e1 = Arc::clone(&entry);
        let e2 = Arc::clone(&entry);

        let h1 = thread::spawn(move || e1.cas(0, 100));
        let h2 = thread::spawn(move || e2.cas(0, 200));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Exactly one must succeed.
        assert!(r1.is_ok() ^ r2.is_ok());

        let final_val = entry.load();
        match (r1, r2) {
            (Ok(_), Err(witness)) => {
                assert_eq!(final_val, 100);
                assert_eq!(witness, 100);
            }
            (Err(witness), Ok(_)) => {
                assert_eq!(final_val, 200);
                assert_eq!(witness, 200);
            }
            _ => unreachable!(),
        }
    });
}

/// CAS retry loop: thread tries to update a value by reading, computing
/// a new value, and CAS-ing.  Two threads compete.  Both must eventually
/// succeed (one on retry).  The final value must reflect both updates.
#[test]
fn test_loom_cas_retry_loop() {
    loom::model(|| {
        let counter = Arc::new(AtomicU64::new(0));

        let c1 = Arc::clone(&counter);
        let c2 = Arc::clone(&counter);

        let increment = |c: &AtomicU64| {
            loop {
                let old = c.load(Ordering::Acquire);
                let new = old + 1;
                if c.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return;
                }
                // CAS failed -- retry with updated value
            }
        };

        let h1 = thread::spawn(move || increment(&c1));
        let h2 = thread::spawn(move || increment(&c2));

        h1.join().unwrap();
        h2.join().unwrap();

        let final_val = counter.load(Ordering::Acquire);
        assert_eq!(final_val, 2);
    });
}

/// Model the hash chain update pattern: a thread reads the current head,
/// writes its record pointing to the old head, then CAS-es the bucket
/// entry to point to the new record.  Two threads race on the same bucket.
///
/// Invariant: both records end up in the chain (one as head, one linked).
#[test]
fn test_loom_hash_chain_insert() {
    loom::model(|| {
        // The bucket entry stores the address of the chain head.
        // 0 = empty chain.
        let head = Arc::new(AtomicU64::new(0));

        // Each thread's "record" stores the previous address it read.
        let record_a_prev = Arc::new(AtomicU64::new(0));
        let record_b_prev = Arc::new(AtomicU64::new(0));

        let h = Arc::clone(&head);
        let ra = Arc::clone(&record_a_prev);
        let ha = thread::spawn(move || {
            // CAS retry loop to insert record A (address = 10)
            loop {
                let old_head = h.load(Ordering::Acquire);
                ra.store(old_head, Ordering::Release);
                if h.compare_exchange(old_head, 10, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return;
                }
            }
        });

        let h2 = Arc::clone(&head);
        let rb = Arc::clone(&record_b_prev);
        let hb = thread::spawn(move || {
            // CAS retry loop to insert record B (address = 20)
            loop {
                let old_head = h2.load(Ordering::Acquire);
                rb.store(old_head, Ordering::Release);
                if h2
                    .compare_exchange(old_head, 20, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return;
                }
            }
        });

        ha.join().unwrap();
        hb.join().unwrap();

        let final_head = head.load(Ordering::Acquire);
        let a_prev = record_a_prev.load(Ordering::Acquire);
        let b_prev = record_b_prev.load(Ordering::Acquire);

        // The final head must be either 10 or 20.
        assert!(final_head == 10 || final_head == 20);

        // The other record must be linked via its prev pointer.
        // If A is head, B's prev must point to A (or B was inserted first,
        // making A's prev point to B).
        if final_head == 10 {
            // A is the head, so A was inserted last.
            // A's prev should be 20 (B was already in the chain).
            assert_eq!(a_prev, 20);
        } else {
            // B is the head, so B was inserted last.
            // B's prev should be 10 (A was already in the chain).
            assert_eq!(b_prev, 10);
        }
    });
}

/// Model the drain-list try_push pattern from LightEpoch:
/// a slot is FREE (u64::MAX), LOCKED (u64::MAX-1), or holds an epoch.
/// Two threads race to push into the same slot.
#[test]
fn test_loom_drain_slot_push() {
    const FREE: u64 = u64::MAX;
    const LOCKED: u64 = u64::MAX - 1;

    loom::model(|| {
        let slot = Arc::new(AtomicU64::new(FREE));

        let s1 = Arc::clone(&slot);
        let s2 = Arc::clone(&slot);

        // Thread A: try to push epoch 5
        let h1 = thread::spawn(move || -> bool {
            match s1.compare_exchange(FREE, LOCKED, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    // Won the lock -- store the epoch
                    s1.store(5, Ordering::Release);
                    true
                }
                Err(_) => false,
            }
        });

        // Thread B: try to push epoch 7
        let h2 = thread::spawn(move || -> bool {
            match s2.compare_exchange(FREE, LOCKED, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    s2.store(7, Ordering::Release);
                    true
                }
                Err(_) => false,
            }
        });

        let a_won = h1.join().unwrap();
        let b_won = h2.join().unwrap();

        // Exactly one must win
        assert!(a_won ^ b_won);

        let val = slot.load(Ordering::Acquire);
        if a_won {
            assert_eq!(val, 5);
        } else {
            assert_eq!(val, 7);
        }
    });
}
