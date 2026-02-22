//! Basic tests designed to run under Miri.
//!
//! These tests exercise core data structures that rely on unsafe code
//! (atomics, Pod types, address manipulation) without touching file I/O,
//! networking, or heavy threading -- all of which Miri cannot handle.

use std::sync::atomic::Ordering;

use oxifaster::address::{Address, AtomicAddress};

// ---------------------------------------------------------------------------
// Address / AtomicAddress
// ---------------------------------------------------------------------------

#[test]
fn test_address_round_trip() {
    let addr = Address::new(42, 1024);
    assert_eq!(addr.page(), 42);
    assert_eq!(addr.offset(), 1024);

    let control: u64 = addr.into();
    let restored = Address::from(control);
    assert_eq!(restored, addr);
}

#[test]
fn test_address_invalid_sentinel() {
    assert!(Address::INVALID.is_invalid());
    assert!(!Address::new(0, 0).is_invalid());
    assert!(!Address::new(0, 2).is_invalid());
}

#[test]
fn test_address_read_cache_bit() {
    let plain = Address::new(1, 100);
    assert!(!plain.in_read_cache());

    let with_rc = Address::from_control(plain.control() | Address::READ_CACHE_MASK);
    assert!(with_rc.in_read_cache());

    let cleared = with_rc.read_cache_address();
    assert!(!cleared.in_read_cache());
    assert_eq!(cleared, plain);
}

#[test]
fn test_address_arithmetic() {
    let a = Address::new(0, 100);
    let b = a + 50;
    assert_eq!(b.offset(), 150);
    assert_eq!(b - a, 50);
}

#[test]
fn test_address_ordering() {
    let a = Address::new(1, 0);
    let b = Address::new(1, 100);
    let c = Address::new(2, 0);
    assert!(a < b);
    assert!(b < c);
}

#[test]
fn test_atomic_address_load_store() {
    let atomic = AtomicAddress::new(Address::new(5, 500));
    let loaded = atomic.load(Ordering::SeqCst);
    assert_eq!(loaded.page(), 5);
    assert_eq!(loaded.offset(), 500);

    atomic.store(Address::new(10, 1000), Ordering::SeqCst);
    let loaded = atomic.load(Ordering::SeqCst);
    assert_eq!(loaded.page(), 10);
    assert_eq!(loaded.offset(), 1000);
}

#[test]
fn test_atomic_address_cas() {
    let atomic = AtomicAddress::new(Address::new(1, 0));

    // Successful CAS
    let result = atomic.compare_exchange(
        Address::new(1, 0),
        Address::new(2, 0),
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    assert!(result.is_ok());
    assert_eq!(atomic.load(Ordering::SeqCst), Address::new(2, 0));

    // Failed CAS (expected does not match)
    let result = atomic.compare_exchange(
        Address::new(1, 0),
        Address::new(3, 0),
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    assert!(result.is_err());
    assert_eq!(atomic.load(Ordering::SeqCst), Address::new(2, 0));
}

#[test]
fn test_atomic_address_fetch_add() {
    let atomic = AtomicAddress::new(Address::new(0, 100));
    let prev = atomic.fetch_add(50, Ordering::SeqCst);
    assert_eq!(prev.offset(), 100);
    assert_eq!(atomic.load(Ordering::SeqCst).offset(), 150);
}

// ---------------------------------------------------------------------------
// Pod type validation (bytemuck)
// ---------------------------------------------------------------------------

#[test]
fn test_pod_types_valid() {
    // Verify that the types we declare as Pod actually satisfy the invariant
    // under Miri's strict aliasing / validity checks.

    // u64 key round-trip through bytemuck
    let key: u64 = 0xDEAD_BEEF_CAFE_BABE;
    let bytes = bytemuck::bytes_of(&key);
    let restored: &u64 = bytemuck::from_bytes(bytes);
    assert_eq!(*restored, key);

    // i32 value round-trip
    let val: i32 = -42;
    let bytes = bytemuck::bytes_of(&val);
    let restored: &i32 = bytemuck::from_bytes(bytes);
    assert_eq!(*restored, val);

    // Fixed-size byte array
    let arr: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let bytes = bytemuck::bytes_of(&arr);
    let restored: &[u8; 16] = bytemuck::from_bytes(bytes);
    assert_eq!(*restored, arr);
}

// ---------------------------------------------------------------------------
// Epoch basic (single-thread, no I/O, no spawning)
// ---------------------------------------------------------------------------

#[test]
fn test_epoch_basic_protect_unprotect() {
    use oxifaster::epoch::LightEpoch;

    let epoch = LightEpoch::new();

    // Thread slot 0 should start unprotected
    assert!(!epoch.is_protected(0));

    let e = epoch.protect(0);
    assert!(epoch.is_protected(0));
    assert_eq!(e, 1); // initial epoch is 1

    epoch.unprotect(0);
    assert!(!epoch.is_protected(0));
}

#[test]
fn test_epoch_bump() {
    use oxifaster::epoch::LightEpoch;

    let epoch = LightEpoch::new();
    assert_eq!(epoch.current_epoch.load(Ordering::Relaxed), 1);

    let next = epoch.bump_current_epoch();
    assert_eq!(next, 2);
    assert_eq!(epoch.current_epoch.load(Ordering::Relaxed), 2);
}

#[test]
fn test_epoch_reentrant() {
    use oxifaster::epoch::LightEpoch;

    let epoch = LightEpoch::new();

    epoch.reentrant_protect(0);
    assert!(epoch.is_protected(0));

    epoch.reentrant_protect(0);
    assert!(epoch.is_protected(0));

    epoch.reentrant_unprotect(0);
    assert!(epoch.is_protected(0)); // still protected (nested)

    epoch.reentrant_unprotect(0);
    assert!(!epoch.is_protected(0)); // fully unprotected
}

#[test]
fn test_epoch_safe_to_reclaim() {
    use oxifaster::epoch::LightEpoch;

    let epoch = LightEpoch::new();

    // No threads protected -- everything is safe
    epoch.compute_new_safe_to_reclaim_epoch(10);
    assert!(epoch.is_safe_to_reclaim(9));

    // Protect slot 0 at epoch 5
    epoch.current_epoch.store(5, Ordering::Relaxed);
    epoch.protect(0);

    epoch.compute_new_safe_to_reclaim_epoch(10);
    assert!(epoch.is_safe_to_reclaim(4));
    assert!(!epoch.is_safe_to_reclaim(5));

    epoch.unprotect(0);
}
