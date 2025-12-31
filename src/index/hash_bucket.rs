//! Hash bucket structures for FASTER's hash index
//!
//! This module defines the hash bucket and entry types used by the hash index
//! to map keys to their locations in the hybrid log.

use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::address::Address;
use crate::constants::CACHE_LINE_BYTES;

/// Entry stored in a hash bucket
///
/// Packed into 8 bytes with the following layout:
/// - address (48 bits): Logical address of the record
/// - reserved (16 bits): Reserved for internal index use
#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct HashBucketEntry(u64);

impl HashBucketEntry {
    /// Invalid/empty entry value
    pub const INVALID: Self = Self(0);

    /// Address mask (48 bits)
    const ADDRESS_MASK: u64 = (1 << 48) - 1;
    
    /// Read cache bit position (bit 47)
    const READ_CACHE_BIT: u64 = 1 << 47;

    /// Create a new entry from an address
    #[inline]
    pub const fn new(address: Address) -> Self {
        Self(address.control() & Self::ADDRESS_MASK)
    }

    /// Create an entry from raw control value
    #[inline]
    pub const fn from_control(control: u64) -> Self {
        Self(control)
    }

    /// Get the address portion of the entry
    #[inline]
    pub const fn address(&self) -> Address {
        Address::from_control(self.0 & Self::ADDRESS_MASK)
    }

    /// Check if this entry is unused/invalid
    #[inline]
    pub const fn is_unused(&self) -> bool {
        self.0 == 0
    }

    /// Check if the address points to read cache
    #[inline]
    pub const fn in_read_cache(&self) -> bool {
        (self.0 & Self::READ_CACHE_BIT) != 0
    }

    /// Get the raw control value
    #[inline]
    pub const fn control(&self) -> u64 {
        self.0
    }
}

impl PartialEq for HashBucketEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for HashBucketEntry {}

impl std::fmt::Debug for HashBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashBucketEntry")
            .field("address", &self.address())
            .field("in_read_cache", &self.in_read_cache())
            .finish()
    }
}

/// Index-specific hash bucket entry with tag for collision detection
///
/// Layout (HotLog variant):
/// - address (48 bits): Logical address
/// - tag (14 bits): Hash tag for quick comparison
/// - reserved (1 bit): Reserved
/// - tentative (1 bit): Entry is being inserted
#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct IndexHashBucketEntry(u64);

impl IndexHashBucketEntry {
    /// Invalid/empty entry value
    pub const INVALID: Self = Self(0);

    /// Number of bits for the tag
    pub const TAG_BITS: u32 = 14;
    
    /// Address mask (48 bits)
    const ADDRESS_MASK: u64 = (1 << 48) - 1;
    
    /// Read cache bit (bit 47 in address portion)
    const READ_CACHE_BIT: u64 = 1 << 47;
    
    /// Tag shift position
    const TAG_SHIFT: u32 = 48;
    
    /// Tag mask
    const TAG_MASK: u64 = (1 << Self::TAG_BITS) - 1;
    
    /// Tentative bit position
    const TENTATIVE_BIT: u64 = 1 << 63;

    /// Create a new entry
    #[inline]
    pub const fn new(address: Address, tag: u16, tentative: bool) -> Self {
        let mut control = address.control() & Self::ADDRESS_MASK;
        control |= ((tag as u64) & Self::TAG_MASK) << Self::TAG_SHIFT;
        if tentative {
            control |= Self::TENTATIVE_BIT;
        }
        Self(control)
    }

    /// Create a new entry with read cache flag
    #[inline]
    pub const fn new_with_read_cache(
        address: Address,
        tag: u16,
        tentative: bool,
        read_cache: bool,
    ) -> Self {
        let mut control = address.control() & (Self::ADDRESS_MASK & !Self::READ_CACHE_BIT);
        if read_cache {
            control |= Self::READ_CACHE_BIT;
        }
        control |= ((tag as u64) & Self::TAG_MASK) << Self::TAG_SHIFT;
        if tentative {
            control |= Self::TENTATIVE_BIT;
        }
        Self(control)
    }

    /// Create an entry from raw control value
    #[inline]
    pub const fn from_control(control: u64) -> Self {
        Self(control)
    }

    /// Check if this entry is unused/invalid
    #[inline]
    pub const fn is_unused(&self) -> bool {
        self.0 == 0
    }

    /// Get the address portion
    #[inline]
    pub const fn address(&self) -> Address {
        Address::from_control(self.0 & Self::ADDRESS_MASK)
    }

    /// Get the tag portion
    #[inline]
    pub const fn tag(&self) -> u16 {
        ((self.0 >> Self::TAG_SHIFT) & Self::TAG_MASK) as u16
    }

    /// Check if entry is tentative (being inserted)
    #[inline]
    pub const fn is_tentative(&self) -> bool {
        (self.0 & Self::TENTATIVE_BIT) != 0
    }

    /// Check if address points to read cache
    #[inline]
    pub const fn in_read_cache(&self) -> bool {
        (self.0 & Self::READ_CACHE_BIT) != 0
    }

    /// Get the raw control value
    #[inline]
    pub const fn control(&self) -> u64 {
        self.0
    }

    /// Convert to base HashBucketEntry
    #[inline]
    pub const fn to_hash_bucket_entry(&self) -> HashBucketEntry {
        HashBucketEntry::from_control(self.0)
    }
}

impl From<HashBucketEntry> for IndexHashBucketEntry {
    fn from(entry: HashBucketEntry) -> Self {
        Self(entry.0)
    }
}

impl PartialEq for IndexHashBucketEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for IndexHashBucketEntry {}

impl std::fmt::Debug for IndexHashBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexHashBucketEntry")
            .field("address", &self.address())
            .field("tag", &self.tag())
            .field("tentative", &self.is_tentative())
            .field("read_cache", &self.in_read_cache())
            .finish()
    }
}

/// Atomic version of HashBucketEntry for thread-safe operations
#[repr(transparent)]
pub struct AtomicHashBucketEntry {
    control: AtomicU64,
}

impl AtomicHashBucketEntry {
    /// Create a new atomic entry
    #[inline]
    pub const fn new(entry: HashBucketEntry) -> Self {
        Self {
            control: AtomicU64::new(entry.0),
        }
    }

    /// Create a new invalid/empty entry
    #[inline]
    pub const fn invalid() -> Self {
        Self {
            control: AtomicU64::new(0),
        }
    }

    /// Load the entry atomically
    #[inline]
    pub fn load(&self, ordering: Ordering) -> HashBucketEntry {
        HashBucketEntry(self.control.load(ordering))
    }

    /// Load as IndexHashBucketEntry
    #[inline]
    pub fn load_index(&self, ordering: Ordering) -> IndexHashBucketEntry {
        IndexHashBucketEntry(self.control.load(ordering))
    }

    /// Store an entry atomically
    #[inline]
    pub fn store(&self, entry: HashBucketEntry, ordering: Ordering) {
        self.control.store(entry.0, ordering);
    }

    /// Store an IndexHashBucketEntry atomically
    #[inline]
    pub fn store_index(&self, entry: IndexHashBucketEntry, ordering: Ordering) {
        self.control.store(entry.0, ordering);
    }

    /// Compare and exchange
    #[inline]
    pub fn compare_exchange(
        &self,
        current: HashBucketEntry,
        new: HashBucketEntry,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HashBucketEntry, HashBucketEntry> {
        self.control
            .compare_exchange(current.0, new.0, success, failure)
            .map(HashBucketEntry)
            .map_err(HashBucketEntry)
    }

    /// Compare and exchange (weak version)
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: HashBucketEntry,
        new: HashBucketEntry,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HashBucketEntry, HashBucketEntry> {
        self.control
            .compare_exchange_weak(current.0, new.0, success, failure)
            .map(HashBucketEntry)
            .map_err(HashBucketEntry)
    }
}

impl Default for AtomicHashBucketEntry {
    fn default() -> Self {
        Self::invalid()
    }
}

impl Clone for AtomicHashBucketEntry {
    fn clone(&self) -> Self {
        Self::new(self.load(Ordering::Relaxed))
    }
}

impl std::fmt::Debug for AtomicHashBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicHashBucketEntry")
            .field("entry", &self.load(Ordering::Relaxed))
            .finish()
    }
}

/// Fixed page address for overflow bucket allocation
#[repr(transparent)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct FixedPageAddress(u64);

impl FixedPageAddress {
    /// Invalid address value
    pub const INVALID: Self = Self(u64::MAX);

    /// Create a new fixed page address
    #[inline]
    pub const fn new(address: u64) -> Self {
        Self(address)
    }

    /// Get the raw address value
    #[inline]
    pub const fn control(&self) -> u64 {
        self.0
    }

    /// Check if the address is invalid
    #[inline]
    pub const fn is_invalid(&self) -> bool {
        self.0 == u64::MAX
    }
}

impl std::fmt::Debug for FixedPageAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_invalid() {
            write!(f, "FixedPageAddress(INVALID)")
        } else {
            write!(f, "FixedPageAddress({})", self.0)
        }
    }
}

/// Entry pointing to the next overflow bucket
#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct HashBucketOverflowEntry(u64);

impl HashBucketOverflowEntry {
    /// Invalid/empty entry value
    pub const INVALID: Self = Self(0);

    /// Address mask (48 bits)
    const ADDRESS_MASK: u64 = (1 << 48) - 1;

    /// Create a new overflow entry
    #[inline]
    pub const fn new(address: FixedPageAddress) -> Self {
        Self(address.control() & Self::ADDRESS_MASK)
    }

    /// Create from raw control value
    #[inline]
    pub const fn from_control(control: u64) -> Self {
        Self(control)
    }

    /// Check if the entry is unused
    #[inline]
    pub const fn is_unused(&self) -> bool {
        (self.0 & Self::ADDRESS_MASK) == 0
    }

    /// Get the overflow bucket address
    #[inline]
    pub const fn address(&self) -> FixedPageAddress {
        FixedPageAddress::new(self.0 & Self::ADDRESS_MASK)
    }

    /// Get the raw control value
    #[inline]
    pub const fn control(&self) -> u64 {
        self.0
    }
}

impl PartialEq for HashBucketOverflowEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for HashBucketOverflowEntry {}

impl std::fmt::Debug for HashBucketOverflowEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashBucketOverflowEntry")
            .field("address", &self.address())
            .field("unused", &self.is_unused())
            .finish()
    }
}

/// Atomic overflow entry
#[repr(transparent)]
pub struct AtomicHashBucketOverflowEntry {
    control: AtomicU64,
}

impl AtomicHashBucketOverflowEntry {
    /// Create a new atomic overflow entry
    #[inline]
    pub const fn new(entry: HashBucketOverflowEntry) -> Self {
        Self {
            control: AtomicU64::new(entry.0),
        }
    }

    /// Create an invalid/empty entry
    #[inline]
    pub const fn invalid() -> Self {
        Self {
            control: AtomicU64::new(0),
        }
    }

    /// Load the entry atomically
    #[inline]
    pub fn load(&self, ordering: Ordering) -> HashBucketOverflowEntry {
        HashBucketOverflowEntry(self.control.load(ordering))
    }

    /// Store an entry atomically
    #[inline]
    pub fn store(&self, entry: HashBucketOverflowEntry, ordering: Ordering) {
        self.control.store(entry.0, ordering);
    }

    /// Compare and exchange
    #[inline]
    pub fn compare_exchange(
        &self,
        current: HashBucketOverflowEntry,
        new: HashBucketOverflowEntry,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HashBucketOverflowEntry, HashBucketOverflowEntry> {
        self.control
            .compare_exchange(current.0, new.0, success, failure)
            .map(HashBucketOverflowEntry)
            .map_err(HashBucketOverflowEntry)
    }
}

impl Default for AtomicHashBucketOverflowEntry {
    fn default() -> Self {
        Self::invalid()
    }
}

impl Clone for AtomicHashBucketOverflowEntry {
    fn clone(&self) -> Self {
        Self::new(self.load(Ordering::Relaxed))
    }
}

/// Hash bucket for the hot log index
///
/// Contains 7 entries plus one overflow pointer, fitting in a cache line (64 bytes).
#[repr(C, align(64))]
pub struct HashBucket {
    /// Hash bucket entries
    pub entries: [AtomicHashBucketEntry; Self::NUM_ENTRIES],
    /// Overflow pointer to next bucket
    pub overflow_entry: AtomicHashBucketOverflowEntry,
}

impl HashBucket {
    /// Number of entries per bucket (excluding overflow)
    pub const NUM_ENTRIES: usize = 7;

    /// Create a new empty hash bucket
    pub const fn new() -> Self {
        Self {
            entries: [
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
            ],
            overflow_entry: AtomicHashBucketOverflowEntry::invalid(),
        }
    }
}

impl Default for HashBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for HashBucket {
    fn clone(&self) -> Self {
        Self {
            entries: [
                self.entries[0].clone(),
                self.entries[1].clone(),
                self.entries[2].clone(),
                self.entries[3].clone(),
                self.entries[4].clone(),
                self.entries[5].clone(),
                self.entries[6].clone(),
            ],
            overflow_entry: self.overflow_entry.clone(),
        }
    }
}

impl std::fmt::Debug for HashBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashBucket")
            .field("entries", &self.entries)
            .field("overflow", &self.overflow_entry.load(Ordering::Relaxed))
            .finish()
    }
}

// Ensure HashBucket fits in a cache line
const _: () = assert!(mem::size_of::<HashBucket>() == CACHE_LINE_BYTES);

/// Cold index hash bucket (no overflow buckets, 8 entries)
#[repr(C, align(64))]
pub struct ColdHashBucket {
    /// Hash bucket entries
    pub entries: [AtomicHashBucketEntry; Self::NUM_ENTRIES],
}

impl ColdHashBucket {
    /// Number of entries per bucket
    pub const NUM_ENTRIES: usize = 8;

    /// Create a new empty cold hash bucket
    pub const fn new() -> Self {
        Self {
            entries: [
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
                AtomicHashBucketEntry::invalid(),
            ],
        }
    }
}

impl Default for ColdHashBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ColdHashBucket {
    fn clone(&self) -> Self {
        Self {
            entries: [
                self.entries[0].clone(),
                self.entries[1].clone(),
                self.entries[2].clone(),
                self.entries[3].clone(),
                self.entries[4].clone(),
                self.entries[5].clone(),
                self.entries[6].clone(),
                self.entries[7].clone(),
            ],
        }
    }
}

// Ensure ColdHashBucket fits in a cache line
const _: () = assert!(mem::size_of::<ColdHashBucket>() == CACHE_LINE_BYTES);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bucket_entry() {
        let addr = Address::new(10, 1000);
        let entry = HashBucketEntry::new(addr);
        
        assert_eq!(entry.address(), addr);
        assert!(!entry.is_unused());
        assert!(!entry.in_read_cache());
    }

    #[test]
    fn test_index_hash_bucket_entry() {
        let addr = Address::new(5, 500);
        let entry = IndexHashBucketEntry::new(addr, 0x1234, false);
        
        assert_eq!(entry.address(), addr);
        assert_eq!(entry.tag(), 0x1234);
        assert!(!entry.is_tentative());
        assert!(!entry.is_unused());
    }

    #[test]
    fn test_index_entry_tentative() {
        let addr = Address::new(1, 100);
        let entry = IndexHashBucketEntry::new(addr, 0x5678, true);
        
        assert!(entry.is_tentative());
    }

    #[test]
    fn test_atomic_entry_cas() {
        let atomic = AtomicHashBucketEntry::invalid();
        let old = HashBucketEntry::INVALID;
        let new = HashBucketEntry::new(Address::new(1, 1));
        
        let result = atomic.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire);
        assert!(result.is_ok());
        
        let loaded = atomic.load(Ordering::Acquire);
        assert_eq!(loaded, new);
    }

    #[test]
    fn test_hash_bucket_size() {
        assert_eq!(mem::size_of::<HashBucket>(), 64);
        assert_eq!(mem::align_of::<HashBucket>(), 64);
    }

    #[test]
    fn test_overflow_entry() {
        let addr = FixedPageAddress::new(12345);
        let entry = HashBucketOverflowEntry::new(addr);
        
        assert_eq!(entry.address().control(), 12345);
        assert!(!entry.is_unused());
    }
}

