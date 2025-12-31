//! Address types for FASTER's hybrid log
//!
//! This module provides the `Address` type used to identify locations in the hybrid log.
//! An address uses 48 bits: 25 bits for the offset within a page and 23 bits for the page number.

use std::cmp::Ordering;
use std::fmt;
use std::ops::{Add, AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// A logical address into persistent memory.
///
/// Identifies a page and an offset within that page.
/// Uses 48 bits: 25 bits for the offset and 23 bits for the page.
/// The remaining 16 bits are reserved for use by the hash table.
#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct Address(u64);

impl Address {
    /// An invalid address, used when you need to initialize an address but don't have a valid
    /// value for it yet.
    ///
    /// Note: set to 1, not 0, to distinguish an invalid hash bucket entry (initialized to all zeros)
    /// from a valid hash bucket entry that points to an invalid address.
    pub const INVALID: Self = Self(1);

    /// Total number of address bits used
    pub const ADDRESS_BITS: u32 = 48;

    /// Number of bits used for page offset (32 MB per page)
    pub const OFFSET_BITS: u32 = 25;

    /// Number of bits used for page number (~8 million pages)
    pub const PAGE_BITS: u32 = Self::ADDRESS_BITS - Self::OFFSET_BITS;

    /// Maximum valid offset within a page
    pub const MAX_OFFSET: u32 = (1 << Self::OFFSET_BITS) - 1;

    /// Maximum valid page number
    pub const MAX_PAGE: u32 = (1 << Self::PAGE_BITS) - 1;

    /// Maximum valid address value
    pub const MAX_ADDRESS: u64 = (1 << Self::ADDRESS_BITS) - 1;

    /// Read cache bit mask (bit 47)
    pub const READ_CACHE_MASK: u64 = 1 << (Self::ADDRESS_BITS - 1);

    /// Create a new address from page and offset
    #[inline]
    pub const fn new(page: u32, offset: u32) -> Self {
        debug_assert!(page <= Self::MAX_PAGE);
        debug_assert!(offset <= Self::MAX_OFFSET);
        Self(((page as u64) << Self::OFFSET_BITS) | (offset as u64))
    }

    /// Create an address from a raw control value
    #[inline]
    pub const fn from_control(control: u64) -> Self {
        Self(control)
    }

    /// Get the page number
    #[inline]
    pub const fn page(&self) -> u32 {
        ((self.0 >> Self::OFFSET_BITS) & ((1 << Self::PAGE_BITS) - 1)) as u32
    }

    /// Get the offset within the page
    #[inline]
    pub const fn offset(&self) -> u32 {
        (self.0 & ((1 << Self::OFFSET_BITS) - 1)) as u32
    }

    /// Get the raw control value
    #[inline]
    pub const fn control(&self) -> u64 {
        self.0
    }

    /// Check if this address is in the read cache
    #[inline]
    pub const fn in_read_cache(&self) -> bool {
        (self.0 & Self::READ_CACHE_MASK) != 0
    }

    /// Get the address with read cache bit cleared
    #[inline]
    pub const fn read_cache_address(&self) -> Self {
        Self(self.0 & !Self::READ_CACHE_MASK)
    }

    /// Check if this is an invalid address
    #[inline]
    pub const fn is_invalid(&self) -> bool {
        self.0 == Self::INVALID.0
    }

    /// Check if this is a valid address (not invalid)
    #[inline]
    pub const fn is_valid(&self) -> bool {
        !self.is_invalid()
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Address")
            .field("page", &self.page())
            .field("offset", &self.offset())
            .field("control", &self.0)
            .finish()
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.page(), self.offset())
    }
}

impl PartialEq for Address {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Address {}

impl PartialOrd for Address {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Address {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Add<u64> for Address {
    type Output = Self;

    #[inline]
    fn add(self, delta: u64) -> Self::Output {
        debug_assert!(delta < u32::MAX as u64);
        Self(self.0 + delta)
    }
}

impl AddAssign<u64> for Address {
    #[inline]
    fn add_assign(&mut self, delta: u64) {
        debug_assert!(delta < u32::MAX as u64);
        self.0 += delta;
    }
}

impl Sub for Address {
    type Output = u64;

    #[inline]
    fn sub(self, other: Self) -> Self::Output {
        self.0 - other.0
    }
}

impl From<u64> for Address {
    #[inline]
    fn from(control: u64) -> Self {
        Self(control)
    }
}

impl From<Address> for u64 {
    #[inline]
    fn from(addr: Address) -> Self {
        addr.0
    }
}

/// Atomic version of Address for thread-safe operations
#[repr(transparent)]
pub struct AtomicAddress {
    control: AtomicU64,
}

impl AtomicAddress {
    /// Create a new atomic address
    #[inline]
    pub const fn new(address: Address) -> Self {
        Self {
            control: AtomicU64::new(address.0),
        }
    }

    /// Load the address atomically
    #[inline]
    pub fn load(&self, ordering: AtomicOrdering) -> Address {
        Address(self.control.load(ordering))
    }

    /// Store an address atomically
    #[inline]
    pub fn store(&self, address: Address, ordering: AtomicOrdering) {
        self.control.store(address.0, ordering);
    }

    /// Compare and exchange the address atomically
    #[inline]
    pub fn compare_exchange(
        &self,
        current: Address,
        new: Address,
        success: AtomicOrdering,
        failure: AtomicOrdering,
    ) -> Result<Address, Address> {
        self.control
            .compare_exchange(current.0, new.0, success, failure)
            .map(Address)
            .map_err(Address)
    }

    /// Compare and exchange the address atomically (weak version)
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: Address,
        new: Address,
        success: AtomicOrdering,
        failure: AtomicOrdering,
    ) -> Result<Address, Address> {
        self.control
            .compare_exchange_weak(current.0, new.0, success, failure)
            .map(Address)
            .map_err(Address)
    }

    /// Fetch and add atomically
    #[inline]
    pub fn fetch_add(&self, delta: u64, ordering: AtomicOrdering) -> Address {
        Address(self.control.fetch_add(delta, ordering))
    }

    /// Get the page number
    #[inline]
    pub fn page(&self) -> u32 {
        self.load(AtomicOrdering::Acquire).page()
    }

    /// Get the offset
    #[inline]
    pub fn offset(&self) -> u32 {
        self.load(AtomicOrdering::Acquire).offset()
    }
}

impl Default for AtomicAddress {
    fn default() -> Self {
        Self::new(Address::default())
    }
}

impl fmt::Debug for AtomicAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addr = self.load(AtomicOrdering::Relaxed);
        f.debug_struct("AtomicAddress")
            .field("address", &addr)
            .finish()
    }
}

impl Clone for AtomicAddress {
    fn clone(&self) -> Self {
        Self::new(self.load(AtomicOrdering::Relaxed))
    }
}

/// Page offset structure for atomic page + offset operations
///
/// Uses 41 bits for offset (giving approximately 2 PB of overflow space for Reserve())
/// and 23 bits for page number.
#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct PageOffset(u64);

impl PageOffset {
    /// Create a new page offset
    #[inline]
    pub const fn new(page: u32, offset: u64) -> Self {
        debug_assert!(page <= Address::MAX_PAGE);
        Self((offset & ((1u64 << (64 - Address::PAGE_BITS)) - 1))
            | ((page as u64) << (64 - Address::PAGE_BITS)))
    }

    /// Get the page number
    #[inline]
    pub const fn page(&self) -> u32 {
        (self.0 >> (64 - Address::PAGE_BITS)) as u32
    }

    /// Get the offset (can exceed MAX_OFFSET due to overflow space)
    #[inline]
    pub const fn offset(&self) -> u64 {
        self.0 & ((1u64 << (64 - Address::PAGE_BITS)) - 1)
    }

    /// Get the raw control value
    #[inline]
    pub const fn control(&self) -> u64 {
        self.0
    }

    /// Convert to Address (truncates offset if > MAX_OFFSET)
    #[inline]
    pub fn to_address(&self) -> Address {
        let offset = std::cmp::min(self.offset() as u32, Address::MAX_OFFSET);
        Address::new(self.page(), offset)
    }
}

impl From<Address> for PageOffset {
    #[inline]
    fn from(address: Address) -> Self {
        Self::new(address.page(), address.offset() as u64)
    }
}

impl fmt::Debug for PageOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageOffset")
            .field("page", &self.page())
            .field("offset", &self.offset())
            .finish()
    }
}

/// Atomic page offset for thread-safe tail management
pub struct AtomicPageOffset {
    control: AtomicU64,
}

impl AtomicPageOffset {
    /// Create a new atomic page offset
    #[inline]
    pub const fn new(page_offset: PageOffset) -> Self {
        Self {
            control: AtomicU64::new(page_offset.0),
        }
    }

    /// Create from address
    #[inline]
    pub fn from_address(address: Address) -> Self {
        Self::new(PageOffset::from(address))
    }

    /// Load the page offset atomically
    #[inline]
    pub fn load(&self, ordering: AtomicOrdering) -> PageOffset {
        PageOffset(self.control.load(ordering))
    }

    /// Store a page offset atomically
    #[inline]
    pub fn store(&self, page_offset: PageOffset, ordering: AtomicOrdering) {
        self.control.store(page_offset.0, ordering);
    }

    /// Store an address atomically
    #[inline]
    pub fn store_address(&self, address: Address, ordering: AtomicOrdering) {
        self.store(PageOffset::from(address), ordering);
    }

    /// Reserve space within the current page
    ///
    /// Returns the page offset before reservation. The result offset can exceed MAX_OFFSET
    /// if the page has overflowed.
    #[inline]
    pub fn reserve(&self, num_slots: u32) -> PageOffset {
        debug_assert!(num_slots <= Address::MAX_OFFSET);
        let delta = num_slots as u64;
        PageOffset(self.control.fetch_add(delta, AtomicOrdering::AcqRel))
    }

    /// Move to a new page
    ///
    /// Returns `true` if some thread advanced the page (this thread or another).
    /// Sets `won_cas` to `true` if this thread won the CAS (responsible for setting up the new page).
    #[inline]
    pub fn new_page(&self, old_page: u32) -> (bool, bool) {
        debug_assert!(old_page < Address::MAX_PAGE);

        let expected = self.load(AtomicOrdering::Acquire);
        if old_page != expected.page() {
            // Another thread already moved to the new page
            debug_assert!(old_page < expected.page());
            return (true, false);
        }

        let new_page = PageOffset::new(old_page + 1, 0);
        match self.control.compare_exchange(
            expected.0,
            new_page.0,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        ) {
            Ok(_) => (true, true),
            Err(actual) => {
                let actual_page = PageOffset(actual).page();
                (actual_page > old_page, false)
            }
        }
    }
}

impl Default for AtomicPageOffset {
    fn default() -> Self {
        Self::new(PageOffset::default())
    }
}

impl fmt::Debug for AtomicPageOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let po = self.load(AtomicOrdering::Relaxed);
        f.debug_struct("AtomicPageOffset")
            .field("page_offset", &po)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_address_new() {
        let addr = Address::new(10, 1000);
        assert_eq!(addr.page(), 10);
        assert_eq!(addr.offset(), 1000);
    }

    #[test]
    fn test_address_invalid() {
        assert!(Address::INVALID.is_invalid());
        assert!(!Address::new(0, 0).is_invalid());
    }

    #[test]
    fn test_address_ordering() {
        let a1 = Address::new(1, 100);
        let a2 = Address::new(1, 200);
        let a3 = Address::new(2, 0);

        assert!(a1 < a2);
        assert!(a2 < a3);
        assert!(a1 < a3);
    }

    #[test]
    fn test_address_arithmetic() {
        let addr = Address::new(0, 100);
        let addr2 = addr + 50;
        assert_eq!(addr2.offset(), 150);

        let diff = addr2 - addr;
        assert_eq!(diff, 50);
    }

    #[test]
    fn test_atomic_address() {
        let atomic = AtomicAddress::new(Address::new(5, 500));
        
        let loaded = atomic.load(AtomicOrdering::Relaxed);
        assert_eq!(loaded.page(), 5);
        assert_eq!(loaded.offset(), 500);

        atomic.store(Address::new(10, 1000), AtomicOrdering::Relaxed);
        let loaded = atomic.load(AtomicOrdering::Relaxed);
        assert_eq!(loaded.page(), 10);
        assert_eq!(loaded.offset(), 1000);
    }

    #[test]
    fn test_page_offset_reserve() {
        let atomic = AtomicPageOffset::new(PageOffset::new(0, 0));
        
        let prev = atomic.reserve(100);
        assert_eq!(prev.page(), 0);
        assert_eq!(prev.offset(), 0);

        let current = atomic.load(AtomicOrdering::Relaxed);
        assert_eq!(current.page(), 0);
        assert_eq!(current.offset(), 100);
    }

    #[test]
    fn test_address_read_cache() {
        let addr = Address::from_control(Address::READ_CACHE_MASK | 0x1234);
        assert!(addr.in_read_cache());
        
        let non_rc_addr = addr.read_cache_address();
        assert!(!non_rc_addr.in_read_cache());
        assert_eq!(non_rc_addr.control(), 0x1234);
    }
}

