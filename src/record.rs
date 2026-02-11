//! Record header for the hybrid log.
//!
//! The hybrid log stores variable-sized records. The common header is `RecordInfo`,
//! which links records into hash chains and stores per-record flags.

use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::address::Address;

#[cfg(feature = "f2")]
use bytemuck::Pod;

/// Record header, internal to FASTER.
///
/// The header is 8 bytes and contains:
/// - Previous address (48 bits): Points to the previous record in the hash chain
/// - Checkpoint version (13 bits): Version when record was created
/// - Invalid bit (1 bit): Whether the record has been invalidated
/// - Tombstone bit (1 bit): Whether this is a delete marker
/// - Final bit (1 bit): Whether this is the final record in a chain
#[repr(C)]
pub struct RecordInfo {
    control: AtomicU64,
}

impl RecordInfo {
    /// Mask for the previous address (48 bits)
    const PREV_ADDR_MASK: u64 = (1 << 48) - 1;

    /// Shift for checkpoint version
    const VERSION_SHIFT: u32 = 48;
    /// Mask for checkpoint version (13 bits)
    const VERSION_MASK: u64 = (1 << 13) - 1;

    /// Bit position for invalid flag
    const INVALID_BIT: u64 = 1 << 61;
    /// Bit position for tombstone flag
    const TOMBSTONE_BIT: u64 = 1 << 62;
    /// Bit position for final flag
    const FINAL_BIT: u64 = 1 << 63;

    /// Read cache bit in the address portion
    const READ_CACHE_BIT: u64 = 1 << 47;

    /// Create a new record info.
    pub fn new(
        previous_address: Address,
        checkpoint_version: u16,
        invalid: bool,
        tombstone: bool,
        final_bit: bool,
    ) -> Self {
        let mut control = previous_address.control() & Self::PREV_ADDR_MASK;
        control |= ((checkpoint_version as u64) & Self::VERSION_MASK) << Self::VERSION_SHIFT;
        if invalid {
            control |= Self::INVALID_BIT;
        }
        if tombstone {
            control |= Self::TOMBSTONE_BIT;
        }
        if final_bit {
            control |= Self::FINAL_BIT;
        }
        Self {
            control: AtomicU64::new(control),
        }
    }

    /// Create a record info from raw control value.
    pub fn from_control(control: u64) -> Self {
        Self {
            control: AtomicU64::new(control),
        }
    }

    /// Check if the record info is null (all zeros).
    #[inline]
    pub fn is_null(&self) -> bool {
        self.control.load(Ordering::Acquire) == 0
    }

    /// Get the previous address in the hash chain.
    #[inline]
    pub fn previous_address(&self) -> Address {
        Address::from_control(self.control.load(Ordering::Acquire) & Self::PREV_ADDR_MASK)
    }

    /// Set the previous address.
    #[inline]
    pub fn set_previous_address(&self, addr: Address) {
        let mut current = self.control.load(Ordering::Acquire);
        loop {
            let new_val =
                (current & !Self::PREV_ADDR_MASK) | (addr.control() & Self::PREV_ADDR_MASK);
            match self.control.compare_exchange_weak(
                current,
                new_val,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Set the previous address assuming the caller has exclusive access to the record header.
    ///
    /// This is intended for the record initialization path before the record is published
    /// (reachable from the hash index). In that phase, using a plain relaxed store avoids an
    /// atomic RMW loop on the hot upsert/delete path.
    #[inline]
    pub fn set_previous_address_relaxed(&self, addr: Address) {
        let current = self.control.load(Ordering::Relaxed);
        let new_val = (current & !Self::PREV_ADDR_MASK) | (addr.control() & Self::PREV_ADDR_MASK);
        self.control.store(new_val, Ordering::Relaxed);
    }

    /// Get the checkpoint version.
    #[inline]
    pub fn checkpoint_version(&self) -> u16 {
        ((self.control.load(Ordering::Acquire) >> Self::VERSION_SHIFT) & Self::VERSION_MASK) as u16
    }

    /// Check if the record is invalid.
    #[inline]
    pub fn is_invalid(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::INVALID_BIT) != 0
    }

    /// Set the invalid flag.
    #[inline]
    pub fn set_invalid(&self, invalid: bool) {
        if invalid {
            self.control.fetch_or(Self::INVALID_BIT, Ordering::AcqRel);
        } else {
            self.control.fetch_and(!Self::INVALID_BIT, Ordering::AcqRel);
        }
    }

    /// Publish a fully initialized record by clearing the invalid flag.
    ///
    /// Readers load the header with `Acquire` (e.g. via `is_invalid()`), so a `Release` store
    /// here is sufficient to publish all prior record writes.
    #[inline]
    pub fn publish_valid(&self) {
        let mut control = self.control.load(Ordering::Relaxed);
        control &= !Self::INVALID_BIT;
        self.control.store(control, Ordering::Release);
    }

    /// Check if this is a tombstone (delete marker).
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::TOMBSTONE_BIT) != 0
    }

    /// Set the tombstone flag.
    #[inline]
    pub fn set_tombstone(&self, tombstone: bool) {
        if tombstone {
            self.control.fetch_or(Self::TOMBSTONE_BIT, Ordering::AcqRel);
        } else {
            self.control
                .fetch_and(!Self::TOMBSTONE_BIT, Ordering::AcqRel);
        }
    }

    /// Check if this is the final record in a chain.
    #[inline]
    pub fn is_final(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::FINAL_BIT) != 0
    }

    /// Check if the previous address points to read cache.
    #[inline]
    pub fn in_read_cache(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::READ_CACHE_BIT) != 0
    }

    /// Get the raw control value.
    #[inline]
    pub fn control(&self) -> u64 {
        self.control.load(Ordering::Acquire)
    }

    /// Load the control value with specified ordering.
    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.control.load(ordering)
    }

    /// Store the control value with specified ordering.
    #[inline]
    pub fn store(&self, value: u64, ordering: Ordering) {
        self.control.store(value, ordering);
    }
}

impl Clone for RecordInfo {
    fn clone(&self) -> Self {
        Self {
            control: AtomicU64::new(self.control.load(Ordering::Acquire)),
        }
    }
}

impl Default for RecordInfo {
    fn default() -> Self {
        Self {
            control: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Debug for RecordInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordInfo")
            .field("previous_address", &self.previous_address())
            .field("checkpoint_version", &self.checkpoint_version())
            .field("invalid", &self.is_invalid())
            .field("tombstone", &self.is_tombstone())
            .field("final", &self.is_final())
            .finish()
    }
}

#[cfg(feature = "f2")]
#[repr(C)]
pub(crate) struct Record<K: Pod, V: Pod> {
    pub(crate) header: RecordInfo,
    _marker: std::marker::PhantomData<(K, V)>,
}

#[cfg(feature = "f2")]
impl<K: Pod, V: Pod> Record<K, V> {
    const ALIGN: usize = 8;

    #[inline]
    pub(crate) const fn size() -> usize {
        Self::align_up(
            mem::size_of::<RecordInfo>() + mem::size_of::<K>() + mem::size_of::<V>(),
            Self::ALIGN,
        )
    }

    #[inline]
    pub(crate) const fn key_offset() -> usize {
        mem::size_of::<RecordInfo>()
    }

    #[inline]
    pub(crate) const fn value_offset() -> usize {
        mem::size_of::<RecordInfo>() + mem::size_of::<K>()
    }

    #[inline]
    const fn align_up(n: usize, align: usize) -> usize {
        debug_assert!(align.is_power_of_two());
        (n + (align - 1)) & !(align - 1)
    }

    #[inline]
    pub(crate) unsafe fn read_key(base: *const u8) -> K {
        let ptr = base.add(Self::key_offset()) as *const K;
        std::ptr::read_unaligned(ptr)
    }

    #[inline]
    pub(crate) unsafe fn read_value(base: *const u8) -> V {
        let ptr = base.add(Self::value_offset()) as *const V;
        std::ptr::read_unaligned(ptr)
    }

    #[inline]
    pub(crate) unsafe fn write_key(base: *mut u8, key: K) {
        let ptr = base.add(Self::key_offset()) as *mut K;
        std::ptr::write_unaligned(ptr, key);
    }

    #[inline]
    pub(crate) unsafe fn write_value(base: *mut u8, value: V) {
        let ptr = base.add(Self::value_offset()) as *mut V;
        std::ptr::write_unaligned(ptr, value);
    }
}

// RecordInfo should be exactly 8 bytes.
const _: () = assert!(mem::size_of::<RecordInfo>() == 8);
