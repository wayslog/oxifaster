//! Record structures for FASTER's hybrid log
//!
//! This module defines the record format stored in FASTER's hybrid log.
//! Each record consists of a header (RecordInfo) followed by a key and value.

use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::address::Address;
use crate::utility::pad_alignment;

/// Record header, internal to FASTER
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

    /// Create a new record info
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

    /// Create a record info from raw control value
    pub fn from_control(control: u64) -> Self {
        Self {
            control: AtomicU64::new(control),
        }
    }

    /// Check if the record info is null (all zeros)
    #[inline]
    pub fn is_null(&self) -> bool {
        self.control.load(Ordering::Acquire) == 0
    }

    /// Get the previous address in the hash chain
    #[inline]
    pub fn previous_address(&self) -> Address {
        Address::from_control(self.control.load(Ordering::Acquire) & Self::PREV_ADDR_MASK)
    }

    /// Set the previous address
    #[inline]
    pub fn set_previous_address(&self, addr: Address) {
        let mut current = self.control.load(Ordering::Acquire);
        loop {
            let new_val = (current & !Self::PREV_ADDR_MASK) | (addr.control() & Self::PREV_ADDR_MASK);
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

    /// Get the checkpoint version
    #[inline]
    pub fn checkpoint_version(&self) -> u16 {
        ((self.control.load(Ordering::Acquire) >> Self::VERSION_SHIFT) & Self::VERSION_MASK) as u16
    }

    /// Check if the record is invalid
    #[inline]
    pub fn is_invalid(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::INVALID_BIT) != 0
    }

    /// Set the invalid flag
    #[inline]
    pub fn set_invalid(&self, invalid: bool) {
        if invalid {
            self.control.fetch_or(Self::INVALID_BIT, Ordering::AcqRel);
        } else {
            self.control.fetch_and(!Self::INVALID_BIT, Ordering::AcqRel);
        }
    }

    /// Check if this is a tombstone (delete marker)
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::TOMBSTONE_BIT) != 0
    }

    /// Set the tombstone flag
    #[inline]
    pub fn set_tombstone(&self, tombstone: bool) {
        if tombstone {
            self.control.fetch_or(Self::TOMBSTONE_BIT, Ordering::AcqRel);
        } else {
            self.control.fetch_and(!Self::TOMBSTONE_BIT, Ordering::AcqRel);
        }
    }

    /// Check if this is the final record in a chain
    #[inline]
    pub fn is_final(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::FINAL_BIT) != 0
    }

    /// Check if the previous address points to read cache
    #[inline]
    pub fn in_read_cache(&self) -> bool {
        (self.control.load(Ordering::Acquire) & Self::READ_CACHE_BIT) != 0
    }

    /// Get the raw control value
    #[inline]
    pub fn control(&self) -> u64 {
        self.control.load(Ordering::Acquire)
    }

    /// Load the control value with specified ordering
    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.control.load(ordering)
    }

    /// Store the control value with specified ordering
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

// RecordInfo should be exactly 8 bytes
const _: () = assert!(mem::size_of::<RecordInfo>() == 8);

/// A record stored in the log
///
/// The log starts at address 0 and consists of Records, one after another.
/// Each record's header is 8 bytes, followed by the key (aligned) and value (aligned).
///
/// # Memory Layout
/// ```text
/// +----------------+
/// | RecordInfo (8) |
/// +----------------+
/// | padding        | (to key alignment)
/// +----------------+
/// | Key            |
/// +----------------+
/// | padding        | (to value alignment)
/// +----------------+
/// | Value          |
/// +----------------+
/// | padding        | (to RecordInfo alignment for next record)
/// +----------------+
/// ```
#[repr(C)]
pub struct Record<K, V> {
    /// Record header
    pub header: RecordInfo,
    /// Marker for key and value types
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Record<K, V> {
    /// Get a reference to the key
    ///
    /// # Safety
    /// The caller must ensure that the record has been properly initialized
    /// and the memory layout is correct.
    #[inline]
    pub unsafe fn key(&self) -> &K {
        let head = self as *const _ as *const u8;
        let offset = Self::key_offset();
        &*(head.add(offset) as *const K)
    }

    /// Get a mutable reference to the key
    ///
    /// # Safety
    /// Same as `key()`
    #[inline]
    pub unsafe fn key_mut(&mut self) -> &mut K {
        let head = self as *mut _ as *mut u8;
        let offset = Self::key_offset();
        &mut *(head.add(offset) as *mut K)
    }

    /// Get a reference to the value
    ///
    /// # Safety
    /// The caller must ensure that the record has been properly initialized
    /// and the memory layout is correct.
    #[inline]
    pub unsafe fn value(&self) -> &V {
        let head = self as *const _ as *const u8;
        let offset = Self::value_offset();
        &*(head.add(offset) as *const V)
    }

    /// Get a mutable reference to the value
    ///
    /// # Safety
    /// Same as `value()`
    #[inline]
    pub unsafe fn value_mut(&mut self) -> &mut V {
        let head = self as *mut _ as *mut u8;
        let offset = Self::value_offset();
        &mut *(head.add(offset) as *mut V)
    }

    /// Calculate the offset of the key from the record start
    #[inline]
    pub const fn key_offset() -> usize {
        pad_alignment(mem::size_of::<RecordInfo>(), mem::align_of::<K>())
    }

    /// Calculate the offset of the value from the record start
    #[inline]
    pub const fn value_offset() -> usize {
        pad_alignment(
            Self::key_offset() + mem::size_of::<K>(),
            mem::align_of::<V>(),
        )
    }

    /// Calculate the total size of a record in memory
    ///
    /// This includes padding after the value so that the next record is properly aligned.
    #[inline]
    pub const fn size() -> usize {
        pad_alignment(
            Self::value_offset() + mem::size_of::<V>(),
            mem::align_of::<RecordInfo>(),
        )
    }

    /// Calculate the size of a record with variable-sized key and value
    #[inline]
    pub fn size_with_sizes(key_size: u32, value_size: u32) -> u32 {
        let key_offset = pad_alignment(mem::size_of::<RecordInfo>(), mem::align_of::<K>());
        let value_offset = pad_alignment(key_offset + key_size as usize, mem::align_of::<V>());
        let total = pad_alignment(value_offset + value_size as usize, mem::align_of::<RecordInfo>());
        total as u32
    }

    /// Minimum size of a disk read to include the header and key size info
    #[inline]
    pub const fn min_disk_key_size() -> u32 {
        (Self::key_offset() + mem::size_of::<K>()) as u32
    }

    /// Size of the record on disk (excludes padding after value)
    #[inline]
    pub const fn disk_size() -> u32 {
        (Self::value_offset() + mem::size_of::<V>()) as u32
    }
}

impl<K: Clone, V: Clone> Clone for Record<K, V> {
    fn clone(&self) -> Self {
        Self {
            header: self.header.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V> std::fmt::Debug for Record<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("header", &self.header)
            .finish()
    }
}

/// Trait for keys that can be stored in FASTER
pub trait Key: Clone + Eq + Send + Sync + 'static {
    /// Get the size of the key in bytes
    fn size(&self) -> u32;

    /// Get the hash of the key
    fn get_hash(&self) -> u64;
}

/// Trait for values that can be stored in FASTER
pub trait Value: Clone + Send + Sync + 'static {
    /// Get the size of the value in bytes
    fn size(&self) -> u32;
}

// Implement Key for common types
impl Key for u64 {
    #[inline]
    fn size(&self) -> u32 {
        mem::size_of::<u64>() as u32
    }

    #[inline]
    fn get_hash(&self) -> u64 {
        crate::utility::murmur3_finalize(*self)
    }
}

impl Key for i64 {
    #[inline]
    fn size(&self) -> u32 {
        mem::size_of::<i64>() as u32
    }

    #[inline]
    fn get_hash(&self) -> u64 {
        crate::utility::murmur3_finalize(*self as u64)
    }
}

impl Key for String {
    #[inline]
    fn size(&self) -> u32 {
        (mem::size_of::<usize>() + self.len()) as u32
    }

    #[inline]
    fn get_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

// Implement Value for common types
impl Value for u64 {
    #[inline]
    fn size(&self) -> u32 {
        mem::size_of::<u64>() as u32
    }
}

impl Value for i64 {
    #[inline]
    fn size(&self) -> u32 {
        mem::size_of::<i64>() as u32
    }
}

impl<T> Value for Vec<T>
where
    T: Clone + Send + Sync + 'static,
{
    #[inline]
    fn size(&self) -> u32 {
        // Size includes length prefix + element data
        (mem::size_of::<usize>() + self.len() * mem::size_of::<T>()) as u32
    }
}

impl<T> Key for Vec<T>
where
    T: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    #[inline]
    fn size(&self) -> u32 {
        // Size includes length prefix + element data
        (mem::size_of::<usize>() + self.len() * mem::size_of::<T>()) as u32
    }

    #[inline]
    fn get_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Value for String {
    #[inline]
    fn size(&self) -> u32 {
        (mem::size_of::<usize>() + self.len()) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_info_creation() {
        let prev_addr = Address::new(10, 1000);
        let info = RecordInfo::new(prev_addr, 5, false, false, false);
        
        assert_eq!(info.previous_address(), prev_addr);
        assert_eq!(info.checkpoint_version(), 5);
        assert!(!info.is_invalid());
        assert!(!info.is_tombstone());
        assert!(!info.is_final());
    }

    #[test]
    fn test_record_info_flags() {
        let info = RecordInfo::new(Address::INVALID, 0, true, true, true);
        
        assert!(info.is_invalid());
        assert!(info.is_tombstone());
        assert!(info.is_final());
    }

    #[test]
    fn test_record_info_set_flags() {
        let info = RecordInfo::new(Address::INVALID, 0, false, false, false);
        
        assert!(!info.is_invalid());
        info.set_invalid(true);
        assert!(info.is_invalid());
        
        assert!(!info.is_tombstone());
        info.set_tombstone(true);
        assert!(info.is_tombstone());
    }

    #[test]
    fn test_record_size() {
        type TestRecord = Record<u64, u64>;
        
        // With 8-byte aligned key and value, size should be:
        // 8 (header) + 8 (key) + 8 (value) = 24 bytes
        assert_eq!(TestRecord::size(), 24);
        assert_eq!(TestRecord::key_offset(), 8);
        assert_eq!(TestRecord::value_offset(), 16);
    }

    #[test]
    fn test_key_hash() {
        let key1: u64 = 12345;
        let key2: u64 = 12345;
        let key3: u64 = 54321;
        
        assert_eq!(key1.get_hash(), key2.get_hash());
        assert_ne!(key1.get_hash(), key3.get_hash());
    }

    // ============ Vec<T> as Key tests ============

    #[test]
    fn test_vec_u8_as_key_size() {
        let key: Vec<u8> = vec![1, 2, 3, 4, 5];
        // size = size_of::<usize>() + len * size_of::<u8>()
        let expected = (mem::size_of::<usize>() + 5 * mem::size_of::<u8>()) as u32;
        assert_eq!(Key::size(&key), expected);
    }

    #[test]
    fn test_vec_u8_as_key_empty() {
        let key: Vec<u8> = vec![];
        let expected = mem::size_of::<usize>() as u32;
        assert_eq!(Key::size(&key), expected);
    }

    #[test]
    fn test_vec_u8_as_key_hash() {
        let key1: Vec<u8> = vec![1, 2, 3];
        let key2: Vec<u8> = vec![1, 2, 3];
        let key3: Vec<u8> = vec![3, 2, 1];
        
        assert_eq!(Key::get_hash(&key1), Key::get_hash(&key2));
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key3));
    }

    #[test]
    fn test_vec_i32_as_key_size() {
        let key: Vec<i32> = vec![1, 2, 3, 4];
        // size = size_of::<usize>() + len * size_of::<i32>()
        let expected = (mem::size_of::<usize>() + 4 * mem::size_of::<i32>()) as u32;
        assert_eq!(Key::size(&key), expected);
    }

    #[test]
    fn test_vec_i32_as_key_hash() {
        let key1: Vec<i32> = vec![100, 200, 300];
        let key2: Vec<i32> = vec![100, 200, 300];
        let key3: Vec<i32> = vec![100, 200, 301];
        
        assert_eq!(Key::get_hash(&key1), Key::get_hash(&key2));
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key3));
    }

    #[test]
    fn test_vec_u64_as_key_size() {
        let key: Vec<u64> = vec![1, 2, 3];
        let expected = (mem::size_of::<usize>() + 3 * mem::size_of::<u64>()) as u32;
        assert_eq!(Key::size(&key), expected);
    }

    #[test]
    fn test_vec_u64_as_key_hash() {
        let key1: Vec<u64> = vec![u64::MAX, 0, 12345];
        let key2: Vec<u64> = vec![u64::MAX, 0, 12345];
        let key3: Vec<u64> = vec![u64::MAX, 0, 12346];
        
        assert_eq!(Key::get_hash(&key1), Key::get_hash(&key2));
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key3));
    }

    #[test]
    fn test_vec_string_as_key_hash() {
        let key1: Vec<String> = vec!["hello".to_string(), "world".to_string()];
        let key2: Vec<String> = vec!["hello".to_string(), "world".to_string()];
        let key3: Vec<String> = vec!["hello".to_string(), "rust".to_string()];
        
        assert_eq!(Key::get_hash(&key1), Key::get_hash(&key2));
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key3));
    }

    // ============ Vec<T> as Value tests ============

    #[test]
    fn test_vec_u8_as_value_size() {
        let value: Vec<u8> = vec![10, 20, 30, 40, 50, 60];
        let expected = (mem::size_of::<usize>() + 6 * mem::size_of::<u8>()) as u32;
        assert_eq!(Value::size(&value), expected);
    }

    #[test]
    fn test_vec_u8_as_value_empty() {
        let value: Vec<u8> = vec![];
        let expected = mem::size_of::<usize>() as u32;
        assert_eq!(Value::size(&value), expected);
    }

    #[test]
    fn test_vec_i64_as_value_size() {
        let value: Vec<i64> = vec![-100, 0, 100, 200];
        let expected = (mem::size_of::<usize>() + 4 * mem::size_of::<i64>()) as u32;
        assert_eq!(Value::size(&value), expected);
    }

    #[test]
    fn test_vec_f64_as_value_size() {
        let value: Vec<f64> = vec![1.1, 2.2, 3.3];
        let expected = (mem::size_of::<usize>() + 3 * mem::size_of::<f64>()) as u32;
        assert_eq!(Value::size(&value), expected);
    }

    #[test]
    fn test_vec_bool_as_value_size() {
        let value: Vec<bool> = vec![true, false, true, true, false];
        let expected = (mem::size_of::<usize>() + 5 * mem::size_of::<bool>()) as u32;
        assert_eq!(Value::size(&value), expected);
    }

    #[test]
    fn test_vec_large_elements_as_value() {
        // Test with a larger element type (only Value, not Key since no Hash/Eq)
        #[derive(Clone)]
        struct LargeStruct {
            _data: [u8; 128],
        }
        
        let value: Vec<LargeStruct> = vec![
            LargeStruct { _data: [0; 128] },
            LargeStruct { _data: [1; 128] },
        ];
        let expected = (mem::size_of::<usize>() + 2 * mem::size_of::<LargeStruct>()) as u32;
        assert_eq!(Value::size(&value), expected);
    }

    // ============ Vec<T> Key equality tests ============

    #[test]
    fn test_vec_key_equality() {
        let key1: Vec<u8> = vec![1, 2, 3];
        let key2: Vec<u8> = vec![1, 2, 3];
        let key3: Vec<u8> = vec![1, 2, 4];
        
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_vec_key_clone() {
        let key1: Vec<i32> = vec![10, 20, 30];
        let key2 = key1.clone();
        
        assert_eq!(key1, key2);
        assert_eq!(Key::get_hash(&key1), Key::get_hash(&key2));
    }

    // ============ Nested Vec tests ============

    #[test]
    fn test_nested_vec_as_value_size() {
        // Vec<Vec<u8>> - nested vectors
        let value: Vec<Vec<u8>> = vec![
            vec![1, 2, 3],
            vec![4, 5],
        ];
        // Outer vec: size_of::<usize>() + 2 * size_of::<Vec<u8>>()
        let expected = (mem::size_of::<usize>() + 2 * mem::size_of::<Vec<u8>>()) as u32;
        assert_eq!(Value::size(&value), expected);
    }

    #[test]
    fn test_nested_vec_as_key_hash() {
        let key1: Vec<Vec<u8>> = vec![vec![1, 2], vec![3, 4]];
        let key2: Vec<Vec<u8>> = vec![vec![1, 2], vec![3, 4]];
        let key3: Vec<Vec<u8>> = vec![vec![1, 2], vec![3, 5]];
        
        assert_eq!(Key::get_hash(&key1), Key::get_hash(&key2));
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key3));
    }

    // ============ Edge case tests ============

    #[test]
    fn test_vec_single_element_as_key() {
        let key: Vec<u64> = vec![42];
        let expected = (mem::size_of::<usize>() + mem::size_of::<u64>()) as u32;
        assert_eq!(Key::size(&key), expected);
        
        // Hash should be consistent
        let key2: Vec<u64> = vec![42];
        assert_eq!(Key::get_hash(&key), Key::get_hash(&key2));
    }

    #[test]
    fn test_vec_hash_order_matters() {
        let key1: Vec<u8> = vec![1, 2, 3];
        let key2: Vec<u8> = vec![3, 2, 1];
        
        // Different order should produce different hash
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key2));
    }

    #[test]
    fn test_vec_hash_length_matters() {
        let key1: Vec<u8> = vec![1, 2];
        let key2: Vec<u8> = vec![1, 2, 0];
        
        // Different length should produce different hash
        assert_ne!(Key::get_hash(&key1), Key::get_hash(&key2));
    }

    // ============ Key and Value both implemented tests ============

    #[test]
    fn test_vec_key_and_value_size_same() {
        // Vec<u8> implements both Key and Value with same size calculation
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        let expected = (mem::size_of::<usize>() + 5 * mem::size_of::<u8>()) as u32;
        
        assert_eq!(Key::size(&data), expected);
        assert_eq!(Value::size(&data), expected);
        assert_eq!(Key::size(&data), Value::size(&data));
    }

    #[test]
    fn test_vec_i32_key_and_value_size_same() {
        let data: Vec<i32> = vec![100, 200, 300];
        let expected = (mem::size_of::<usize>() + 3 * mem::size_of::<i32>()) as u32;
        
        assert_eq!(Key::size(&data), expected);
        assert_eq!(Value::size(&data), expected);
    }
}

