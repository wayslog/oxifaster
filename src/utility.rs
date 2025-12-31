//! Utility functions for FASTER
//!
//! This module provides various utility functions used throughout the library.

use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

/// Check if a value is a power of two
#[inline]
pub const fn is_power_of_two(n: u64) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

/// Round up to the next power of two
#[inline]
pub const fn next_power_of_two(mut n: u64) -> u64 {
    if n == 0 {
        return 1;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n + 1
}

/// Pad a size to the specified alignment
#[inline]
pub const fn pad_alignment(size: usize, alignment: usize) -> usize {
    debug_assert!(is_power_of_two(alignment as u64));
    (size + alignment - 1) & !(alignment - 1)
}

/// Aligned memory allocation
///
/// # Safety
/// The caller must ensure that:
/// - `alignment` is a power of two
/// - `size` is non-zero
/// - The returned memory must be deallocated with `aligned_free`
pub unsafe fn aligned_alloc(alignment: usize, size: usize) -> Option<NonNull<u8>> {
    debug_assert!(is_power_of_two(alignment as u64));
    debug_assert!(size > 0);

    let layout = Layout::from_size_align(size, alignment).ok()?;
    let ptr = alloc(layout);
    NonNull::new(ptr)
}

/// Aligned zeroed memory allocation
///
/// # Safety
/// Same as `aligned_alloc`
pub unsafe fn aligned_alloc_zeroed(alignment: usize, size: usize) -> Option<NonNull<u8>> {
    debug_assert!(is_power_of_two(alignment as u64));
    debug_assert!(size > 0);

    let layout = Layout::from_size_align(size, alignment).ok()?;
    let ptr = alloc_zeroed(layout);
    NonNull::new(ptr)
}

/// Free aligned memory
///
/// # Safety
/// The caller must ensure that:
/// - `ptr` was allocated with `aligned_alloc` or `aligned_alloc_zeroed`
/// - `alignment` and `size` match the original allocation
pub unsafe fn aligned_free(ptr: NonNull<u8>, alignment: usize, size: usize) {
    let layout = Layout::from_size_align(size, alignment).unwrap();
    dealloc(ptr.as_ptr(), layout);
}

/// RAII wrapper for aligned memory
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    alignment: usize,
}

impl AlignedBuffer {
    /// Allocate a new aligned buffer
    pub fn new(alignment: usize, size: usize) -> Option<Self> {
        unsafe {
            aligned_alloc(alignment, size).map(|ptr| Self {
                ptr,
                size,
                alignment,
            })
        }
    }

    /// Allocate a new zeroed aligned buffer
    pub fn zeroed(alignment: usize, size: usize) -> Option<Self> {
        unsafe {
            aligned_alloc_zeroed(alignment, size).map(|ptr| Self {
                ptr,
                size,
                alignment,
            })
        }
    }

    /// Get a pointer to the buffer
    pub fn as_ptr(&self) -> *const u8 {
        // Explicitly cast from *mut to *const for const correctness
        self.ptr.as_ptr() as *const u8
    }

    /// Get a mutable pointer to the buffer
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        // NonNull::as_ptr() returns *mut T
        self.ptr.as_ptr()
    }

    /// Get the size of the buffer
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get a slice view of the buffer
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Get a mutable slice view of the buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            aligned_free(self.ptr, self.alignment, self.size);
        }
    }
}

// Safety: AlignedBuffer owns its memory and doesn't share references
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

/// Hash combine function (similar to boost::hash_combine)
#[inline]
pub const fn hash_combine(seed: u64, value: u64) -> u64 {
    seed ^ (value.wrapping_add(0x9e3779b9).wrapping_add(seed << 6).wrapping_add(seed >> 2))
}

/// MurmurHash3 finalizer (64-bit)
#[inline]
pub const fn murmur3_finalize(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_power_of_two() {
        assert!(!is_power_of_two(0));
        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(!is_power_of_two(3));
        assert!(is_power_of_two(4));
        assert!(is_power_of_two(1024));
        assert!(!is_power_of_two(1023));
    }

    #[test]
    fn test_next_power_of_two() {
        assert_eq!(next_power_of_two(0), 1);
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(2), 2);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(5), 8);
        assert_eq!(next_power_of_two(1000), 1024);
    }

    #[test]
    fn test_pad_alignment() {
        assert_eq!(pad_alignment(1, 8), 8);
        assert_eq!(pad_alignment(8, 8), 8);
        assert_eq!(pad_alignment(9, 8), 16);
        assert_eq!(pad_alignment(100, 64), 128);
    }

    #[test]
    fn test_aligned_buffer() {
        let mut buf = AlignedBuffer::zeroed(64, 1024).unwrap();
        assert_eq!(buf.size(), 1024);
        assert_eq!(buf.as_ptr() as usize % 64, 0); // Check alignment

        // Write and read
        buf.as_mut_slice()[0] = 42;
        assert_eq!(buf.as_slice()[0], 42);
    }
}

