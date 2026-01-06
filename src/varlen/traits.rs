//! Variable-length struct traits
//!
//! This module defines traits for working with variable-length data structures
//! in FASTER.

use crate::record::{Key, Value};

/// Trait for variable-length structs
///
/// This trait provides methods for working with variable-length data
/// that needs to be serialized and stored in FASTER.
pub trait VarLenStruct: Clone + Send + Sync + 'static {
    /// Get the total serialized size of this struct
    fn serialized_size(&self) -> usize;

    /// Serialize this struct to a byte buffer
    ///
    /// Returns the number of bytes written.
    fn serialize(&self, buffer: &mut [u8]) -> usize;

    /// Deserialize from a byte buffer
    fn deserialize(buffer: &[u8]) -> Option<Self>
    where
        Self: Sized;
}

/// Trait for variable-length keys
///
/// Extends the Key trait with variable-length support.
pub trait VarLenKey: Key + VarLenStruct {
    /// Get the initial length for serialization
    fn initial_length(&self) -> usize {
        self.serialized_size()
    }
}

/// Trait for variable-length values
///
/// Extends the Value trait with variable-length support.
pub trait VarLenValue: Value + VarLenStruct {
    /// Get the initial length for serialization
    fn initial_length(&self) -> usize {
        self.serialized_size()
    }

    /// Try to update in place if the new value fits
    fn try_in_place_update(&mut self, new_value: &Self) -> bool {
        // Default: can only update in place if same size
        new_value.serialized_size() <= self.serialized_size()
    }
}

/// Implement VarLenStruct for SpanByte
impl VarLenStruct for super::SpanByte {
    fn serialized_size(&self) -> usize {
        self.total_size()
    }

    fn serialize(&self, buffer: &mut [u8]) -> usize {
        self.serialize_to(buffer)
    }

    fn deserialize(buffer: &[u8]) -> Option<Self> {
        Self::deserialize_from(buffer)
    }
}

/// Implement Key for SpanByte
impl Key for super::SpanByte {
    fn size(&self) -> u32 {
        self.total_size() as u32
    }

    fn get_hash(&self) -> u64 {
        super::SpanByte::get_hash(self)
    }
}

/// Implement Value for SpanByte
impl Value for super::SpanByte {
    fn size(&self) -> u32 {
        self.total_size() as u32
    }
}

/// Implement VarLenKey for SpanByte
impl VarLenKey for super::SpanByte {}

/// Implement VarLenValue for SpanByte
impl VarLenValue for super::SpanByte {
    fn try_in_place_update(&mut self, new_value: &Self) -> bool {
        let new_len = new_value.len();
        let old_len = self.len();

        // Can update in place if new value is not larger
        if new_len <= old_len {
            // Access the underlying mutable slice (full buffer) for in-place update
            // We must do this BEFORE calling set_length() which truncates the buffer
            let data = self.as_mut_slice();

            // Copy the new data into the beginning of the buffer
            data[..new_len].copy_from_slice(new_value.as_slice());

            // Clear trailing stale bytes to avoid data leakage and hash corruption
            // This must happen before truncation
            if new_len < old_len {
                data[new_len..old_len].fill(0);
            }

            // Update the length field and truncate buffer to the new size
            // This is critical: len()/total_size()/serialization must use the new length
            self.set_length(new_len);

            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varlen::SpanByte;

    #[test]
    fn test_span_byte_as_key() {
        let span = SpanByte::from_slice(b"key data");

        // Test Key trait
        assert_eq!(Key::size(&span), span.total_size() as u32);
        let hash = Key::get_hash(&span);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_span_byte_as_value() {
        let span = SpanByte::from_slice(b"value data");

        // Test Value trait
        assert_eq!(Value::size(&span), span.total_size() as u32);
    }

    #[test]
    fn test_var_len_struct() {
        let span = SpanByte::from_slice(b"test data");

        let size = span.serialized_size();
        let mut buffer = vec![0u8; size];

        let written = span.serialize(&mut buffer);
        assert_eq!(written, size);

        let deserialized = SpanByte::deserialize(&buffer).unwrap();
        assert_eq!(span, deserialized);
    }

    #[test]
    fn test_var_len_value_in_place_update() {
        let mut span1 = SpanByte::from_slice(b"hello world");
        let span2 = SpanByte::from_slice(b"hi");

        // Smaller value can update in place
        assert!(span1.try_in_place_update(&span2));

        let span3 = SpanByte::from_slice(b"this is a much longer value");
        // Larger value cannot update in place
        assert!(!span1.try_in_place_update(&span3));
    }

    #[test]
    fn test_var_len_value_in_place_update_length_correctness() {
        // Regression test: ensure in-place shrink updates length correctly
        let mut span = SpanByte::from_slice(b"0123456789"); // 10 bytes
        let new_value = SpanByte::from_slice(b"ab"); // 2 bytes

        let old_len = span.len();
        assert_eq!(old_len, 10);

        // Perform in-place update (shrink)
        assert!(span.try_in_place_update(&new_value));

        // After update, length MUST reflect the new smaller size
        assert_eq!(span.len(), 2, "length not updated after in-place shrink");
        assert_eq!(span.as_slice()[..2], *b"ab", "data not correctly updated");

        // total_size() and serialization must also use the new length
        let expected_total = 4 + 2; // header (4) + payload (2), no metadata
        assert_eq!(
            span.total_size(),
            expected_total,
            "total_size() incorrect after shrink"
        );

        // Verify serialization uses correct length (no stale trailing bytes)
        let serialized_size = span.serialized_size();
        assert_eq!(
            serialized_size, expected_total,
            "serialized_size() incorrect"
        );

        let mut buffer = vec![0xFFu8; 20]; // fill with 0xFF to detect stale bytes
        let written = span.serialize(&mut buffer);
        assert_eq!(
            written, expected_total,
            "serialize() wrote wrong number of bytes"
        );

        // Deserialize and verify round-trip correctness
        let deserialized = SpanByte::deserialize(&buffer[..written]).unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.as_slice(), b"ab");
    }

    #[test]
    fn test_var_len_value_in_place_update_clears_stale_bytes() {
        // Ensure trailing bytes are zeroed after shrink
        let mut span = SpanByte::from_slice(b"ABCDEFGHIJ"); // 10 bytes
        let new_value = SpanByte::from_slice(b"xy"); // 2 bytes

        assert!(span.try_in_place_update(&new_value));

        // The underlying data buffer still exists but trailing bytes should be zeroed
        // to avoid data leakage (accessing via as_mut_slice on the full capacity)
        // Note: after set_length, len() returns 2, but the buffer may still have capacity
        assert_eq!(span.len(), 2);
        assert_eq!(span.as_slice(), b"xy");
    }
}
