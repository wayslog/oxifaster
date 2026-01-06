//! SpanByte - Variable-length byte array
//!
//! SpanByte represents a pinned variable-length byte array that can be used
//! as a key or value in FASTER. It is similar to C# FASTER's SpanByte.
//!
//! ## Format
//!
//! The on-disk format is:
//! ```text
//! [4-byte length header][optional 8-byte metadata][payload bytes...]
//! ```
//!
//! The first 2 bits of the length field are used for flags:
//! - Bit 31: Serialized (0) or Unserialized (1)
//! - Bit 30: Extra metadata present (1) or absent (0)
//!
//! Maximum payload size is 1GB (2^30 - 1 bytes).

use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Bit mask for unserialized flag (bit 31)
const UNSERIALIZED_BIT_MASK: u32 = 1 << 31;

/// Bit mask for extra metadata flag (bit 30)
const EXTRA_METADATA_BIT_MASK: u32 = 1 << 30;

/// Mask for header bits
const HEADER_MASK: u32 = 0x3 << 30;

/// Maximum length of payload (1GB - 1)
pub const MAX_SPAN_BYTE_LENGTH: u32 = (1 << 30) - 1;

/// SpanByte header size
pub const SPAN_BYTE_HEADER_SIZE: usize = 4;

/// SpanByte metadata size
pub const SPAN_BYTE_METADATA_SIZE: usize = 8;

/// A variable-length byte array for use as key or value in FASTER.
///
/// SpanByte can represent both:
/// - Serialized (inline) data where the bytes follow the header
/// - Unserialized (pointer-based) data where we hold a reference to external bytes
///
/// ## Memory Layout
///
/// When serialized (on-disk):
/// ```text
/// [length: 4 bytes][metadata?: 8 bytes][payload: N bytes]
/// ```
///
/// When unserialized (in-memory):
/// The SpanByte holds a reference to the actual byte slice.
#[derive(Clone)]
pub struct SpanByte {
    /// Length with header flags in upper 2 bits
    length: u32,
    /// The actual byte data (owned or borrowed)
    data: Vec<u8>,
    /// Optional extra metadata
    metadata: Option<u64>,
}

impl SpanByte {
    /// Create an empty SpanByte
    pub fn new() -> Self {
        Self {
            length: 0,
            data: Vec::new(),
            metadata: None,
        }
    }

    /// Create a SpanByte from a byte slice
    pub fn from_slice(data: &[u8]) -> Self {
        assert!(data.len() <= MAX_SPAN_BYTE_LENGTH as usize);
        Self {
            length: data.len() as u32,
            data: data.to_vec(),
            metadata: None,
        }
    }

    /// Create a SpanByte from owned bytes
    pub fn from_vec(data: Vec<u8>) -> Self {
        assert!(data.len() <= MAX_SPAN_BYTE_LENGTH as usize);
        let len = data.len() as u32;
        Self {
            length: len,
            data,
            metadata: None,
        }
    }

    /// Create a SpanByte from a string
    pub fn from_str(s: &str) -> Self {
        Self::from_slice(s.as_bytes())
    }

    /// Get the length of the payload (excluding header and metadata)
    #[inline]
    pub fn len(&self) -> usize {
        (self.length & !HEADER_MASK) as usize
    }

    /// Set the length of the payload (preserving header flags)
    ///
    /// This method updates both the logical length field and truncates the
    /// underlying data vector to maintain the invariant that `data.len() == len()`.
    ///
    /// # Panics
    /// Panics if `new_len` exceeds MAX_SPAN_BYTE_LENGTH or the underlying data capacity.
    #[inline]
    pub fn set_length(&mut self, new_len: usize) {
        assert!(
            new_len <= MAX_SPAN_BYTE_LENGTH as usize,
            "length exceeds maximum"
        );
        assert!(new_len <= self.data.len(), "length exceeds data capacity");
        // Preserve header flags (upper 2 bits), update length (lower 30 bits)
        let header_flags = self.length & HEADER_MASK;
        self.length = header_flags | (new_len as u32);
        // Truncate the data vector to maintain invariant: data.len() == len()
        self.data.truncate(new_len);
    }

    /// Check if the SpanByte is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the total serialized size (header + metadata + payload)
    #[inline]
    pub fn total_size(&self) -> usize {
        SPAN_BYTE_HEADER_SIZE + self.metadata_size() + self.len()
    }

    /// Check if the SpanByte has extra metadata
    #[inline]
    pub fn has_metadata(&self) -> bool {
        (self.length & EXTRA_METADATA_BIT_MASK) != 0
    }

    /// Get the metadata size (0 or 8)
    #[inline]
    pub fn metadata_size(&self) -> usize {
        if self.has_metadata() {
            SPAN_BYTE_METADATA_SIZE
        } else {
            0
        }
    }

    /// Get the extra metadata if present
    #[inline]
    pub fn metadata(&self) -> Option<u64> {
        self.metadata
    }

    /// Set the extra metadata
    pub fn set_metadata(&mut self, value: u64) {
        self.metadata = Some(value);
        self.length |= EXTRA_METADATA_BIT_MASK;
    }

    /// Clear the extra metadata
    pub fn clear_metadata(&mut self) {
        self.metadata = None;
        self.length &= !EXTRA_METADATA_BIT_MASK;
    }

    /// Get a slice of the payload
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get a mutable slice of the payload
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Check if this is a serialized SpanByte
    #[inline]
    pub fn is_serialized(&self) -> bool {
        (self.length & UNSERIALIZED_BIT_MASK) == 0
    }

    /// Serialize the SpanByte to a byte buffer
    ///
    /// Returns the number of bytes written.
    pub fn serialize_to(&self, buffer: &mut [u8]) -> usize {
        let total = self.total_size();
        assert!(buffer.len() >= total);

        // Write length header (without unserialized flag)
        let header = self.length & !UNSERIALIZED_BIT_MASK;
        buffer[0..4].copy_from_slice(&header.to_le_bytes());

        let mut offset = SPAN_BYTE_HEADER_SIZE;

        // Write metadata if present
        if let Some(meta) = self.metadata {
            buffer[offset..offset + 8].copy_from_slice(&meta.to_le_bytes());
            offset += SPAN_BYTE_METADATA_SIZE;
        }

        // Write payload
        buffer[offset..offset + self.len()].copy_from_slice(&self.data);

        total
    }

    /// Deserialize a SpanByte from a byte buffer
    pub fn deserialize_from(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < SPAN_BYTE_HEADER_SIZE {
            return None;
        }

        let header = u32::from_le_bytes(buffer[0..4].try_into().ok()?);
        let payload_len = (header & !HEADER_MASK) as usize;
        let has_metadata = (header & EXTRA_METADATA_BIT_MASK) != 0;

        let metadata_size = if has_metadata {
            SPAN_BYTE_METADATA_SIZE
        } else {
            0
        };
        let total_size = SPAN_BYTE_HEADER_SIZE + metadata_size + payload_len;

        if buffer.len() < total_size {
            return None;
        }

        let mut offset = SPAN_BYTE_HEADER_SIZE;

        let metadata = if has_metadata {
            let meta = u64::from_le_bytes(buffer[offset..offset + 8].try_into().ok()?);
            offset += SPAN_BYTE_METADATA_SIZE;
            Some(meta)
        } else {
            None
        };

        let data = buffer[offset..offset + payload_len].to_vec();

        Some(Self {
            length: header,
            data,
            metadata,
        })
    }

    /// Compute a hash of the SpanByte data
    pub fn get_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        self.data.hash(&mut hasher);
        hasher.finish()
    }

    /// Compare two SpanBytes for equality
    pub fn equals(&self, other: &SpanByte) -> bool {
        self.data == other.data
    }

    /// Copy the payload to a destination buffer
    pub fn copy_to(&self, dest: &mut [u8]) {
        let len = self.len().min(dest.len());
        dest[..len].copy_from_slice(&self.data[..len]);
    }

    /// Try to convert to a UTF-8 string
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

impl Default for SpanByte {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for SpanByte {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl AsRef<[u8]> for SpanByte {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl From<&[u8]> for SpanByte {
    fn from(data: &[u8]) -> Self {
        Self::from_slice(data)
    }
}

impl From<Vec<u8>> for SpanByte {
    fn from(data: Vec<u8>) -> Self {
        Self::from_vec(data)
    }
}

impl From<&str> for SpanByte {
    fn from(s: &str) -> Self {
        Self::from_str(s)
    }
}

impl From<String> for SpanByte {
    fn from(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }
}

impl PartialEq for SpanByte {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for SpanByte {}

impl Hash for SpanByte {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl fmt::Debug for SpanByte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.len() <= 32 {
            write!(f, "SpanByte({:?})", &self.data)
        } else {
            write!(f, "SpanByte([{} bytes])", self.len())
        }
    }
}

impl fmt::Display for SpanByte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_lossy())
    }
}

/// Builder for creating SpanByte instances
pub struct SpanByteBuilder {
    data: Vec<u8>,
    metadata: Option<u64>,
}

impl SpanByteBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            metadata: None,
        }
    }

    /// Create a builder with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            metadata: None,
        }
    }

    /// Append bytes to the builder (consuming builder pattern)
    pub fn append(mut self, data: &[u8]) -> Self {
        self.data.extend_from_slice(data);
        self
    }

    /// Append a single byte (consuming builder pattern)
    pub fn push(mut self, byte: u8) -> Self {
        self.data.push(byte);
        self
    }

    /// Set the metadata (consuming builder pattern)
    pub fn metadata(mut self, value: u64) -> Self {
        self.metadata = Some(value);
        self
    }

    /// Build the SpanByte
    pub fn build(self) -> SpanByte {
        let mut span = SpanByte::from_vec(self.data);
        if let Some(meta) = self.metadata {
            span.set_metadata(meta);
        }
        span
    }
}

impl Default for SpanByteBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_byte_new() {
        let span = SpanByte::new();
        assert_eq!(span.len(), 0);
        assert!(span.is_empty());
    }

    #[test]
    fn test_span_byte_from_slice() {
        let data = b"hello world";
        let span = SpanByte::from_slice(data);
        assert_eq!(span.len(), 11);
        assert_eq!(span.as_slice(), data);
    }

    #[test]
    fn test_span_byte_from_string() {
        let span = SpanByte::from_str("hello");
        assert_eq!(span.len(), 5);
        assert_eq!(span.to_string_lossy(), "hello");
    }

    #[test]
    fn test_span_byte_metadata() {
        let mut span = SpanByte::from_slice(b"data");
        assert!(!span.has_metadata());
        assert_eq!(span.metadata(), None);

        span.set_metadata(12345);
        assert!(span.has_metadata());
        assert_eq!(span.metadata(), Some(12345));

        span.clear_metadata();
        assert!(!span.has_metadata());
    }

    #[test]
    fn test_span_byte_serialize_deserialize() {
        let original = SpanByte::from_slice(b"test data");
        let mut buffer = vec![0u8; original.total_size()];

        original.serialize_to(&mut buffer);

        let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_span_byte_serialize_with_metadata() {
        let mut original = SpanByte::from_slice(b"test data");
        original.set_metadata(999);

        let mut buffer = vec![0u8; original.total_size()];
        original.serialize_to(&mut buffer);

        let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
        assert_eq!(original.as_slice(), deserialized.as_slice());
        assert_eq!(deserialized.metadata(), Some(999));
    }

    #[test]
    fn test_span_byte_hash() {
        let span1 = SpanByte::from_slice(b"hello");
        let span2 = SpanByte::from_slice(b"hello");
        let span3 = SpanByte::from_slice(b"world");

        assert_eq!(span1.get_hash(), span2.get_hash());
        assert_ne!(span1.get_hash(), span3.get_hash());
    }

    #[test]
    fn test_span_byte_equality() {
        let span1 = SpanByte::from_slice(b"hello");
        let span2 = SpanByte::from_slice(b"hello");
        let span3 = SpanByte::from_slice(b"world");

        assert_eq!(span1, span2);
        assert_ne!(span1, span3);
    }

    #[test]
    fn test_span_byte_builder() {
        let span = SpanByteBuilder::new()
            .append(b"hello")
            .push(b' ')
            .append(b"world")
            .metadata(42)
            .build();

        assert_eq!(span.as_slice(), b"hello world");
        assert_eq!(span.metadata(), Some(42));
    }

    #[test]
    fn test_span_byte_total_size() {
        let span = SpanByte::from_slice(b"data");
        assert_eq!(span.total_size(), 4 + 4); // header + data

        let mut span_with_meta = SpanByte::from_slice(b"data");
        span_with_meta.set_metadata(1);
        assert_eq!(span_with_meta.total_size(), 4 + 8 + 4); // header + metadata + data
    }
}
