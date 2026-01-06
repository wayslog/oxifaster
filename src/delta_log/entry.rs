//! Delta Log entry types and header structure
//!
//! This module defines the format of entries in the delta (incremental) log.
//! Each entry consists of a fixed-size header followed by variable-length payload.
//!
//! ## Entry Format
//!
//! ```text
//! +----------------+----------------+----------------+
//! |   Checksum     |    Length      |     Type       |
//! |    8 bytes     |    4 bytes     |    4 bytes     |
//! +----------------+----------------+----------------+
//! |                    Payload                       |
//! |                  (variable)                      |
//! +--------------------------------------------------+
//! ```
//!
//! Based on C# FASTER's DeltaLog implementation.

use std::io;

/// Header size in bytes (8 + 4 + 4 = 16)
pub const DELTA_LOG_HEADER_SIZE: usize = 16;

/// The type of a record in the delta (incremental) log
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum DeltaLogEntryType {
    /// The entry is a delta record (changed data)
    Delta = 0,
    /// The entry is checkpoint metadata
    CheckpointMetadata = 1,
}

impl DeltaLogEntryType {
    /// Convert from i32
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(DeltaLogEntryType::Delta),
            1 => Some(DeltaLogEntryType::CheckpointMetadata),
            _ => None,
        }
    }

    /// Convert to i32
    pub fn to_i32(self) -> i32 {
        self as i32
    }
}

impl Default for DeltaLogEntryType {
    fn default() -> Self {
        DeltaLogEntryType::Delta
    }
}

/// Delta log entry header
///
/// Layout (16 bytes total):
/// - Checksum: 8 bytes (XOR checksum from offset 8)
/// - Length: 4 bytes (payload length, not including header)
/// - Type: 4 bytes (entry type)
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct DeltaLogHeader {
    /// XOR checksum computed over bytes from offset 8 to end of entry
    pub checksum: u64,
    /// Length of the payload (not including header)
    pub length: i32,
    /// Type of the entry
    pub entry_type: i32,
}

impl DeltaLogHeader {
    /// Create a new header
    pub fn new(length: i32, entry_type: DeltaLogEntryType) -> Self {
        Self {
            checksum: 0,
            length,
            entry_type: entry_type.to_i32(),
        }
    }

    /// Get the entry type
    pub fn get_entry_type(&self) -> Option<DeltaLogEntryType> {
        DeltaLogEntryType::from_i32(self.entry_type)
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; DELTA_LOG_HEADER_SIZE] {
        let mut bytes = [0u8; DELTA_LOG_HEADER_SIZE];
        bytes[0..8].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.length.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.entry_type.to_le_bytes());
        bytes
    }

    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < DELTA_LOG_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Buffer too small for header: {} < {}",
                    bytes.len(),
                    DELTA_LOG_HEADER_SIZE
                ),
            ));
        }

        let checksum = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let length = i32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let entry_type = i32::from_le_bytes(bytes[12..16].try_into().unwrap());

        Ok(Self {
            checksum,
            length,
            entry_type,
        })
    }

    /// Get the total size of this entry (header + payload)
    pub fn total_size(&self) -> usize {
        DELTA_LOG_HEADER_SIZE + self.length as usize
    }

    /// Check if this is a valid entry (length > 0 and valid type)
    pub fn is_valid(&self) -> bool {
        self.length > 0 && self.get_entry_type().is_some()
    }
}

/// Compute XOR checksum over a byte slice
///
/// This matches C# FASTER's Utility.XorBytes implementation.
pub fn compute_xor_checksum(data: &[u8]) -> u64 {
    let mut checksum: u64 = 0;

    // Process 8 bytes at a time
    let chunks = data.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let value = u64::from_le_bytes(chunk.try_into().unwrap());
        checksum ^= value;
    }

    // Handle remaining bytes
    if !remainder.is_empty() {
        let mut last_bytes = [0u8; 8];
        last_bytes[..remainder.len()].copy_from_slice(remainder);
        checksum ^= u64::from_le_bytes(last_bytes);
    }

    checksum
}

/// Verify the checksum of an entry
///
/// The checksum is computed over bytes from offset 8 (after checksum field)
/// to the end of the entry (header + payload).
pub fn verify_checksum(header: &DeltaLogHeader, payload: &[u8]) -> bool {
    // Build the data to checksum: header bytes from offset 8 + payload
    let mut data = Vec::with_capacity(8 + payload.len());
    data.extend_from_slice(&header.length.to_le_bytes());
    data.extend_from_slice(&header.entry_type.to_le_bytes());
    data.extend_from_slice(payload);

    let computed = compute_xor_checksum(&data);
    computed == header.checksum
}

/// Compute and set the checksum for an entry
pub fn compute_entry_checksum(header: &mut DeltaLogHeader, payload: &[u8]) {
    // Build the data to checksum: header bytes from offset 8 + payload
    let mut data = Vec::with_capacity(8 + payload.len());
    data.extend_from_slice(&header.length.to_le_bytes());
    data.extend_from_slice(&header.entry_type.to_le_bytes());
    data.extend_from_slice(payload);

    header.checksum = compute_xor_checksum(&data);
}

/// A delta log entry with header and payload
#[derive(Debug, Clone)]
pub struct DeltaLogEntry {
    /// Entry header
    pub header: DeltaLogHeader,
    /// Entry payload
    pub payload: Vec<u8>,
}

impl DeltaLogEntry {
    /// Create a new entry with the given payload and type
    pub fn new(payload: Vec<u8>, entry_type: DeltaLogEntryType) -> Self {
        let mut header = DeltaLogHeader::new(payload.len() as i32, entry_type);
        compute_entry_checksum(&mut header, &payload);
        Self { header, payload }
    }

    /// Create a delta entry
    pub fn delta(payload: Vec<u8>) -> Self {
        Self::new(payload, DeltaLogEntryType::Delta)
    }

    /// Create a checkpoint metadata entry
    pub fn checkpoint_metadata(payload: Vec<u8>) -> Self {
        Self::new(payload, DeltaLogEntryType::CheckpointMetadata)
    }

    /// Get the entry type
    pub fn entry_type(&self) -> Option<DeltaLogEntryType> {
        self.header.get_entry_type()
    }

    /// Verify the entry checksum
    pub fn verify(&self) -> bool {
        verify_checksum(&self.header, &self.payload)
    }

    /// Serialize the entire entry to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.header.total_size());
        bytes.extend_from_slice(&self.header.to_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// Deserialize an entry from bytes
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let header = DeltaLogHeader::from_bytes(bytes)?;
        let payload_start = DELTA_LOG_HEADER_SIZE;
        let payload_end = payload_start + header.length as usize;

        if bytes.len() < payload_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Buffer too small for entry payload: {} < {}",
                    bytes.len(),
                    payload_end
                ),
            ));
        }

        let payload = bytes[payload_start..payload_end].to_vec();

        Ok(Self { header, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_type_conversion() {
        assert_eq!(
            DeltaLogEntryType::from_i32(0),
            Some(DeltaLogEntryType::Delta)
        );
        assert_eq!(
            DeltaLogEntryType::from_i32(1),
            Some(DeltaLogEntryType::CheckpointMetadata)
        );
        assert_eq!(DeltaLogEntryType::from_i32(2), None);
        assert_eq!(DeltaLogEntryType::from_i32(-1), None);

        assert_eq!(DeltaLogEntryType::Delta.to_i32(), 0);
        assert_eq!(DeltaLogEntryType::CheckpointMetadata.to_i32(), 1);
    }

    #[test]
    fn test_header_serialization() {
        let header = DeltaLogHeader {
            checksum: 0x123456789ABCDEF0,
            length: 1024,
            entry_type: DeltaLogEntryType::Delta.to_i32(),
        };

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), DELTA_LOG_HEADER_SIZE);

        let restored = DeltaLogHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.checksum, header.checksum);
        assert_eq!(restored.length, header.length);
        assert_eq!(restored.entry_type, header.entry_type);
    }

    #[test]
    fn test_xor_checksum() {
        let data = b"hello world";
        let checksum = compute_xor_checksum(data);
        // Checksum should be deterministic
        assert_eq!(checksum, compute_xor_checksum(data));

        // Different data should give different checksum (usually)
        let data2 = b"hello world!";
        assert_ne!(checksum, compute_xor_checksum(data2));
    }

    #[test]
    fn test_entry_checksum() {
        let payload = b"test payload data".to_vec();
        let entry = DeltaLogEntry::delta(payload);

        assert!(entry.verify());
        assert_eq!(entry.entry_type(), Some(DeltaLogEntryType::Delta));
    }

    #[test]
    fn test_entry_serialization() {
        let payload = b"checkpoint metadata content".to_vec();
        let entry = DeltaLogEntry::checkpoint_metadata(payload.clone());

        let bytes = entry.to_bytes();
        let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();

        assert_eq!(restored.payload, payload);
        assert_eq!(
            restored.entry_type(),
            Some(DeltaLogEntryType::CheckpointMetadata)
        );
        assert!(restored.verify());
    }

    #[test]
    fn test_header_validity() {
        let valid_header = DeltaLogHeader::new(100, DeltaLogEntryType::Delta);
        assert!(valid_header.is_valid());

        let zero_length = DeltaLogHeader::new(0, DeltaLogEntryType::Delta);
        assert!(!zero_length.is_valid());

        let negative_length = DeltaLogHeader::new(-1, DeltaLogEntryType::Delta);
        assert!(!negative_length.is_valid());

        let invalid_type = DeltaLogHeader {
            checksum: 0,
            length: 100,
            entry_type: 999,
        };
        assert!(!invalid_type.is_valid());
    }

    #[test]
    fn test_checksum_verification_failure() {
        let payload = b"original data".to_vec();
        let mut entry = DeltaLogEntry::delta(payload);

        // Corrupt the checksum
        entry.header.checksum ^= 1;
        assert!(!entry.verify());
    }
}
