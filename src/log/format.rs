//! FasterLog on-disk format helpers.

use std::fmt;

/// Errors related to metadata parsing/validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogMetadataError {
    BufferTooSmall,
    MagicMismatch,
    ChecksumMismatch,
    UnsupportedVersion(u32),
}

impl fmt::Display for LogMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogMetadataError::BufferTooSmall => write!(f, "metadata buffer too small"),
            LogMetadataError::MagicMismatch => write!(f, "metadata magic mismatch"),
            LogMetadataError::ChecksumMismatch => write!(f, "metadata checksum mismatch"),
            LogMetadataError::UnsupportedVersion(version) => {
                write!(f, "unsupported metadata version {version}")
            }
        }
    }
}

/// Errors related to log entry parsing/validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryError {
    BufferTooSmall,
}

impl fmt::Display for LogEntryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogEntryError::BufferTooSmall => write!(f, "entry buffer too small"),
        }
    }
}

/// Persistent log metadata stored at the beginning of the device.
#[derive(Debug, Clone)]
pub struct LogMetadata {
    pub version: u32,
    pub page_size: u32,
    pub segment_size: u64,
    pub data_offset: u64,
    pub begin_address: u64,
    pub committed_address: u64,
    pub tail_address: u64,
}

impl LogMetadata {
    pub const MAGIC: [u8; 8] = *b"OXFLOG1\0";
    pub const VERSION: u32 = 1;
    pub const ENCODED_SIZE: usize = 64;
    const CHECKSUM_OFFSET: usize = Self::ENCODED_SIZE - 8;

    pub fn new(page_size: u32, segment_size: u64, data_offset: u64) -> Self {
        Self {
            version: Self::VERSION,
            page_size,
            segment_size,
            data_offset,
            begin_address: 0,
            committed_address: 0,
            tail_address: 0,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) -> Result<(), LogMetadataError> {
        if buf.len() < Self::ENCODED_SIZE {
            return Err(LogMetadataError::BufferTooSmall);
        }

        buf[..Self::ENCODED_SIZE].fill(0);
        let mut offset = 0;

        buf[offset..offset + 8].copy_from_slice(&Self::MAGIC);
        offset += 8;
        buf[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;
        buf[offset..offset + 4].copy_from_slice(&self.page_size.to_le_bytes());
        offset += 4;
        buf[offset..offset + 8].copy_from_slice(&self.segment_size.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&self.data_offset.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&self.begin_address.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&self.committed_address.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&self.tail_address.to_le_bytes());

        let checksum = compute_xor_checksum(&buf[..Self::CHECKSUM_OFFSET]);
        buf[Self::CHECKSUM_OFFSET..Self::ENCODED_SIZE].copy_from_slice(&checksum.to_le_bytes());
        Ok(())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, LogMetadataError> {
        if buf.len() < Self::ENCODED_SIZE {
            return Err(LogMetadataError::BufferTooSmall);
        }

        if buf[..8] != Self::MAGIC {
            return Err(LogMetadataError::MagicMismatch);
        }

        let checksum = u64::from_le_bytes(
            buf[Self::CHECKSUM_OFFSET..Self::ENCODED_SIZE]
                .try_into()
                .unwrap(),
        );
        let computed = compute_xor_checksum(&buf[..Self::CHECKSUM_OFFSET]);
        if checksum != computed {
            return Err(LogMetadataError::ChecksumMismatch);
        }

        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        if version != Self::VERSION {
            return Err(LogMetadataError::UnsupportedVersion(version));
        }

        let page_size = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let segment_size = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let data_offset = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let begin_address = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let committed_address = u64::from_le_bytes(buf[40..48].try_into().unwrap());
        let tail_address = u64::from_le_bytes(buf[48..56].try_into().unwrap());

        Ok(Self {
            version,
            page_size,
            segment_size,
            data_offset,
            begin_address,
            committed_address,
            tail_address,
        })
    }
}

/// Entry flag used to mark padding records.
pub const LOG_ENTRY_FLAG_PADDING: u32 = 1 << 0;

/// Log entry header stored before each payload.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LogEntryHeader {
    pub length: u32,
    pub flags: u32,
    pub checksum: u64,
}

impl LogEntryHeader {
    pub const SIZE: usize = 16;

    pub fn new(length: u32, flags: u32, payload: &[u8]) -> Self {
        let checksum = compute_entry_checksum(length, flags, payload);
        Self {
            length,
            flags,
            checksum,
        }
    }

    pub fn padding(length: u32) -> Self {
        let flags = LOG_ENTRY_FLAG_PADDING;
        let mut header_bytes = [0u8; 8];
        header_bytes[..4].copy_from_slice(&length.to_le_bytes());
        header_bytes[4..8].copy_from_slice(&flags.to_le_bytes());
        let checksum = compute_xor_checksum(&header_bytes);
        Self {
            length,
            flags,
            checksum,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) -> Result<(), LogEntryError> {
        if buf.len() < Self::SIZE {
            return Err(LogEntryError::BufferTooSmall);
        }
        buf[..4].copy_from_slice(&self.length.to_le_bytes());
        buf[4..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.checksum.to_le_bytes());
        Ok(())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, LogEntryError> {
        if buf.len() < Self::SIZE {
            return Err(LogEntryError::BufferTooSmall);
        }
        let length = u32::from_le_bytes(buf[..4].try_into().unwrap());
        let flags = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let checksum = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok(Self {
            length,
            flags,
            checksum,
        })
    }

    pub fn is_padding(&self) -> bool {
        self.flags & LOG_ENTRY_FLAG_PADDING != 0
    }

    pub fn verify(&self, payload: &[u8]) -> bool {
        compute_entry_checksum(self.length, self.flags, payload) == self.checksum
    }
}

pub fn compute_xor_checksum(data: &[u8]) -> u64 {
    let mut checksum: u64 = 0;

    let mut chunks = data.chunks_exact(8);
    for chunk in chunks.by_ref() {
        checksum ^= u64::from_le_bytes(chunk.try_into().unwrap());
    }

    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut last = [0u8; 8];
        last[..remainder.len()].copy_from_slice(remainder);
        checksum ^= u64::from_le_bytes(last);
    }

    checksum
}

fn compute_entry_checksum(length: u32, flags: u32, payload: &[u8]) -> u64 {
    let mut header_bytes = [0u8; 8];
    header_bytes[..4].copy_from_slice(&length.to_le_bytes());
    header_bytes[4..8].copy_from_slice(&flags.to_le_bytes());
    compute_xor_checksum(&header_bytes) ^ compute_xor_checksum(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_roundtrip() {
        let meta = LogMetadata {
            version: LogMetadata::VERSION,
            page_size: 4096,
            segment_size: 1 << 20,
            data_offset: 4096,
            begin_address: 128,
            committed_address: 256,
            tail_address: 512,
        };

        let mut buf = vec![0u8; LogMetadata::ENCODED_SIZE];
        meta.encode(&mut buf).unwrap();
        let decoded = LogMetadata::decode(&buf).unwrap();

        assert_eq!(decoded.page_size, meta.page_size);
        assert_eq!(decoded.segment_size, meta.segment_size);
        assert_eq!(decoded.data_offset, meta.data_offset);
        assert_eq!(decoded.begin_address, meta.begin_address);
        assert_eq!(decoded.committed_address, meta.committed_address);
        assert_eq!(decoded.tail_address, meta.tail_address);
    }

    #[test]
    fn test_metadata_checksum_mismatch() {
        let mut buf = vec![0u8; LogMetadata::ENCODED_SIZE];
        let meta = LogMetadata::new(4096, 1 << 20, 4096);
        meta.encode(&mut buf).unwrap();
        buf[12] ^= 1;
        assert_eq!(
            LogMetadata::decode(&buf).unwrap_err(),
            LogMetadataError::ChecksumMismatch
        );
    }

    #[test]
    fn test_entry_header_roundtrip() {
        let payload = b"payload";
        let header = LogEntryHeader::new(payload.len() as u32, 0, payload);
        let mut buf = [0u8; LogEntryHeader::SIZE];
        header.encode(&mut buf).unwrap();
        let decoded = LogEntryHeader::decode(&buf).unwrap();
        assert_eq!(decoded.length, payload.len() as u32);
        assert!(decoded.verify(payload));
    }
}
