//! Read cache record information
//!
//! Based on C++ FASTER's read_cache_utils.h ReadCacheRecordInfo.

use crate::address::Address;
use crate::record::RecordInfo;

/// Read cache specific record information
///
/// This extends the base RecordInfo with read-cache specific fields:
/// - `in_cold_hlog`: Whether the original record was from cold HybridLog (F2)
/// - `invalid`: Whether this cache entry has been invalidated
/// - `previous_address`: The address in the main HybridLog (not read cache)
#[derive(Debug, Clone, Copy)]
pub struct ReadCacheRecordInfo {
    /// Version of the record
    pub version: u16,
    /// Whether the original record was from cold HybridLog
    pub in_cold_hlog: bool,
    /// Whether this cache entry is invalid
    pub invalid: bool,
    /// Whether this is a tombstone
    pub tombstone: bool,
    /// Previous address in the main HybridLog (not read cache)
    pub previous_address: Address,
}

impl ReadCacheRecordInfo {
    /// Create a new read cache record info
    pub fn new(
        version: u16,
        in_cold_hlog: bool,
        invalid: bool,
        tombstone: bool,
        previous_address: Address,
    ) -> Self {
        Self {
            version,
            in_cold_hlog,
            invalid,
            tombstone,
            previous_address,
        }
    }

    /// Create from a base RecordInfo
    pub fn from_record_info(info: &RecordInfo, in_cold_hlog: bool) -> Self {
        Self {
            version: 0,
            in_cold_hlog,
            invalid: info.is_invalid(),
            tombstone: info.is_tombstone(),
            previous_address: info.previous_address(),
        }
    }

    /// Convert to a base RecordInfo
    pub fn to_record_info(&self) -> RecordInfo {
        RecordInfo::new(
            self.previous_address,
            self.version,
            self.invalid,
            self.tombstone,
            false, // final_bit
        )
    }

    /// Check if this record info represents a null/empty entry
    pub fn is_null(&self) -> bool {
        self.previous_address == Address::INVALID && !self.invalid && !self.tombstone
    }

    /// Get the previous address (in main HybridLog)
    pub fn get_previous_address(&self) -> Address {
        self.previous_address
    }

    /// Check if the record is invalid
    pub fn is_invalid(&self) -> bool {
        self.invalid
    }

    /// Mark the record as invalid
    pub fn set_invalid(&mut self, invalid: bool) {
        self.invalid = invalid;
    }

    /// Check if the record is from cold HybridLog
    pub fn is_in_cold_hlog(&self) -> bool {
        self.in_cold_hlog
    }

    /// Check if the record is a tombstone
    pub fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

impl Default for ReadCacheRecordInfo {
    fn default() -> Self {
        Self {
            version: 0,
            in_cold_hlog: false,
            invalid: false,
            tombstone: false,
            previous_address: Address::INVALID,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let info = ReadCacheRecordInfo::default();
        assert_eq!(info.version, 0);
        assert!(!info.in_cold_hlog);
        assert!(!info.invalid);
        assert!(!info.tombstone);
        assert_eq!(info.previous_address, Address::INVALID);
        assert!(info.is_null());
    }

    #[test]
    fn test_new() {
        let addr = Address::new(1, 100);
        let info = ReadCacheRecordInfo::new(1, true, false, false, addr);

        assert_eq!(info.version, 1);
        assert!(info.in_cold_hlog);
        assert!(!info.invalid);
        assert!(!info.tombstone);
        assert_eq!(info.previous_address, addr);
        assert!(!info.is_null());
    }

    #[test]
    fn test_invalid_flag() {
        let mut info = ReadCacheRecordInfo::default();
        assert!(!info.is_invalid());

        info.set_invalid(true);
        assert!(info.is_invalid());
    }
}
