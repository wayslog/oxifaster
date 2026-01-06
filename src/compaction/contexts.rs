//! Compaction operation contexts
//!
//! Based on C++ FASTER's compact.h implementation.

use crate::address::Address;
use crate::record::{Key, Record, Value};

/// Context for a compaction operation
#[derive(Debug)]
pub struct CompactionContext {
    /// Start address for compaction scan
    pub scan_begin: Address,
    /// End address for compaction scan
    pub scan_end: Address,
    /// New begin address after compaction
    pub new_begin_address: Address,
    /// Number of records scanned
    pub records_scanned: u64,
    /// Number of records compacted (moved)
    pub records_compacted: u64,
    /// Number of records skipped (obsolete)
    pub records_skipped: u64,
    /// Number of tombstones found
    pub tombstones_found: u64,
}

impl CompactionContext {
    /// Create a new compaction context
    pub fn new(scan_begin: Address, scan_end: Address) -> Self {
        Self {
            scan_begin,
            scan_end,
            new_begin_address: scan_end,
            records_scanned: 0,
            records_compacted: 0,
            records_skipped: 0,
            tombstones_found: 0,
        }
    }

    /// Check if this is a valid compaction range
    pub fn is_valid(&self) -> bool {
        self.scan_begin < self.scan_end
    }

    /// Record a scanned record
    pub fn record_scanned(&mut self) {
        self.records_scanned += 1;
    }

    /// Record a compacted (moved) record
    pub fn record_compacted(&mut self) {
        self.records_compacted += 1;
    }

    /// Record a skipped (obsolete) record
    pub fn record_skipped(&mut self) {
        self.records_skipped += 1;
    }

    /// Record a tombstone found
    pub fn record_tombstone(&mut self) {
        self.tombstones_found += 1;
    }
}

/// Context for conditional insert during compaction
pub struct CompactionInsertContext<K, V>
where
    K: Key,
    V: Value,
{
    /// Original address of the record
    pub original_address: Address,
    /// Copy of the record data
    record_data: Vec<u8>,
    /// Phantom data for type parameters
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> CompactionInsertContext<K, V>
where
    K: Key,
    V: Value,
{
    /// Create a new insert context from a record
    pub fn new(record: &Record<K, V>, original_address: Address) -> Self {
        let record_size = Record::<K, V>::size();
        let mut record_data = vec![0u8; record_size];

        // Copy record data
        unsafe {
            let src = record as *const Record<K, V> as *const u8;
            std::ptr::copy_nonoverlapping(src, record_data.as_mut_ptr(), record_size);
        }

        Self {
            original_address,
            record_data,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the record reference
    pub fn record(&self) -> &Record<K, V> {
        unsafe { &*(self.record_data.as_ptr() as *const Record<K, V>) }
    }

    /// Get the key reference
    pub fn key(&self) -> &K {
        unsafe { self.record().key() }
    }

    /// Get the value reference
    pub fn value(&self) -> &V {
        unsafe { self.record().value() }
    }

    /// Check if this record is a tombstone
    pub fn is_tombstone(&self) -> bool {
        self.record().header.is_tombstone()
    }

    /// Get the record size
    pub fn record_size(&self) -> usize {
        self.record_data.len()
    }

    /// Insert the record data into a destination
    pub fn insert_into(&self, dest: &mut [u8]) -> bool {
        if dest.len() < self.record_data.len() {
            return false;
        }
        dest[..self.record_data.len()].copy_from_slice(&self.record_data);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_context() {
        let mut ctx = CompactionContext::new(Address::new(0, 0), Address::new(10, 0));

        assert!(ctx.is_valid());
        assert_eq!(ctx.records_scanned, 0);

        ctx.record_scanned();
        ctx.record_scanned();
        ctx.record_compacted();
        ctx.record_skipped();
        ctx.record_tombstone();

        assert_eq!(ctx.records_scanned, 2);
        assert_eq!(ctx.records_compacted, 1);
        assert_eq!(ctx.records_skipped, 1);
        assert_eq!(ctx.tombstones_found, 1);
    }

    #[test]
    fn test_invalid_range() {
        let ctx = CompactionContext::new(Address::new(10, 0), Address::new(5, 0));
        assert!(!ctx.is_valid());
    }
}
