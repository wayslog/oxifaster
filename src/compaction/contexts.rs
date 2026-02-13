//! Compaction operation contexts
//!
//! Based on C++ FASTER's compact.h implementation.

use std::slice;

use crate::address::Address;
use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::status::Status;
use crate::store::RecordView;
use crate::store::record_format;

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
    K: PersistKey,
    V: PersistValue,
{
    /// Original address of the record
    pub original_address: Address,
    /// Copy of the record data (8-byte aligned).
    ///
    /// The record header contains an `AtomicU64`, so reading it via `RecordInfo` requires
    /// proper alignment.
    record_data: Vec<u64>,
    /// Actual byte length of the record in `record_data`.
    byte_len: usize,
    /// Phantom data for type parameters
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> CompactionInsertContext<K, V>
where
    K: PersistKey,
    V: PersistValue,
{
    /// Create a new insert context from encoded record bytes.
    pub fn new(record_bytes: &[u8], original_address: Address) -> Self {
        let word_len = record_bytes.len().div_ceil(std::mem::size_of::<u64>());
        let mut record_data = vec![0u64; word_len];
        unsafe {
            std::ptr::copy_nonoverlapping(
                record_bytes.as_ptr(),
                record_data.as_mut_ptr().cast::<u8>(),
                record_bytes.len(),
            );
        }

        Self {
            original_address,
            record_data,
            byte_len: record_bytes.len(),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    fn record_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.record_data.as_ptr().cast::<u8>(), self.byte_len) }
    }

    /// Parse the owned bytes into a zero-copy view.
    pub fn view(&self) -> Result<RecordView<'_>, Status> {
        unsafe {
            record_format::record_view_from_memory::<K, V>(
                self.original_address,
                self.record_data.as_ptr().cast::<u8>(),
                self.byte_len,
            )
        }
    }

    /// Decode and return the key.
    pub fn key(&self) -> Result<K, Status> {
        let view = self.view()?;
        <K as PersistKey>::Codec::decode(view.key_bytes())
    }

    /// Decode and return the value (if not a tombstone).
    pub fn value(&self) -> Result<Option<V>, Status> {
        let view = self.view()?;
        if view.is_tombstone() {
            return Ok(None);
        }
        let bytes = view.value_bytes().ok_or(Status::Corruption)?;
        Ok(Some(<V as PersistValue>::Codec::decode(bytes)?))
    }

    /// Check if this record is a tombstone
    pub fn is_tombstone(&self) -> Result<bool, Status> {
        Ok(self.view()?.is_tombstone())
    }

    /// Get the record size
    pub fn record_size(&self) -> usize {
        self.byte_len
    }

    /// Insert the record data into a destination
    pub fn insert_into(&self, dest: &mut [u8]) -> bool {
        if dest.len() < self.byte_len {
            return false;
        }
        dest[..self.byte_len].copy_from_slice(self.record_bytes());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fixed_record_u64(key: u64, value: u64, tombstone: bool) -> Vec<u8> {
        let disk_len = crate::store::record_format::fixed_disk_len::<u64, u64>();
        let mut buf = vec![0u8; disk_len];

        let header = crate::record::RecordInfo::new(
            Address::INVALID,
            0,     // checkpoint_version
            false, // invalid
            tombstone,
            false, // final_bit
        );
        let header_size = std::mem::size_of::<crate::record::RecordInfo>();
        buf[0..header_size].copy_from_slice(&header.control().to_le_bytes());
        buf[header_size..header_size + 8].copy_from_slice(&key.to_ne_bytes());
        buf[header_size + 8..header_size + 16].copy_from_slice(&value.to_ne_bytes());
        buf
    }

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

    #[test]
    fn test_compaction_context_debug() {
        let ctx = CompactionContext::new(Address::new(0, 0), Address::new(10, 0));
        let debug_str = format!("{ctx:?}");
        assert!(debug_str.contains("CompactionContext"));
        assert!(debug_str.contains("scan_begin"));
        assert!(debug_str.contains("scan_end"));
    }

    #[test]
    fn test_compaction_context_new_begin_address() {
        let ctx = CompactionContext::new(Address::new(0, 0), Address::new(10, 0));
        // new_begin_address should default to scan_end
        assert_eq!(ctx.new_begin_address, ctx.scan_end);
    }

    #[test]
    fn test_compaction_context_equal_range() {
        // Equal begin and end is considered invalid
        let ctx = CompactionContext::new(Address::new(5, 0), Address::new(5, 0));
        assert!(!ctx.is_valid());
    }

    #[test]
    fn test_compaction_context_large_range() {
        let ctx = CompactionContext::new(Address::from_control(0), Address::from_control(1000000));
        assert!(ctx.is_valid());
        assert_eq!(ctx.scan_begin.control(), 0);
        assert_eq!(ctx.scan_end.control(), 1000000);
    }

    #[test]
    fn test_compaction_context_multiple_operations() {
        let mut ctx = CompactionContext::new(Address::new(0, 0), Address::new(100, 0));

        // Simulate scanning many records
        for _ in 0..1000 {
            ctx.record_scanned();
        }
        assert_eq!(ctx.records_scanned, 1000);

        // Simulate compacting some records
        for _ in 0..300 {
            ctx.record_compacted();
        }
        assert_eq!(ctx.records_compacted, 300);

        // Simulate skipping some records
        for _ in 0..500 {
            ctx.record_skipped();
        }
        assert_eq!(ctx.records_skipped, 500);

        // Simulate finding tombstones
        for _ in 0..200 {
            ctx.record_tombstone();
        }
        assert_eq!(ctx.tombstones_found, 200);
    }

    #[test]
    fn test_compaction_insert_context_record_size() {
        let buffer = make_fixed_record_u64(42, 100, false);
        let record_size = buffer.len();

        let ctx = CompactionInsertContext::<u64, u64>::new(&buffer, Address::from_control(1000));

        // Verify original address
        assert_eq!(ctx.original_address.control(), 1000);

        // Verify record size
        assert_eq!(ctx.record_size(), record_size);
    }

    #[test]
    fn test_compaction_insert_context_key_value() {
        let buffer = make_fixed_record_u64(123, 456, false);
        let ctx = CompactionInsertContext::<u64, u64>::new(&buffer, Address::new(2000, 0));

        // Test key access
        let key = ctx.key().unwrap();
        assert_eq!(key, 123);

        // Test value access
        let value = ctx.value().unwrap();
        assert_eq!(value, Some(456));
    }

    #[test]
    fn test_compaction_insert_context_is_tombstone() {
        let buffer = make_fixed_record_u64(1, 1, false);
        let ctx = CompactionInsertContext::<u64, u64>::new(&buffer, Address::new(0, 0));
        assert!(!ctx.is_tombstone().unwrap());

        let buffer = make_fixed_record_u64(1, 1, true);
        let ctx_tombstone = CompactionInsertContext::<u64, u64>::new(&buffer, Address::new(0, 0));
        assert!(ctx_tombstone.is_tombstone().unwrap());
        assert_eq!(ctx_tombstone.value().unwrap(), None);
    }

    #[test]
    fn test_compaction_insert_context_insert_into() {
        let buffer = make_fixed_record_u64(999, 888, false);
        let record_size = buffer.len();
        let ctx = CompactionInsertContext::<u64, u64>::new(&buffer, Address::new(0, 0));

        // Test insert into sufficient buffer
        let mut dest = vec![0u8; record_size + 100];
        let result = ctx.insert_into(&mut dest);
        assert!(result);

        // Test insert into exact size buffer
        let mut dest_exact = vec![0u8; record_size];
        let result_exact = ctx.insert_into(&mut dest_exact);
        assert!(result_exact);

        // Test insert into insufficient buffer
        let mut dest_small = vec![0u8; record_size - 1];
        let result_small = ctx.insert_into(&mut dest_small);
        assert!(!result_small);
    }
}
