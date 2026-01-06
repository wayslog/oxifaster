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

    #[derive(Clone, Default, Debug, PartialEq, Eq)]
    struct TestKey(u64);

    impl Key for TestKey {
        fn size(&self) -> u32 {
            8
        }
        fn get_hash(&self) -> u64 {
            self.0
        }
    }

    #[derive(Clone, Default, Debug)]
    struct TestValue(u64);

    impl Value for TestValue {
        fn size(&self) -> u32 {
            8
        }
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
    fn test_compaction_insert_context_basic() {
        // Test the record size method
        let record_size = Record::<TestKey, TestValue>::size();
        assert!(record_size > 0);
    }

    #[test]
    fn test_compaction_insert_context_record_size() {
        // Create a buffer for the record
        let record_size = Record::<TestKey, TestValue>::size();
        let mut buffer = vec![0u8; record_size];

        // Initialize the record in the buffer
        let record = unsafe { &mut *(buffer.as_mut_ptr() as *mut Record<TestKey, TestValue>) };

        // Initialize fields
        record.header = crate::record::RecordInfo::default();
        unsafe {
            *record.key_mut() = TestKey(42);
            *record.value_mut() = TestValue(100);
        }

        // Create insert context
        let ctx = CompactionInsertContext::new(record, Address::from_control(1000));

        // Verify original address
        assert_eq!(ctx.original_address.control(), 1000);

        // Verify record size
        assert_eq!(ctx.record_size(), record_size);
    }

    #[test]
    fn test_compaction_insert_context_key_value() {
        let record_size = Record::<TestKey, TestValue>::size();
        let mut buffer = vec![0u8; record_size];

        let record = unsafe { &mut *(buffer.as_mut_ptr() as *mut Record<TestKey, TestValue>) };
        record.header = crate::record::RecordInfo::default();
        unsafe {
            *record.key_mut() = TestKey(123);
            *record.value_mut() = TestValue(456);
        }

        let ctx = CompactionInsertContext::new(record, Address::new(2000, 0));

        // Test key access
        let key = ctx.key();
        assert_eq!(key.0, 123);

        // Test value access
        let value = ctx.value();
        assert_eq!(value.0, 456);
    }

    #[test]
    fn test_compaction_insert_context_is_tombstone() {
        let record_size = Record::<TestKey, TestValue>::size();
        let mut buffer = vec![0u8; record_size];

        let record = unsafe { &mut *(buffer.as_mut_ptr() as *mut Record<TestKey, TestValue>) };
        record.header = crate::record::RecordInfo::default();
        unsafe {
            *record.key_mut() = TestKey(1);
            *record.value_mut() = TestValue(1);
        }

        // Without tombstone flag
        let ctx = CompactionInsertContext::new(record, Address::new(0, 0));
        assert!(!ctx.is_tombstone());

        // Set tombstone flag
        record.header.set_tombstone(true);
        let ctx_tombstone = CompactionInsertContext::new(record, Address::new(0, 0));
        assert!(ctx_tombstone.is_tombstone());
    }

    #[test]
    fn test_compaction_insert_context_insert_into() {
        let record_size = Record::<TestKey, TestValue>::size();
        let mut buffer = vec![0u8; record_size];

        let record = unsafe { &mut *(buffer.as_mut_ptr() as *mut Record<TestKey, TestValue>) };
        record.header = crate::record::RecordInfo::default();
        unsafe {
            *record.key_mut() = TestKey(999);
            *record.value_mut() = TestValue(888);
        }

        let ctx = CompactionInsertContext::new(record, Address::new(0, 0));

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

    #[test]
    fn test_compaction_insert_context_record_reference() {
        let record_size = Record::<TestKey, TestValue>::size();
        let mut buffer = vec![0u8; record_size];

        let record = unsafe { &mut *(buffer.as_mut_ptr() as *mut Record<TestKey, TestValue>) };
        record.header = crate::record::RecordInfo::default();
        unsafe {
            *record.key_mut() = TestKey(777);
            *record.value_mut() = TestValue(666);
        }

        let ctx = CompactionInsertContext::new(record, Address::new(0, 0));

        // Test getting the record reference
        let record_ref = ctx.record();
        assert_eq!(unsafe { record_ref.key() }.0, 777);
        assert_eq!(unsafe { record_ref.value() }.0, 666);
    }
}
