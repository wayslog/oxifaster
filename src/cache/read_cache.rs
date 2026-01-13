//! Read cache implementation
//!
//! Stores hot (frequently read) records in memory to avoid disk reads.
//! Based on C++ FASTER's read_cache.h implementation.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

use crate::address::Address;
use crate::cache::config::ReadCacheConfig;
use crate::cache::record_info::ReadCacheRecordInfo;
use crate::cache::stats::ReadCacheStats;
use crate::record::{Key, Record, RecordInfo, Value};
use crate::status::Status;

/// Page size for read cache (32 MB)
const PAGE_SIZE: u64 = 1 << 25;
const START_ADDRESS: u64 = 64;
const WORD_BYTES: usize = std::mem::size_of::<u64>();

/// Read cache for storing hot records in memory
pub struct ReadCache<K, V>
where
    K: Key,
    V: Value,
{
    /// Configuration
    config: ReadCacheConfig,
    /// Memory buffer for cached records
    ///
    /// The buffer is stored as `u64` words to guarantee at least 8-byte alignment for record
    /// headers (and typical key/value types). All offsets into the buffer are in bytes.
    buffer: RwLock<Vec<u64>>,
    /// Current tail address (where new records are allocated)
    tail_address: AtomicU64,
    /// Safe head address (below which eviction has completed)
    safe_head_address: AtomicU64,
    /// Head address (being evicted)
    head_address: AtomicU64,
    /// Read-only address (records below this may be evicted)
    read_only_address: AtomicU64,
    /// Eviction in progress
    eviction_in_progress: AtomicBool,
    /// Statistics
    stats: ReadCacheStats,
    /// Phantom data for type parameters
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> ReadCache<K, V>
where
    K: Key,
    V: Value,
{
    /// Create a new read cache with the given configuration
    pub fn new(config: ReadCacheConfig) -> Self {
        let mem_size = usize::try_from(config.mem_size).unwrap_or(usize::MAX);
        let word_capacity = mem_size.saturating_add(WORD_BYTES - 1) / WORD_BYTES;
        let buffer = if config.pre_allocate {
            vec![0u64; word_capacity]
        } else {
            Vec::with_capacity(word_capacity)
        };

        Self {
            config,
            buffer: RwLock::new(buffer),
            tail_address: AtomicU64::new(START_ADDRESS), // Start after cache line
            safe_head_address: AtomicU64::new(START_ADDRESS),
            head_address: AtomicU64::new(START_ADDRESS),
            read_only_address: AtomicU64::new(0),
            eviction_in_progress: AtomicBool::new(false),
            stats: ReadCacheStats::new(),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    fn buffer_bytes_len(buffer: &[u64]) -> usize {
        buffer.len().saturating_mul(WORD_BYTES)
    }

    #[inline]
    fn buffer_as_ptr(buffer: &[u64]) -> *const u8 {
        buffer.as_ptr().cast::<u8>()
    }

    #[inline]
    fn buffer_as_mut_ptr(buffer: &mut [u64]) -> *mut u8 {
        buffer.as_mut_ptr().cast::<u8>()
    }

    #[inline]
    fn miss<T>(&self) -> Option<T> {
        self.stats.record_miss();
        None
    }

    fn offset_in_cache(&self, control: u64) -> Option<usize> {
        let mem_size = usize::try_from(self.config.mem_size).ok()?;
        if mem_size == 0 {
            return None;
        }

        let alignment = std::mem::align_of::<RecordInfo>();
        debug_assert!(alignment.is_power_of_two());

        let mut offset = (control as usize) % mem_size;
        offset &= !(alignment - 1);
        Some(offset)
    }

    fn record_offset(&self, control: u64, buffer_bytes_len: usize) -> Option<usize> {
        if buffer_bytes_len == 0 {
            return None;
        }

        let offset = self.offset_in_cache(control)?;
        let record_size = Record::<K, V>::size();
        if offset.checked_add(record_size)? > buffer_bytes_len {
            return None;
        }

        Some(offset)
    }

    fn record_at<'a>(&self, buffer: &'a [u64], address: Address) -> Option<&'a Record<K, V>> {
        let buffer_bytes_len = Self::buffer_bytes_len(buffer);
        let offset = self.record_offset(address.control(), buffer_bytes_len)?;
        let ptr = unsafe { Self::buffer_as_ptr(buffer).add(offset) }.cast::<Record<K, V>>();
        Some(unsafe { &*ptr })
    }

    fn record_at_mut<'a>(
        &self,
        buffer: &'a mut [u64],
        address: Address,
    ) -> Option<&'a mut Record<K, V>> {
        let buffer_bytes_len = Self::buffer_bytes_len(buffer);
        let offset = self.record_offset(address.control(), buffer_bytes_len)?;
        let ptr = unsafe { Self::buffer_as_mut_ptr(buffer).add(offset) }.cast::<Record<K, V>>();
        Some(unsafe { &mut *ptr })
    }

    fn ensure_buffer_bytes_len(buffer: &mut Vec<u64>, required_bytes_len: usize) {
        let required_words = required_bytes_len.saturating_add(WORD_BYTES - 1) / WORD_BYTES;
        if required_words > buffer.len() {
            buffer.resize(required_words, 0);
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &ReadCacheConfig {
        &self.config
    }

    /// Get the statistics
    pub fn stats(&self) -> &ReadCacheStats {
        &self.stats
    }

    /// Get the current tail address
    pub fn tail_address(&self) -> Address {
        Address::from_control(self.tail_address.load(Ordering::Acquire))
    }

    /// Get the safe head address
    pub fn safe_head_address(&self) -> Address {
        Address::from_control(self.safe_head_address.load(Ordering::Acquire))
    }

    /// Get the read-only address
    pub fn read_only_address(&self) -> Address {
        Address::from_control(self.read_only_address.load(Ordering::Acquire))
    }

    /// Try to read a record from the cache
    ///
    /// # Arguments
    /// * `cache_address` - Address in the read cache (must have readcache flag set)
    /// * `key` - Key to match
    ///
    /// # Returns
    /// * `Some((value, record_info))` - If record found and key matches
    /// * `None` - If not found, invalid, or key mismatch
    pub fn read(&self, cache_address: Address, key: &K) -> Option<(V, ReadCacheRecordInfo)> {
        self.stats.record_read();

        // Must be a read cache address
        if !cache_address.in_readcache() {
            return self.miss();
        }

        let rc_address = cache_address.readcache_address();
        let safe_head = self.safe_head_address.load(Ordering::Acquire);

        // Check if address is still valid (not evicted)
        if rc_address.control() < safe_head {
            return self.miss();
        }

        // Get the record
        let buffer = self.buffer.read().ok()?;
        let record = self.record_at(&buffer, rc_address)?;

        // Create record info
        let rc_info = ReadCacheRecordInfo::from_record_info(&record.header, false);

        // Check if record is valid
        if rc_info.is_invalid() {
            return self.miss();
        }

        // Check if key matches
        if unsafe { record.key() } != key {
            return self.miss();
        }

        // Read cache doesn't store tombstones
        if rc_info.is_tombstone() {
            return self.miss();
        }

        self.stats.record_hit();
        Some((unsafe { record.value().clone() }, rc_info))
    }

    /// Try to insert a record into the read cache
    ///
    /// # Arguments
    /// * `key` - Key of the record
    /// * `value` - Value of the record
    /// * `previous_address` - Address in the main HybridLog
    /// * `is_cold_log_record` - Whether the record is from cold HybridLog (F2)
    ///
    /// # Returns
    /// * `Ok(address)` - If successfully inserted, returns the cache address
    /// * `Err(status)` - If failed to insert
    pub fn try_insert(
        &self,
        key: &K,
        value: &V,
        previous_address: Address,
        _is_cold_log_record: bool,
    ) -> Result<Address, Status> {
        self.stats.record_insert();

        let record_size = Record::<K, V>::size();

        // Try to allocate space
        let new_address = self.allocate(record_size)?;

        // Get the buffer for writing
        let mut buffer = self.buffer.write().map_err(|_| Status::Aborted)?;

        let offset = self
            .offset_in_cache(new_address.control())
            .ok_or(Status::Aborted)?;

        // Ensure buffer is large enough
        Self::ensure_buffer_bytes_len(&mut buffer, offset.saturating_add(record_size));

        // Create the record
        let record_ptr =
            unsafe { Self::buffer_as_mut_ptr(&mut buffer).add(offset) } as *mut Record<K, V>;
        unsafe {
            // Initialize record info with previous address pointing to HybridLog
            let record_info = RecordInfo::new(
                previous_address,
                0,     // checkpoint_version
                false, // invalid
                false, // tombstone
                false, // final_bit
            );

            // Write header
            (*record_ptr).header = record_info;

            // Write key
            let key_ptr = (record_ptr as *mut u8).add(Record::<K, V>::key_offset()) as *mut K;
            std::ptr::write(key_ptr, key.clone());

            // Write value
            let value_ptr = (record_ptr as *mut u8).add(Record::<K, V>::value_offset()) as *mut V;
            std::ptr::write(value_ptr, value.clone());
        }

        self.stats.record_insert_success();

        // Return address with readcache flag
        Ok(Address::from_control(
            new_address.control() | Address::READCACHE_BIT,
        ))
    }

    /// Allocate space in the read cache
    fn allocate(&self, size: usize) -> Result<Address, Status> {
        let size = size as u64;

        loop {
            let tail = self.tail_address.load(Ordering::Acquire);
            let new_tail = tail + size;

            // Check if we need to trigger eviction
            let head = self.head_address.load(Ordering::Acquire);
            if new_tail - head > self.config.mem_size {
                // Trigger eviction
                self.trigger_eviction()?;
                continue;
            }

            // Check if we're crossing a page boundary
            let current_page = tail / PAGE_SIZE;
            let new_page = new_tail / PAGE_SIZE;

            if new_page > current_page {
                // Would cross page boundary - need new page
                let page_start = new_page * PAGE_SIZE;
                if self
                    .tail_address
                    .compare_exchange(tail, page_start + size, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(Address::from_control(page_start));
                }
                continue;
            }

            // Try to allocate
            if self
                .tail_address
                .compare_exchange(tail, new_tail, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(Address::from_control(tail));
            }
        }
    }

    /// Trigger eviction of old records
    fn trigger_eviction(&self) -> Result<(), Status> {
        // Try to start eviction
        if self
            .eviction_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Eviction already in progress
            return Ok(());
        }

        // Calculate new head address
        let current_head = self.head_address.load(Ordering::Acquire);
        let current_tail = self.tail_address.load(Ordering::Acquire);
        let target_size = self.config.mutable_size();

        let new_head = if current_tail > target_size {
            current_tail - target_size
        } else {
            current_head
        };

        if new_head > current_head {
            // Evict records from current_head to new_head
            self.evict(
                Address::from_control(current_head),
                Address::from_control(new_head),
            );

            // Update head address
            self.head_address.store(new_head, Ordering::Release);
            self.safe_head_address.store(new_head, Ordering::Release);
        }

        // Update read-only address
        let ro_threshold = current_tail.saturating_sub(self.config.read_only_size());
        self.read_only_address
            .store(ro_threshold, Ordering::Release);

        self.eviction_in_progress.store(false, Ordering::Release);
        Ok(())
    }

    /// Evict records from the given address range
    fn evict(&self, from_address: Address, to_address: Address) {
        let buffer = match self.buffer.read() {
            Ok(b) => b,
            Err(_) => return,
        };

        let mut evicted = 0u64;
        let mut invalid = 0u64;
        let mut current = from_address;

        while current < to_address {
            let record = match self.record_at(&buffer, current) {
                Some(record) => record,
                None => break,
            };

            let rc_info = ReadCacheRecordInfo::from_record_info(&record.header, false);

            if record.header.is_null() {
                break;
            }

            evicted += 1;
            if rc_info.is_invalid() {
                invalid += 1;
            }

            // Move to next record
            current = Address::from_control(current.control() + Record::<K, V>::size() as u64);
        }

        self.stats.record_eviction(evicted, invalid);
    }

    /// Skip read cache addresses and return the underlying HybridLog address
    pub fn skip(&self, address: Address) -> Address {
        if !address.in_readcache() {
            return address;
        }

        let rc_address = address.readcache_address();
        let buffer = match self.buffer.read() {
            Ok(b) => b,
            Err(_) => return Address::INVALID,
        };
        let record = match self.record_at(&buffer, rc_address) {
            Some(record) => record,
            None => return Address::INVALID,
        };

        let rc_info = ReadCacheRecordInfo::from_record_info(&record.header, false);
        rc_info.get_previous_address()
    }

    /// Invalidate a cache entry if the key matches
    pub fn invalidate(&self, address: Address, key: &K) -> Address {
        if !address.in_readcache() {
            return address;
        }

        let rc_address = address.readcache_address();
        let mut buffer = match self.buffer.write() {
            Ok(b) => b,
            Err(_) => return Address::INVALID,
        };
        let record = match self.record_at_mut(&mut buffer, rc_address) {
            Some(record) => record,
            None => return Address::INVALID,
        };

        // Check if key matches
        if unsafe { record.key() } == key {
            record.header.set_invalid(true);
        }

        let rc_info = ReadCacheRecordInfo::from_record_info(&record.header, false);
        rc_info.get_previous_address()
    }

    /// Clear the read cache
    pub fn clear(&self) {
        if let Ok(mut buffer) = self.buffer.write() {
            buffer.clear();
        }

        self.tail_address.store(START_ADDRESS, Ordering::Release);
        self.safe_head_address
            .store(START_ADDRESS, Ordering::Release);
        self.head_address.store(START_ADDRESS, Ordering::Release);
        self.read_only_address.store(0, Ordering::Release);
        self.stats.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
    struct TestKey(u64);

    impl Key for TestKey {
        fn size(&self) -> u32 {
            std::mem::size_of::<Self>() as u32
        }

        fn get_hash(&self) -> u64 {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.hash(&mut hasher);
            hasher.finish()
        }
    }

    #[derive(Clone, Debug, PartialEq, Default)]
    struct TestValue(u64);

    impl Value for TestValue {
        fn size(&self) -> u32 {
            std::mem::size_of::<Self>() as u32
        }
    }

    #[test]
    fn test_create_cache() {
        let config = ReadCacheConfig::new(1024 * 1024);
        let cache = ReadCache::<TestKey, TestValue>::new(config);

        assert_eq!(cache.tail_address().control(), 64);
        assert_eq!(cache.stats().read_calls(), 0);
    }

    #[test]
    fn test_insert_and_read() {
        let config = ReadCacheConfig::new(1024 * 1024);
        let cache = ReadCache::<TestKey, TestValue>::new(config);

        let key = TestKey(42);
        let value = TestValue(100);
        let prev_addr = Address::new(1, 500);

        // Insert
        let result = cache.try_insert(&key, &value, prev_addr, false);
        assert!(result.is_ok());

        let cache_addr = result.unwrap();
        assert!(cache_addr.in_readcache());

        // Read
        let read_result = cache.read(cache_addr, &key);
        assert!(read_result.is_some());

        let (read_value, rc_info) = read_result.unwrap();
        assert_eq!(read_value, value);
        assert_eq!(rc_info.get_previous_address(), prev_addr);
    }

    #[test]
    fn test_read_miss_wrong_key() {
        let config = ReadCacheConfig::new(1024 * 1024);
        let cache = ReadCache::<TestKey, TestValue>::new(config);

        let key = TestKey(42);
        let wrong_key = TestKey(999);
        let value = TestValue(100);
        let prev_addr = Address::new(1, 500);

        // Insert
        let cache_addr = cache.try_insert(&key, &value, prev_addr, false).unwrap();

        // Read with wrong key
        let result = cache.read(cache_addr, &wrong_key);
        assert!(result.is_none());
    }

    #[test]
    fn test_skip() {
        let config = ReadCacheConfig::new(1024 * 1024);
        let cache = ReadCache::<TestKey, TestValue>::new(config);

        let key = TestKey(42);
        let value = TestValue(100);
        let prev_addr = Address::new(1, 500);

        // Insert
        let cache_addr = cache.try_insert(&key, &value, prev_addr, false).unwrap();

        // Skip should return the previous address
        let skipped = cache.skip(cache_addr);
        assert_eq!(skipped, prev_addr);

        // Skip on non-cache address should return same address
        let hlog_addr = Address::new(5, 100);
        assert_eq!(cache.skip(hlog_addr), hlog_addr);
    }

    #[test]
    fn test_stats() {
        let config = ReadCacheConfig::new(1024 * 1024);
        let cache = ReadCache::<TestKey, TestValue>::new(config);

        let key = TestKey(42);
        let value = TestValue(100);

        // Insert
        cache
            .try_insert(&key, &value, Address::INVALID, false)
            .unwrap();
        assert_eq!(cache.stats().insert_calls(), 1);

        // Read
        let addr = Address::from_control(64 | Address::READCACHE_BIT);
        let _ = cache.read(addr, &key);
        assert_eq!(cache.stats().read_calls(), 1);
    }
}
