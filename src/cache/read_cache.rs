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

/// Read cache for storing hot records in memory
pub struct ReadCache<K, V>
where
    K: Key,
    V: Value,
{
    /// Configuration
    config: ReadCacheConfig,
    /// Memory buffer for cached records
    buffer: RwLock<Vec<u8>>,
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
        let mem_size = config.mem_size as usize;
        let buffer = if config.pre_allocate {
            vec![0u8; mem_size]
        } else {
            Vec::with_capacity(mem_size)
        };

        Self {
            config,
            buffer: RwLock::new(buffer),
            tail_address: AtomicU64::new(64), // Start after cache line
            safe_head_address: AtomicU64::new(64),
            head_address: AtomicU64::new(64),
            read_only_address: AtomicU64::new(0),
            eviction_in_progress: AtomicBool::new(false),
            stats: ReadCacheStats::new(),
            _marker: std::marker::PhantomData,
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
            self.stats.record_miss();
            return None;
        }

        let rc_address = cache_address.readcache_address();
        let safe_head = self.safe_head_address.load(Ordering::Acquire);

        // Check if address is still valid (not evicted)
        if rc_address.control() < safe_head {
            self.stats.record_miss();
            return None;
        }

        // Get the record
        let buffer = self.buffer.read().ok()?;
        let offset = (rc_address.control() as usize) % buffer.len();
        
        if offset + Record::<K, V>::size() > buffer.len() {
            self.stats.record_miss();
            return None;
        }

        let record = unsafe {
            &*(buffer[offset..].as_ptr() as *const Record<K, V>)
        };

        // Create record info
        let rc_info = ReadCacheRecordInfo::from_record_info(&record.header, false);

        // Check if record is valid
        if rc_info.is_invalid() {
            self.stats.record_miss();
            return None;
        }

        // Check if key matches
        if unsafe { record.key() } != key {
            self.stats.record_miss();
            return None;
        }

        // Read cache doesn't store tombstones
        if rc_info.is_tombstone() {
            self.stats.record_miss();
            return None;
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
        is_cold_log_record: bool,
    ) -> Result<Address, Status> {
        self.stats.record_insert();

        let record_size = Record::<K, V>::size();
        
        // Try to allocate space
        let new_address = self.allocate(record_size)?;
        
        // Get the buffer for writing
        let mut buffer = self.buffer.write().map_err(|_| Status::Aborted)?;
        
        // Ensure buffer is large enough
        let offset = (new_address.control() as usize) % self.config.mem_size as usize;
        if offset + record_size > buffer.len() {
            // Extend buffer if needed
            buffer.resize(offset + record_size, 0);
        }

        // Create the record
        let record_ptr = buffer[offset..].as_mut_ptr() as *mut Record<K, V>;
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
        Ok(Address::from_control(new_address.control() | Address::READCACHE_BIT))
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
                if self.tail_address.compare_exchange(
                    tail, page_start + size, Ordering::AcqRel, Ordering::Acquire
                ).is_ok() {
                    return Ok(Address::from_control(page_start));
                }
                continue;
            }
            
            // Try to allocate
            if self.tail_address.compare_exchange(
                tail, new_tail, Ordering::AcqRel, Ordering::Acquire
            ).is_ok() {
                return Ok(Address::from_control(tail));
            }
        }
    }

    /// Trigger eviction of old records
    fn trigger_eviction(&self) -> Result<(), Status> {
        // Try to start eviction
        if self.eviction_in_progress.compare_exchange(
            false, true, Ordering::AcqRel, Ordering::Acquire
        ).is_err() {
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
            self.evict(Address::from_control(current_head), Address::from_control(new_head));
            
            // Update head address
            self.head_address.store(new_head, Ordering::Release);
            self.safe_head_address.store(new_head, Ordering::Release);
        }

        // Update read-only address
        let ro_threshold = current_tail.saturating_sub(self.config.read_only_size());
        self.read_only_address.store(ro_threshold, Ordering::Release);

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
            let offset = (current.control() as usize) % buffer.len();
            
            if offset + Record::<K, V>::size() > buffer.len() {
                break;
            }

            let record = unsafe {
                &*(buffer[offset..].as_ptr() as *const Record<K, V>)
            };

            let rc_info = ReadCacheRecordInfo::from_record_info(&record.header, false);
            
            if rc_info.is_null() {
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

        let offset = (rc_address.control() as usize) % buffer.len();
        
        if offset + Record::<K, V>::size() > buffer.len() {
            return Address::INVALID;
        }

        let record = unsafe {
            &*(buffer[offset..].as_ptr() as *const Record<K, V>)
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
        let buffer = match self.buffer.write() {
            Ok(b) => b,
            Err(_) => return Address::INVALID,
        };

        let offset = (rc_address.control() as usize) % buffer.len();
        
        if offset + Record::<K, V>::size() > buffer.len() {
            return Address::INVALID;
        }

        let record = unsafe {
            &mut *(buffer[offset..].as_ptr() as *mut Record<K, V>)
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
        
        self.tail_address.store(64, Ordering::Release);
        self.safe_head_address.store(64, Ordering::Release);
        self.head_address.store(64, Ordering::Release);
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
        cache.try_insert(&key, &value, Address::INVALID, false).unwrap();
        assert_eq!(cache.stats().insert_calls(), 1);
        
        // Read
        let addr = Address::from_control(64 | Address::READCACHE_BIT);
        let _ = cache.read(addr, &key);
        assert_eq!(cache.stats().read_calls(), 1);
    }
}
