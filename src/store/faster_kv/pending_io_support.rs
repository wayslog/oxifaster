use crate::address::Address;
use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::device::StorageDevice;
use crate::epoch::current_thread_tag_for;
use crate::record::RecordInfo;
use crate::status::Status;

use super::record_format;
use super::{DiskReadCacheEntry, DiskReadResult, FasterKv, PendingIoKey};

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    // Keep the disk read cache bounded. This cache is only an optimization / rendezvous point for
    // pending reads; it is safe to clear under pressure.
    const DISK_READ_CACHE_MAX_ENTRIES: usize = 4096;

    const RECORD_INFO_SIZE: usize = std::mem::size_of::<RecordInfo>();
    const VARLEN_LENGTHS_SIZE: usize = 2 * std::mem::size_of::<u32>();
    const VARLEN_HEADER_SIZE: usize = Self::RECORD_INFO_SIZE + Self::VARLEN_LENGTHS_SIZE;

    /// Drive and process background I/O completions (global).
    pub(super) fn process_pending_io_completions(&self) {
        let completions = self.pending_io.drain_completions();
        if completions.is_empty() {
            return;
        }

        let mut completed = self.pending_io_completed.lock();

        for c in completions {
            match c {
                crate::store::pending_io::IoCompletion::ReadBytesDone {
                    thread_id,
                    thread_tag,
                    ctx_id,
                    address,
                    result,
                } => {
                    let parsed = match result {
                        Ok(bytes) => {
                            if self.stats_collector.is_enabled() {
                                self.stats_collector
                                    .store_stats
                                    .operational
                                    .record_pending_io_completed();
                            }
                            self.parse_disk_record(&bytes)
                        }
                        Err(err) => {
                            if self.stats_collector.is_enabled() {
                                self.stats_collector
                                    .store_stats
                                    .operational
                                    .record_pending_io_failed();
                            }
                            if tracing::enabled!(tracing::Level::WARN) {
                                tracing::warn!(
                                    address = %address,
                                    error = %err,
                                    "pending I/O failed"
                                );
                            }
                            Err(Status::IoError)
                        }
                    };
                    {
                        let mut cache = self.disk_read_results.lock();
                        if cache.len() >= Self::DISK_READ_CACHE_MAX_ENTRIES {
                            cache.clear();
                        }
                        cache.insert(address.control(), DiskReadCacheEntry { parsed });
                    }

                    if current_thread_tag_for(thread_id) != thread_tag {
                        continue;
                    }

                    *completed
                        .entry(PendingIoKey {
                            thread_id,
                            thread_tag,
                            ctx_id,
                        })
                        .or_insert(0) += 1;
                }
            }
        }
    }

    /// Consume and return the number of completed I/Os for the given execution context.
    pub(crate) fn take_completed_io_for_context(
        &self,
        thread_id: usize,
        thread_tag: u64,
        ctx_id: u64,
    ) -> u32 {
        self.process_pending_io_completions();
        self.pending_io_completed
            .lock()
            .remove(&PendingIoKey {
                thread_id,
                thread_tag,
                ctx_id,
            })
            .unwrap_or(0)
    }

    /// Peek a disk read result for `address` (if present).
    ///
    /// This is primarily used to advance pending requests inside `complete_pending()`.
    pub(crate) fn peek_disk_read_result(
        &self,
        address: Address,
    ) -> Option<Result<DiskReadResult<K, V>, Status>> {
        self.disk_read_results
            .lock()
            .get(&address.control())
            .cloned()
            .map(|e| e.parsed)
    }

    /// Consume a parsed disk read result for `address` (if present).
    ///
    /// New code should prefer `peek_disk_read_result` to allow reusing the cached result
    /// without re-submitting I/O.
    pub(crate) fn take_disk_read_result(
        &self,
        address: Address,
    ) -> Option<Result<DiskReadResult<K, V>, Status>> {
        self.disk_read_results
            .lock()
            .remove(&address.control())
            .map(|e| e.parsed)
    }

    /// Submit a disk read for a specific execution context.
    ///
    /// Returns `true` if a new I/O was submitted, or `false` if the address is already inflight.
    pub(crate) fn submit_disk_read_for_context(
        &self,
        thread_id: usize,
        thread_tag: u64,
        ctx_id: u64,
        address: Address,
    ) -> bool {
        let submitted = if record_format::is_fixed_record::<K, V>() {
            self.pending_io.submit_read_bytes(
                thread_id,
                thread_tag,
                ctx_id,
                address,
                record_format::fixed_disk_len::<K, V>(),
            )
        } else {
            self.pending_io
                .submit_read_varlen_record(thread_id, thread_tag, ctx_id, address)
        };

        if submitted && self.stats_collector.is_enabled() {
            self.stats_collector
                .store_stats
                .operational
                .record_pending_io_submitted();
        }
        if submitted && tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(address = %address, "pending I/O submitted");
        }

        submitted
    }

    /// Parse key/value from disk-read record bytes (producing an owned result).
    fn parse_disk_record(&self, bytes: &[u8]) -> Result<DiskReadResult<K, V>, Status> {
        if bytes.len() < Self::RECORD_INFO_SIZE {
            return Err(Status::Corruption);
        }

        let header_control =
            u64::from_le_bytes(bytes[0..Self::RECORD_INFO_SIZE].try_into().unwrap());
        let header = RecordInfo::from_control(header_control);
        let previous_address = header.previous_address();

        // Skip invalid records to avoid returning partially written/corrupt data.
        if header.is_invalid() {
            return Ok(DiskReadResult {
                key: None,
                value: None,
                previous_address,
            });
        }

        if record_format::is_fixed_record::<K, V>() {
            let disk_len = record_format::fixed_disk_len::<K, V>();
            if bytes.len() < disk_len {
                return Err(Status::Corruption);
            }

            let key_len = <K as PersistKey>::Codec::FIXED_LEN;
            let value_len = <V as PersistValue>::Codec::FIXED_LEN;

            let key_bytes = &bytes[Self::RECORD_INFO_SIZE..Self::RECORD_INFO_SIZE + key_len];
            let key = <K as PersistKey>::Codec::decode(key_bytes)?;

            let value = if header.is_tombstone() {
                None
            } else {
                let value_start = Self::RECORD_INFO_SIZE + key_len;
                let value_bytes = &bytes[value_start..value_start + value_len];
                Some(<V as PersistValue>::Codec::decode(value_bytes)?)
            };

            Ok(DiskReadResult {
                key: Some(key),
                value,
                previous_address,
            })
        } else {
            if bytes.len() < Self::VARLEN_HEADER_SIZE {
                return Err(Status::Corruption);
            }
            let key_len = u32::from_le_bytes(
                bytes[Self::RECORD_INFO_SIZE..Self::RECORD_INFO_SIZE + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let value_len = u32::from_le_bytes(
                bytes
                    [Self::RECORD_INFO_SIZE + std::mem::size_of::<u32>()..Self::VARLEN_HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let total = Self::VARLEN_HEADER_SIZE
                .checked_add(key_len)
                .and_then(|n| n.checked_add(value_len))
                .ok_or(Status::ResourceExhausted)?;
            if bytes.len() < total {
                return Err(Status::Corruption);
            }

            let key_bytes = &bytes[Self::VARLEN_HEADER_SIZE..Self::VARLEN_HEADER_SIZE + key_len];
            let key = <K as PersistKey>::Codec::decode(key_bytes)?;

            let value = if header.is_tombstone() {
                None
            } else {
                let value_start = Self::VARLEN_HEADER_SIZE + key_len;
                let value_bytes = &bytes[value_start..value_start + value_len];
                Some(<V as PersistValue>::Codec::decode(value_bytes)?)
            };

            Ok(DiskReadResult {
                key: Some(key),
                value,
                previous_address,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{RawBytes, Utf8};
    use crate::device::NullDisk;
    use crate::store::FasterKvConfig;
    use std::sync::Arc;

    fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device).unwrap())
    }

    fn create_varlen_test_store() -> Arc<FasterKv<Utf8, Utf8, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device).unwrap())
    }

    fn create_rawbytes_store() -> Arc<FasterKv<RawBytes, RawBytes, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 12,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device).unwrap())
    }

    #[test]
    fn test_parse_disk_record_too_small() {
        let store = create_test_store();
        let bytes = vec![0u8; 4];
        let result = store.parse_disk_record(&bytes);
        assert!(matches!(result, Err(Status::Corruption)));
    }

    #[test]
    fn test_parse_disk_record_invalid_header() {
        let store = create_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, true, false, false);
        let mut bytes = vec![0u8; 24];
        let control = header.control();
        bytes[0..8].copy_from_slice(&control.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.key.is_none());
        assert!(parsed.value.is_none());
    }

    #[test]
    fn test_parse_disk_record_valid_fixed() {
        let store = create_test_store();
        let header = RecordInfo::new(Address::new(1, 100), 0, false, false, false);
        let key: u64 = 42;
        let value: u64 = 100;

        let mut bytes = vec![0u8; 24];
        let control = header.control();
        bytes[0..8].copy_from_slice(&control.to_le_bytes());
        bytes[8..16].copy_from_slice(&key.to_le_bytes());
        bytes[16..24].copy_from_slice(&value.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, Some(42u64));
        assert_eq!(parsed.value, Some(100u64));
        assert_eq!(parsed.previous_address, Address::new(1, 100));
    }

    #[test]
    fn test_parse_disk_record_tombstone_fixed() {
        let store = create_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, false, true, false);
        let key: u64 = 42;

        let mut bytes = vec![0u8; 24];
        let control = header.control();
        bytes[0..8].copy_from_slice(&control.to_le_bytes());
        bytes[8..16].copy_from_slice(&key.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, Some(42u64));
        assert!(parsed.value.is_none());
    }

    #[test]
    fn test_parse_disk_record_fixed_too_short_for_data() {
        let store = create_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, false, false, false);
        let mut bytes = vec![0u8; 12];
        let control = header.control();
        bytes[0..8].copy_from_slice(&control.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        assert!(matches!(result, Err(Status::Corruption)));
    }

    #[test]
    fn test_parse_disk_record_varlen_too_small_header() {
        let store = create_varlen_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, false, false, false);
        let mut bytes = vec![0u8; 10];
        let control = header.control();
        bytes[0..8].copy_from_slice(&control.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        assert!(matches!(result, Err(Status::Corruption)));
    }

    #[test]
    fn test_parse_disk_record_varlen_valid() {
        let store = create_varlen_test_store();
        let header = RecordInfo::new(Address::new(2, 200), 0, false, false, false);
        let key_str = "hello";
        let value_str = "world";
        let key_bytes = key_str.as_bytes();
        let value_bytes = value_str.as_bytes();

        let total_size = 16 + key_bytes.len() + value_bytes.len();
        let mut bytes = vec![0u8; total_size];
        bytes[0..8].copy_from_slice(&header.control().to_le_bytes());
        bytes[8..12].copy_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        bytes[12..16].copy_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        bytes[16..16 + key_bytes.len()].copy_from_slice(key_bytes);
        bytes[16 + key_bytes.len()..].copy_from_slice(value_bytes);

        let result = store.parse_disk_record(&bytes);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, Some(Utf8(key_str.to_string())));
        assert_eq!(parsed.value, Some(Utf8(value_str.to_string())));
        assert_eq!(parsed.previous_address, Address::new(2, 200));
    }

    #[test]
    fn test_parse_disk_record_varlen_tombstone() {
        let store = create_varlen_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, false, true, false);
        let key_str = "deletekey";
        let key_bytes = key_str.as_bytes();

        let total_size = 16 + key_bytes.len();
        let mut bytes = vec![0u8; total_size];
        bytes[0..8].copy_from_slice(&header.control().to_le_bytes());
        bytes[8..12].copy_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        bytes[16..].copy_from_slice(key_bytes);

        let result = store.parse_disk_record(&bytes);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.key, Some(Utf8(key_str.to_string())));
        assert!(parsed.value.is_none());
    }

    #[test]
    fn test_parse_disk_record_varlen_insufficient_data() {
        let store = create_varlen_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, false, false, false);

        let mut bytes = vec![0u8; 20];
        bytes[0..8].copy_from_slice(&header.control().to_le_bytes());
        bytes[8..12].copy_from_slice(&10u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&10u32.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        assert!(matches!(result, Err(Status::Corruption)));
    }

    #[test]
    fn test_parse_disk_record_varlen_overflow() {
        let store = create_varlen_test_store();
        let header = RecordInfo::new(Address::INVALID, 0, false, false, false);

        let mut bytes = vec![0u8; 16];
        bytes[0..8].copy_from_slice(&header.control().to_le_bytes());
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        bytes[12..16].copy_from_slice(&u32::MAX.to_le_bytes());

        let result = store.parse_disk_record(&bytes);
        // When key_len and value_len overflow when added to header size, it returns ResourceExhausted
        // Or if total exceeds bytes.len(), it returns Corruption
        assert!(result.is_err());
    }

    #[test]
    fn test_take_completed_io_for_context_empty() {
        let store = create_test_store();
        let count = store.take_completed_io_for_context(0, 0, 0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_take_completed_io_for_context_nonexistent() {
        let store = create_test_store();
        let count = store.take_completed_io_for_context(999, 12345, 42);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_take_disk_read_result_empty() {
        let store = create_test_store();
        let result = store.take_disk_read_result(Address::new(0, 100));
        assert!(result.is_none());
    }

    #[test]
    fn test_take_disk_read_result_nonexistent_address() {
        let store = create_test_store();
        let result = store.take_disk_read_result(Address::new(999, 999));
        assert!(result.is_none());
    }

    #[test]
    fn test_submit_disk_read_for_context_fixed() {
        let store = create_test_store();
        let addr = Address::new(0, 100);
        let submitted = store.submit_disk_read_for_context(0, 1, 1, addr);
        assert!(submitted);
    }

    #[test]
    fn test_submit_disk_read_for_context_dedup() {
        let store = create_test_store();
        let addr = Address::new(0, 200);

        let first = store.submit_disk_read_for_context(0, 1, 1, addr);
        assert!(first);

        let second = store.submit_disk_read_for_context(0, 1, 2, addr);
        assert!(!second);
    }

    #[test]
    fn test_submit_disk_read_for_context_varlen() {
        let store = create_varlen_test_store();
        let addr = Address::new(0, 300);
        let submitted = store.submit_disk_read_for_context(0, 1, 1, addr);
        assert!(submitted);
    }

    #[test]
    fn test_disk_read_result_debug() {
        let result: DiskReadResult<u64, u64> = DiskReadResult {
            key: Some(42),
            value: Some(100),
            previous_address: Address::new(1, 50),
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("DiskReadResult"));
    }

    #[test]
    fn test_disk_read_result_clone() {
        let result: DiskReadResult<u64, u64> = DiskReadResult {
            key: Some(42),
            value: Some(100),
            previous_address: Address::new(1, 50),
        };
        let cloned = result.clone();
        assert_eq!(cloned.key, result.key);
        assert_eq!(cloned.value, result.value);
        assert_eq!(cloned.previous_address, result.previous_address);
    }

    #[test]
    fn test_pending_io_key_eq() {
        let key1 = PendingIoKey {
            thread_id: 1,
            thread_tag: 100,
            ctx_id: 50,
        };
        let key2 = PendingIoKey {
            thread_id: 1,
            thread_tag: 100,
            ctx_id: 50,
        };
        let key3 = PendingIoKey {
            thread_id: 2,
            thread_tag: 100,
            ctx_id: 50,
        };
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_pending_io_key_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let key1 = PendingIoKey {
            thread_id: 1,
            thread_tag: 100,
            ctx_id: 50,
        };
        let key2 = PendingIoKey {
            thread_id: 1,
            thread_tag: 100,
            ctx_id: 50,
        };

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        key1.hash(&mut hasher1);
        key2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_pending_io_key_debug() {
        let key = PendingIoKey {
            thread_id: 1,
            thread_tag: 100,
            ctx_id: 50,
        };
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("PendingIoKey"));
    }

    #[test]
    fn test_disk_read_cache_entry_debug() {
        let entry: DiskReadCacheEntry<u64, u64> = DiskReadCacheEntry {
            parsed: Ok(DiskReadResult {
                key: Some(1),
                value: Some(2),
                previous_address: Address::INVALID,
            }),
        };
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("DiskReadCacheEntry"));
    }

    #[test]
    fn test_disk_read_cache_entry_clone() {
        let entry: DiskReadCacheEntry<u64, u64> = DiskReadCacheEntry {
            parsed: Ok(DiskReadResult {
                key: Some(1),
                value: Some(2),
                previous_address: Address::INVALID,
            }),
        };
        let cloned = entry.clone();
        assert!(cloned.parsed.is_ok());
    }

    #[test]
    fn test_multiple_disk_read_submissions() {
        let store = create_test_store();

        for i in 0..5 {
            let addr = Address::new(i, 100 * i);
            let submitted = store.submit_disk_read_for_context(i as usize, 1, i as u64, addr);
            assert!(submitted);
        }
    }

    #[test]
    fn test_concurrent_take_completed_io() {
        let store = create_test_store();

        for i in 0..5 {
            let count = store.take_completed_io_for_context(i, i as u64, i as u64);
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn test_parse_disk_record_rawbytes() {
        let store = create_rawbytes_store();
        let header = RecordInfo::new(Address::new(3, 300), 0, false, false, false);
        let key_data = b"rawkey";
        let value_data = b"rawvalue";

        let total_size = 16 + key_data.len() + value_data.len();
        let mut bytes = vec![0u8; total_size];
        bytes[0..8].copy_from_slice(&header.control().to_le_bytes());
        bytes[8..12].copy_from_slice(&(key_data.len() as u32).to_le_bytes());
        bytes[12..16].copy_from_slice(&(value_data.len() as u32).to_le_bytes());
        bytes[16..16 + key_data.len()].copy_from_slice(key_data);
        bytes[16 + key_data.len()..].copy_from_slice(value_data);

        let result = store.parse_disk_record(&bytes);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.key.is_some());
        assert!(parsed.value.is_some());
        assert_eq!(parsed.key.as_ref().unwrap().0.as_ref(), key_data);
        assert_eq!(parsed.value.as_ref().unwrap().0.as_ref(), value_data);
    }

    #[test]
    fn test_record_info_size_constant() {
        assert_eq!(
            FasterKv::<u64, u64, NullDisk>::RECORD_INFO_SIZE,
            std::mem::size_of::<RecordInfo>()
        );
    }

    #[test]
    fn test_varlen_lengths_size_constant() {
        assert_eq!(
            FasterKv::<u64, u64, NullDisk>::VARLEN_LENGTHS_SIZE,
            2 * std::mem::size_of::<u32>()
        );
    }

    #[test]
    fn test_varlen_header_size_constant() {
        assert_eq!(
            FasterKv::<u64, u64, NullDisk>::VARLEN_HEADER_SIZE,
            FasterKv::<u64, u64, NullDisk>::RECORD_INFO_SIZE
                + FasterKv::<u64, u64, NullDisk>::VARLEN_LENGTHS_SIZE
        );
    }
}
