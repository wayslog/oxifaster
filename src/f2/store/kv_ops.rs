use std::ptr;
use std::sync::atomic::Ordering;

use crate::address::Address;
use crate::codec::hash64;
use crate::f2::HotToColdMigrationStrategy;
use crate::index::KeyHash;
use crate::record::{Record, RecordInfo};
use crate::status::Status;
use bytemuck::Pod;

use super::internal_store::InternalStore;
use super::F2Kv;

impl<K, V, D> F2Kv<K, V, D>
where
    K: Pod + Eq + Clone + Send + Sync + 'static,
    V: Pod + Clone + Send + Sync + 'static,
    D: crate::device::StorageDevice + 'static,
{
    /// Read: hot first, then cold.
    pub fn read(&self, key: &K) -> Result<Option<V>, Status> {
        let hash = KeyHash::new(hash64(bytemuck::bytes_of(key)));

        if let Some(value) = self.internal_read(&self.hot_store, true, key, hash)? {
            return Ok(Some(value));
        }

        if let Some(value) = self.internal_read(&self.cold_store, false, key, hash)? {
            return Ok(Some(value));
        }

        Ok(None)
    }

    /// Upsert: always writes to the hot store.
    pub fn upsert(&self, key: K, value: V) -> Result<(), Status> {
        // Basic throttling: wait for background compaction/migration to advance begin.
        let max_size = self.hot_store.max_hlog_size.load(Ordering::Acquire);
        while self.hot_store.size() >= max_size {
            self.refresh();
            std::hint::spin_loop();
        }

        let hash = KeyHash::new(hash64(bytemuck::bytes_of(&key)));
        self.track_key_access(hash.hash());
        self.upsert_into_store(&self.hot_store, hash, key, value)
    }

    /// Delete: writes a hot-store tombstone to override a cold-store value.
    pub fn delete(&self, key: &K) -> Result<(), Status> {
        let hash = KeyHash::new(hash64(bytemuck::bytes_of(key)));
        self.tombstone_into_store(&self.hot_store, hash, *key)
    }

    /// Read-Modify-Write:
    /// - If the hot record is mutable and matches, update in-place (fast path).
    /// - Otherwise, read hot/cold (or use `Default`), apply the update, and
    ///   conditionally insert into hot with a bounded retry loop to handle
    ///   concurrent modifications (avoids TOCTOU lost updates).
    pub fn rmw<F>(&self, key: K, mut modify: F) -> Result<(), Status>
    where
        F: FnMut(&mut V),
        V: Default,
    {
        let hash = KeyHash::new(hash64(bytemuck::bytes_of(&key)));
        self.track_key_access(hash.hash());

        // Fast path: in-place update in the mutable region.
        if let Some(record_base) = self.try_get_mutable_record_ptr(&self.hot_store, &key, hash)? {
            // SAFETY: `record_base` points to a record in the mutable region and is epoch-protected.
            let value_ptr = unsafe { record_base.add(Record::<K, V>::value_offset()) as *mut V };
            let mut current = unsafe { ptr::read_unaligned(value_ptr) };
            modify(&mut current);
            unsafe { ptr::write_unaligned(value_ptr, current) };
            return Ok(());
        }

        // Slow path: read-modify-conditional_insert with bounded retry.
        // If another thread modifies the same key between our read and CAS,
        // the conditional upsert will abort and we retry with fresh data.
        const MAX_RMW_RETRIES: usize = 256;
        for _ in 0..MAX_RMW_RETRIES {
            // Snapshot the current hash-index address for the key.
            let expected_address = self.get_hot_entry_address(&key);

            // Read the current value (from hot or cold store).
            let mut value: V = (self.read(&key)?).unwrap_or_default();

            // Apply the modification.
            modify(&mut value);

            // Attempt conditional insert -- succeeds only if the index entry
            // still points to `expected_address`.
            match self.conditional_upsert_into_hot(key, value, expected_address) {
                Ok(()) => return Ok(()),
                Err(Status::Aborted) => continue, // CAS failed, retry
                Err(e) => return Err(e),           // other error, propagate
            }
        }

        // Fallback after exhausting retries: regular upsert to avoid livelock.
        // This may lose a concurrent update but guarantees progress.
        let mut value: V = (self.read(&key)?).unwrap_or_default();
        modify(&mut value);
        self.upsert(key, value)
    }

    fn internal_read(
        &self,
        store: &InternalStore<D>,
        is_hot: bool,
        key: &K,
        hash: KeyHash,
    ) -> Result<Option<V>, Status> {
        let find_result = store.hash_index.find_entry(hash);
        if !find_result.found() {
            return Ok(None);
        }

        let mut address = find_result.entry.address();
        if address == Address::INVALID {
            return Ok(None);
        }

        // Only the in-memory read path is implemented: `PersistentMemoryMalloc` does not currently
        // expose a synchronous disk-read API.
        while address.is_valid() {
            let record_ptr = unsafe { store.hlog().get(address) };
            let Some(ptr) = record_ptr else {
                break;
            };

            // SAFETY: `address` points to a log record and the access is epoch-protected.
            let record: &Record<K, V> = unsafe { &*(ptr.as_ptr() as *const _) };
            let record_key = unsafe { Record::<K, V>::read_key(ptr.as_ptr()) };

            if record_key == *key {
                if record.header.is_tombstone() {
                    return Ok(None);
                }
                let value = unsafe { Record::<K, V>::read_value(ptr.as_ptr()) };
                if is_hot {
                    self.track_key_access(hash.hash());
                }
                return Ok(Some(value));
            }

            address = record.header.previous_address();
        }

        Ok(None)
    }

    fn track_key_access(&self, key_hash: u64) {
        if matches!(
            self.config.compaction.hot_to_cold_migration,
            HotToColdMigrationStrategy::AccessFrequency { .. }
        ) {
            self.key_access.record(key_hash);
        }
    }

    pub(super) fn upsert_into_store(
        &self,
        store: &InternalStore<D>,
        hash: KeyHash,
        key: K,
        value: V,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        // To avoid dropping writes under high contention due to CAS failures, retry a bounded number of times.
        const MAX_RETRIES: usize = 32;

        // Avoid allocating log space repeatedly across CAS retries:
        // allocate and initialize the record (key/value) once, then only update
        // `header.previous_address` and retry updating the index.
        let record_size = Record::<K, V>::size();
        let address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Err(Status::OutOfMemory);
        };

        let record = ptr.as_ptr() as *mut Record<K, V>;
        unsafe {
            Record::<K, V>::write_key(ptr.as_ptr(), key);
            Record::<K, V>::write_value(ptr.as_ptr(), value);
        }

        for _ in 0..MAX_RETRIES {
            let result = store.hash_index.find_or_create_entry(hash);
            let old_address = result.entry.address().readcache_address();

            unsafe {
                let header = RecordInfo::new(
                    old_address,
                    self.checkpoint.version() as u16,
                    false,
                    false,
                    false,
                );
                ptr::write(&mut (*record).header, header);
            }

            let status = store.hash_index.try_update_entry(
                result.atomic_entry,
                result.entry,
                address,
                hash,
                false,
            );

            if status == Status::Ok {
                return Ok(());
            }
        }

        Err(Status::Aborted)
    }

    pub(super) fn tombstone_into_store(
        &self,
        store: &InternalStore<D>,
        hash: KeyHash,
        key: K,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        const MAX_RETRIES: usize = 32;

        // Same rationale as upsert: avoid creating holes in the log due to CAS retries.
        let record_size = Record::<K, V>::size();
        let address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Err(Status::OutOfMemory);
        };

        let record = ptr.as_ptr() as *mut Record<K, V>;
        unsafe {
            Record::<K, V>::write_key(ptr.as_ptr(), key);
        }

        for _ in 0..MAX_RETRIES {
            let result = store.hash_index.find_or_create_entry(hash);
            let old_address = result.entry.address().readcache_address();

            unsafe {
                let header = RecordInfo::new(
                    old_address,
                    self.checkpoint.version() as u16,
                    false,
                    true,
                    false,
                );
                ptr::write(&mut (*record).header, header);
            }

            let status = store.hash_index.try_update_entry(
                result.atomic_entry,
                result.entry,
                address,
                hash,
                false,
            );
            if status == Status::Ok {
                return Ok(());
            }
        }

        Err(Status::Aborted)
    }

    /// Return the current hash-index address for `key` in the hot store.
    ///
    /// If the key has no entry (or the entry is unused), returns `Address::INVALID`.
    /// This is used by RMW to snapshot the expected address before a conditional upsert.
    pub(crate) fn get_hot_entry_address(&self, key: &K) -> Address {
        let hash = KeyHash::new(hash64(bytemuck::bytes_of(key)));
        let find_result = self.hot_store.hash_index.find_entry(hash);
        if find_result.found() {
            find_result.entry.address().readcache_address()
        } else {
            Address::INVALID
        }
    }

    /// Conditionally insert into hot store only if the hash index entry still points
    /// to `expected_address`. Returns `Err(Status::Aborted)` if the entry was modified
    /// concurrently (CAS failure or address mismatch).
    ///
    /// This is the F2 equivalent of C++ FASTER's `ConditionalInsert`.
    pub fn conditional_upsert_into_hot(
        &self,
        key: K,
        value: V,
        expected_address: Address,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        let hash = KeyHash::new(hash64(bytemuck::bytes_of(&key)));
        let store = &self.hot_store;

        // Step 1: Check current index entry against expected_address.
        let result = store.hash_index.find_or_create_entry(hash);

        let current_address = result.entry.address().readcache_address();

        if current_address != expected_address {
            return Err(Status::Aborted);
        }

        // Step 2: Allocate and write the record.
        let record_size = Record::<K, V>::size();
        let address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Err(Status::OutOfMemory);
        };

        let record = ptr.as_ptr() as *mut Record<K, V>;
        unsafe {
            Record::<K, V>::write_key(ptr.as_ptr(), key);
            Record::<K, V>::write_value(ptr.as_ptr(), value);
        }

        // Step 3: Build header with previous_address pointing to the expected (old) entry.
        unsafe {
            let header = RecordInfo::new(
                expected_address,
                self.checkpoint.version() as u16,
                false,
                false,
                false,
            );
            ptr::write(&mut (*record).header, header);
        }

        // Step 4: Single CAS attempt -- if it fails, another thread changed the entry.
        let status = store.hash_index.try_update_entry(
            result.atomic_entry,
            result.entry,
            address,
            hash,
            false,
        );

        if status == Status::Ok {
            Ok(())
        } else {
            Err(Status::Aborted)
        }
    }

    fn try_get_mutable_record_ptr(
        &self,
        store: &InternalStore<D>,
        key: &K,
        hash: KeyHash,
    ) -> Result<Option<*mut u8>, Status> {
        // If mutable_pages is 0, all records are read-only by definition.
        // We must check this explicitly because read_only_address starts at 0
        // and only advances when pages fill up, so freshly written records
        // on the current page would incorrectly appear mutable.
        if store.hlog().config().mutable_pages == 0 {
            return Ok(None);
        }

        let find_result = store.hash_index.find_entry(hash);
        if !find_result.found() {
            return Ok(None);
        }

        let address = find_result.entry.address().readcache_address();
        if !address.is_valid() {
            return Ok(None);
        }

        if address < store.hlog().get_read_only_address() {
            return Ok(None);
        }

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Ok(None);
        };

        unsafe {
            let record = &*(ptr.as_ptr() as *const Record<K, V>);
            let record_key = Record::<K, V>::read_key(ptr.as_ptr());
            if record_key != *key {
                return Ok(None);
            }
            if record.header.is_tombstone() {
                return Ok(None);
            }

            Ok(Some(ptr.as_ptr()))
        }
    }
}
