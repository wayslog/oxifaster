use std::ptr;
use std::sync::atomic::Ordering;

use crate::address::Address;
use crate::f2::HotToColdMigrationStrategy;
use crate::index::KeyHash;
use crate::record::{Key, Record, RecordInfo, Value};
use crate::status::Status;

use super::internal_store::InternalStore;
use super::F2Kv;

impl<K, V, D> F2Kv<K, V, D>
where
    K: Key + Clone + 'static,
    V: Value + Clone + 'static,
    D: crate::device::StorageDevice + 'static,
{
    /// 读取：先 hot 再 cold。
    pub fn read(&self, key: &K) -> Result<Option<V>, Status> {
        let hash = KeyHash::new(key.get_hash());

        if let Some(value) = self.internal_read(&self.hot_store, true, key, hash)? {
            return Ok(Some(value));
        }

        if let Some(value) = self.internal_read(&self.cold_store, false, key, hash)? {
            return Ok(Some(value));
        }

        Ok(None)
    }

    /// Upsert：总是写入 hot store。
    pub fn upsert(&self, key: K, value: V) -> Result<(), Status> {
        // 简单节流：hot log 超预算时等待后台 compaction/migration 推进 begin。
        let max_size = self.hot_store.max_hlog_size.load(Ordering::Acquire);
        while self.hot_store.size() >= max_size {
            self.refresh();
            std::hint::spin_loop();
        }

        let hash = KeyHash::new(key.get_hash());
        self.track_key_access(hash.hash());
        self.upsert_into_store(&self.hot_store, hash, key, value)
    }

    /// Delete：写入 hot store tombstone，确保能覆盖 cold store 的旧值。
    pub fn delete(&self, key: &K) -> Result<(), Status> {
        let hash = KeyHash::new(key.get_hash());
        self.tombstone_into_store(&self.hot_store, hash, key.clone())
    }

    /// Read-Modify-Write：
    /// - 若 hot 可 in-place（mutable region 且命中同 key 且非 tombstone），直接原地修改；
    /// - 否则先读 hot/cold 得到值（若都不存在则用 Default），修改后 upsert 回 hot。
    pub fn rmw<F>(&self, key: K, modify: F) -> Result<(), Status>
    where
        F: FnOnce(&mut V),
        V: Default,
    {
        let hash = KeyHash::new(key.get_hash());
        self.track_key_access(hash.hash());

        if let Some(value_ptr) = self.try_get_mutable_value_ptr(&self.hot_store, &key, hash)? {
            // SAFETY: value_ptr 指向 mutable region 的记录 value，且 epoch 已保护。
            let value = unsafe { &mut *value_ptr };
            modify(value);
            return Ok(());
        }

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

        // 只实现内存读取路径：PersistentMemoryMalloc 当前没有对外暴露“同步读盘”能力。
        while address.is_valid() {
            let record_ptr = unsafe { store.hlog().get(address) };
            let Some(ptr) = record_ptr else {
                break;
            };

            // SAFETY: address 指向日志中一条 record，且访问受 epoch 保护。
            let record: &Record<K, V> = unsafe { &*(ptr.as_ptr() as *const _) };
            let record_key = unsafe { record.key() };

            if record_key == key {
                if record.header.is_tombstone() {
                    return Ok(None);
                }
                let value = unsafe { record.value() };
                if is_hot {
                    self.track_key_access(hash.hash());
                }
                return Ok(Some(value.clone()));
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

        // 为避免在高并发下 CAS 失败直接丢写，这里做有限次重试。
        const MAX_RETRIES: usize = 32;

        // 避免在 CAS 重试时反复分配日志空间：先分配并初始化 record（key/value），
        // 每次重试只更新 header.previous_address 并尝试更新 index。
        let record_size = Record::<K, V>::size();
        let address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Err(Status::OutOfMemory);
        };

        let record = ptr.as_ptr() as *mut Record<K, V>;
        unsafe {
            let key_ptr = ptr.as_ptr().add(Record::<K, V>::key_offset()) as *mut K;
            ptr::write(key_ptr, key.clone());

            let value_ptr = ptr.as_ptr().add(Record::<K, V>::value_offset()) as *mut V;
            ptr::write(value_ptr, value.clone());
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

        // 与 upsert 同理：避免 CAS 重试造成日志空间“空洞”。
        let record_size = Record::<K, V>::size();
        let address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Err(Status::OutOfMemory);
        };

        let record = ptr.as_ptr() as *mut Record<K, V>;
        unsafe {
            let key_ptr = ptr.as_ptr().add(Record::<K, V>::key_offset()) as *mut K;
            ptr::write(key_ptr, key.clone());
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

    fn try_get_mutable_value_ptr(
        &self,
        store: &InternalStore<D>,
        key: &K,
        hash: KeyHash,
    ) -> Result<Option<*mut V>, Status> {
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
            let record = ptr.as_ptr() as *mut Record<K, V>;
            let record_key = (*record).key();
            if record_key != key {
                return Ok(None);
            }
            if (*record).header.is_tombstone() {
                return Ok(None);
            }

            let value_ptr = ptr.as_ptr().add(Record::<K, V>::value_offset()) as *mut V;
            Ok(Some(value_ptr))
        }
    }
}
