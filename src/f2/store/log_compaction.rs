use std::ptr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::address::Address;
use crate::codec::hash64;
use crate::compaction::{CompactionResult, CompactionStats};
use crate::f2::config::HotToColdMigrationStrategy;
use crate::f2::state::{F2CheckpointPhase, StoreCheckpointStatus};
use crate::index::KeyHash;
use crate::record::{Record, RecordInfo};
use crate::status::Status;
use bytemuck::Pod;

use super::internal_store::InternalStore;
use super::{F2Kv, StoreType};

impl<K, V, D> F2Kv<K, V, D>
where
    K: Pod + Eq + Clone + Send + Sync + 'static,
    V: Pod + Clone + Send + Sync + 'static,
    D: crate::device::StorageDevice + 'static,
{
    /// Compact the hot log
    ///
    /// Moves cold data from hot log to cold log.
    pub fn compact_hot_log(&self, until_address: Address) -> Result<CompactionResult, Status> {
        self.compact_log(StoreType::Hot, until_address, true)
    }

    /// Compact the cold log
    ///
    /// Reclaims space in the cold log.
    pub fn compact_cold_log(&self, until_address: Address) -> Result<CompactionResult, Status> {
        self.compact_log(StoreType::Cold, until_address, true)
    }

    /// Check if hot log should be compacted
    pub fn should_compact_hot_log(&self) -> Option<Address> {
        self.should_compact_log(StoreType::Hot)
    }

    /// Check if cold log should be compacted
    pub fn should_compact_cold_log(&self) -> Option<Address> {
        self.should_compact_log(StoreType::Cold)
    }

    /// Internal log compaction
    fn compact_log(
        &self,
        store_type: StoreType,
        until_address: Address,
        shift_begin_address: bool,
    ) -> Result<CompactionResult, Status> {
        let (store, compactor) = match store_type {
            StoreType::Hot => (&self.hot_store, &self.hot_compactor),
            StoreType::Cold => (&self.cold_store, &self.cold_compactor),
        };

        // Validate until_address (must not exceed tail).
        let tail = store.tail_address();
        if until_address.control() > tail.control() {
            return Err(Status::InvalidArgument);
        }

        // Try to start compaction
        if compactor.try_start().is_err() {
            return Err(Status::Aborted);
        }

        let start = Instant::now();

        let begin_addr = store.begin_address();
        let mut new_begin_address = until_address;

        let mut stats = CompactionStats {
            bytes_reclaimed: until_address.control().saturating_sub(begin_addr.control()),
            ..Default::default()
        };

        if store_type == StoreType::Hot && shift_begin_address {
            self.compact_hot_log_and_migrate(
                store,
                compactor,
                begin_addr,
                until_address,
                &mut new_begin_address,
                &mut stats,
            )?;
        } else if shift_begin_address {
            if let Err(_e) = store.hlog().flush_until(until_address) {
                compactor.complete();
                return Err(Status::Corruption);
            }
            stats.bytes_scanned = stats.bytes_reclaimed;
            unsafe { store.hlog_mut().shift_begin_address(until_address) };
            store.hash_index.garbage_collect(until_address);
        }

        stats.duration_ms = start.elapsed().as_millis() as u64;

        compactor.complete();

        let new_begin = if store_type == StoreType::Hot {
            new_begin_address
        } else {
            until_address
        };
        Ok(CompactionResult::success(new_begin, stats))
    }

    fn compact_hot_log_and_migrate(
        &self,
        store: &InternalStore<D>,
        compactor: &crate::compaction::Compactor,
        begin_addr: Address,
        until_address: Address,
        new_begin_address: &mut Address,
        stats: &mut CompactionStats,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        // Advancing begin_address makes old pages eligible for reuse. Flush first so that the
        // reclaimed range is durably persisted; flushing also updates safe_read_only_address.
        if let Err(_e) = store.hlog().flush_until(until_address) {
            compactor.complete();
            return Err(Status::Corruption);
        }

        // Hot-log compaction is migration: copy still-live records from the compacted range into
        // the cold store, and clear the hot index entry. Only then is it safe to advance begin.
        let mut current_address = begin_addr;
        let strategy = &self.config.compaction.hot_to_cold_migration;
        while current_address < until_address {
            let record_ptr = unsafe { store.hlog().get(current_address) };

            if let Some(ptr) = record_ptr {
                // SAFETY: Protected by epoch; `current_address` points to a record in the log.
                let record: &Record<K, V> = unsafe { &*(ptr.as_ptr() as *const _) };
                let record_size_u64 = Record::<K, V>::size() as u64;

                stats.records_scanned += 1;
                stats.bytes_scanned += record_size_u64;

                let record_key = unsafe { Record::<K, V>::read_key(ptr.as_ptr()) };
                let hash = KeyHash::new(hash64(bytemuck::bytes_of(&record_key)));
                let is_tombstone = record.header.is_tombstone();
                if is_tombstone {
                    stats.tombstones_found += 1;
                }

                let index_result = store.hash_index.find_entry(hash);
                if index_result.found() {
                    let index_address = index_result.entry.address().readcache_address();
                    if compactor.should_compact_record(current_address, index_address, is_tombstone)
                    {
                        let keep_hot = match strategy {
                            HotToColdMigrationStrategy::AddressAging => false,
                            HotToColdMigrationStrategy::AccessFrequency {
                                min_hot_accesses,
                                ..
                            } => self.key_access.get_estimated(hash.hash()) >= *min_hot_accesses,
                        };

                        // Always migrate tombstones to cold to prevent cold-store resurrection.
                        let should_migrate_to_cold = is_tombstone || !keep_hot;

                        if should_migrate_to_cold {
                            if self
                                .migrate_record_to_cold(
                                    store,
                                    &index_result,
                                    record_key,
                                    ptr.as_ptr(),
                                    is_tombstone,
                                    hash,
                                )
                                .is_ok()
                            {
                                stats.records_compacted += 1;
                                stats.bytes_compacted += record_size_u64;
                            } else {
                                stats.records_skipped += 1;
                                if *new_begin_address == until_address {
                                    *new_begin_address = current_address;
                                }
                            }
                        } else {
                            let value = unsafe { Record::<K, V>::read_value(ptr.as_ptr()) };
                            if self
                                .copy_record_to_hot_tail(
                                    store,
                                    &index_result,
                                    record_key,
                                    value,
                                    hash,
                                    current_address,
                                )
                                .is_ok()
                            {
                                stats.records_compacted += 1;
                                stats.bytes_compacted += Record::<K, V>::size() as u64;
                            } else {
                                stats.records_skipped += 1;
                                if *new_begin_address == until_address {
                                    *new_begin_address = current_address;
                                }
                            }
                        }
                    } else {
                        // Not the latest record.
                        stats.records_skipped += 1;
                    }
                } else {
                    // Not in index: reclaimable.
                    stats.records_skipped += 1;
                }

                current_address =
                    Address::from_control(current_address.control() + record_size_u64);
            } else {
                // There may be unwritten space within the page; jump to the next page boundary.
                let page_size = store.hlog().page_size() as u64;
                let next_page = (current_address.control() / page_size + 1) * page_size;
                current_address = Address::from_control(next_page);
            }
        }

        unsafe { store.hlog_mut().shift_begin_address(*new_begin_address) };
        store.hash_index.garbage_collect(*new_begin_address);

        stats.bytes_reclaimed = new_begin_address
            .control()
            .saturating_sub(begin_addr.control());

        if let HotToColdMigrationStrategy::AccessFrequency { decay_shift, .. } = strategy {
            self.key_access.decay_shift(*decay_shift);
        }

        Ok(())
    }

    fn migrate_record_to_cold(
        &self,
        store: &InternalStore<D>,
        index_result: &crate::index::FindResult,
        record_key: K,
        record_base: *const u8,
        is_tombstone: bool,
        hash: KeyHash,
    ) -> Result<(), Status> {
        let write_result = if is_tombstone {
            self.tombstone_into_store(&self.cold_store, hash, record_key)
        } else {
            let value = unsafe { Record::<K, V>::read_value(record_base) };
            self.upsert_into_store(&self.cold_store, hash, record_key, value)
        };

        if write_result.is_err() {
            return Err(Status::Corruption);
        }

        let Some(atomic_entry) = index_result.atomic_entry else {
            return Err(Status::Aborted);
        };

        let clear_status = store.hash_index.try_update_entry(
            Some(atomic_entry),
            index_result.entry,
            Address::INVALID,
            hash,
            false,
        );

        if clear_status == Status::Ok {
            self.key_access.remove(hash.hash());
            Ok(())
        } else {
            Err(Status::Aborted)
        }
    }

    fn copy_record_to_hot_tail(
        &self,
        store: &InternalStore<D>,
        index_result: &crate::index::FindResult,
        record_key: K,
        value: V,
        hash: KeyHash,
        old_address: Address,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        let record_size = Record::<K, V>::size();
        let new_address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;
        let record_ptr =
            unsafe { store.hlog_mut().get_mut(new_address) }.ok_or(Status::OutOfMemory)?;

        unsafe {
            let new_record = record_ptr.as_ptr() as *mut Record<K, V>;
            let header = RecordInfo::new(
                old_address,
                self.checkpoint.version() as u16,
                false,
                false,
                false,
            );
            ptr::write(&mut (*new_record).header, header);

            Record::<K, V>::write_key(record_ptr.as_ptr(), record_key);
            Record::<K, V>::write_value(record_ptr.as_ptr(), value);
        }

        let Some(atomic_entry) = index_result.atomic_entry else {
            return Err(Status::Aborted);
        };

        let update_status = store.hash_index.try_update_entry(
            Some(atomic_entry),
            index_result.entry,
            new_address,
            hash,
            false,
        );

        if update_status == Status::Ok {
            Ok(())
        } else {
            Err(Status::Aborted)
        }
    }

    /// Internal check for compaction need
    fn should_compact_log(&self, store_type: StoreType) -> Option<Address> {
        let (store, compaction_enabled, log_size_budget) = match store_type {
            StoreType::Hot => (
                &self.hot_store,
                self.config.compaction.hot_store_enabled,
                self.config.compaction.hot_log_size_budget,
            ),
            StoreType::Cold => (
                &self.cold_store,
                self.config.compaction.cold_store_enabled,
                self.config.compaction.cold_log_size_budget,
            ),
        };

        if !compaction_enabled {
            return None;
        }

        let hlog_size_threshold =
            (log_size_budget as f64 * self.config.compaction.trigger_percentage) as u64;

        if store.size() < hlog_size_threshold {
            return None;
        }

        // Check checkpoint phase
        let phase = self.checkpoint.phase.load(Ordering::Acquire);
        match phase {
            F2CheckpointPhase::Rest => {}
            F2CheckpointPhase::HotStoreCheckpoint if store_type == StoreType::Hot => {
                return None; // Can't compact hot store during hot checkpoint
            }
            F2CheckpointPhase::ColdStoreCheckpoint => {
                if self.checkpoint.cold_store_status.load(Ordering::Acquire)
                    == StoreCheckpointStatus::Active
                {
                    return None; // Can't compact during active cold checkpoint
                }
            }
            F2CheckpointPhase::Recover => {
                return None; // Can't compact during recovery
            }
            _ => {}
        }

        // Calculate until address
        let begin_address = store.begin_address().control();
        let compact_size = (store.size() as f64 * self.config.compaction.compact_percentage) as u64;
        let mut until_address = begin_address + compact_size;

        // Respect max compacted size
        until_address = until_address.min(begin_address + self.config.compaction.max_compact_size);

        // Do not exceed tail (flush happens during compaction).
        let tail = store.tail_address().control();
        until_address = until_address.min(tail);

        if until_address <= begin_address {
            return None;
        }

        Some(Address::from_control(until_address))
    }
}
