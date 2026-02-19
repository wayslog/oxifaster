use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::address::Address;
use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::device::StorageDevice;
use crate::epoch::LightEpoch;

use super::store_index::StoreIndex;
use super::types::StoreType;

/// Internal store wrapper for hot or cold store
pub(super) struct InternalStore<D>
where
    D: StorageDevice,
{
    /// Epoch protection
    pub(super) epoch: Arc<LightEpoch>,
    /// Hash index
    pub(super) hash_index: StoreIndex,
    /// Hybrid log
    pub(super) hlog: UnsafeCell<PersistentMemoryMalloc<D>>,
    /// Storage device
    pub(super) device: Arc<D>,
    /// Store type
    pub(super) store_type: StoreType,
    /// Max log size (for throttling)
    pub(super) max_hlog_size: AtomicU64,
}

// SAFETY: InternalStore uses UnsafeCell for hlog but access is protected by epoch
// All mutable access goes through hlog_mut() which requires caller to ensure safety
unsafe impl<D: StorageDevice + Send> Send for InternalStore<D> {}
unsafe impl<D: StorageDevice + Send + Sync> Sync for InternalStore<D> {}

impl<D: StorageDevice> InternalStore<D> {
    /// Create a new internal store
    pub(super) fn new(
        table_size: u64,
        log_mem_size: u64,
        page_size_bits: u8,
        device: D,
        store_type: StoreType,
        cold_index: Option<crate::index::ColdIndexConfig>,
    ) -> Result<Self, String> {
        let device = Arc::new(device);
        let epoch = Arc::new(LightEpoch::new());

        let hash_index = match (store_type, cold_index) {
            (StoreType::Cold, Some(cfg)) => {
                StoreIndex::new_cold(cfg).map_err(|e| format!("ColdIndex 初始化失败：{e:?}"))?
            }
            _ => StoreIndex::new_memory(table_size),
        };

        let log_config = HybridLogConfig::new(log_mem_size, page_size_bits as u32);
        let hlog = PersistentMemoryMalloc::new(log_config, device.clone());

        Ok(Self {
            epoch,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            device,
            store_type,
            max_hlog_size: AtomicU64::new(u64::MAX),
        })
    }

    /// Get mutable reference to hlog (unsafe)
    ///
    /// # Safety
    /// Caller must ensure exclusive access, typically via epoch protection.
    #[allow(clippy::mut_from_ref)]
    pub(super) unsafe fn hlog_mut(&self) -> &mut PersistentMemoryMalloc<D> {
        // SAFETY: Caller guarantees exclusive access via epoch protection.
        unsafe { &mut *self.hlog.get() }
    }

    /// Get immutable reference to hlog
    pub(super) fn hlog(&self) -> &PersistentMemoryMalloc<D> {
        // SAFETY: UnsafeCell access is safe for shared reads when no concurrent writes.
        unsafe { &*self.hlog.get() }
    }

    /// Get the current log size
    pub(super) fn size(&self) -> u64 {
        let hlog = self.hlog();
        let tail = hlog.get_tail_address().control();
        let begin = hlog.get_begin_address().control();
        tail.saturating_sub(begin)
    }

    /// Get tail address
    pub(super) fn tail_address(&self) -> Address {
        self.hlog().get_tail_address()
    }

    /// Get begin address
    pub(super) fn begin_address(&self) -> Address {
        self.hlog().get_begin_address()
    }

    /// Get safe read-only address
    pub(super) fn safe_read_only_address(&self) -> Address {
        self.hlog().get_safe_read_only_address()
    }
}
