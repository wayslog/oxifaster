use crate::allocator::{AutoFlushConfig, AutoFlushMetrics};
use crate::codec::{PersistKey, PersistValue};
use crate::device::StorageDevice;

use super::FasterKv;

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Enable automatic background flushing for the hybrid log.
    ///
    /// This applies the provided configuration immediately. If a worker is
    /// already running, it is restarted with the new configuration.
    pub fn enable_auto_flush(&self, config: AutoFlushConfig) {
        // SAFETY: `set_auto_flush_config` is internally synchronized.
        unsafe { self.hlog().set_auto_flush_config(config.with_enabled(true)) };
    }

    /// Disable automatic background flushing.
    pub fn disable_auto_flush(&self) {
        // SAFETY: `set_auto_flush_config` is internally synchronized.
        unsafe {
            let config = self.hlog().auto_flush_config().with_enabled(false);
            self.hlog().set_auto_flush_config(config);
        }
    }

    /// Returns whether the hybrid log auto-flush worker is currently running.
    pub fn auto_flush_enabled(&self) -> bool {
        // SAFETY: read-only, internally synchronized.
        unsafe { self.hlog().is_auto_flush_running() }
    }

    /// Get the currently active automatic flush configuration.
    pub fn auto_flush_config(&self) -> AutoFlushConfig {
        // SAFETY: read-only, internally synchronized.
        unsafe { self.hlog().auto_flush_config() }
    }

    /// Snapshot automatic flush metrics.
    pub fn auto_flush_metrics(&self) -> AutoFlushMetrics {
        // SAFETY: read-only, internally synchronized.
        unsafe { self.hlog().auto_flush_metrics() }
    }
}
