use crate::codec::{PersistKey, PersistValue};
use crate::device::StorageDevice;
use crate::index::{GrowCompleteCallback, GrowResult, GrowState};
use crate::status::Status;

use super::FasterKv;

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    // ============ Index Growth API ============

    /// Get the current hash table size (number of buckets)
    pub fn index_size(&self) -> u64 {
        self.hash_index.size()
    }

    /// Check if index growth is in progress
    pub fn is_grow_in_progress(&self) -> bool {
        // SAFETY: Read-only check
        unsafe { (*self.grow_state.get()).is_some() }
    }

    /// Start growing the hash index
    ///
    /// # Arguments
    /// * `new_size` - New hash table size (must be power of 2 and > current size)
    ///
    /// # Returns
    /// Ok(()) if growth started, Err if already in progress or invalid size
    pub fn start_grow(&self, new_size: u64) -> Result<(), Status> {
        // Check if grow is already in progress
        if self.is_grow_in_progress() {
            if self.stats_collector.is_enabled() {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_index_grow_failed();
            }
            return Err(Status::Aborted);
        }

        let current_size = self.index_size();

        // Validate new size
        if new_size <= current_size {
            if self.stats_collector.is_enabled() {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_index_grow_failed();
            }
            return Err(Status::InvalidArgument);
        }
        if !new_size.is_power_of_two() {
            if self.stats_collector.is_enabled() {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_index_grow_failed();
            }
            return Err(Status::InvalidArgument);
        }

        // Calculate number of chunks for growth
        let num_chunks = crate::index::calculate_num_chunks(current_size);

        // Create grow state and initialize
        let mut state = GrowState::new();
        state.initialize(0, num_chunks);

        // SAFETY: We checked no grow is in progress
        unsafe {
            *self.grow_state.get() = Some(state);
        }

        if self.stats_collector.is_enabled() {
            self.stats_collector
                .store_stats
                .operational
                .record_index_grow_started();
        }
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(old_size = current_size, new_size, "index growth started");
        }

        Ok(())
    }

    /// Complete a pending index growth operation
    ///
    /// This should be called after all threads have processed their chunks.
    pub fn complete_grow(&self) -> GrowResult {
        // SAFETY: Access to grow_state
        let state = unsafe { (*self.grow_state.get()).take() };

        let result = match state {
            Some(grow_state) => grow_state.complete(),
            None => GrowResult::failure(Status::InvalidOperation),
        };

        if self.stats_collector.is_enabled() {
            if result.success {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_index_grow_completed();
            } else {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_index_grow_failed();
            }
        }
        if tracing::enabled!(tracing::Level::INFO) {
            if result.success {
                tracing::info!(
                    old_size = result.old_size,
                    new_size = result.new_size,
                    entries_migrated = result.entries_migrated,
                    duration_ms = result.duration_ms,
                    "index growth completed"
                );
            } else {
                tracing::warn!(status = ?result.status, "index growth failed");
            }
        }

        result
    }

    /// Get grow progress (completed chunks / total chunks)
    pub fn grow_progress(&self) -> Option<(u64, u64)> {
        // SAFETY: Read-only access
        unsafe {
            (*self.grow_state.get())
                .as_ref()
                .map(|s| (s.progress().0, s.progress().1))
        }
    }

    /// Trigger hash index growth with a completion callback.
    ///
    /// This method initiates index growth and stores the callback to be invoked
    /// when growth completes. The callback receives the new table size.
    ///
    /// # Arguments
    /// * `new_size` - New hash table size (must be power of 2 and > current size)
    /// * `callback` - Optional callback invoked when growth completes with the new size
    ///
    /// # Returns
    /// `Status::Ok` if growth started successfully, error status otherwise
    ///
    /// # Example
    /// ```ignore
    /// use oxifaster::index::GrowCompleteCallback;
    ///
    /// let callback: Option<GrowCompleteCallback> = Some(Box::new(|new_size| {
    ///     println!("Index growth completed, new size: {}", new_size);
    /// }));
    /// store.grow_index_with_callback(new_size, callback)?;
    /// ```
    pub fn grow_index_with_callback(
        &self,
        new_size: u64,
        callback: Option<GrowCompleteCallback>,
    ) -> Status {
        // Start the grow operation
        match self.start_grow(new_size) {
            Ok(()) => {
                // For now, we complete the grow synchronously and invoke the callback.
                // In a full implementation, the callback would be stored and invoked
                // when background growth workers complete.
                let result = self.complete_grow();

                // Invoke the callback with the new size
                if let Some(cb) = callback {
                    let final_size = if result.success {
                        result.new_size
                    } else {
                        self.index_size()
                    };
                    cb(final_size);
                }

                if result.success {
                    Status::Ok
                } else {
                    result.status.unwrap_or(Status::Aborted)
                }
            }
            Err(status) => status,
        }
    }
}
