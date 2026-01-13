use crate::device::StorageDevice;
use crate::index::{GrowResult, GrowState};
use crate::record::{Key, Value};
use crate::status::Status;

use super::FasterKv;

impl<K, V, D> FasterKv<K, V, D>
where
    K: Key,
    V: Value,
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
            return Err(Status::Aborted);
        }

        let current_size = self.index_size();

        // Validate new size
        if new_size <= current_size {
            return Err(Status::InvalidArgument);
        }
        if !new_size.is_power_of_two() {
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

        Ok(())
    }

    /// Complete a pending index growth operation
    ///
    /// This should be called after all threads have processed their chunks.
    pub fn complete_grow(&self) -> GrowResult {
        // SAFETY: Access to grow_state
        let state = unsafe { (*self.grow_state.get()).take() };

        match state {
            Some(grow_state) => grow_state.complete(),
            None => GrowResult::failure(Status::InvalidOperation),
        }
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
}
