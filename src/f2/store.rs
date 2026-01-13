//! F2 Key-Value Store implementation
//!
//! The F2 (Fast & Fair) architecture provides two-tier storage with
//! automatic hot-cold data separation.

mod internal_store;
mod key_access;
mod kv_ops;
mod log_compaction;
mod store_index;
mod types;

pub use types::{ReadOperationStage, RmwOperationStage, StoreStats, StoreType};

#[cfg(test)]
mod tests;

use self::internal_store::InternalStore;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use parking_lot::Mutex;
use uuid::Uuid;

use crate::compaction::{CompactionConfig, Compactor};
use crate::device::StorageDevice;
use crate::f2::config::{F2CompactionConfig, F2Config};
use crate::f2::state::{F2CheckpointPhase, F2CheckpointState, StoreCheckpointStatus};
use crate::record::{Key, Value};
use crate::status::Status;

use self::key_access::KeyAccessTracker;

/// F2 Key-Value Store
///
/// Provides a two-tier storage architecture with automatic hot-cold separation.
/// The hot store holds frequently accessed data with optional read cache,
/// while the cold store holds less frequently accessed data.
pub struct F2Kv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Configuration
    config: F2Config,
    /// Hot store (frequently accessed data)
    hot_store: InternalStore<D>,
    /// Cold store (infrequently accessed data)
    cold_store: InternalStore<D>,
    /// Checkpoint state
    checkpoint: F2CheckpointState,
    /// Background worker active flag
    background_worker_active: AtomicBool,
    /// Background worker join handle (joined on stop/drop).
    background_worker_handle: Mutex<Option<thread::JoinHandle<()>>>,
    /// Compaction scheduled flag
    compaction_scheduled: AtomicBool,
    /// Compactor for hot store
    hot_compactor: Compactor,
    /// Compactor for cold store
    cold_compactor: Compactor,
    /// Number of active sessions
    num_active_sessions: AtomicU64,
    /// Checkpoint directory for background checkpoint operations
    checkpoint_dir: Option<std::path::PathBuf>,
    /// Key access statistics (used only for the access-frequency migration strategy).
    key_access: KeyAccessTracker,
    /// Phantom data for type parameters
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V, D> F2Kv<K, V, D>
where
    K: Key + Clone + 'static,
    V: Value + Clone + 'static,
    D: StorageDevice + 'static,
{
    /// Default page size bits
    const DEFAULT_PAGE_SIZE_BITS: u8 = 22;

    /// Create a new F2 key-value store with the given configuration
    pub fn new(config: F2Config, hot_device: D, cold_device: D) -> Result<Self, String> {
        config.validate()?;

        // Safety constraint: F2 currently supports only POD (Plain Old Data) keys/values.
        //
        // Rationale:
        // - Records are written into the log memory via `ptr::write` directly as `K/V`. If `K/V`
        //   contains heap pointers (e.g., `String`/`Vec`), it would leak heap memory and any form
        //   of persistence/recovery would result in dangling pointers.
        // - F2 does not yet define a general serialization format for records, so we must reject
        //   non-POD types to avoid data corruption.
        if std::mem::needs_drop::<K>() || std::mem::needs_drop::<V>() {
            return Err(
                "F2 currently supports only POD (Plain Old Data) keys/values (e.g. u64, i64, [u8; N]). \
                 Non-POD types (e.g. String, Vec, SpanByte) require serialization support and are not supported yet."
                    .to_string(),
            );
        }

        // Create hot store
        let hot_store = InternalStore::new(
            config.hot_store.index_size,
            config.hot_store.log_mem_size,
            Self::DEFAULT_PAGE_SIZE_BITS,
            hot_device,
            StoreType::Hot,
            None,
        )?;

        // Set max log size for hot store
        hot_store
            .max_hlog_size
            .store(config.compaction.hot_log_size_budget, Ordering::Release);

        // Create cold store
        let cold_store = InternalStore::new(
            config.cold_store.index_size,
            config.cold_store.log_mem_size,
            Self::DEFAULT_PAGE_SIZE_BITS,
            cold_device,
            StoreType::Cold,
            config.cold_store.cold_index.clone(),
        )?;

        // Set max log size for cold store
        cold_store
            .max_hlog_size
            .store(config.compaction.cold_log_size_budget, Ordering::Release);

        // Create compactors
        let hot_compaction_config = CompactionConfig::new()
            .with_max_compact_bytes(config.compaction.max_compact_size)
            .with_num_threads(config.compaction.num_threads);

        let cold_compaction_config = CompactionConfig::new()
            .with_max_compact_bytes(config.compaction.max_compact_size)
            .with_num_threads(config.compaction.num_threads);

        Ok(Self {
            config,
            hot_store,
            cold_store,
            checkpoint: F2CheckpointState::new(),
            background_worker_active: AtomicBool::new(false),
            background_worker_handle: Mutex::new(None),
            compaction_scheduled: AtomicBool::new(false),
            hot_compactor: Compactor::with_config(hot_compaction_config),
            cold_compactor: Compactor::with_config(cold_compaction_config),
            num_active_sessions: AtomicU64::new(0),
            checkpoint_dir: None,
            key_access: KeyAccessTracker::new(),
            _marker: std::marker::PhantomData,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &F2Config {
        &self.config
    }

    /// Set the checkpoint directory for background checkpoint operations
    pub fn set_checkpoint_dir(&mut self, dir: impl Into<std::path::PathBuf>) {
        self.checkpoint_dir = Some(dir.into());
    }

    /// Get the checkpoint directory
    pub fn checkpoint_dir(&self) -> Option<&std::path::Path> {
        self.checkpoint_dir.as_deref()
    }

    /// Start a new session
    ///
    /// # Returns
    /// A unique session GUID
    pub fn start_session(&self) -> Result<Uuid, Status> {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            return Err(Status::Aborted);
        }

        let guid = Uuid::new_v4();

        // Protect epoch on both stores
        // Use thread_id 0 for simplicity; in production, use actual thread IDs
        self.hot_store.epoch.protect(0);
        self.cold_store.epoch.protect(0);

        self.num_active_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(guid)
    }

    /// Continue an existing session
    ///
    /// # Arguments
    /// * `session_id` - The session GUID to continue
    ///
    /// # Returns
    /// The last serial number for this session
    pub fn continue_session(&self, _session_id: Uuid) -> Result<u64, Status> {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            return Err(Status::Aborted);
        }

        // Protect epoch on both stores
        self.hot_store.epoch.protect(0);
        self.cold_store.epoch.protect(0);

        self.num_active_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(0)
    }

    /// Stop the current session
    pub fn stop_session(&self) {
        // Wait for pending operations and checkpointing to complete
        while self.checkpoint.is_in_progress() {
            std::hint::spin_loop();
        }

        // Release epoch on both stores
        self.hot_store.epoch.unprotect(0);
        self.cold_store.epoch.unprotect(0);

        self.num_active_sessions.fetch_sub(1, Ordering::AcqRel);
    }

    /// Refresh the session - called periodically to check system state
    pub fn refresh(&self) {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            self.heavy_enter();
        }

        // Bump epoch on both stores
        self.hot_store.epoch.bump_current_epoch();
        self.cold_store.epoch.bump_current_epoch();
    }

    /// Complete pending asynchronous operations
    ///
    /// # Arguments
    /// * `wait` - If true, wait for all pending operations to complete
    ///
    /// # Returns
    /// true if all operations are complete
    pub fn complete_pending(&self, wait: bool) -> bool {
        // Refresh to process any pending epoch actions.
        self.refresh();

        // In this simplified implementation, operations are synchronous.
        // `wait` is kept for API compatibility.
        let _ = wait;
        true
    }

    /// Wait for pending compactions to complete
    pub fn complete_pending_compactions(&self) {
        while self.compaction_scheduled.load(Ordering::Acquire) {
            if self.hot_store.epoch.is_protected(0) {
                self.complete_pending(false);
            }
            std::hint::spin_loop();
        }
    }

    /// Start a checkpoint
    ///
    /// # Arguments
    /// * `lazy` - If true, wait for compaction before cold store checkpoint
    ///
    /// # Returns
    /// The checkpoint token
    pub fn checkpoint(&mut self, lazy: bool) -> Result<Uuid, Status> {
        // Try to start checkpoint
        let result = self.checkpoint.phase.compare_exchange(
            F2CheckpointPhase::Rest,
            F2CheckpointPhase::HotStoreCheckpoint,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if result.is_err() {
            return Err(Status::Aborted);
        }

        let token = Uuid::new_v4();
        let num_sessions = self.num_active_sessions.load(Ordering::Acquire) as u32;
        self.checkpoint.initialize(token, num_sessions.max(1));

        // Request hot store checkpoint
        self.checkpoint
            .hot_store_status
            .store(StoreCheckpointStatus::Requested, Ordering::Release);

        let _ = lazy; // Used by background worker

        Ok(token)
    }

    /// Recover from a checkpoint
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory containing checkpoints
    /// * `token` - The checkpoint token to recover from
    pub fn recover(&mut self, checkpoint_dir: &Path, token: Uuid) -> Result<u32, Status> {
        let result = self.checkpoint.phase.compare_exchange(
            F2CheckpointPhase::Rest,
            F2CheckpointPhase::Recover,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if result.is_err() {
            return Err(Status::Aborted);
        }

        // Create checkpoint directory paths
        let cp_dir = checkpoint_dir.join(token.to_string());
        let hot_dir = cp_dir.join("hot");
        let cold_dir = cp_dir.join("cold");

        // Verify that the checkpoint directory exists
        if !cp_dir.exists() {
            self.checkpoint
                .phase
                .store(F2CheckpointPhase::Rest, Ordering::Release);
            return Err(Status::NotFound);
        }

        // At least one of hot or cold store must exist for a valid checkpoint
        let hot_exists = hot_dir.exists();
        let cold_exists = cold_dir.exists();

        if !hot_exists && !cold_exists {
            // Neither store checkpoint exists - this is not a valid checkpoint
            self.checkpoint
                .phase
                .store(F2CheckpointPhase::Rest, Ordering::Release);
            return Err(Status::NotFound);
        }

        let mut version = 0u32;

        // Recover hot store
        if hot_exists {
            // Recover hot store hash index
            if let Err(_e) = self.hot_store.hash_index.recover(&hot_dir, None) {
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
                return Err(Status::Corruption);
            }

            // Recover hot store hybrid log
            unsafe {
                if let Err(_e) = self.hot_store.hlog_mut().recover(&hot_dir, None) {
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                    return Err(Status::Corruption);
                }
            }

            // Get version from hot store log metadata
            if let Ok(log_meta) =
                crate::checkpoint::LogMetadata::read_from_file(&hot_dir.join("log.meta"))
            {
                version = log_meta.version;
            }
        }

        // Recover cold store
        if cold_exists {
            // Recover cold store hash index
            if let Err(_e) = self.cold_store.hash_index.recover(&cold_dir, None) {
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
                return Err(Status::Corruption);
            }

            // Recover cold store hybrid log
            unsafe {
                if let Err(_e) = self.cold_store.hlog_mut().recover(&cold_dir, None) {
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                    return Err(Status::Corruption);
                }
            }

            // Get version from cold store log metadata if not already set from hot store
            // This handles the case where only cold store checkpoint exists
            if version == 0 {
                if let Ok(log_meta) =
                    crate::checkpoint::LogMetadata::read_from_file(&cold_dir.join("log.meta"))
                {
                    version = log_meta.version;
                }
            }
        }

        // Update checkpoint state with recovered version so subsequent checkpoints
        // continue from the correct version number
        self.checkpoint.set_version(version);

        // Move back to REST phase
        self.checkpoint
            .phase
            .store(F2CheckpointPhase::Rest, Ordering::Release);

        Ok(version)
    }

    /// Save checkpoint to disk
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory to save checkpoint files
    /// * `token` - The checkpoint token
    pub fn save_checkpoint(&self, checkpoint_dir: &Path, token: Uuid) -> Result<(), Status> {
        let result = (|| {
            // Create checkpoint directories
            let cp_dir = checkpoint_dir.join(token.to_string());
            let hot_dir = cp_dir.join("hot");
            let cold_dir = cp_dir.join("cold");

            std::fs::create_dir_all(&hot_dir).map_err(|_| Status::Corruption)?;
            std::fs::create_dir_all(&cold_dir).map_err(|_| Status::Corruption)?;

            // Get the checkpoint version
            let checkpoint_version = self.checkpoint.version();

            // Checkpoint hot store index
            let _hot_index_meta = self
                .hot_store
                .hash_index
                .checkpoint(&hot_dir, token)
                .map_err(|_| Status::Corruption)?;

            // Checkpoint hot store hybrid log (writes both metadata and snapshot)
            let _hot_log_meta = self
                .hot_store
                .hlog()
                .checkpoint(&hot_dir, token, checkpoint_version)
                .map_err(|_| Status::Corruption)?;

            // Checkpoint cold store index
            let _cold_index_meta = self
                .cold_store
                .hash_index
                .checkpoint(&cold_dir, token)
                .map_err(|_| Status::Corruption)?;

            // Checkpoint cold store hybrid log (writes both metadata and snapshot)
            let _cold_log_meta = self
                .cold_store
                .hlog()
                .checkpoint(&cold_dir, token, checkpoint_version)
                .map_err(|_| Status::Corruption)?;

            Ok(())
        })();

        // Synchronous path: after saving finishes, the checkpoint state must return to REST.
        if result.is_ok() {
            self.checkpoint
                .hot_store_status
                .store(StoreCheckpointStatus::Finished, Ordering::Release);
            self.checkpoint
                .cold_store_status
                .store(StoreCheckpointStatus::Finished, Ordering::Release);
        } else {
            self.checkpoint
                .hot_store_status
                .store(StoreCheckpointStatus::Failed, Ordering::Release);
            self.checkpoint
                .cold_store_status
                .store(StoreCheckpointStatus::Failed, Ordering::Release);
        }
        self.checkpoint
            .phase
            .store(F2CheckpointPhase::Rest, Ordering::Release);

        result
    }

    /// Get the total size of both stores
    pub fn size(&self) -> u64 {
        self.hot_store.size() + self.cold_store.size()
    }

    /// Get the hot store size
    pub fn hot_store_size(&self) -> u64 {
        self.hot_store.size()
    }

    /// Get the cold store size
    pub fn cold_store_size(&self) -> u64 {
        self.cold_store.size()
    }

    /// Get the number of active sessions
    pub fn num_active_sessions(&self) -> u32 {
        self.num_active_sessions.load(Ordering::Acquire) as u32
    }

    /// Check if automatic compaction is scheduled
    pub fn is_compaction_scheduled(&self) -> bool {
        self.compaction_scheduled.load(Ordering::Acquire)
    }

    /// Handle heavy enter for checkpoint phases
    fn heavy_enter(&self) {
        let phase = self.checkpoint.phase.load(Ordering::Acquire);

        if phase == F2CheckpointPhase::ColdStoreCheckpoint {
            let status = self.checkpoint.cold_store_status.load(Ordering::Acquire);
            if !status.is_done() {
                // All done - can move to REST
                // Note: Full implementation would issue callbacks here
            }
        }
    }

    /// Start the background worker thread
    pub fn start_background_worker(self: &Arc<Self>) {
        let mut handle_guard = self.background_worker_handle.lock();

        if let Some(handle) = handle_guard.as_ref() {
            if !handle.is_finished() {
                return;
            }
        }

        if let Some(handle) = handle_guard.take() {
            self.background_worker_active
                .store(false, Ordering::Release);
            let _ = handle.join();
        }

        if self.background_worker_active.swap(true, Ordering::AcqRel) {
            return;
        }

        let weak = Arc::downgrade(self);
        *handle_guard = Some(thread::spawn(move || loop {
            let Some(f2) = weak.upgrade() else {
                break;
            };

            if !f2.background_worker_active.load(Ordering::Acquire) {
                break;
            }

            f2.background_worker_tick();
            thread::park_timeout(f2.config.compaction.check_interval);
        }));
    }

    /// Background worker single iteration.
    fn background_worker_tick(&self) {
        struct ClearOnDrop<'a>(&'a AtomicBool);

        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        self.compaction_scheduled.store(true, Ordering::Release);
        let _clear = ClearOnDrop(&self.compaction_scheduled);

        // Check hot store checkpoint
        if self.checkpoint.hot_store_status.load(Ordering::Acquire)
            == StoreCheckpointStatus::Requested
        {
            // Issue hot store checkpoint
            self.checkpoint
                .hot_store_status
                .store(StoreCheckpointStatus::Active, Ordering::Release);

            // Actually checkpoint hot store - index first
            let token = self.checkpoint.token();
            let checkpoint_version = self.checkpoint.version();
            let checkpoint_success = if let Some(cp_dir) = self.checkpoint_dir.as_ref() {
                let hot_dir = cp_dir.join(token.to_string()).join("hot");
                let dir_created = std::fs::create_dir_all(&hot_dir).is_ok();

                if dir_created {
                    // Checkpoint hot store index
                    let index_ok = self
                        .hot_store
                        .hash_index
                        .checkpoint(&hot_dir, token)
                        .is_ok();

                    // Checkpoint hot store hybrid log (writes both metadata and snapshot)
                    let log_ok = self
                        .hot_store
                        .hlog()
                        .checkpoint(&hot_dir, token, checkpoint_version)
                        .is_ok();

                    index_ok && log_ok
                } else {
                    false
                }
            } else {
                // No checkpoint directory configured - cannot checkpoint without a directory.
                false
            };

            if checkpoint_success {
                self.checkpoint
                    .hot_store_status
                    .store(StoreCheckpointStatus::Finished, Ordering::Release);

                // Move to cold store checkpoint phase
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::ColdStoreCheckpoint, Ordering::Release);
                self.checkpoint
                    .cold_store_status
                    .store(StoreCheckpointStatus::Requested, Ordering::Release);
            } else {
                // Checkpoint failed - mark as failed and reset phase
                self.checkpoint
                    .hot_store_status
                    .store(StoreCheckpointStatus::Failed, Ordering::Release);
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
            }
        }

        // Check cold store checkpoint
        if self.checkpoint.cold_store_status.load(Ordering::Acquire)
            == StoreCheckpointStatus::Requested
        {
            // Issue cold store checkpoint
            self.checkpoint
                .cold_store_status
                .store(StoreCheckpointStatus::Active, Ordering::Release);

            // Actually checkpoint cold store
            let token = self.checkpoint.token();
            let checkpoint_version = self.checkpoint.version();
            let checkpoint_success = if let Some(cp_dir) = self.checkpoint_dir.as_ref() {
                let cold_dir = cp_dir.join(token.to_string()).join("cold");
                let dir_created = std::fs::create_dir_all(&cold_dir).is_ok();

                if dir_created {
                    // Checkpoint cold store index
                    let index_ok = self
                        .cold_store
                        .hash_index
                        .checkpoint(&cold_dir, token)
                        .is_ok();

                    // Checkpoint cold store hybrid log (writes both metadata and snapshot)
                    let log_ok = self
                        .cold_store
                        .hlog()
                        .checkpoint(&cold_dir, token, checkpoint_version)
                        .is_ok();

                    index_ok && log_ok
                } else {
                    false
                }
            } else {
                // No checkpoint directory configured - cannot checkpoint without a directory.
                false
            };

            if checkpoint_success {
                self.checkpoint
                    .cold_store_status
                    .store(StoreCheckpointStatus::Finished, Ordering::Release);

                // Move back to REST
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
            } else {
                // Checkpoint failed - mark as failed and reset phase
                self.checkpoint
                    .cold_store_status
                    .store(StoreCheckpointStatus::Failed, Ordering::Release);
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
            }
        }

        // Hot-cold compaction
        if let Some(until_addr) = self.should_compact_hot_log() {
            let _ = self.compact_hot_log(until_addr);
        }

        // Cold-cold compaction
        if let Some(until_addr) = self.should_compact_cold_log() {
            let _ = self.compact_cold_log(until_addr);
        }
    }

    /// Stop the background worker thread
    pub fn stop_background_worker(&self) {
        let handle = {
            let mut handle = self.background_worker_handle.lock();
            self.background_worker_active
                .store(false, Ordering::Release);
            handle.take()
        };
        if let Some(handle) = handle {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }

    /// Get compaction configuration
    pub fn compaction_config(&self) -> &F2CompactionConfig {
        &self.config.compaction
    }

    /// Get hot store statistics
    pub fn hot_store_stats(&self) -> StoreStats {
        StoreStats {
            size: self.hot_store.size(),
            begin_address: self.hot_store.begin_address(),
            tail_address: self.hot_store.tail_address(),
            safe_read_only_address: self.hot_store.safe_read_only_address(),
        }
    }

    /// Get cold store statistics
    pub fn cold_store_stats(&self) -> StoreStats {
        StoreStats {
            size: self.cold_store.size(),
            begin_address: self.cold_store.begin_address(),
            tail_address: self.cold_store.tail_address(),
            safe_read_only_address: self.cold_store.safe_read_only_address(),
        }
    }
}

impl<K, V, D> Drop for F2Kv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    fn drop(&mut self) {
        // Wait for operations to complete
        while self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            std::hint::spin_loop();
        }

        // Stop background worker
        self.stop_background_worker();
    }
}
