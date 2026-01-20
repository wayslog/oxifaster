use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::cache::{ReadCache, ReadCacheConfig};
use crate::checkpoint::{
    create_checkpoint_directory, delta_log_path, delta_metadata_path, CheckpointState,
    CheckpointToken, CheckpointType, DeltaLogMetadata, IndexMetadata, LogMetadata, RecoveryState,
    SessionState,
};
use crate::codec::{PersistKey, PersistValue};
use crate::compaction::{CompactionConfig, Compactor};
use crate::delta_log::{DeltaLog, DeltaLogConfig};
use crate::device::StorageDevice;
use crate::epoch::try_get_thread_id;
use crate::epoch::LightEpoch;
use crate::index::{MemHashIndex, MemHashIndexConfig};
use crate::stats::StatsCollector;
use crate::store::pending_io::PendingIoManager;
use crate::store::state_transitions::{Action, AtomicSystemState, Phase, SystemState};
use crate::store::ThreadContext;

use super::cpr::{
    ActiveCheckpoint, ActiveCheckpointStart, CheckpointDurability, LogCheckpointBackend,
};
use super::{CheckpointKind, FasterKv, FasterKvConfig, ReadCacheOps};

fn fsync_dir_best_effort(dir: &Path) {
    if let Ok(file) = std::fs::File::open(dir) {
        let _ = file.sync_all();
    }
}

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Create a full checkpoint (index + log) in `checkpoint_dir`.
    pub fn checkpoint(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointFull)
    }

    /// Create an index-only checkpoint in `checkpoint_dir`.
    pub fn checkpoint_index(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointIndex)
    }

    /// Create a hybrid-log-only checkpoint in `checkpoint_dir`.
    pub fn checkpoint_hybrid_log(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointHybridLog)
    }

    /// Create an incremental checkpoint, falling back to a full checkpoint when needed.
    pub fn checkpoint_incremental(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        let prev_snapshot = self.last_snapshot_checkpoint.read();
        let can_use_incremental = prev_snapshot
            .as_ref()
            .is_some_and(|state| state.checkpoint_type.uses_snapshot_file());
        drop(prev_snapshot);

        if can_use_incremental {
            self.checkpoint_with_action(checkpoint_dir, Action::CheckpointIncremental)
        } else {
            self.checkpoint_full_snapshot(checkpoint_dir)
        }
    }

    /// Force a full snapshot checkpoint and record it as the incremental base.
    pub fn checkpoint_full_snapshot(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        let token = Uuid::new_v4();
        self.checkpoint_with_action_internal_with_backend(
            checkpoint_dir,
            Action::CheckpointFull,
            token,
            Some(LogCheckpointBackend::Snapshot),
        )
    }

    fn checkpoint_with_action(
        &self,
        checkpoint_dir: &Path,
        action: Action,
    ) -> io::Result<CheckpointToken> {
        let token = Uuid::new_v4();
        self.checkpoint_with_action_internal(checkpoint_dir, action, token)
    }

    /// Internal entry point used by `checkpoint_with_action` and unit tests (deterministic token).
    pub(super) fn checkpoint_with_action_internal(
        &self,
        checkpoint_dir: &Path,
        action: Action,
        token: CheckpointToken,
    ) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action_internal_with_backend(checkpoint_dir, action, token, None)
    }

    fn checkpoint_with_action_internal_with_backend(
        &self,
        checkpoint_dir: &Path,
        action: Action,
        token: CheckpointToken,
        backend_override: Option<LogCheckpointBackend>,
    ) -> io::Result<CheckpointToken> {
        let durability = self.checkpoint_durability();
        let start_result = self.system_state.try_start_action(action);
        let start_state = match start_result {
            Ok(prev) => prev,
            Err(current_state) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "Cannot start checkpoint: action {:?} phase {:?} already in progress",
                        current_state.action, current_state.phase
                    ),
                ));
            }
        };
        let start_version = start_state.version;
        let started_at = Instant::now();

        if self.stats_collector.is_enabled() {
            self.stats_collector
                .store_stats
                .operational
                .record_checkpoint_started();
        }
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(action = ?action, token = %token, "checkpoint start");
        }

        let result = create_checkpoint_directory(checkpoint_dir, token).and_then(|cp_dir| {
            // Make the newly created checkpoint directory entry durable.
            if durability == CheckpointDurability::FsyncOnCheckpoint {
                fsync_dir_best_effort(checkpoint_dir);
            }
            self.execute_checkpoint_state_machine(&cp_dir, token, action, backend_override)
        });

        if result.is_err() {
            self.system_state.store(
                SystemState::rest(start_version),
                std::sync::atomic::Ordering::Release,
            );
        }

        if self.stats_collector.is_enabled() {
            if result.is_ok() {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_checkpoint_completed();
            } else {
                self.stats_collector
                    .store_stats
                    .operational
                    .record_checkpoint_failed();
            }
        }
        if tracing::enabled!(tracing::Level::INFO) {
            let duration_ms = started_at.elapsed().as_millis();
            if let Err(ref err) = result {
                tracing::warn!(
                    action = ?action,
                    token = %token,
                    duration_ms,
                    error = %err,
                    "checkpoint failed"
                );
            } else {
                tracing::info!(
                    action = ?action,
                    token = %token,
                    duration_ms,
                    "checkpoint completed"
                );
            }
        }

        result.map(|_| token)
    }

    fn execute_checkpoint_state_machine(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        action: Action,
        backend_override: Option<LogCheckpointBackend>,
    ) -> io::Result<()> {
        let is_incremental = action == Action::CheckpointIncremental;

        let driver_thread_id = try_get_thread_id().ok_or_else(|| {
            io::Error::other("Checkpoint requires a registered LightEpoch thread id")
        })?;

        struct CheckpointCleanup<'a, K, V, D>
        where
            K: PersistKey,
            V: PersistValue,
            D: StorageDevice,
        {
            store: &'a FasterKv<K, V, D>,
            thread_id: usize,
            unregister_thread: bool,
            clear_cpr: bool,
        }

        impl<'a, K, V, D> Drop for CheckpointCleanup<'a, K, V, D>
        where
            K: PersistKey,
            V: PersistValue,
            D: StorageDevice,
        {
            fn drop(&mut self) {
                if self.clear_cpr {
                    self.store.cpr.clear();
                }
                self.store.epoch().reentrant_unprotect(self.thread_id);
                if self.unregister_thread {
                    self.store.unregister_thread(self.thread_id);
                }
            }
        }

        let driver_bit = 1u128
            .checked_shl(driver_thread_id as u32)
            .ok_or_else(|| io::Error::other("Checkpoint requires driver thread_id < 128"))?;
        let already_active = (self.active_threads_snapshot() & driver_bit) != 0;
        if !already_active {
            self.register_thread(driver_thread_id);
        }
        self.epoch().reentrant_protect(driver_thread_id);
        let mut cleanup = CheckpointCleanup {
            store: self,
            thread_id: driver_thread_id,
            unregister_thread: !already_active,
            clear_cpr: false,
        };

        let backend = if is_incremental {
            LogCheckpointBackend::Snapshot
        } else if let Some(backend) = backend_override {
            backend
        } else {
            LogCheckpointBackend::from(
                self.default_log_checkpoint_backend
                    .load(std::sync::atomic::Ordering::Acquire) as u8,
            )
        };
        let durability = CheckpointDurability::from(
            self.default_checkpoint_durability
                .load(std::sync::atomic::Ordering::Acquire) as u8,
        );

        let participants = self.active_threads_snapshot() | driver_bit;
        self.cpr.start(ActiveCheckpoint::new(ActiveCheckpointStart {
            token,
            dir: cp_dir.to_path_buf(),
            action,
            backend,
            durability,
            driver_thread_id,
            participants,
            version: self.system_state.version(),
        }));
        cleanup.clear_cpr = true;

        let mut driver_ctx = ThreadContext::new(driver_thread_id);

        loop {
            let current_state = self.system_state.load(std::sync::atomic::Ordering::Acquire);
            self.cpr_refresh(&mut driver_ctx);

            if current_state.phase == Phase::Rest {
                break;
            }

            let is_driver = self
                .cpr
                .with_active(|active| active.driver_thread_id == driver_thread_id)
                .unwrap_or(false);
            if !is_driver {
                break;
            }

            // Drive current phase (driver thread only).
            match current_state.phase {
                Phase::PrepIndexChkpt => {
                    // Rendezvous: wait for all participant threads to observe the phase at an
                    // operation boundary (via refresh/operation-entry hook).
                    let _ = self.cpr.with_active_mut(|active| {
                        active.set_phase(current_state.phase, current_state.version);
                    });
                    let barrier_complete = self
                        .cpr
                        .with_active(|active| active.barrier_complete())
                        .unwrap_or(false);
                    if barrier_complete {
                        let _ = self.system_state.try_advance();
                    }
                }

                Phase::IndexChkpt => {
                    // Driver writes the index checkpoint (other threads continue running).
                    let meta = self.handle_index_checkpoint(cp_dir, token)?;
                    let _ = self
                        .cpr
                        .with_active_mut(|active| active.index_metadata = Some(meta));

                    if durability == CheckpointDurability::FsyncOnCheckpoint {
                        let index_dat = cp_dir.join("index.dat");
                        let index_meta = cp_dir.join("index.meta");
                        std::fs::File::open(&index_dat).and_then(|f| f.sync_all())?;
                        std::fs::File::open(&index_meta).and_then(|f| f.sync_all())?;
                        fsync_dir_best_effort(cp_dir);
                    }

                    let _ = self.system_state.try_advance();
                }

                Phase::Prepare => {
                    // Rendezvous: ensure all participant threads reach a safe operation boundary
                    // before we cut the prefix boundary and advance to IN_PROGRESS.
                    let _ = self.cpr.with_active_mut(|active| {
                        active.set_phase(current_state.phase, current_state.version);
                    });

                    let barrier_complete = self
                        .cpr
                        .with_active(|active| active.barrier_complete())
                        .unwrap_or(false);
                    if barrier_complete {
                        // Capture a coherent view for this checkpoint.
                        let session_states = self.get_session_states();
                        let (begin_address, final_address) = unsafe {
                            let hlog = &*self.hlog.get();
                            (hlog.get_begin_address(), hlog.get_tail_address())
                        };

                        let _ = self.cpr.with_active_mut(|active| {
                            active.begin_address = begin_address;
                            active.final_address = final_address;
                            active.session_states = session_states;
                        });

                        let _ = self.system_state.try_advance();
                    }
                }

                Phase::InProgress => {
                    // Wait for all participant threads to perform the execution-context swap to the
                    // new version.
                    let _ = self.cpr.with_active_mut(|active| {
                        active.set_phase(current_state.phase, current_state.version);
                    });
                    let barrier_complete = self
                        .cpr
                        .with_active(|active| active.barrier_complete())
                        .unwrap_or(false);
                    if barrier_complete {
                        let _ = self.system_state.try_advance();
                    }
                }

                Phase::WaitPending => {
                    // WAIT_PENDING semantics: wait only for the previous execution context to drain
                    // (pending I/Os + retry_requests).
                    let _ = self.cpr.with_active_mut(|active| {
                        active.set_phase(current_state.phase, current_state.version);
                    });
                    let barrier_complete = self
                        .cpr
                        .with_active(|active| active.barrier_complete())
                        .unwrap_or(false);
                    if barrier_complete {
                        let _ = self.system_state.try_advance();
                    }
                }

                Phase::WaitFlush => {
                    // Wait for the checkpoint backend to meet its durability boundary.
                    let _ = self.cpr.with_active_mut(|active| {
                        active.set_phase(current_state.phase, current_state.version);
                    });

                    if is_incremental {
                        self.drive_incremental_wait_flush(token, current_state.version)?;
                    } else {
                        self.drive_wait_flush(token, current_state.version)?;
                    }

                    let barrier_complete = self
                        .cpr
                        .with_active(|active| active.barrier_complete())
                        .unwrap_or(false);
                    if barrier_complete {
                        let _ = self.system_state.try_advance();
                    }
                }

                Phase::PersistenceCallback => {
                    let (index_meta, log_meta) = self
                        .cpr
                        .with_active(|active| {
                            (active.index_metadata.clone(), active.log_metadata.clone())
                        })
                        .unwrap_or((None, None));

                    // Invoke persistence callback (currently a no-op).
                    self.handle_persistence_callback(
                        cp_dir,
                        token,
                        index_meta.as_ref(),
                        log_meta.as_ref(),
                    )?;

                    // Save checkpoint state for future incremental checkpoints.
                    if !is_incremental && backend == LogCheckpointBackend::Snapshot {
                        if let Some(log_meta) = log_meta {
                            let mut state = CheckpointState::new(CheckpointType::Snapshot);
                            state.log_metadata = log_meta;
                            if let Some(index_meta) = index_meta {
                                state.index_metadata = index_meta;
                            }
                            state.index_metadata.token = token;
                            state.log_metadata.token = token;
                            *self.last_snapshot_checkpoint.write() = Some(state);
                        }
                    }

                    let _ = self.system_state.try_advance();
                }

                Phase::Rest => {
                    break;
                }

                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Unexpected phase {:?} during checkpoint",
                            current_state.phase
                        ),
                    ));
                }
            }

            std::thread::yield_now();
        }

        Ok(())
    }

    fn handle_prep_index_checkpoint(&self) -> io::Result<()> {
        // In a multi-threaded implementation, this would:
        // 1. Signal all threads that index checkpoint is starting
        // 2. Wait for threads to reach a safe point
        // For now, we assume single-threaded checkpoint
        Ok(())
    }

    fn handle_index_checkpoint(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
    ) -> io::Result<IndexMetadata> {
        self.hash_index.checkpoint(cp_dir, token)
    }

    fn drive_wait_flush(&self, token: CheckpointToken, version: u32) -> io::Result<()> {
        let (dir, begin, final_address, session_states, backend, durability, already_done) = self
            .cpr
            .with_active(|active| {
                (
                    active.dir.clone(),
                    active.begin_address,
                    active.final_address,
                    active.session_states.clone(),
                    active.backend,
                    active.durability,
                    active.snapshot_written,
                )
            })
            .ok_or_else(|| io::Error::other("No active checkpoint"))?;

        let meta_path = dir.join("log.meta");
        let snapshot_path = dir.join("log.snapshot");

        let write_log_meta = |use_snapshot_file: bool| -> io::Result<LogMetadata> {
            let flushed_until = unsafe { (*self.hlog.get()).get_flushed_until_address() };
            let mut meta = unsafe {
                (*self.hlog.get()).checkpoint_metadata_at(
                    token,
                    version,
                    begin,
                    final_address,
                    flushed_until,
                    use_snapshot_file,
                )
            };
            meta.session_states = session_states.clone();
            meta.num_threads = meta.session_states.len() as u32;
            meta.write_to_file(&meta_path)?;
            Ok(meta)
        };

        let fsync_checkpoint_artifacts = |include_snapshot: bool| -> io::Result<()> {
            if durability != CheckpointDurability::FsyncOnCheckpoint {
                return Ok(());
            }

            if include_snapshot {
                std::fs::File::open(&snapshot_path).and_then(|f| f.sync_all())?;
            }
            std::fs::File::open(&meta_path).and_then(|f| f.sync_all())?;
            fsync_dir_best_effort(&dir);
            Ok(())
        };

        match backend {
            LogCheckpointBackend::Snapshot => {
                if already_done {
                    return Ok(());
                }

                unsafe {
                    (*self.hlog.get()).write_log_snapshot(&snapshot_path, begin, final_address)?;
                }

                let meta = write_log_meta(true)?;
                fsync_checkpoint_artifacts(true)?;

                let _ = self.cpr.with_active_mut(|active| {
                    active.log_metadata = Some(meta);
                    active.snapshot_written = true;
                });
            }
            LogCheckpointBackend::FoldOver => {
                // Fold-over uses the main log device: ensure the prefix is flushed to the device.
                unsafe {
                    (*self.hlog.get()).flush_until(final_address)?;
                }

                let meta = write_log_meta(false)?;
                fsync_checkpoint_artifacts(false)?;

                let _ = self.cpr.with_active_mut(|active| {
                    active.log_metadata = Some(meta);
                });
            }
        }

        Ok(())
    }

    fn handle_incremental_checkpoint(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        version: u32,
    ) -> io::Result<LogMetadata> {
        let prev_snapshot = self.last_snapshot_checkpoint.read();
        let prev_state = prev_snapshot.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "No previous snapshot for incremental checkpoint",
            )
        })?;
        if !prev_state.checkpoint_type.uses_snapshot_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Incremental checkpoint requires a snapshot base checkpoint",
            ));
        }

        let prev_token = prev_state.token();
        let prev_final_address = prev_state.log_metadata.final_address;

        drop(prev_snapshot);

        let session_states = self.get_session_states();

        let mut metadata = unsafe { (*self.hlog.get()).checkpoint_metadata(token, version, true) };
        metadata.session_states = session_states;
        metadata.num_threads = metadata.session_states.len() as u32;
        metadata.is_incremental = true;
        metadata.prev_snapshot_token = Some(prev_token);
        metadata.start_logical_address = prev_final_address;

        let meta_path = cp_dir.join("log.meta");
        metadata.write_to_file(&meta_path)?;

        Ok(metadata)
    }

    fn drive_incremental_wait_flush(&self, token: CheckpointToken, version: u32) -> io::Result<()> {
        let (dir, begin, final_address, session_states, durability, already_done) = self
            .cpr
            .with_active(|active| {
                (
                    active.dir.clone(),
                    active.begin_address,
                    active.final_address,
                    active.session_states.clone(),
                    active.durability,
                    active.snapshot_written,
                )
            })
            .ok_or_else(|| io::Error::other("No active checkpoint"))?;

        if already_done {
            return Ok(());
        }

        let prev_snapshot = self.last_snapshot_checkpoint.read();
        let prev_state = prev_snapshot.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "No previous snapshot for incremental checkpoint",
            )
        })?;

        let prev_token = prev_state.token();
        let prev_final_address = prev_state.log_metadata.final_address;
        drop(prev_snapshot);

        let flushed_address = unsafe { (*self.hlog.get()).get_flushed_until_address() };

        let delta_path = delta_log_path(&dir, 0);
        let delta_device = Arc::new(crate::device::FileSystemDisk::single_file(&delta_path)?);
        let delta_config = DeltaLogConfig::new(22); // 4MB pages
        let delta_log = DeltaLog::new(delta_device.clone(), delta_config, 0);

        let num_entries = unsafe {
            (*self.hlog.get()).flush_delta_to_device(
                flushed_address,
                final_address,
                prev_final_address,
                version,
                &delta_log,
            )?
        };

        let delta_meta_path = delta_metadata_path(&dir);
        let delta_meta = DeltaLogMetadata {
            token: token.to_string(),
            base_snapshot_token: prev_token.to_string(),
            version,
            delta_tail_address: delta_log.tail_address(),
            prev_snapshot_final_address: prev_final_address.control(),
            current_final_address: final_address.control(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            num_entries,
        };
        delta_meta.write_to_file(&delta_meta_path)?;

        let flushed_until = unsafe { (*self.hlog.get()).get_flushed_until_address() };
        let mut meta = unsafe {
            (*self.hlog.get()).checkpoint_metadata_at(
                token,
                version,
                begin,
                final_address,
                flushed_until,
                true,
            )
        };
        meta.session_states = session_states;
        meta.num_threads = meta.session_states.len() as u32;
        meta.is_incremental = true;
        meta.prev_snapshot_token = Some(prev_token);
        meta.start_logical_address = prev_final_address;

        let meta_path = dir.join("log.meta");
        meta.write_to_file(&meta_path)?;

        if durability == CheckpointDurability::FsyncOnCheckpoint {
            std::fs::File::open(&delta_path).and_then(|f| f.sync_all())?;
            std::fs::File::open(&delta_meta_path).and_then(|f| f.sync_all())?;
            std::fs::File::open(&meta_path).and_then(|f| f.sync_all())?;
            fsync_dir_best_effort(&dir);
        }

        let _ = self.cpr.with_active_mut(|active| {
            active.log_metadata = Some(meta);
            active.snapshot_written = true;
        });

        Ok(())
    }

    fn handle_persistence_callback(
        &self,
        _cp_dir: &Path,
        _token: CheckpointToken,
        _index_metadata: Option<&IndexMetadata>,
        _log_metadata: Option<&LogMetadata>,
    ) -> io::Result<()> {
        // Invoke user persistence callback if registered
        // For now, this is a no-op
        Ok(())
    }

    /// Create a checkpoint with an explicit `CheckpointType` (deprecated; prefer typed helpers).
    pub fn checkpoint_with_type(
        &self,
        checkpoint_dir: &Path,
        checkpoint_type: CheckpointType,
    ) -> io::Result<CheckpointToken> {
        let action = match checkpoint_type {
            CheckpointType::Full | CheckpointType::Snapshot | CheckpointType::FoldOver => {
                Action::CheckpointFull
            }
            CheckpointType::IndexOnly => Action::CheckpointIndex,
            CheckpointType::HybridLogOnly => Action::CheckpointHybridLog,
            CheckpointType::IncrementalSnapshot => Action::CheckpointFull,
        };
        self.checkpoint_with_action(checkpoint_dir, action)
    }

    /// Load `(IndexMetadata, LogMetadata)` for an existing checkpoint.
    pub fn get_checkpoint_info(
        checkpoint_dir: &Path,
        token: CheckpointToken,
    ) -> io::Result<(IndexMetadata, LogMetadata)> {
        let cp_dir = checkpoint_dir.join(token.to_string());

        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;

        Ok((index_meta, log_meta))
    }

    /// Recover a store from a full checkpoint.
    pub fn recover(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
    ) -> io::Result<Self> {
        Self::recover_with_compaction_config(
            checkpoint_dir,
            token,
            config,
            device,
            CompactionConfig::default(),
        )
    }

    /// Recover a store from a full checkpoint with a custom compaction configuration.
    pub fn recover_with_compaction_config(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
    ) -> io::Result<Self> {
        Self::recover_with_full_config(
            checkpoint_dir,
            token,
            config,
            device,
            compaction_config,
            None,
        )
    }

    /// Recover a store from a full checkpoint with a read cache enabled.
    pub fn recover_with_read_cache(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        cache_config: ReadCacheConfig,
    ) -> io::Result<Self> {
        Self::recover_with_full_config(
            checkpoint_dir,
            token,
            config,
            device,
            CompactionConfig::default(),
            Some(cache_config),
        )
    }

    /// Recover a store from a full checkpoint with full configuration.
    pub fn recover_with_full_config(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
        compaction_config: CompactionConfig,
        cache_config: Option<ReadCacheConfig>,
    ) -> io::Result<Self> {
        let started_at = Instant::now();
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());
        let stats_collector = StatsCollector::with_defaults();
        if stats_collector.is_enabled() {
            stats_collector
                .store_stats
                .operational
                .record_recovery_started();
        }
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(token = %token, "recovery start");
        }
        let record_failure = |err: io::Error, context: &'static str| {
            if stats_collector.is_enabled() {
                stats_collector
                    .store_stats
                    .operational
                    .record_recovery_failed();
            }
            if tracing::enabled!(tracing::Level::WARN) {
                tracing::warn!(token = %token, context, error = %err, "recovery failed");
            }
            err
        };

        let mut recovery_state = RecoveryState::new();
        let status = recovery_state.start_recovery(checkpoint_dir, token);
        if status != crate::Status::Ok {
            return Err(record_failure(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    recovery_state
                        .error_message()
                        .unwrap_or("Unknown recovery error"),
                ),
                "start_recovery",
            ));
        }

        let epoch = Arc::new(LightEpoch::new());

        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))
            .map_err(|err| record_failure(err, "read_index_meta"))?;
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))
            .map_err(|err| record_failure(err, "read_log_meta"))?;

        let mut hash_index = MemHashIndex::new();
        hash_index
            .recover(&cp_dir, Some(&index_meta))
            .map_err(|err| record_failure(err, "recover_index"))?;
        recovery_state.index_recovered();

        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let mut hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        hlog.recover(&cp_dir, Some(&log_meta))
            .map_err(|err| record_failure(err, "recover_log"))?;
        recovery_state.log_recovered();

        let system_state = AtomicSystemState::new(SystemState::rest(log_meta.version));

        let compactor = Compactor::with_config(compaction_config);

        let read_cache: Option<Arc<dyn ReadCacheOps<K, V>>> = cache_config
            .map(|cfg| Arc::new(ReadCache::<K, V>::new(cfg)) as Arc<dyn ReadCacheOps<K, V>>);
        let pending_io = PendingIoManager::new(device.clone(), hlog.page_size());

        let mut session_registry = HashMap::new();
        for session_state in &log_meta.session_states {
            session_registry.insert(session_state.guid, session_state.clone());
        }
        let next_session_id = log_meta.num_threads;

        recovery_state.complete_recovery();

        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(
                token = %token,
                duration_ms = started_at.elapsed().as_millis(),
                "recovery completed"
            );
        }

        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(next_session_id),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector,
            session_registry: RwLock::new(session_registry),
            active_threads: Mutex::new(0),
            cpr: super::cpr::CprCoordinator::default(),
            default_log_checkpoint_backend: AtomicU32::new(
                super::cpr::LogCheckpointBackend::Snapshot as u32,
            ),
            default_checkpoint_durability: AtomicU32::new(
                super::cpr::CheckpointDurability::FasterLike as u32,
            ),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: std::marker::PhantomData,
        })
    }

    /// Get recovered session states (for continuation via `continue_session`).
    pub fn get_recovered_sessions(&self) -> Vec<SessionState> {
        self.get_session_states()
    }

    /// Return `true` if a full checkpoint exists (both index and log).
    pub fn checkpoint_exists(checkpoint_dir: &Path, token: CheckpointToken) -> bool {
        Self::get_checkpoint_kind(checkpoint_dir, token) == CheckpointKind::Full
    }

    /// Determine what kind of checkpoint exists for `token`.
    pub fn get_checkpoint_kind(checkpoint_dir: &Path, token: CheckpointToken) -> CheckpointKind {
        let cp_dir = checkpoint_dir.join(token.to_string());
        let has_index = cp_dir.join("index.meta").exists();
        let has_log = cp_dir.join("log.meta").exists();

        match (has_index, has_log) {
            (true, true) => CheckpointKind::Full,
            (true, false) => CheckpointKind::IndexOnly,
            (false, true) => CheckpointKind::LogOnly,
            (false, false) => CheckpointKind::None,
        }
    }

    /// Return `true` if any checkpoint files exist (full or partial).
    pub fn any_checkpoint_exists(checkpoint_dir: &Path, token: CheckpointToken) -> bool {
        Self::get_checkpoint_kind(checkpoint_dir, token) != CheckpointKind::None
    }

    /// List full checkpoint tokens in `checkpoint_dir`.
    pub fn list_checkpoints(checkpoint_dir: &Path) -> io::Result<Vec<CheckpointToken>> {
        Self::list_checkpoints_by_kind(checkpoint_dir, Some(CheckpointKind::Full))
    }

    /// List checkpoint tokens filtered by `kind` (or all non-empty kinds when `None`).
    pub fn list_checkpoints_by_kind(
        checkpoint_dir: &Path,
        kind: Option<CheckpointKind>,
    ) -> io::Result<Vec<CheckpointToken>> {
        let mut tokens = Vec::new();

        if !checkpoint_dir.exists() {
            return Ok(tokens);
        }

        for entry in std::fs::read_dir(checkpoint_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    if let Some(name_str) = name.to_str() {
                        if let Ok(token) = uuid::Uuid::parse_str(name_str) {
                            let cp_kind = Self::get_checkpoint_kind(checkpoint_dir, token);
                            let include = match kind {
                                Some(k) => cp_kind == k,
                                None => cp_kind != CheckpointKind::None,
                            };
                            if include {
                                tokens.push(token);
                            }
                        }
                    }
                }
            }
        }

        Ok(tokens)
    }

    /// Recover only the hash index from a checkpoint.
    pub fn recover_index_only(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
    ) -> io::Result<Self> {
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());

        let kind = Self::get_checkpoint_kind(checkpoint_dir, token);
        if kind != CheckpointKind::Full && kind != CheckpointKind::IndexOnly {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Index checkpoint not found",
            ));
        }

        let epoch = Arc::new(LightEpoch::new());

        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;

        let mut hash_index = MemHashIndex::new();
        hash_index.recover(&cp_dir, Some(&index_meta))?;

        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let hlog = PersistentMemoryMalloc::new(log_config, device.clone());

        let system_state = AtomicSystemState::new(SystemState::rest(index_meta.version));

        let compactor = Compactor::with_config(CompactionConfig::default());
        let pending_io = PendingIoManager::new(device.clone(), hlog.page_size());

        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache: None,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(0),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(HashMap::new()),
            active_threads: Mutex::new(0),
            cpr: super::cpr::CprCoordinator::default(),
            default_log_checkpoint_backend: AtomicU32::new(
                super::cpr::LogCheckpointBackend::Snapshot as u32,
            ),
            default_checkpoint_durability: AtomicU32::new(
                super::cpr::CheckpointDurability::FasterLike as u32,
            ),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: std::marker::PhantomData,
        })
    }

    /// Recover only the hybrid log from a checkpoint (the hash index starts empty).
    pub fn recover_log_only(
        checkpoint_dir: &Path,
        token: CheckpointToken,
        config: FasterKvConfig,
        device: D,
    ) -> io::Result<Self> {
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());

        let kind = Self::get_checkpoint_kind(checkpoint_dir, token);
        if kind != CheckpointKind::Full && kind != CheckpointKind::LogOnly {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Log checkpoint not found",
            ));
        }

        let epoch = Arc::new(LightEpoch::new());

        let mut hash_index = MemHashIndex::new();
        let index_config = MemHashIndexConfig::new(config.table_size);
        hash_index.initialize(&index_config);

        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;

        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let mut hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        hlog.recover(&cp_dir, Some(&log_meta))?;

        let system_state = AtomicSystemState::new(SystemState::rest(log_meta.version));

        let compactor = Compactor::with_config(CompactionConfig::default());
        let pending_io = PendingIoManager::new(device.clone(), hlog.page_size());

        let mut session_registry = HashMap::new();
        for session_state in &log_meta.session_states {
            session_registry.insert(session_state.guid, session_state.clone());
        }

        Ok(Self {
            epoch,
            system_state,
            hash_index,
            hlog: UnsafeCell::new(hlog),
            read_cache: None,
            device,
            pending_io,
            disk_read_results: Mutex::new(HashMap::new()),
            pending_io_completed: Mutex::new(HashMap::new()),
            next_session_id: AtomicU32::new(log_meta.num_threads),
            compactor,
            grow_state: UnsafeCell::new(None),
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(session_registry),
            active_threads: Mutex::new(0),
            cpr: super::cpr::CprCoordinator::default(),
            default_log_checkpoint_backend: AtomicU32::new(
                super::cpr::LogCheckpointBackend::Snapshot as u32,
            ),
            default_checkpoint_durability: AtomicU32::new(
                super::cpr::CheckpointDurability::FasterLike as u32,
            ),
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: std::marker::PhantomData,
        })
    }
}
