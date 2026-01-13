use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use crate::allocator::{HybridLogConfig, PersistentMemoryMalloc};
use crate::cache::{ReadCache, ReadCacheConfig};
use crate::checkpoint::{
    create_checkpoint_directory, delta_log_path, delta_metadata_path, CheckpointState,
    CheckpointToken, CheckpointType, DeltaLogMetadata, IndexMetadata, LogMetadata, RecoveryState,
    SessionState,
};
use crate::compaction::{CompactionConfig, Compactor};
use crate::delta_log::{DeltaLog, DeltaLogConfig};
use crate::device::StorageDevice;
use crate::epoch::LightEpoch;
use crate::index::{MemHashIndex, MemHashIndexConfig};
use crate::record::{Key, Value};
use crate::stats::StatsCollector;
use crate::store::pending_io::PendingIoManager;
use crate::store::state_transitions::{Action, AtomicSystemState, Phase, SystemState};

use super::{CheckpointKind, FasterKv, FasterKvConfig};

impl<K, V, D> FasterKv<K, V, D>
where
    K: Key,
    V: Value,
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
        let can_use_incremental = prev_snapshot.is_some();
        drop(prev_snapshot);

        if can_use_incremental {
            self.checkpoint_with_action(checkpoint_dir, Action::CheckpointIncremental)
        } else {
            let token = self.checkpoint(checkpoint_dir)?;
            Ok(token)
        }
    }

    /// Force a full snapshot checkpoint and record it as the incremental base.
    pub fn checkpoint_full_snapshot(&self, checkpoint_dir: &Path) -> io::Result<CheckpointToken> {
        self.checkpoint_with_action(checkpoint_dir, Action::CheckpointFull)
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

        let result = create_checkpoint_directory(checkpoint_dir, token)
            .and_then(|cp_dir| self.execute_checkpoint_state_machine(&cp_dir, token, action));

        if result.is_err() {
            self.system_state.store(
                SystemState::rest(start_version),
                std::sync::atomic::Ordering::Release,
            );
        }

        result.map(|_| token)
    }

    fn execute_checkpoint_state_machine(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        action: Action,
    ) -> io::Result<()> {
        let mut index_metadata: Option<IndexMetadata> = None;
        let mut log_metadata: Option<LogMetadata> = None;
        let is_incremental = action == Action::CheckpointIncremental;

        loop {
            let current_state = self.system_state.load(std::sync::atomic::Ordering::Acquire);

            // Handle current phase
            match current_state.phase {
                Phase::PrepIndexChkpt => {
                    // Phase 1: Prepare index checkpoint
                    // In a full implementation, this would synchronize all threads
                    // For now, we just advance to the next phase
                    self.handle_prep_index_checkpoint()?;
                }

                Phase::IndexChkpt => {
                    // Phase 2: Write index to disk
                    index_metadata = Some(self.handle_index_checkpoint(cp_dir, token)?);
                }

                Phase::Prepare => {
                    // Phase 3: Prepare hybrid log checkpoint
                    self.handle_prepare_checkpoint()?;
                }

                Phase::InProgress => {
                    // Phase 4: Checkpoint in progress
                    // Version is already incremented by state machine
                    if is_incremental {
                        log_metadata = Some(self.handle_incremental_checkpoint(
                            cp_dir,
                            token,
                            current_state.version,
                        )?);
                    } else {
                        log_metadata = Some(self.handle_in_progress_checkpoint(
                            cp_dir,
                            token,
                            current_state.version,
                        )?);
                    }
                }

                Phase::WaitPending => {
                    // Phase 5: Wait for pending operations
                    self.handle_wait_pending()?;
                }

                Phase::WaitFlush => {
                    // Phase 6: Wait for flush to complete
                    if is_incremental {
                        self.handle_incremental_wait_flush(cp_dir, token, current_state.version)?;
                    } else {
                        self.handle_wait_flush()?;
                    }
                }

                Phase::PersistenceCallback => {
                    // Phase 7: Invoke persistence callback
                    self.handle_persistence_callback(
                        cp_dir,
                        token,
                        index_metadata.as_ref(),
                        log_metadata.as_ref(),
                    )?;

                    // Save checkpoint state for future incremental checkpoints
                    if !is_incremental && log_metadata.is_some() {
                        let mut state = CheckpointState::new(CheckpointType::Snapshot);
                        if let Some(ref log_meta) = log_metadata {
                            state.log_metadata = log_meta.clone();
                        }
                        if let Some(ref index_meta) = index_metadata {
                            state.index_metadata = index_meta.clone();
                        }
                        // Set tokens AFTER cloning to ensure they're not overwritten
                        // Both index and log metadata must have the same token for consistency
                        state.index_metadata.token = token;
                        state.log_metadata.token = token;
                        *self.last_snapshot_checkpoint.write() = Some(state);
                    }
                }

                Phase::Rest => {
                    // Phase 8: Back to rest state - checkpoint complete
                    return Ok(());
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

            // Advance to the next phase
            if let Err(err_state) = self.system_state.try_advance() {
                // If we can't advance and we're in rest state, we're done
                if err_state.phase == Phase::Rest {
                    return Ok(());
                }
                // Otherwise, retry (another thread may have advanced)
            }
        }
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

    fn handle_prepare_checkpoint(&self) -> io::Result<()> {
        // Prepare for hybrid log checkpoint
        // Get flushed addresses, etc.
        Ok(())
    }

    fn handle_in_progress_checkpoint(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        version: u32,
    ) -> io::Result<LogMetadata> {
        // Collect session states for persistence
        let session_states = self.get_session_states();

        // SAFETY: checkpoint() is protected by the epoch system
        unsafe {
            (*self.hlog.get()).checkpoint_with_sessions(cp_dir, token, version, session_states)
        }
    }

    fn handle_wait_pending(&self) -> io::Result<()> {
        // Wait for all pending operations to complete
        // In async implementation, this would drain pending I/Os
        Ok(())
    }

    fn handle_wait_flush(&self) -> io::Result<()> {
        // Wait for log flush to complete
        // SAFETY: Flush is protected by epoch
        unsafe {
            (*self.hlog.get()).flush_to_disk()?;
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

    fn handle_incremental_wait_flush(
        &self,
        cp_dir: &Path,
        token: CheckpointToken,
        version: u32,
    ) -> io::Result<()> {
        // Get the previous snapshot state
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

        // Get current log addresses
        let (flushed_address, final_address) = unsafe {
            let hlog = &*self.hlog.get();
            (hlog.get_flushed_until_address(), hlog.get_tail_address())
        };

        // Create delta log device (using a file in the checkpoint directory)
        let delta_path = delta_log_path(cp_dir, 0);
        let delta_device = Arc::new(crate::device::FileSystemDisk::single_file(&delta_path)?);
        let delta_config = DeltaLogConfig::new(22); // 4MB pages
        let delta_log = DeltaLog::new(delta_device.clone(), delta_config, 0);

        // Flush delta records
        let num_entries = unsafe {
            (*self.hlog.get()).flush_delta_to_device(
                flushed_address,
                final_address,
                prev_final_address,
                version,
                &delta_log,
            )?
        };

        // Write delta log metadata (use checkpoint token, not a random UUID)
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
        delta_meta.write_to_file(&delta_metadata_path(cp_dir))?;

        // Flush the regular log as well
        unsafe {
            (*self.hlog.get()).flush_to_disk()?;
        }

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
        let device = Arc::new(device);
        let cp_dir = checkpoint_dir.join(token.to_string());

        let mut recovery_state = RecoveryState::new();
        let status = recovery_state.start_recovery(checkpoint_dir, token);
        if status != crate::Status::Ok {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                recovery_state
                    .error_message()
                    .unwrap_or("Unknown recovery error"),
            ));
        }

        let epoch = Arc::new(LightEpoch::new());

        let index_meta = IndexMetadata::read_from_file(&cp_dir.join("index.meta"))?;
        let log_meta = LogMetadata::read_from_file(&cp_dir.join("log.meta"))?;

        let mut hash_index = MemHashIndex::new();
        hash_index.recover(&cp_dir, Some(&index_meta))?;
        recovery_state.index_recovered();

        let log_config = HybridLogConfig::new(config.log_memory_size, config.page_size_bits);
        let mut hlog = PersistentMemoryMalloc::new(log_config, device.clone());
        hlog.recover(&cp_dir, Some(&log_meta))?;
        recovery_state.log_recovered();

        let system_state = AtomicSystemState::new(SystemState::rest(log_meta.version));

        let compactor = Compactor::with_config(compaction_config);

        let read_cache = cache_config.map(ReadCache::new);
        let pending_io = PendingIoManager::new(device.clone());

        let mut session_registry = HashMap::new();
        for session_state in &log_meta.session_states {
            session_registry.insert(session_state.guid, session_state.clone());
        }
        let next_session_id = log_meta.num_threads;

        recovery_state.complete_recovery();

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
            stats_collector: StatsCollector::with_defaults(),
            session_registry: RwLock::new(session_registry),
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
        let pending_io = PendingIoManager::new(device.clone());

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
        let pending_io = PendingIoManager::new(device.clone());

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
            last_snapshot_checkpoint: RwLock::new(None),
            _marker: std::marker::PhantomData,
        })
    }
}
