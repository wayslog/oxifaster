//! F2 checkpoint state management
//!
//! Manages checkpoint state for the F2 hot-cold architecture.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use uuid::Uuid;

/// Checkpoint phase for F2
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum F2CheckpointPhase {
    /// No checkpoint in progress
    #[default]
    Rest = 0,
    /// Checkpointing hot store
    HotStoreCheckpoint = 1,
    /// Checkpointing cold store
    ColdStoreCheckpoint = 2,
    /// Recovery in progress
    Recover = 3,
}

impl F2CheckpointPhase {
    /// Convert from u8
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Rest),
            1 => Some(Self::HotStoreCheckpoint),
            2 => Some(Self::ColdStoreCheckpoint),
            3 => Some(Self::Recover),
            _ => None,
        }
    }
}

/// Status of a store's checkpoint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum StoreCheckpointStatus {
    /// No checkpoint operation
    #[default]
    Idle = 0,
    /// Checkpoint requested
    Requested = 1,
    /// Checkpoint in progress
    Active = 2,
    /// Checkpoint completed successfully
    Finished = 3,
    /// Checkpoint failed
    Failed = 4,
}

impl StoreCheckpointStatus {
    /// Convert from u8
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Idle),
            1 => Some(Self::Requested),
            2 => Some(Self::Active),
            3 => Some(Self::Finished),
            4 => Some(Self::Failed),
            _ => None,
        }
    }

    /// Check if the checkpoint is done (finished or failed)
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }
}

/// Atomic wrapper for F2CheckpointPhase
pub struct AtomicCheckpointPhase(AtomicU32);

impl AtomicCheckpointPhase {
    /// Create a new atomic checkpoint phase
    pub fn new(phase: F2CheckpointPhase) -> Self {
        Self(AtomicU32::new(phase as u32))
    }

    /// Load the current phase
    pub fn load(&self, ordering: Ordering) -> F2CheckpointPhase {
        F2CheckpointPhase::from_u8(self.0.load(ordering) as u8).unwrap_or_default()
    }

    /// Store a phase
    pub fn store(&self, phase: F2CheckpointPhase, ordering: Ordering) {
        self.0.store(phase as u32, ordering);
    }

    /// Compare and exchange
    pub fn compare_exchange(
        &self,
        current: F2CheckpointPhase,
        new: F2CheckpointPhase,
        success: Ordering,
        failure: Ordering,
    ) -> Result<F2CheckpointPhase, F2CheckpointPhase> {
        self.0
            .compare_exchange(current as u32, new as u32, success, failure)
            .map(|v| F2CheckpointPhase::from_u8(v as u8).unwrap_or_default())
            .map_err(|v| F2CheckpointPhase::from_u8(v as u8).unwrap_or_default())
    }
}

impl Default for AtomicCheckpointPhase {
    fn default() -> Self {
        Self::new(F2CheckpointPhase::Rest)
    }
}

/// Atomic wrapper for StoreCheckpointStatus
pub struct AtomicCheckpointStatus(AtomicU32);

impl AtomicCheckpointStatus {
    /// Create a new atomic checkpoint status
    pub fn new(status: StoreCheckpointStatus) -> Self {
        Self(AtomicU32::new(status as u32))
    }

    /// Load the current status
    pub fn load(&self, ordering: Ordering) -> StoreCheckpointStatus {
        StoreCheckpointStatus::from_u8(self.0.load(ordering) as u8).unwrap_or_default()
    }

    /// Store a status
    pub fn store(&self, status: StoreCheckpointStatus, ordering: Ordering) {
        self.0.store(status as u32, ordering);
    }
}

impl Default for AtomicCheckpointStatus {
    fn default() -> Self {
        Self::new(StoreCheckpointStatus::Idle)
    }
}

/// F2 checkpoint state
pub struct F2CheckpointState {
    /// Current checkpoint phase
    pub phase: AtomicCheckpointPhase,
    /// Hot store checkpoint status
    pub hot_store_status: AtomicCheckpointStatus,
    /// Cold store checkpoint status
    pub cold_store_status: AtomicCheckpointStatus,
    /// Checkpoint token
    token: Uuid,
    /// Checkpoint version (incremented with each successful checkpoint)
    version: AtomicU32,
    /// Number of threads pending hot store persistence
    threads_pending_hot_store: AtomicU32,
    /// Number of threads pending callback issue
    threads_pending_callback: AtomicU32,
    /// Persistent serial numbers per thread (simplified)
    persistent_serial_nums: Vec<AtomicU64>,
}

impl F2CheckpointState {
    /// Maximum number of threads supported
    const MAX_THREADS: usize = 96;

    /// Create a new checkpoint state
    pub fn new() -> Self {
        let persistent_serial_nums = (0..Self::MAX_THREADS).map(|_| AtomicU64::new(0)).collect();

        Self {
            phase: AtomicCheckpointPhase::default(),
            hot_store_status: AtomicCheckpointStatus::default(),
            cold_store_status: AtomicCheckpointStatus::default(),
            token: Uuid::nil(),
            version: AtomicU32::new(0),
            threads_pending_hot_store: AtomicU32::new(0),
            threads_pending_callback: AtomicU32::new(0),
            persistent_serial_nums,
        }
    }

    /// Initialize the checkpoint state for a new checkpoint
    pub fn initialize(&mut self, token: Uuid, num_active_threads: u32) {
        self.token = token;
        // Increment version for this new checkpoint
        self.version.fetch_add(1, Ordering::AcqRel);
        self.threads_pending_hot_store
            .store(num_active_threads, Ordering::Release);
        self.threads_pending_callback
            .store(num_active_threads, Ordering::Release);

        for psn in &self.persistent_serial_nums {
            psn.store(0, Ordering::Release);
        }

        self.hot_store_status
            .store(StoreCheckpointStatus::Idle, Ordering::Release);
        self.cold_store_status
            .store(StoreCheckpointStatus::Idle, Ordering::Release);
    }

    /// Get the checkpoint token
    pub fn token(&self) -> Uuid {
        self.token
    }

    /// Get the current checkpoint version
    pub fn version(&self) -> u32 {
        self.version.load(Ordering::Acquire)
    }

    /// Set the checkpoint version (used during recovery)
    pub fn set_version(&self, version: u32) {
        self.version.store(version, Ordering::Release);
    }

    /// Reset the checkpoint state
    pub fn reset(&mut self) {
        self.token = Uuid::nil();
        self.threads_pending_hot_store.store(0, Ordering::Release);
        self.threads_pending_callback.store(0, Ordering::Release);

        for psn in &self.persistent_serial_nums {
            psn.store(0, Ordering::Release);
        }

        self.hot_store_status
            .store(StoreCheckpointStatus::Idle, Ordering::Release);
        self.cold_store_status
            .store(StoreCheckpointStatus::Idle, Ordering::Release);
        self.phase.store(F2CheckpointPhase::Rest, Ordering::Release);
    }

    /// Set persistent serial number for a thread
    pub fn set_persistent_serial_num(&self, thread_id: usize, serial_num: u64) {
        if thread_id < Self::MAX_THREADS {
            self.persistent_serial_nums[thread_id].store(serial_num, Ordering::Release);
        }
    }

    /// Get persistent serial number for a thread
    pub fn get_persistent_serial_num(&self, thread_id: usize) -> u64 {
        if thread_id < Self::MAX_THREADS {
            self.persistent_serial_nums[thread_id].load(Ordering::Acquire)
        } else {
            0
        }
    }

    /// Decrement and get threads pending hot store persistence
    pub fn decrement_pending_hot_store(&self) -> u32 {
        self.threads_pending_hot_store
            .fetch_sub(1, Ordering::AcqRel)
    }

    /// Decrement and get threads pending callback
    pub fn decrement_pending_callback(&self) -> u32 {
        self.threads_pending_callback.fetch_sub(1, Ordering::AcqRel)
    }

    /// Check if checkpoint is in progress
    pub fn is_in_progress(&self) -> bool {
        self.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest
    }
}

impl Default for F2CheckpointState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_phase() {
        assert_eq!(F2CheckpointPhase::from_u8(0), Some(F2CheckpointPhase::Rest));
        assert_eq!(
            F2CheckpointPhase::from_u8(1),
            Some(F2CheckpointPhase::HotStoreCheckpoint)
        );
        assert_eq!(F2CheckpointPhase::from_u8(5), None);
    }

    #[test]
    fn test_checkpoint_status() {
        assert!(!StoreCheckpointStatus::Idle.is_done());
        assert!(!StoreCheckpointStatus::Active.is_done());
        assert!(StoreCheckpointStatus::Finished.is_done());
        assert!(StoreCheckpointStatus::Failed.is_done());
    }

    #[test]
    fn test_atomic_phase() {
        let phase = AtomicCheckpointPhase::new(F2CheckpointPhase::Rest);
        assert_eq!(phase.load(Ordering::Acquire), F2CheckpointPhase::Rest);

        phase.store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
        assert_eq!(
            phase.load(Ordering::Acquire),
            F2CheckpointPhase::HotStoreCheckpoint
        );
    }

    #[test]
    fn test_checkpoint_state_lifecycle() {
        let mut state = F2CheckpointState::new();
        assert!(!state.is_in_progress());

        let token = Uuid::new_v4();
        state.initialize(token, 4);
        assert_eq!(state.token(), token);

        state
            .phase
            .store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
        assert!(state.is_in_progress());

        state.reset();
        assert!(!state.is_in_progress());
        assert_eq!(state.token(), Uuid::nil());
    }

    #[test]
    fn test_persistent_serial_nums() {
        let state = F2CheckpointState::new();

        state.set_persistent_serial_num(0, 100);
        state.set_persistent_serial_num(1, 200);

        assert_eq!(state.get_persistent_serial_num(0), 100);
        assert_eq!(state.get_persistent_serial_num(1), 200);
        assert_eq!(state.get_persistent_serial_num(99), 0); // Out of range
    }
}
