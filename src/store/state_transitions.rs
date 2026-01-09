//! State transitions for FASTER operations
//!
//! This module implements the state machine for checkpoint, recovery, GC, and index growth
//! operations. Based on C++ FASTER's state_transitions.h implementation.
//!
//! # CPR (Concurrent Prefix Recovery)
//!
//! CPR ensures that all threads see a consistent view of the store during checkpointing.
//! The state machine coordinates the phases:
//!
//! 1. **PREP_INDEX_CHKPT**: Prepare index checkpoint - threads synchronize
//! 2. **INDEX_CHKPT**: Write index to disk
//! 3. **PREPARE**: Prepare HybridLog checkpoint - get flushed addresses
//! 4. **IN_PROGRESS**: Increment version, mark checkpoint in progress
//! 5. **WAIT_PENDING**: Wait for pending operations to complete
//! 6. **WAIT_FLUSH**: Wait for log flush to complete
//! 7. **PERSISTENCE_CALLBACK**: Invoke persistence callback
//! 8. **REST**: Return to rest state

use std::sync::atomic::{AtomicU64, Ordering};

/// Actions that can be performed by the FASTER store.
/// Only one action can be active at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Action {
    /// No action in progress
    #[default]
    None = 0,
    /// Full checkpoint (index + hybrid log)
    CheckpointFull = 1,
    /// Index-only checkpoint
    CheckpointIndex = 2,
    /// HybridLog-only checkpoint
    CheckpointHybridLog = 3,
    /// Recovery from checkpoint
    Recover = 4,
    /// Garbage collection
    GC = 5,
    /// Index growth (rehashing)
    GrowIndex = 6,
    /// Incremental checkpoint (delta log)
    CheckpointIncremental = 7,
}

impl From<u8> for Action {
    fn from(v: u8) -> Self {
        match v {
            0 => Action::None,
            1 => Action::CheckpointFull,
            2 => Action::CheckpointIndex,
            3 => Action::CheckpointHybridLog,
            4 => Action::Recover,
            5 => Action::GC,
            6 => Action::GrowIndex,
            7 => Action::CheckpointIncremental,
            _ => Action::None,
        }
    }
}

/// Phases during checkpoint/recovery/GC/grow operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Phase {
    // Checkpoint phases
    /// Prepare index checkpoint - synchronize threads
    PrepIndexChkpt = 0,
    /// Index checkpoint in progress
    IndexChkpt = 1,
    /// Prepare hybrid log checkpoint
    Prepare = 2,
    /// Checkpoint in progress
    InProgress = 3,
    /// Waiting for pending operations
    WaitPending = 4,
    /// Waiting for flush to complete
    WaitFlush = 5,
    /// Rest state - no operation in progress
    #[default]
    Rest = 6,
    /// Persistence callback
    PersistenceCallback = 7,

    // Garbage collection phases
    /// GC I/O pending - finish outstanding I/Os
    GcIoPending = 8,
    /// GC in progress - cleaning hash table
    GcInProgress = 9,

    // Grow index phases
    /// Grow prepare - wait for threads to complete operations
    GrowPrepare = 10,
    /// Grow in progress - copying hash table
    GrowInProgress = 11,

    /// Invalid phase
    Invalid = 255,
}

impl From<u8> for Phase {
    fn from(v: u8) -> Self {
        match v {
            0 => Phase::PrepIndexChkpt,
            1 => Phase::IndexChkpt,
            2 => Phase::Prepare,
            3 => Phase::InProgress,
            4 => Phase::WaitPending,
            5 => Phase::WaitFlush,
            6 => Phase::Rest,
            7 => Phase::PersistenceCallback,
            8 => Phase::GcIoPending,
            9 => Phase::GcInProgress,
            10 => Phase::GrowPrepare,
            11 => Phase::GrowInProgress,
            _ => Phase::Invalid,
        }
    }
}

/// System state combining action, phase, and version.
/// Packed into 64 bits for atomic operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemState {
    /// Action being performed
    pub action: Action,
    /// Current phase of the action
    pub phase: Phase,
    /// Checkpoint version (for CPR)
    pub version: u32,
}

impl SystemState {
    /// Create a new system state
    pub const fn new(action: Action, phase: Phase, version: u32) -> Self {
        Self {
            action,
            phase,
            version,
        }
    }

    /// Create a rest state
    pub const fn rest(version: u32) -> Self {
        Self::new(Action::None, Phase::Rest, version)
    }

    /// Pack into u64 for atomic operations
    #[inline]
    pub fn to_control(&self) -> u64 {
        (self.action as u64) | ((self.phase as u64) << 8) | ((self.version as u64) << 16)
    }

    /// Unpack from u64
    #[inline]
    pub fn from_control(control: u64) -> Self {
        Self {
            action: Action::from((control & 0xFF) as u8),
            phase: Phase::from(((control >> 8) & 0xFF) as u8),
            version: ((control >> 16) & 0xFFFFFFFF) as u32,
        }
    }

    /// Get the next state in the state machine
    pub fn get_next_state(&self) -> Result<SystemState, crate::status::Status> {
        use crate::status::Status;
        match self.action {
            Action::None => Err(Status::InvalidOperation),

            Action::CheckpointFull => match self.phase {
                Phase::Rest => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::PrepIndexChkpt,
                    self.version,
                )),
                Phase::PrepIndexChkpt => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::IndexChkpt,
                    self.version,
                )),
                Phase::IndexChkpt => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::Prepare,
                    self.version,
                )),
                Phase::Prepare => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::InProgress,
                    self.version + 1, // Version increment happens here
                )),
                Phase::InProgress => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::WaitPending,
                    self.version,
                )),
                Phase::WaitPending => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::WaitFlush,
                    self.version,
                )),
                Phase::WaitFlush => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::PersistenceCallback,
                    self.version,
                )),
                Phase::PersistenceCallback => Ok(SystemState::new(
                    Action::CheckpointFull,
                    Phase::Rest,
                    self.version,
                )),
                _ => Err(Status::InvalidOperation),
            },

            Action::CheckpointIndex => match self.phase {
                Phase::Rest => Ok(SystemState::new(
                    Action::CheckpointIndex,
                    Phase::PrepIndexChkpt,
                    self.version,
                )),
                Phase::PrepIndexChkpt => Ok(SystemState::new(
                    Action::CheckpointIndex,
                    Phase::IndexChkpt,
                    self.version,
                )),
                Phase::IndexChkpt => Ok(SystemState::new(
                    Action::CheckpointIndex,
                    Phase::Rest,
                    self.version,
                )),
                _ => Err(Status::InvalidOperation),
            },

            Action::CheckpointHybridLog => match self.phase {
                Phase::Rest => Ok(SystemState::new(
                    Action::CheckpointHybridLog,
                    Phase::Prepare,
                    self.version,
                )),
                Phase::Prepare => Ok(SystemState::new(
                    Action::CheckpointHybridLog,
                    Phase::InProgress,
                    self.version + 1,
                )),
                Phase::InProgress => Ok(SystemState::new(
                    Action::CheckpointHybridLog,
                    Phase::WaitPending,
                    self.version,
                )),
                Phase::WaitPending => Ok(SystemState::new(
                    Action::CheckpointHybridLog,
                    Phase::WaitFlush,
                    self.version,
                )),
                Phase::WaitFlush => Ok(SystemState::new(
                    Action::CheckpointHybridLog,
                    Phase::PersistenceCallback,
                    self.version,
                )),
                Phase::PersistenceCallback => Ok(SystemState::new(
                    Action::CheckpointHybridLog,
                    Phase::Rest,
                    self.version,
                )),
                _ => Err(Status::InvalidOperation),
            },

            Action::GC => match self.phase {
                Phase::Rest => Ok(SystemState::new(
                    Action::GC,
                    Phase::GcIoPending,
                    self.version,
                )),
                Phase::GcIoPending => Ok(SystemState::new(
                    Action::GC,
                    Phase::GcInProgress,
                    self.version,
                )),
                Phase::GcInProgress => Ok(SystemState::new(Action::GC, Phase::Rest, self.version)),
                _ => Err(Status::InvalidOperation),
            },

            Action::GrowIndex => match self.phase {
                Phase::Rest => Ok(SystemState::new(
                    Action::GrowIndex,
                    Phase::GrowPrepare,
                    self.version,
                )),
                Phase::GrowPrepare => Ok(SystemState::new(
                    Action::GrowIndex,
                    Phase::GrowInProgress,
                    self.version,
                )),
                Phase::GrowInProgress => Ok(SystemState::new(
                    Action::GrowIndex,
                    Phase::Rest,
                    self.version,
                )),
                _ => Err(Status::InvalidOperation),
            },

            Action::Recover => {
                // Recovery doesn't have standard state transitions
                // It's handled separately by the recovery module
                Err(Status::InvalidOperation)
            }

            // Incremental checkpoints also need to persist index files (index.meta/index.dat),
            // otherwise validation/recovery will fail. Therefore, we reuse the full checkpoint
            // phase transitions (including index phases), and only write delta during IN_PROGRESS.
            Action::CheckpointIncremental => match self.phase {
                Phase::Rest => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::PrepIndexChkpt,
                    self.version,
                )),
                Phase::PrepIndexChkpt => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::IndexChkpt,
                    self.version,
                )),
                Phase::IndexChkpt => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::Prepare,
                    self.version,
                )),
                Phase::Prepare => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::InProgress,
                    self.version + 1,
                )),
                Phase::InProgress => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::WaitPending,
                    self.version,
                )),
                Phase::WaitPending => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::WaitFlush,
                    self.version,
                )),
                Phase::WaitFlush => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::PersistenceCallback,
                    self.version,
                )),
                Phase::PersistenceCallback => Ok(SystemState::new(
                    Action::CheckpointIncremental,
                    Phase::Rest,
                    self.version,
                )),
                _ => Err(Status::InvalidOperation),
            },
        }
    }

    /// Check if this state is a rest state
    pub fn is_rest(&self) -> bool {
        self.phase == Phase::Rest
    }

    /// Check if an action is in progress
    pub fn is_action_in_progress(&self) -> bool {
        self.action != Action::None && self.phase != Phase::Rest
    }

    /// Check if this is a checkpoint action
    pub fn is_checkpoint(&self) -> bool {
        matches!(
            self.action,
            Action::CheckpointFull
                | Action::CheckpointIndex
                | Action::CheckpointHybridLog
                | Action::CheckpointIncremental
        )
    }

    /// Check if this is an incremental checkpoint action
    pub fn is_incremental_checkpoint(&self) -> bool {
        self.action == Action::CheckpointIncremental
    }
}

impl Default for SystemState {
    fn default() -> Self {
        Self::rest(0)
    }
}

/// Atomic version of SystemState for thread-safe access.
#[derive(Debug)]
pub struct AtomicSystemState {
    control: AtomicU64,
}

impl AtomicSystemState {
    /// Create a new atomic system state
    pub fn new(state: SystemState) -> Self {
        Self {
            control: AtomicU64::new(state.to_control()),
        }
    }

    /// Load the current state
    #[inline]
    pub fn load(&self, ordering: Ordering) -> SystemState {
        SystemState::from_control(self.control.load(ordering))
    }

    /// Store a new state
    #[inline]
    pub fn store(&self, state: SystemState, ordering: Ordering) {
        self.control.store(state.to_control(), ordering);
    }

    /// Compare and exchange
    #[inline]
    pub fn compare_exchange(
        &self,
        expected: SystemState,
        desired: SystemState,
        success: Ordering,
        failure: Ordering,
    ) -> Result<SystemState, SystemState> {
        self.control
            .compare_exchange(
                expected.to_control(),
                desired.to_control(),
                success,
                failure,
            )
            .map(SystemState::from_control)
            .map_err(SystemState::from_control)
    }

    /// Compare and exchange weak
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        expected: SystemState,
        desired: SystemState,
        success: Ordering,
        failure: Ordering,
    ) -> Result<SystemState, SystemState> {
        self.control
            .compare_exchange_weak(
                expected.to_control(),
                desired.to_control(),
                success,
                failure,
            )
            .map(SystemState::from_control)
            .map_err(SystemState::from_control)
    }

    /// Get the current phase
    #[inline]
    pub fn phase(&self) -> Phase {
        self.load(Ordering::Acquire).phase
    }

    /// Get the current version
    #[inline]
    pub fn version(&self) -> u32 {
        self.load(Ordering::Acquire).version
    }

    /// Get the current action
    #[inline]
    pub fn action(&self) -> Action {
        self.load(Ordering::Acquire).action
    }

    /// Try to start a new action from the rest state
    /// Returns Ok(previous_state) on success, Err(current_state) on failure
    pub fn try_start_action(&self, action: Action) -> Result<SystemState, SystemState> {
        let current = self.load(Ordering::Acquire);

        if !current.is_rest() {
            return Err(current);
        }

        let new_state = SystemState::new(action, Phase::Rest, current.version);

        // Get the first state in the action's sequence
        let next_state = match new_state.get_next_state() {
            Ok(s) => s,
            Err(_) => return Err(current),
        };

        self.compare_exchange(current, next_state, Ordering::AcqRel, Ordering::Acquire)
    }

    /// Try to advance to the next state in the current action
    pub fn try_advance(&self) -> Result<SystemState, SystemState> {
        loop {
            let current = self.load(Ordering::Acquire);

            let next = match current.get_next_state() {
                Ok(s) => s,
                Err(_) => return Err(current),
            };

            match self.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(prev) => return Ok(prev),
                Err(actual) => {
                    // If state changed, check if we should retry or give up
                    if actual != current {
                        continue;
                    }
                    return Err(actual);
                }
            }
        }
    }
}

impl Default for AtomicSystemState {
    fn default() -> Self {
        Self::new(SystemState::default())
    }
}

impl Clone for AtomicSystemState {
    fn clone(&self) -> Self {
        Self::new(self.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_state_packing() {
        let state = SystemState::new(Action::CheckpointFull, Phase::InProgress, 42);
        let control = state.to_control();
        let unpacked = SystemState::from_control(control);

        assert_eq!(state, unpacked);
        assert_eq!(unpacked.action, Action::CheckpointFull);
        assert_eq!(unpacked.phase, Phase::InProgress);
        assert_eq!(unpacked.version, 42);
    }

    #[test]
    fn test_checkpoint_full_state_machine() {
        // Start checkpoint
        let mut state = SystemState::new(Action::CheckpointFull, Phase::Rest, 0);

        // Follow the state machine
        let phases = vec![
            Phase::PrepIndexChkpt,
            Phase::IndexChkpt,
            Phase::Prepare,
            Phase::InProgress,
            Phase::WaitPending,
            Phase::WaitFlush,
            Phase::PersistenceCallback,
            Phase::Rest,
        ];

        for expected_phase in phases {
            state = state.get_next_state().unwrap();
            assert_eq!(state.phase, expected_phase);
        }

        // Verify version incremented after PREPARE phase
        assert_eq!(state.version, 1);
    }

    #[test]
    fn test_checkpoint_incremental_state_machine_includes_index_phases() {
        let mut state = SystemState::new(Action::CheckpointIncremental, Phase::Rest, 0);

        let phases = vec![
            Phase::PrepIndexChkpt,
            Phase::IndexChkpt,
            Phase::Prepare,
            Phase::InProgress,
            Phase::WaitPending,
            Phase::WaitFlush,
            Phase::PersistenceCallback,
            Phase::Rest,
        ];

        for expected_phase in phases {
            state = state.get_next_state().unwrap();
            assert_eq!(state.phase, expected_phase);
        }

        assert_eq!(state.version, 1);
    }

    #[test]
    fn test_checkpoint_index_state_machine() {
        let mut state = SystemState::new(Action::CheckpointIndex, Phase::Rest, 5);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::PrepIndexChkpt);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::IndexChkpt);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::Rest);

        // Version should not change for index-only checkpoint
        assert_eq!(state.version, 5);
    }

    #[test]
    fn test_gc_state_machine() {
        let mut state = SystemState::new(Action::GC, Phase::Rest, 0);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::GcIoPending);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::GcInProgress);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::Rest);
    }

    #[test]
    fn test_grow_index_state_machine() {
        let mut state = SystemState::new(Action::GrowIndex, Phase::Rest, 0);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::GrowPrepare);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::GrowInProgress);

        state = state.get_next_state().unwrap();
        assert_eq!(state.phase, Phase::Rest);
    }

    #[test]
    fn test_atomic_system_state() {
        let atomic = AtomicSystemState::new(SystemState::rest(10));

        assert_eq!(atomic.phase(), Phase::Rest);
        assert_eq!(atomic.version(), 10);
        assert_eq!(atomic.action(), Action::None);
    }

    #[test]
    fn test_try_start_action() {
        let atomic = AtomicSystemState::new(SystemState::rest(0));

        // Should succeed starting from rest
        let result = atomic.try_start_action(Action::CheckpointFull);
        assert!(result.is_ok());

        // Should fail - action already in progress
        let result = atomic.try_start_action(Action::GC);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_rest() {
        let rest = SystemState::rest(0);
        assert!(rest.is_rest());

        let in_progress = SystemState::new(Action::CheckpointFull, Phase::InProgress, 0);
        assert!(!in_progress.is_rest());
    }

    #[test]
    fn test_is_checkpoint() {
        assert!(SystemState::new(Action::CheckpointFull, Phase::Rest, 0).is_checkpoint());
        assert!(SystemState::new(Action::CheckpointIndex, Phase::Rest, 0).is_checkpoint());
        assert!(SystemState::new(Action::CheckpointHybridLog, Phase::Rest, 0).is_checkpoint());
        assert!(!SystemState::new(Action::GC, Phase::Rest, 0).is_checkpoint());
        assert!(!SystemState::new(Action::GrowIndex, Phase::Rest, 0).is_checkpoint());
    }
}
