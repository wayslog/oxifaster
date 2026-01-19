use std::path::PathBuf;

use parking_lot::Mutex;

use crate::address::Address;
use crate::checkpoint::{CheckpointToken, IndexMetadata, LogMetadata, SessionState};
use crate::constants::MAX_THREADS;
use crate::store::{Action, Phase};

/// Log checkpoint backend.
///
/// - `Snapshot`: writes a `log.snapshot` file in the checkpoint directory.
/// - `FoldOver`: relies on the main log device + `log.meta` (no `log.snapshot`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogCheckpointBackend {
    /// Snapshot checkpoint: writes `log.snapshot` into the checkpoint directory.
    Snapshot,
    /// Fold-over checkpoint: relies on the main log device (no `log.snapshot`).
    FoldOver,
}

impl From<u8> for LogCheckpointBackend {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::FoldOver,
            _ => Self::Snapshot,
        }
    }
}

impl From<LogCheckpointBackend> for u8 {
    fn from(value: LogCheckpointBackend) -> Self {
        value as u8
    }
}

/// Checkpoint artifact durability mode.
///
/// `FasterLike` avoids forced fsync, while `FsyncOnCheckpoint` fsyncs checkpoint artifacts
/// (not the main log device) and fails the checkpoint if the file fsyncs fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CheckpointDurability {
    /// Faster-like: do not force `fsync`; rely on normal completion semantics.
    FasterLike,
    /// Fsync checkpoint artifacts only (snapshot/delta/index/meta); errors if file fsync fails.
    FsyncOnCheckpoint,
}

impl From<u8> for CheckpointDurability {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::FsyncOnCheckpoint,
            _ => Self::FasterLike,
        }
    }
}

impl From<CheckpointDurability> for u8 {
    fn from(value: CheckpointDurability) -> Self {
        value as u8
    }
}

#[derive(Debug)]
pub(crate) struct ActiveCheckpoint {
    pub(crate) token: CheckpointToken,
    pub(crate) dir: PathBuf,
    pub(crate) action: Action,
    pub(crate) backend: LogCheckpointBackend,
    pub(crate) durability: CheckpointDurability,
    pub(crate) driver_thread_id: usize,

    participants: u128,
    phase: Phase,
    version: u32,
    acked: u128,

    pub(crate) begin_address: Address,
    pub(crate) final_address: Address,
    pub(crate) index_metadata: Option<IndexMetadata>,
    pub(crate) log_metadata: Option<LogMetadata>,
    pub(crate) session_states: Vec<SessionState>,

    pub(crate) snapshot_written: bool,
}

#[derive(Debug)]
pub(crate) struct ActiveCheckpointStart {
    pub(crate) token: CheckpointToken,
    pub(crate) dir: PathBuf,
    pub(crate) action: Action,
    pub(crate) backend: LogCheckpointBackend,
    pub(crate) durability: CheckpointDurability,
    pub(crate) driver_thread_id: usize,
    pub(crate) participants: u128,
    pub(crate) version: u32,
}

impl ActiveCheckpoint {
    pub(crate) fn new(start: ActiveCheckpointStart) -> Self {
        Self {
            token: start.token,
            dir: start.dir,
            action: start.action,
            backend: start.backend,
            durability: start.durability,
            driver_thread_id: start.driver_thread_id,
            participants: start.participants,
            phase: Phase::Rest,
            version: start.version,
            acked: 0,
            begin_address: Address::INVALID,
            final_address: Address::INVALID,
            index_metadata: None,
            log_metadata: None,
            session_states: Vec::new(),
            snapshot_written: false,
        }
    }

    pub(crate) fn is_participant(&self, thread_id: usize) -> bool {
        if thread_id >= MAX_THREADS {
            return false;
        }
        (self.participants & (1u128 << thread_id)) != 0
    }

    pub(crate) fn mark_thread_inactive(&mut self, thread_id: usize) {
        if thread_id >= MAX_THREADS {
            return;
        }
        self.participants &= !(1u128 << thread_id);
        self.acked &= self.participants;
    }

    pub(crate) fn set_phase(&mut self, phase: Phase, version: u32) {
        if self.phase == phase && self.version == version {
            return;
        }
        self.phase = phase;
        self.version = version;
        self.acked = 0;
    }

    pub(crate) fn ack(&mut self, thread_id: usize) {
        if thread_id >= MAX_THREADS {
            return;
        }
        if !self.is_participant(thread_id) {
            return;
        }
        self.acked |= 1u128 << thread_id;
    }

    pub(crate) fn barrier_complete(&self) -> bool {
        self.acked == self.participants
    }
}

#[derive(Debug, Default)]
pub(crate) struct CprCoordinator {
    inner: Mutex<Option<ActiveCheckpoint>>,
}

impl CprCoordinator {
    pub(crate) fn start(&self, active: ActiveCheckpoint) {
        *self.inner.lock() = Some(active);
    }

    pub(crate) fn clear(&self) {
        *self.inner.lock() = None;
    }

    pub(crate) fn with_active_mut<R>(
        &self,
        f: impl FnOnce(&mut ActiveCheckpoint) -> R,
    ) -> Option<R> {
        let mut guard = self.inner.lock();
        let active = guard.as_mut()?;
        Some(f(active))
    }

    pub(crate) fn with_active<R>(&self, f: impl FnOnce(&ActiveCheckpoint) -> R) -> Option<R> {
        let guard = self.inner.lock();
        let active = guard.as_ref()?;
        Some(f(active))
    }
}
