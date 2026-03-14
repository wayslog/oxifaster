use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
    pub(crate) driver_thread_id: AtomicUsize,

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

    /// Driver heartbeat counter (incremented each driver loop iteration).
    pub(crate) driver_heartbeat: AtomicU64,
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
            driver_thread_id: AtomicUsize::new(start.driver_thread_id),
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
            driver_heartbeat: AtomicU64::new(0),
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

    /// Returns thread IDs of participants that have not yet acked.
    pub(crate) fn pending_threads(&self) -> Vec<usize> {
        let not_acked = self.participants & !self.acked;
        (0..MAX_THREADS)
            .filter(|&i| (not_acked & (1u128 << i)) != 0)
            .collect()
    }

    /// Update the driver heartbeat (called by driver thread each loop iteration).
    pub(crate) fn update_heartbeat(&self) {
        self.driver_heartbeat.fetch_add(1, Ordering::Release);
    }

    /// Get current heartbeat value.
    pub(crate) fn heartbeat_value(&self) -> u64 {
        self.driver_heartbeat.load(Ordering::Acquire)
    }

    /// Get driver thread ID.
    pub(crate) fn current_driver_thread_id(&self) -> usize {
        self.driver_thread_id.load(Ordering::Acquire)
    }

    /// Try to take over as driver if the current driver appears stalled.
    /// `last_observed_heartbeat` is the heartbeat value the caller last observed.
    /// Takeover succeeds only if the heartbeat hasn't changed since then.
    #[allow(dead_code)] // Reserved for future driver failover feature
    pub(crate) fn try_takeover(&self, new_driver_id: usize, last_observed_heartbeat: u64) -> bool {
        let current = self.driver_heartbeat.load(Ordering::Acquire);
        if current != last_observed_heartbeat {
            return false;
        }
        let old_driver = self.driver_thread_id.load(Ordering::Acquire);
        match self.driver_thread_id.compare_exchange(
            old_driver,
            new_driver_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(prev) => {
                tracing::warn!(
                    old_driver = prev,
                    new_driver = new_driver_id,
                    "Checkpoint driver takeover"
                );
                true
            }
            Err(_) => false,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Action, Phase};
    use std::path::PathBuf;
    use uuid::Uuid;

    fn make_active(participants: u128, driver: usize) -> ActiveCheckpoint {
        ActiveCheckpoint::new(ActiveCheckpointStart {
            token: Uuid::new_v4(),
            dir: PathBuf::from("/tmp/test"),
            action: Action::CheckpointFull,
            backend: LogCheckpointBackend::Snapshot,
            durability: CheckpointDurability::FasterLike,
            driver_thread_id: driver,
            participants,
            version: 1,
        })
    }

    #[test]
    fn test_pending_threads_all_pending() {
        let active = make_active(0b1011, 0);
        let pending = active.pending_threads();
        assert_eq!(pending, vec![0, 1, 3]);
    }

    #[test]
    fn test_pending_threads_some_acked() {
        let mut active = make_active(0b1011, 0);
        active.set_phase(Phase::WaitPending, 1);
        active.ack(0);
        active.ack(3);
        let pending = active.pending_threads();
        assert_eq!(pending, vec![1]);
    }

    #[test]
    fn test_pending_threads_all_acked() {
        let mut active = make_active(0b1011, 0);
        active.set_phase(Phase::WaitPending, 1);
        active.ack(0);
        active.ack(1);
        active.ack(3);
        let pending = active.pending_threads();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_driver_heartbeat_increments() {
        let active = make_active(0b11, 0);
        assert_eq!(active.heartbeat_value(), 0);
        active.update_heartbeat();
        assert_eq!(active.heartbeat_value(), 1);
        active.update_heartbeat();
        assert_eq!(active.heartbeat_value(), 2);
    }

    #[test]
    fn test_driver_takeover_stale_heartbeat() {
        let active = make_active(0b111, 0);
        assert_eq!(active.current_driver_thread_id(), 0);

        // Heartbeat is 0 and we pass 0 -> stale, takeover succeeds
        assert!(active.try_takeover(1, 0));
        assert_eq!(active.current_driver_thread_id(), 1);
    }

    #[test]
    fn test_driver_takeover_fresh_heartbeat_fails() {
        let active = make_active(0b111, 0);
        active.update_heartbeat(); // heartbeat is now 1

        // Caller observed 0, but heartbeat is 1 -> fresh, takeover fails
        assert!(!active.try_takeover(1, 0));
        assert_eq!(active.current_driver_thread_id(), 0);
    }
}
