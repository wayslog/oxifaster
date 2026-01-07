//! Session management for FASTER
//!
//! This module provides session management for FasterKV operations.
//! Each thread should maintain its own session for optimal performance.
//!
//! ## Session Persistence
//!
//! Sessions support persistence through checkpoint/recovery:
//! - Each session has a unique GUID for identification
//! - A monotonically increasing serial number tracks operations
//! - Session state can be saved to and restored from checkpoints

use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::checkpoint::SessionState;
use crate::device::StorageDevice;
use crate::record::{Key, Value};
use crate::status::Status;
use crate::store::FasterKv;

/// Thread context for FASTER operations
///
/// Tracks per-thread state including the serial number for CPR (Concurrent Prefix Recovery).
/// The serial number is incremented with each operation and is used to determine
/// which operations have been persisted during recovery.
#[derive(Debug, Clone)]
pub struct ThreadContext {
    /// Thread ID
    pub thread_id: usize,
    /// Current version for CPR
    pub version: u32,
    /// Monotonically increasing serial number for this session
    /// Used for Concurrent Prefix Recovery (CPR) to track operation ordering
    pub serial_num: u64,
    /// Number of pending operations
    pub pending_count: u32,
    /// Retry counter
    pub retry_count: u32,
}

impl ThreadContext {
    /// Create a new thread context
    pub fn new(thread_id: usize) -> Self {
        Self {
            thread_id,
            version: 0,
            serial_num: 0,
            pending_count: 0,
            retry_count: 0,
        }
    }

    /// Get the current serial number
    #[inline]
    pub fn serial_num(&self) -> u64 {
        self.serial_num
    }

    /// Increment and return the new serial number
    /// Called after each successful operation for CPR tracking
    #[inline]
    pub fn increment_serial(&mut self) -> u64 {
        self.serial_num += 1;
        self.serial_num
    }

    /// Set the serial number (used during recovery)
    #[inline]
    pub fn set_serial_num(&mut self, serial_num: u64) {
        self.serial_num = serial_num;
    }
}

/// Session for FASTER operations
///
/// A session encapsulates a thread's interaction with a FasterKV store.
/// Sessions are not thread-safe and should not be shared between threads.
///
/// ## Persistence
///
/// Each session has a unique GUID that persists across checkpoint/recovery.
/// The serial number tracks operations for Concurrent Prefix Recovery (CPR).
/// Use `to_session_state()` to capture the session state for checkpointing,
/// and `continue_from_state()` to restore after recovery.
pub struct Session<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Reference to the store
    store: Arc<FasterKv<K, V, D>>,
    /// Unique session identifier (GUID)
    guid: Uuid,
    /// Thread context
    ctx: ThreadContext,
    /// Whether the session is active
    active: bool,
}

impl<K, V, D> Session<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Create a new session with a fresh GUID
    pub(crate) fn new(store: Arc<FasterKv<K, V, D>>, thread_id: usize) -> Self {
        Self {
            store,
            guid: Uuid::new_v4(),
            ctx: ThreadContext::new(thread_id),
            active: false,
        }
    }

    /// Create a session from a saved state (for recovery)
    ///
    /// This restores a session from a previously checkpointed state,
    /// preserving the GUID and serial number.
    pub(crate) fn from_state(
        store: Arc<FasterKv<K, V, D>>,
        thread_id: usize,
        state: &SessionState,
    ) -> Self {
        let mut ctx = ThreadContext::new(thread_id);
        ctx.set_serial_num(state.serial_num);
        Self {
            store,
            guid: state.guid,
            ctx,
            active: false,
        }
    }

    /// Get the session GUID
    #[inline]
    pub fn guid(&self) -> Uuid {
        self.guid
    }

    /// Get the thread ID
    #[inline]
    pub fn thread_id(&self) -> usize {
        self.ctx.thread_id
    }

    /// Get the current version
    #[inline]
    pub fn version(&self) -> u32 {
        self.ctx.version
    }

    /// Get the current serial number
    #[inline]
    pub fn serial_num(&self) -> u64 {
        self.ctx.serial_num()
    }

    /// Convert to SessionState for checkpointing
    ///
    /// Returns a snapshot of the session's persistent state (GUID + serial number)
    /// that can be saved during checkpoint and restored during recovery.
    pub fn to_session_state(&self) -> SessionState {
        SessionState::new(self.guid, self.ctx.serial_num())
    }

    /// Continue session from a restored state
    ///
    /// Called after recovery to update the session's serial number
    /// to match the last checkpointed state.
    pub fn continue_from_state(&mut self, state: &SessionState) {
        if self.guid == state.guid {
            self.ctx.set_serial_num(state.serial_num);
        }
    }

    /// Start the session
    pub fn start(&mut self) {
        if !self.active {
            self.store.epoch().protect(self.ctx.thread_id);
            self.active = true;
            // Register session when starting
            self.store.register_session(&self.to_session_state());
        }
    }

    /// End the session
    pub fn end(&mut self) {
        if self.active {
            // Update final session state before unregistering
            self.store.update_session(&self.to_session_state());
            self.store.epoch().unprotect(self.ctx.thread_id);
            self.active = false;
            // Note: We don't unregister on end() to allow checkpoint to capture
            // sessions that have ended but whose operations should be persisted.
            // Sessions are only fully removed on drop.
        }
    }

    /// Check if the session is active
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Refresh the epoch protection
    ///
    /// This also updates the session state in the registry to keep
    /// checkpoint information current.
    pub fn refresh(&mut self) {
        if self.active {
            self.store.epoch().protect_and_drain(self.ctx.thread_id);
            // Periodically update session state in registry
            self.store.update_session(&self.to_session_state());
        }
    }

    /// Read a value from the store
    ///
    /// Note: Read operations do NOT increment the serial number because they
    /// don't modify state or write to the log. The CPR serial number only tracks
    /// operations that are persisted and need to be recovered.
    pub fn read(&mut self, key: &K) -> Result<Option<V>, Status> {
        if !self.active {
            self.start();
        }

        self.store.read_sync(&mut self.ctx, key)
    }

    /// Upsert a key-value pair
    pub fn upsert(&mut self, key: K, value: V) -> Status {
        if !self.active {
            self.start();
        }

        let status = self.store.upsert_sync(&mut self.ctx, key, value);
        // Increment serial number on successful upsert and update registry
        // This ensures checkpoint captures the correct serial number for CPR
        if status == Status::Ok {
            self.ctx.increment_serial();
            self.store.update_session(&self.to_session_state());
        }
        status
    }

    /// Delete a key
    ///
    /// Only increments the serial number when the delete actually modifies state
    /// (i.e., when the key exists and is deleted). Deleting a non-existent key
    /// doesn't write to the log and shouldn't affect CPR recovery.
    pub fn delete(&mut self, key: &K) -> Status {
        if !self.active {
            self.start();
        }

        let status = self.store.delete_sync(&mut self.ctx, key);
        // Only increment serial number on actual deletion (Status::Ok)
        // NotFound means no state change, so no serial number increment
        if status == Status::Ok {
            self.ctx.increment_serial();
            self.store.update_session(&self.to_session_state());
        }
        status
    }

    /// Perform a read-modify-write operation
    pub fn rmw<F>(&mut self, key: K, modifier: F) -> Status
    where
        F: FnMut(&mut V) -> bool,
    {
        if !self.active {
            self.start();
        }

        let status = self.store.rmw_sync(&mut self.ctx, key, modifier);
        // Increment serial number on successful RMW and update registry
        // This ensures checkpoint captures the correct serial number for CPR
        if status == Status::Ok {
            self.ctx.increment_serial();
            self.store.update_session(&self.to_session_state());
        }
        status
    }

    /// Complete all pending operations
    ///
    /// # Arguments
    ///
    /// * `wait` - If true, blocks until all pending operations complete (with timeout protection)
    ///
    /// # Returns
    ///
    /// Returns `true` if all pending operations have completed, `false` otherwise.
    ///
    /// # Timeout
    ///
    /// When `wait=true`, this method will timeout after 30 seconds to prevent infinite blocking
    /// in case of I/O worker thread failure. Use `complete_pending_with_timeout` for custom timeout.
    pub fn complete_pending(&mut self, wait: bool) -> bool {
        self.complete_pending_with_timeout(wait, Duration::from_secs(30))
    }

    /// Complete all pending operations with custom timeout
    ///
    /// # Arguments
    ///
    /// * `wait` - If true, blocks until all pending operations complete or timeout
    /// * `timeout` - Maximum duration to wait when `wait=true`
    ///
    /// # Returns
    ///
    /// Returns `true` if all pending operations have completed, `false` if timeout or still pending.
    pub fn complete_pending_with_timeout(&mut self, wait: bool, timeout: Duration) -> bool {
        // 即使 session 未 active，也允许驱动全局 I/O 完成事件，避免"只有发起者能推动完成"造成停滞。
        let start = Instant::now();

        loop {
            let completed = self.store.take_completed_io_for_thread(self.ctx.thread_id);
            if completed > 0 {
                self.ctx.pending_count = self.ctx.pending_count.saturating_sub(completed);
            }

            if !wait || self.ctx.pending_count == 0 {
                break;
            }

            // 超时保护：防止 I/O worker 线程崩溃导致无限阻塞
            if start.elapsed() >= timeout {
                break;
            }

            // wait=true：阻塞等待，期间刷新 epoch 并让出时间片
            if self.active {
                self.refresh();
            }
            std::thread::yield_now();
        }

        self.ctx.pending_count == 0
    }

    /// Get a reference to the thread context
    pub fn context(&self) -> &ThreadContext {
        &self.ctx
    }

    /// Get a mutable reference to the thread context
    pub fn context_mut(&mut self) -> &mut ThreadContext {
        &mut self.ctx
    }
}

impl<K, V, D> Drop for Session<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    fn drop(&mut self) {
        self.end();
        // Unregister from the session registry on drop
        self.store.unregister_session(self.guid);
    }
}

/// Session builder for configuring session options
pub struct SessionBuilder<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    store: Arc<FasterKv<K, V, D>>,
    thread_id: Option<usize>,
    session_state: Option<SessionState>,
}

impl<K, V, D> SessionBuilder<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Create a new session builder
    pub fn new(store: Arc<FasterKv<K, V, D>>) -> Self {
        Self {
            store,
            thread_id: None,
            session_state: None,
        }
    }

    /// Set the thread ID
    pub fn thread_id(mut self, id: usize) -> Self {
        self.thread_id = Some(id);
        self
    }

    /// Restore from a saved session state (for recovery)
    ///
    /// When building with a session state, the session will use
    /// the same GUID and continue from the checkpointed serial number.
    pub fn from_state(mut self, state: SessionState) -> Self {
        self.session_state = Some(state);
        self
    }

    /// Build the session
    pub fn build(self) -> Session<K, V, D> {
        let thread_id = self.thread_id.unwrap_or(0);

        match self.session_state {
            Some(state) => Session::from_state(self.store, thread_id, &state),
            None => Session::new(self.store, thread_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_context() {
        let ctx = ThreadContext::new(5);
        assert_eq!(ctx.thread_id, 5);
        assert_eq!(ctx.version, 0);
        assert_eq!(ctx.serial_num, 0);
        assert_eq!(ctx.pending_count, 0);
    }

    #[test]
    fn test_thread_context_serial_num() {
        let mut ctx = ThreadContext::new(0);
        assert_eq!(ctx.serial_num(), 0);

        let new_serial = ctx.increment_serial();
        assert_eq!(new_serial, 1);
        assert_eq!(ctx.serial_num(), 1);

        ctx.set_serial_num(100);
        assert_eq!(ctx.serial_num(), 100);
    }

    #[test]
    fn test_session_state_conversion() {
        let guid = Uuid::new_v4();
        let state = SessionState::new(guid, 42);

        assert_eq!(state.guid, guid);
        assert_eq!(state.serial_num, 42);
    }
}
