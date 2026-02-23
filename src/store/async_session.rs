//! Async session management for FASTER
//!
//! This module provides async session management for FasterKV operations.
//! It enables non-blocking operations using Rust's async/await syntax.
//!
//! ## Pending Status Handling
//!
//! When an operation returns `Status::Pending`, it means the data is on disk
//! and requires I/O to complete. The async wrappers will:
//! 1. Yield to allow other tasks to run
//! 2. Attempt to drive I/O completions via `complete_pending()`
//! 3. Retry the operation with adaptive backoff
//! 4. Return `Status::Pending` when retry budget is exhausted
//!
//! **Note**: Full async I/O for disk-backed records is not yet implemented.
//! Operations that would require reading from disk will eventually return
//! `Status::Pending` to the caller, who can then decide how to handle it
//! (e.g., retry later, use read cache, or accept the miss).
//!
//! ## Example
//!
//! ```ignore
//! use oxifaster::store::{FasterKv, AsyncSession};
//!
//! async fn example() {
//!     let store = FasterKv::new(config, device).unwrap();
//!     let mut session = store
//!         .start_async_session()
//!         .expect("failed to start async session");
//!     
//!     // Async operations
//!     session.upsert_async(key, value).await;
//!     let result = session.read_async(&key).await;
//!     
//!     // Handle pending status
//!     match result {
//!         Ok(value) => { /* use value */ }
//!         Err(Status::Pending) => { /* data is on disk, retry later */ }
//!         Err(status) => { /* other error */ }
//!     }
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::checkpoint::SessionState;
use crate::codec::{PersistKey, PersistValue};
use crate::device::StorageDevice;
use crate::status::Status;
use crate::store::{FasterKv, Session, ThreadContext};

/// Upper bound of pending polls for a single async operation.
///
/// Timeout is still the primary budget (`FasterKv::wait_pending_timeout`), while this
/// prevents pathological infinite loops when the clock cannot advance as expected.
const MAX_PENDING_POLLS: u32 = 256;

/// Initial delay before retrying a pending operation.
const PENDING_RETRY_DELAY_MIN: Duration = Duration::from_micros(50);

/// Maximum delay between pending retries.
const PENDING_RETRY_DELAY_MAX: Duration = Duration::from_millis(5);

#[derive(Debug, Clone)]
struct PendingRetryState {
    attempts: u32,
    deadline: Instant,
    next_delay: Duration,
}

impl PendingRetryState {
    fn new(timeout: Duration) -> Self {
        let now = Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        Self {
            attempts: 0,
            deadline,
            next_delay: PENDING_RETRY_DELAY_MIN,
        }
    }

    #[inline]
    fn exhausted(&self) -> bool {
        self.attempts >= MAX_PENDING_POLLS || Instant::now() >= self.deadline
    }

    fn next_backoff(&mut self) -> Duration {
        self.attempts = self.attempts.saturating_add(1);
        let delay = self.next_delay;
        self.next_delay = self
            .next_delay
            .saturating_mul(2)
            .min(PENDING_RETRY_DELAY_MAX);
        delay
    }
}

/// Async session for FASTER operations
///
/// Provides async/await versions of the standard session operations.
/// Each async session wraps a synchronous session and provides
/// non-blocking versions of operations that may require I/O.
pub struct AsyncSession<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Inner synchronous session
    inner: Session<K, V, D>,
}

impl<K, V, D> AsyncSession<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Helper to retry an operation that returns Status until it succeeds or retries are exhausted.
    async fn retry_status_op<F>(&mut self, mut op: F) -> Status
    where
        F: FnMut(&mut Session<K, V, D>) -> Status,
    {
        let mut retry = PendingRetryState::new(self.inner.wait_pending_timeout());
        loop {
            let status = op(&mut self.inner);
            if status != Status::Pending {
                return status;
            }
            if retry.exhausted() {
                return Status::Pending;
            }
            self.drive_pending_once(retry.next_backoff()).await;
        }
    }

    async fn retry_result_op<T, F>(&mut self, mut op: F) -> Result<T, Status>
    where
        F: FnMut(&mut Session<K, V, D>) -> Result<T, Status>,
    {
        let mut retry = PendingRetryState::new(self.inner.wait_pending_timeout());
        loop {
            match op(&mut self.inner) {
                Ok(value) => return Ok(value),
                Err(Status::Pending) => {
                    if retry.exhausted() {
                        return Err(Status::Pending);
                    }
                    self.drive_pending_once(retry.next_backoff()).await;
                }
                Err(status) => return Err(status),
            }
        }
    }

    async fn drive_pending_once(&mut self, delay: Duration) {
        self.inner.complete_pending(false);
        self.refresh();
        runtime_pause(delay).await;
    }

    /// Create a new async session wrapping a synchronous session
    pub(crate) fn new(inner: Session<K, V, D>) -> Self {
        Self { inner }
    }

    /// Get the session GUID
    #[inline]
    pub fn guid(&self) -> Uuid {
        self.inner.guid()
    }

    /// Get the thread ID
    #[inline]
    pub fn thread_id(&self) -> usize {
        self.inner.thread_id()
    }

    /// Get the current version
    #[inline]
    pub fn version(&self) -> u32 {
        self.inner.version()
    }

    /// Get the current serial number
    #[inline]
    pub fn serial_num(&self) -> u64 {
        self.inner.serial_num()
    }

    /// Convert to SessionState for checkpointing
    pub fn to_session_state(&self) -> SessionState {
        self.inner.to_session_state()
    }

    /// Continue session from a restored state
    pub fn continue_from_state(&mut self, state: &SessionState) {
        self.inner.continue_from_state(state);
    }

    /// Start the session
    pub fn start(&mut self) {
        self.inner.start();
    }

    /// End the session
    pub fn end(&mut self) {
        self.inner.end();
    }

    /// Check if the session is active
    #[inline]
    pub fn is_active(&self) -> bool {
        self.inner.is_active()
    }

    /// Refresh the epoch protection
    pub fn refresh(&mut self) {
        self.inner.refresh();
    }

    /// Get a reference to the thread context
    pub fn context(&self) -> &ThreadContext {
        self.inner.context()
    }

    /// Get a mutable reference to the thread context
    pub fn context_mut(&mut self) -> &mut ThreadContext {
        self.inner.context_mut()
    }

    // ==================== Async Operations ====================

    /// Async read operation
    ///
    /// Reads a value from the store. Returns a future that resolves to
    /// the value if found, or an error status.
    ///
    /// # Returns
    /// - `Ok(Some(value))` if the key was found
    /// - `Ok(None)` if the key was not found
    /// - `Err(Status::Pending)` if the data is on disk and async I/O exhausted retries
    /// - `Err(status)` for other errors
    ///
    /// # Pending Handling
    /// This wrapper retries with adaptive backoff until the store timeout budget
    /// (`FasterKv::wait_pending_timeout`) is exhausted.
    pub async fn read_async(&mut self, key: &K) -> Result<Option<V>, Status> {
        self.retry_result_op(|session| session.read(key)).await
    }

    /// Async upsert operation
    ///
    /// Inserts or updates a key-value pair. Returns a future that
    /// resolves to the operation status.
    ///
    /// # Returns
    /// - `Status::Ok` on success
    /// - `Status::Pending` if the operation couldn't complete after retries
    /// - Other status for errors
    pub async fn upsert_async(&mut self, key: K, value: V) -> Status {
        self.retry_status_op(|session| session.upsert(key.clone(), value.clone()))
            .await
    }

    /// Async delete operation
    ///
    /// Deletes a key from the store. Returns a future that
    /// resolves to the operation status.
    ///
    /// # Returns
    /// - `Status::Ok` on success
    /// - `Status::Pending` if the operation couldn't complete after retries
    /// - Other status for errors
    pub async fn delete_async(&mut self, key: &K) -> Status {
        let key_clone = key.clone();
        self.retry_status_op(|session| session.delete(&key_clone))
            .await
    }

    /// Async RMW (read-modify-write) operation
    ///
    /// Performs an atomic read-modify-write operation. The modifier
    /// function is called with the current value and should return
    /// true to commit the modification, false to abort.
    ///
    /// # Returns
    /// - `Status::Ok` on success
    /// - `Status::Pending` if the operation couldn't complete after retries
    /// - Other status for errors
    pub async fn rmw_async<F>(&mut self, key: K, modifier: F) -> Status
    where
        F: FnMut(&mut V) -> bool + Clone,
    {
        self.retry_status_op(|session| session.rmw(key.clone(), modifier.clone()))
            .await
    }

    /// Async conditional insert operation
    ///
    /// Inserts the key-value pair only if the key does not already exist.
    /// Returns `Status::Ok` if inserted, `Status::Aborted` if key already exists.
    pub async fn conditional_insert_async(&mut self, key: K, value: V) -> Status {
        self.retry_status_op(|session| session.conditional_insert(key.clone(), value.clone()))
            .await
    }

    /// Complete all pending operations asynchronously
    ///
    /// # Returns
    /// - `true` if all pending operations completed
    /// - `false` if timeout/retry budget exhausted with operations still pending
    pub async fn complete_pending_async(&mut self) -> bool {
        let mut retry = PendingRetryState::new(self.inner.wait_pending_timeout());
        loop {
            if self.inner.complete_pending(false) {
                return true;
            }
            if retry.exhausted() {
                return false;
            }
            self.drive_pending_once(retry.next_backoff()).await;
        }
    }

    // ==================== Sync Fallback Operations ====================
    // These provide sync operations for cases where async is not needed

    /// Synchronous read (delegates to inner session)
    pub fn read(&mut self, key: &K) -> Result<Option<V>, Status> {
        self.inner.read(key)
    }

    /// Synchronous upsert (delegates to inner session)
    pub fn upsert(&mut self, key: K, value: V) -> Status {
        self.inner.upsert(key, value)
    }

    /// Synchronous delete (delegates to inner session)
    pub fn delete(&mut self, key: &K) -> Status {
        self.inner.delete(key)
    }

    /// Synchronous RMW (delegates to inner session)
    pub fn rmw<F>(&mut self, key: K, modifier: F) -> Status
    where
        F: FnMut(&mut V) -> bool,
    {
        self.inner.rmw(key, modifier)
    }

    /// Synchronous conditional insert (delegates to inner session)
    pub fn conditional_insert(&mut self, key: K, value: V) -> Status {
        self.inner.conditional_insert(key, value)
    }

    /// Complete pending operations synchronously
    pub fn complete_pending(&mut self, wait: bool) -> bool {
        self.inner.complete_pending(wait)
    }
}

/// Runtime-aware async pause used by pending retry loops.
///
/// When a Tokio runtime is available, this yields/sleeps without blocking.
/// Outside Tokio, it falls back to a tiny blocking sleep to avoid busy spinning.
async fn runtime_pause(delay: Duration) {
    if delay.is_zero() {
        runtime_yield().await;
        return;
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::time::sleep(delay).await;
    } else {
        std::thread::sleep(delay);
    }
}

async fn runtime_yield() {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::yield_now().await;
    } else {
        YieldOnce::new().await;
    }
}

/// Simple yield-once future
struct YieldOnce {
    yielded: bool,
}

impl YieldOnce {
    fn new() -> Self {
        Self { yielded: false }
    }
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Async session builder
pub struct AsyncSessionBuilder<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    store: Arc<FasterKv<K, V, D>>,
    thread_id: Option<usize>,
    session_state: Option<SessionState>,
}

impl<K, V, D> AsyncSessionBuilder<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Create a new async session builder
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
    pub fn from_state(mut self, state: SessionState) -> Self {
        self.session_state = Some(state);
        self
    }

    /// Build the async session
    pub fn build(self) -> AsyncSession<K, V, D> {
        let thread_id = self.thread_id.unwrap_or(0);
        let inner = match self.session_state {
            Some(state) => Session::from_state(self.store, thread_id, &state),
            None => Session::new(self.store, thread_id),
        };
        AsyncSession::new(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::NullDisk;
    use crate::store::FasterKvConfig;
    use std::time::Duration;

    fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device).unwrap())
    }

    #[test]
    fn test_async_session_creation() {
        let store = create_test_store();
        let session = AsyncSession::new(Session::new(store.clone(), 0));

        assert!(!session.is_active());
    }

    #[test]
    fn test_async_session_builder() {
        let store = create_test_store();
        let session = AsyncSessionBuilder::new(store).thread_id(5).build();

        assert_eq!(session.thread_id(), 5);
    }

    #[test]
    fn test_async_session_sync_operations() {
        let store = create_test_store();
        let mut session = AsyncSession::new(Session::new(store, 0));

        // Sync operations should work
        assert_eq!(session.upsert(1u64, 100u64), Status::Ok);
        assert_eq!(session.read(&1u64), Ok(Some(100u64)));
    }

    #[test]
    fn test_yield_once() {
        // Test that YieldOnce works correctly
        use std::task::{RawWaker, RawWakerVTable, Waker};

        static VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );

        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);

        let mut future = YieldOnce::new();
        let pinned = Pin::new(&mut future);

        // First poll should return Pending
        assert!(matches!(pinned.poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn test_retry_status_op_honors_zero_timeout_budget() {
        let store = create_test_store();
        store.set_wait_pending_timeout(Duration::ZERO);
        let mut session = AsyncSession::new(Session::new(store, 0));

        let mut attempts = 0u32;
        let status = tokio_test::block_on(async {
            session
                .retry_status_op(|_| {
                    attempts = attempts.saturating_add(1);
                    Status::Pending
                })
                .await
        });

        assert_eq!(status, Status::Pending);
        assert_eq!(attempts, 1);
    }

    #[test]
    fn test_pending_retry_state_backoff_is_capped() {
        let mut state = PendingRetryState::new(Duration::from_secs(1));
        let mut saw_cap = false;

        for _ in 0..64 {
            let delay = state.next_backoff();
            if delay == PENDING_RETRY_DELAY_MAX {
                saw_cap = true;
                break;
            }
        }

        assert!(saw_cap);
    }
}
