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
//! 3. Retry the operation up to `MAX_PENDING_RETRIES` times
//! 4. Return `Status::Pending` to the caller if retries are exhausted
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
//!     let store = FasterKv::new(config, device);
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

use uuid::Uuid;

use crate::checkpoint::SessionState;
use crate::codec::{PersistKey, PersistValue};
use crate::device::StorageDevice;
use crate::status::Status;
use crate::store::{FasterKv, Session, ThreadContext};

/// Maximum number of retries for pending operations before returning Pending to caller.
///
/// This prevents infinite loops when data is on disk and async I/O is not available.
/// After this many retries, the operation returns `Status::Pending` to let the caller
/// decide how to handle it.
const MAX_PENDING_RETRIES: u32 = 10;

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
        for _ in 0..=MAX_PENDING_RETRIES {
            let status = op(&mut self.inner);
            if status != Status::Pending {
                return status;
            }
            self.drive_pending_once().await;
        }
        Status::Pending
    }

    async fn drive_pending_once(&mut self) {
        self.inner.complete_pending(false);
        tokio_yield().await;
        self.refresh();
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
    /// This wrapper retries up to `MAX_PENDING_RETRIES` times when the operation
    /// returns `Pending`. If retries are exhausted, it returns `Pending` to the
    /// caller to handle (e.g., retry later with read cache enabled).
    pub async fn read_async(&mut self, key: &K) -> Result<Option<V>, Status> {
        let mut retries = 0u32;
        loop {
            match self.inner.read(key) {
                Ok(value) => return Ok(value),
                Err(Status::Pending) => {
                    retries += 1;
                    if retries > MAX_PENDING_RETRIES {
                        // Exhausted retries - data is likely on disk without async I/O support
                        return Err(Status::Pending);
                    }
                    self.drive_pending_once().await;
                }
                Err(status) => return Err(status),
            }
        }
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
    /// - `false` if retries exhausted with operations still pending
    pub async fn complete_pending_async(&mut self) -> bool {
        for _ in 0..=MAX_PENDING_RETRIES {
            if self.inner.complete_pending(false) {
                return true;
            }
            self.drive_pending_once().await;
        }
        false
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

/// Yield point for async operations
///
/// This is a placeholder that yields control to the async runtime.
/// In a full implementation, this would integrate with actual I/O completion.
async fn tokio_yield() {
    YieldOnce::new().await
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

    fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20,
            page_size_bits: 14,
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device))
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
}
