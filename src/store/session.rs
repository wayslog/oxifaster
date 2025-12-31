//! Session management for FASTER
//!
//! This module provides session management for FasterKV operations.
//! Each thread should maintain its own session for optimal performance.

use std::sync::Arc;

use crate::record::{Key, Value};
use crate::status::Status;
use crate::store::FasterKv;
use crate::device::StorageDevice;

/// Thread context for FASTER operations
#[derive(Debug)]
pub struct ThreadContext {
    /// Thread ID
    pub thread_id: usize,
    /// Current version for CPR
    pub version: u32,
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
            pending_count: 0,
            retry_count: 0,
        }
    }
}

/// Session for FASTER operations
///
/// A session encapsulates a thread's interaction with a FasterKV store.
/// Sessions are not thread-safe and should not be shared between threads.
pub struct Session<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Reference to the store
    store: Arc<FasterKv<K, V, D>>,
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
    /// Create a new session
    pub(crate) fn new(store: Arc<FasterKv<K, V, D>>, thread_id: usize) -> Self {
        Self {
            store,
            ctx: ThreadContext::new(thread_id),
            active: false,
        }
    }

    /// Get the thread ID
    pub fn thread_id(&self) -> usize {
        self.ctx.thread_id
    }

    /// Get the current version
    pub fn version(&self) -> u32 {
        self.ctx.version
    }

    /// Start the session
    pub fn start(&mut self) {
        if !self.active {
            self.store.epoch().protect(self.ctx.thread_id);
            self.active = true;
        }
    }

    /// End the session
    pub fn end(&mut self) {
        if self.active {
            self.store.epoch().unprotect(self.ctx.thread_id);
            self.active = false;
        }
    }

    /// Refresh the epoch protection
    pub fn refresh(&mut self) {
        if self.active {
            self.store.epoch().protect_and_drain(self.ctx.thread_id);
        }
    }

    /// Read a value from the store
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

        self.store.upsert_sync(&mut self.ctx, key, value)
    }

    /// Delete a key
    pub fn delete(&mut self, key: &K) -> Status {
        if !self.active {
            self.start();
        }

        self.store.delete_sync(&mut self.ctx, key)
    }

    /// Perform a read-modify-write operation
    pub fn rmw<F>(&mut self, key: K, modifier: F) -> Status
    where
        F: FnMut(&mut V) -> bool,
    {
        if !self.active {
            self.start();
        }

        self.store.rmw_sync(&mut self.ctx, key, modifier)
    }

    /// Complete all pending operations
    pub fn complete_pending(&mut self, wait: bool) -> bool {
        if !self.active {
            return true;
        }

        if self.ctx.pending_count == 0 {
            return true;
        }

        if wait {
            while self.ctx.pending_count > 0 {
                self.refresh();
            }
        }

        self.ctx.pending_count == 0
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
        }
    }

    /// Set the thread ID
    pub fn thread_id(mut self, id: usize) -> Self {
        self.thread_id = Some(id);
        self
    }

    /// Build the session
    pub fn build(self) -> Session<K, V, D> {
        let thread_id = self.thread_id.unwrap_or_else(|| {
            // Get a thread ID from the store
            0 // Default for now
        });
        
        Session::new(self.store, thread_id)
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
        assert_eq!(ctx.pending_count, 0);
    }
}

