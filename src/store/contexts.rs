//! Operation contexts for FASTER
//!
//! This module defines the context traits and types for FASTER operations.

use crate::address::Address;
use crate::record::{Key, Value};
use crate::status::{OperationStatus, OperationType};

/// Context trait for Read operations
pub trait ReadContext<K: Key, V: Value>: Send + Sync {
    /// Get the key to read
    fn key(&self) -> &K;

    /// Called when the value is found
    fn get(&mut self, value: &V);

    /// Called when the operation completes
    fn on_complete(&mut self, status: OperationStatus) {
        let _ = status;
    }
}

/// Context trait for Upsert operations
pub trait UpsertContext<K: Key, V: Value>: Send + Sync {
    /// Get the key to upsert
    fn key(&self) -> &K;

    /// Get the value to upsert
    fn value(&self) -> &V;

    /// Called when the operation completes
    fn on_complete(&mut self, status: OperationStatus) {
        let _ = status;
    }
}

/// Context trait for RMW (Read-Modify-Write) operations
pub trait RmwContext<K: Key, V: Value>: Send + Sync {
    /// Get the key
    fn key(&self) -> &K;

    /// Get the initial value (used if key doesn't exist)
    fn initial_value(&self) -> V;

    /// Modify an existing value in place
    ///
    /// Returns true if modification was successful, false to abort.
    fn in_place_update(&mut self, value: &mut V) -> bool;

    /// Create a new value based on the old value
    ///
    /// Used when in-place update is not possible.
    fn copy_update(&mut self, old_value: &V) -> V {
        // Default: just use in_place_update logic
        let mut new_value = old_value.clone();
        self.in_place_update(&mut new_value);
        new_value
    }

    /// Called when the operation completes
    fn on_complete(&mut self, status: OperationStatus) {
        let _ = status;
    }
}

/// Context trait for Delete operations
pub trait DeleteContext<K: Key, V: Value>: Send + Sync {
    /// Get the key to delete
    fn key(&self) -> &K;

    /// Called when the operation completes
    fn on_complete(&mut self, status: OperationStatus) {
        let _ = status;
    }
}

/// Base class for pending async contexts
#[derive(Debug)]
pub struct PendingContext {
    /// Operation type
    pub operation_type: OperationType,
    /// Address of the record (for async operations)
    pub address: Address,
    /// Entry address in hash index
    pub entry_address: Address,
    /// Version when operation was started
    pub version: u32,
    /// Thread ID
    pub thread_id: usize,
}

impl PendingContext {
    /// Create a new pending context
    pub fn new(operation_type: OperationType, thread_id: usize) -> Self {
        Self {
            operation_type,
            address: Address::INVALID,
            entry_address: Address::INVALID,
            version: 0,
            thread_id,
        }
    }
}

impl Default for PendingContext {
    fn default() -> Self {
        Self::new(OperationType::Read, 0)
    }
}

/// Simple read context implementation
pub struct SimpleReadContext<K: Key, V: Value> {
    /// The key to read
    pub key: K,
    /// The value found (if any)
    pub value: Option<V>,
}

impl<K: Key, V: Value> SimpleReadContext<K, V> {
    /// Create a new simple read context
    pub fn new(key: K) -> Self {
        Self { key, value: None }
    }

    /// Take the value out
    pub fn take_value(&mut self) -> Option<V> {
        self.value.take()
    }
}

impl<K: Key, V: Value> ReadContext<K, V> for SimpleReadContext<K, V> {
    fn key(&self) -> &K {
        &self.key
    }

    fn get(&mut self, value: &V) {
        self.value = Some(value.clone());
    }
}

/// Simple upsert context implementation
pub struct SimpleUpsertContext<K: Key, V: Value> {
    /// The key to upsert
    pub key: K,
    /// The value to upsert
    pub value: V,
}

impl<K: Key, V: Value> SimpleUpsertContext<K, V> {
    /// Create a new simple upsert context
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K: Key, V: Value> UpsertContext<K, V> for SimpleUpsertContext<K, V> {
    fn key(&self) -> &K {
        &self.key
    }

    fn value(&self) -> &V {
        &self.value
    }
}

/// Simple delete context implementation
pub struct SimpleDeleteContext<K: Key> {
    /// The key to delete
    pub key: K,
}

impl<K: Key> SimpleDeleteContext<K> {
    /// Create a new simple delete context
    pub fn new(key: K) -> Self {
        Self { key }
    }
}

impl<K: Key, V: Value> DeleteContext<K, V> for SimpleDeleteContext<K> {
    fn key(&self) -> &K {
        &self.key
    }
}

/// RMW context for incrementing numeric values
pub struct IncrementContext<K: Key> {
    /// The key
    pub key: K,
    /// Amount to increment by
    pub delta: i64,
}

impl<K: Key> IncrementContext<K> {
    /// Create a new increment context
    pub fn new(key: K, delta: i64) -> Self {
        Self { key, delta }
    }
}

impl<K: Key> RmwContext<K, i64> for IncrementContext<K> {
    fn key(&self) -> &K {
        &self.key
    }

    fn initial_value(&self) -> i64 {
        self.delta
    }

    fn in_place_update(&mut self, value: &mut i64) -> bool {
        *value = value.wrapping_add(self.delta);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_read_context() {
        let mut ctx = SimpleReadContext::<u64, u64>::new(42);
        assert_eq!(*ctx.key(), 42);
        assert!(ctx.value.is_none());

        ctx.get(&100);
        assert_eq!(ctx.value, Some(100));
    }

    #[test]
    fn test_simple_upsert_context() {
        let ctx = SimpleUpsertContext::new(42u64, 100u64);
        assert_eq!(*ctx.key(), 42);
        assert_eq!(*ctx.value(), 100);
    }

    #[test]
    fn test_increment_context() {
        let mut ctx = IncrementContext::new(42u64, 10);
        assert_eq!(ctx.initial_value(), 10);

        let mut value = 5i64;
        ctx.in_place_update(&mut value);
        assert_eq!(value, 15);
    }

    #[test]
    fn test_pending_context() {
        let ctx = PendingContext::new(OperationType::Read, 0);
        assert_eq!(ctx.operation_type, OperationType::Read);
        assert!(ctx.address.is_invalid());
    }
}
