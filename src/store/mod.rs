//! FasterKV key-value store implementation
//!
//! This module provides the main FasterKV store implementation
//! along with session management and operation contexts.

mod faster_kv;
mod session;
mod contexts;

pub use faster_kv::{FasterKv, FasterKvConfig, SystemPhase, SystemState};
pub use session::{Session, ThreadContext};
pub use contexts::{
    ReadContext, UpsertContext, RmwContext, DeleteContext,
    PendingContext,
};

/// Callback type for async operations
pub type AsyncCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;

