//! FasterKV key-value store implementation
//!
//! This module provides the main FasterKV store implementation
//! along with session management and operation contexts.
//!
//! ## Session Types
//!
//! - `Session`: Synchronous session for blocking operations
//! - `AsyncSession`: Asynchronous session for non-blocking operations

mod async_session;
mod contexts;
mod faster_kv;
mod session;
mod state_transitions;

pub use async_session::{AsyncSession, AsyncSessionBuilder};
pub use contexts::{DeleteContext, PendingContext, ReadContext, RmwContext, UpsertContext};
pub use faster_kv::{CheckpointKind, FasterKv, FasterKvConfig};
pub use session::{Session, SessionBuilder, ThreadContext};
pub use state_transitions::{Action, AtomicSystemState, Phase, SystemState};

/// Callback type for async operations
pub type AsyncCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;

