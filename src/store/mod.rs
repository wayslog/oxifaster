//! FasterKV key-value store implementation
//!
//! This module provides the main FasterKV store implementation
//! along with session management and operation contexts.
//!
//! # Overview
//!
//! FasterKV is a high-performance concurrent key-value store that supports
//! datasets larger than memory through its hybrid log architecture.
//!
//! # Session Types
//!
//! - [`Session`]: Synchronous session for blocking operations
//! - [`AsyncSession`]: Asynchronous session for non-blocking operations
//!
//! # Key Features
//!
//! - **CRUD Operations**: Read, Upsert, Delete, and Read-Modify-Write (RMW)
//! - **Concurrent Access**: Lock-free operations with epoch protection
//! - **Persistence**: Checkpoint/Recovery with CPR protocol
//! - **Session Persistence**: Sessions survive checkpoint/recovery cycles
//!
//! # Usage
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use oxifaster::store::{FasterKv, FasterKvConfig, Session};
//! use oxifaster::device::NullDisk;
//!
//! // Create store
//! let store = Arc::new(FasterKv::new(FasterKvConfig::default(), NullDisk::new()));
//!
//! // Start a session (bound to current thread)
//! let mut session = store.start_session().expect("failed to start session");
//!
//! // Perform operations
//! session.upsert(1u64, 100u64);
//! let value = session.read(&1u64)?;
//! session.rmw(1u64, |v| { *v += 1; true });
//! session.delete(&1u64);
//!
//! // Session is automatically ended on drop
//! ```
//!
//! # Thread Safety
//!
//! - `FasterKv` is `Send + Sync` and can be shared across threads
//! - `Session` is NOT thread-safe; each thread needs its own session
//! - Use `Arc<FasterKv>` to share the store across threads

mod async_session;
mod contexts;
mod faster_kv;
mod pending_io;
pub(crate) mod record_format;
mod session;
mod state_transitions;

pub use async_session::{AsyncSession, AsyncSessionBuilder};
pub use contexts::{DeleteContext, PendingContext, ReadContext, RmwContext, UpsertContext};
pub use faster_kv::{
    CheckpointDurability, CheckpointKind, FasterKv, FasterKvConfig, LogCheckpointBackend,
};
pub use record_format::RecordView;
pub use session::{Session, SessionBuilder, ThreadContext};
pub use state_transitions::{Action, AtomicSystemState, Phase, SystemState};

/// Callback type for async operations
pub type AsyncCallback = Box<dyn FnOnce(crate::Status) + Send + 'static>;
