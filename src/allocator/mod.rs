//! Memory allocators for FASTER
//!
//! This module provides the hybrid log allocator (PersistentMemoryMalloc)
//! and related memory management utilities.

mod hybrid_log;
pub mod page_allocator;

pub use hybrid_log::{HybridLogConfig, LogStats, PersistentMemoryMalloc};
pub use page_allocator::{CloseStatus, FlushStatus, FullPageStatus, PageInfo};
