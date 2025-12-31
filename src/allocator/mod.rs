//! Memory allocators for FASTER
//!
//! This module provides the hybrid log allocator (PersistentMemoryMalloc)
//! and related memory management utilities.

pub mod page_allocator;
mod hybrid_log;

pub use page_allocator::{FullPageStatus, FlushStatus, CloseStatus, PageInfo};
pub use hybrid_log::{PersistentMemoryMalloc, HybridLogConfig, LogStats};

