//! Memory allocators for FASTER
//!
//! This module provides the hybrid log allocator (PersistentMemoryMalloc)
//! and related memory management utilities.
//!
//! # Overview
//!
//! The hybrid log allocator manages a circular buffer of pages that span
//! both memory and disk. It provides:
//!
//! - **Append-only allocation**: New records are always written at the tail
//! - **Automatic tiering**: Hot pages in memory, cold pages on disk
//! - **Efficient I/O**: Page-aligned reads and writes
//!
//! # Memory Regions
//!
//! The hybrid log is divided into three regions:
//!
//! ```text
//! ┌─────────┬──────────────┬────────────────────┐
//! │ On-Disk │  Read-Only   │     Mutable        │
//! │ Region  │    Region    │      Region        │
//! └─────────┴──────────────┴────────────────────┘
//!     ↑            ↑               ↑
//!   begin     head_address    tail_address
//! ```
//!
//! - **Mutable Region**: Most recent pages, supports in-place updates
//! - **Read-Only Region**: Older pages in memory, cannot be updated
//! - **On-Disk Region**: Cold pages flushed to disk
//!
//! # Key Types
//!
//! - [`PersistentMemoryMalloc`]: The main hybrid log allocator
//! - [`HybridLogConfig`]: Configuration for the allocator
//! - [`PageInfo`]: Per-page metadata and status

mod hybrid_log;
pub mod page_allocator;

pub use hybrid_log::{AutoFlushConfig, HybridLogConfig, LogStats, PersistentMemoryMalloc};
pub use page_allocator::{CloseStatus, FlushStatus, FullPageStatus, PageInfo};
