//! Sector-aligned buffer pool for I/O operations
//!
//! This module provides a pool of pre-allocated sector-aligned memory buffers
//! for efficient I/O operations. Based on C++ FASTER's NativeSectorAlignedBufferPool.

mod aligned_pool;

pub use aligned_pool::{AlignedBufferPool, PooledBuffer};
