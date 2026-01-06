//! Read cache for FASTER
//!
//! This module provides a read cache implementation that stores
//! hot (frequently read) records in memory to avoid disk reads.
//!
//! Based on C++ FASTER's read_cache.h implementation.
//!
//! # Overview
//!
//! The read cache stores copies of frequently accessed records from the
//! read-only and on-disk regions of the hybrid log. When a record is read
//! from these cold regions, it can be inserted into the cache for faster
//! subsequent access.
//!
//! # Key Features
//!
//! - **Transparent Integration**: Automatically checked during read operations
//! - **LRU-like Eviction**: Old entries are evicted as new ones are inserted
//! - **Lock-free Operations**: Uses atomic operations for concurrent access
//! - **Configurable Size**: Memory usage is bounded by configuration
//!
//! # Architecture
//!
//! The read cache uses a separate address space from the main hybrid log.
//! Cache addresses have a special "read cache" bit set to distinguish them
//! from regular log addresses.
//!
//! ```text
//! Hash Index Entry
//!        │
//!        ▼
//! ┌──────────────┐     ┌──────────────┐
//! │ Read Cache   │────▶│ HybridLog    │
//! │ (hot copy)   │     │ (original)   │
//! └──────────────┘     └──────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::cache::ReadCacheConfig;
//!
//! // Enable read cache with 256 MB
//! let cache_config = ReadCacheConfig::new(256 * 1024 * 1024);
//! let store = FasterKv::with_read_cache(config, device, cache_config);
//!
//! // Cache statistics
//! if let Some(stats) = store.read_cache_stats() {
//!     println!("Cache hits: {}", stats.hits());
//!     println!("Cache misses: {}", stats.misses());
//! }
//! ```

mod config;
mod read_cache;
mod record_info;
mod stats;

pub use config::ReadCacheConfig;
pub use read_cache::ReadCache;
pub use record_info::ReadCacheRecordInfo;
pub use stats::ReadCacheStats;
