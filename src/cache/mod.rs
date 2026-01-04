//! Read cache for FASTER
//!
//! This module provides a read cache implementation that stores
//! hot (frequently read) records in memory to avoid disk reads.
//!
//! Based on C++ FASTER's read_cache.h implementation.

mod read_cache;
mod config;
mod record_info;
mod stats;

pub use config::ReadCacheConfig;
pub use read_cache::ReadCache;
pub use record_info::ReadCacheRecordInfo;
pub use stats::ReadCacheStats;
