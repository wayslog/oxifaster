//! Integration tests to increase coverage for modules near 80%.
//!
//! Target modules:
//! 1. src/index/mem_index/grow.rs - 78.5%
//! 2. src/delta_log/iterator.rs - 79.3%
//! 3. src/log/faster_log.rs - 77.2%
//! 4. src/store/faster_kv.rs - 77.0%
//! 5. src/allocator/hybrid_log.rs - 76.7%
//! 6. src/allocator/hybrid_log/checkpoint.rs - 76.8%
//! 7. src/device/io_uring_mock.rs - 74.0%
//! 8. src/cache/read_cache.rs - 74.0%
//! 9. src/scan/log_iterator.rs - 72.6%

use std::sync::Arc;

use oxifaster::cache::{ReadCache, ReadCacheConfig};
use oxifaster::device::{IoUringConfig, IoUringDevice, IoUringFile, NullDisk};
use oxifaster::index::{GrowConfig, MemHashIndex, MemHashIndexConfig};
use oxifaster::log::{FasterLog, FasterLogConfig, FasterLogOpenOptions, FasterLogSelfCheckOptions};
use oxifaster::scan::{
    ConcurrentLogScanIterator, LogPage, LogPageIterator, LogPageStatus, LogScanIterator, ScanRange,
};
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::Address;

// ===========================================================================
// 1. Index grow module tests (src/index/mem_index/grow.rs)
// ===========================================================================

include!("coverage_near80/part1.rs");
include!("coverage_near80/part2.rs");
