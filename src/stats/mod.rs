//! Statistics collection system for FASTER
//!
//! This module provides comprehensive statistics collection for monitoring
//! and debugging FASTER operations.
//!
//! # Overview
//!
//! The statistics system tracks various metrics across FASTER components:
//!
//! - **Operation Stats**: Read/write/delete/RMW counts and latencies
//! - **Allocator Stats**: Memory allocation and page usage
//! - **Hash Index Stats**: Bucket utilization and collision counts
//! - **Hybrid Log Stats**: Region sizes and flush statistics
//!
//! # Enabling Statistics
//!
//! Statistics collection is enabled by default but can be controlled:
//!
//! ```rust,ignore
//! // Enable/disable at runtime
//! store.enable_stats();
//! store.disable_stats();
//!
//! // Check if enabled
//! if store.is_stats_enabled() {
//!     let snapshot = store.stats_snapshot();
//!     println!("Throughput: {} ops/sec", snapshot.throughput);
//! }
//! ```
//!
//! # Performance Impact
//!
//! Statistics collection has minimal overhead (atomic increments) but can
//! be disabled in performance-critical scenarios.
//!
//! # Available Metrics
//!
//! - Total operations (reads, upserts, RMWs, deletes)
//! - Hit rate (successful reads / total reads)
//! - Average latency per operation type
//! - Throughput (operations per second)
//! - Memory usage and allocation patterns

pub mod collector;
pub mod metrics;
#[cfg(feature = "prometheus")]
pub mod prometheus;
pub mod reporter;

pub use collector::{StatsCollector, StatsConfig, StatsSnapshot};
pub use metrics::{
    AllocatorStats, HashIndexStats, HybridLogStats, OperationStats, OperationalStats, SessionStats,
    StoreStats,
};
pub use reporter::{ReportFormat, StatsReporter};
