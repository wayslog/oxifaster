//! Comprehensive integration tests for the store module to increase code coverage.
//!
//! Covers: FasterKv, Session, AsyncSession, Compaction, and StateTransitions.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use oxifaster::checkpoint::SessionState;
use oxifaster::compaction::CompactionConfig;
use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{
    Action, AsyncSessionBuilder, AtomicSystemState, FasterKv, FasterKvConfig, Phase,
    SessionBuilder, SystemState, ThreadContext,
};
use oxifaster::Address;
use uuid::Uuid;

// ============ Helpers ============

fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    Arc::new(FasterKv::new(config, NullDisk::new()))
}

fn create_large_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 4096,
        log_memory_size: 1 << 24,
        page_size_bits: 16,
        mutable_fraction: 0.9,
    };
    Arc::new(FasterKv::new(config, NullDisk::new()))
}

fn create_store_with_compaction() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let compaction_config = CompactionConfig::new()
        .with_target_utilization(0.5)
        .with_min_compact_bytes(0);
    Arc::new(FasterKv::with_compaction_config(
        config,
        NullDisk::new(),
        compaction_config,
    ))
}

// ============ 1. FasterKv - Store creation and configuration ============

include!("coverage_store/part1.rs");
include!("coverage_store/part2.rs");
include!("coverage_store/part3.rs");
