//! Integration tests to improve code coverage for store-internal modules.
//!
//! Target modules:
//! - src/store/faster_kv/compaction.rs
//! - src/store/faster_kv/checkpoint.rs
//! - src/store/faster_kv/pending_io_support.rs
//! - src/store/async_session.rs
//! - src/checkpoint/recovery.rs
//! - src/store/faster_kv/index_grow.rs
//! - src/codec/mod.rs

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use oxifaster::Address;
use oxifaster::cache::ReadCacheConfig;
use oxifaster::checkpoint::{
    CheckpointInfo, PageRecoveryStatus, RecoveryState, RecoveryStatus, SessionState,
    find_latest_checkpoint, list_checkpoints, validate_checkpoint,
};
use oxifaster::codec::{KeyCodec, RawBytes, Utf8, hash64};
use oxifaster::compaction::CompactionConfig;
use oxifaster::device::{FileSystemDisk, NullDisk};
use oxifaster::status::Status;
use oxifaster::store::{
    AsyncSessionBuilder, CheckpointKind, FasterKv, FasterKvConfig, SessionBuilder,
};
use uuid::Uuid;

// ============ Test Helpers ============

fn create_small_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 256,
        log_memory_size: 1 << 16, // 64 KB - very small to force pages
        page_size_bits: 12,       // 4 KB pages
        mutable_fraction: 0.5,
    };
    Arc::new(FasterKv::new(config, NullDisk::new()))
}

fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MB
        page_size_bits: 14,       // 16 KB pages
        mutable_fraction: 0.9,
    };
    Arc::new(FasterKv::new(config, NullDisk::new()))
}

fn create_store_with_compaction(
    target_util: f64,
    min_bytes: u64,
) -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let compaction_config = CompactionConfig::new()
        .with_target_utilization(target_util)
        .with_min_compact_bytes(min_bytes);
    Arc::new(FasterKv::with_compaction_config(
        config,
        NullDisk::new(),
        compaction_config,
    ))
}

fn create_filesystem_store(dir: &std::path::Path) -> Arc<FasterKv<u64, u64, FileSystemDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let device = FileSystemDisk::single_file(dir.join("store.dat")).unwrap();
    Arc::new(FasterKv::new(config, device))
}

// ============ 1. Compaction Tests (compaction.rs) ============

include!("coverage_store_internal/part1.rs");
include!("coverage_store_internal/part2.rs");
include!("coverage_store_internal/part3.rs");
