# oxifaster

[![CI](https://github.com/wayslog/oxifaster/actions/workflows/ci.yml/badge.svg)](https://github.com/wayslog/oxifaster/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/wayslog/oxifaster/graph/badge.svg?token=WUMZDF60BQ)](https://codecov.io/gh/wayslog/oxifaster)

[中文](README.cn.md) | English

oxifaster is a Rust port of Microsoft's [FASTER](https://github.com/microsoft/FASTER) project, providing a high-performance concurrent key-value store and log engine.

## Features

- **High Performance**: Efficient read/write operations for large-scale data exceeding memory capacity
- **Concurrency Safe**: Lock-free concurrent control based on Epoch protection mechanism
- **Persistence**: Complete Checkpoint and Recovery support with CPR protocol
- **Hybrid Log**: HybridLog architecture with hot data in memory + cold data on disk
- **Async I/O**: Async operation support based on Tokio runtime
- **Read Cache**: Hot data memory cache for accelerated reads, transparently integrated into the read path
- **Log Compaction**: Log compaction and space reclamation with automatic background compaction support
- **Index Growth**: Dynamic hash table expansion with rehash callback for correctness
- **F2 Architecture**: Two-tier storage with hot-cold data separation, including complete Checkpoint/Recovery
- **Statistics**: Comprehensive statistics collection integrated into all CRUD operations

## Quick Start

### Installation

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
oxifaster = { path = "oxifaster" }
```

### Basic Usage

```rust
use std::sync::Arc;
use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn main() {
    // Create configuration
    let config = FasterKvConfig::default();
    
    // Create storage device (using NullDisk for in-memory testing)
    let device = NullDisk::new();
    
    // Create store
    let store = Arc::new(FasterKv::new(config, device));
    
    // Start session
    let mut session = store.start_session();
    
    // Insert data
    session.upsert(42u64, 100u64);
    
    // Read data
    if let Ok(Some(value)) = session.read(&42u64) {
        println!("Value: {}", value);
    }
    
    // Delete data
    session.delete(&42u64);
}
```

---

## Implementation Status and Roadmap

This section details the feature comparison between oxifaster and the original C++/C# FASTER project, as well as future development plans.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Application                                │
├─────────────────────────────────────────────────────────────────────┤
│                     Session / ThreadContext                          │
├──────────────────────────┬──────────────────────────────────────────┤
│       FasterKV           │           FasterLog                       │
├──────────────────────────┼──────────────────────────────────────────┤
│      Read Cache          │         Log Compaction                    │
├──────────────────────────┴──────────────────────────────────────────┤
│                         Epoch Protection                             │
├─────────────────────────────────────────────────────────────────────┤
│    Hash Index    │    Hybrid Log (PersistentMemoryMalloc)           │
│  (MemHashIndex)  │  ┌─────────┬─────────┬─────────────────┐         │
│                  │  │ Mutable │ReadOnly │    On-Disk      │         │
│                  │  │ Region  │ Region  │    Region       │         │
│                  │  └─────────┴─────────┴─────────────────┘         │
├─────────────────────────────────────────────────────────────────────┤
│                      Storage Device Layer                            │
│         (NullDisk / FileSystemDisk / io_uring / Azure)              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Feature Comparison Table (Rust vs C++ vs C#)

### Core Features

| Module | C++ | C# | Rust | Status |
|--------|:---:|:---:|:----:|--------|
| **Address System** | Y | Y | Y | Complete |
| **Epoch Protection** | Y | Y | Y | Complete |
| **Hash Index** | Y | Y | Y | Complete |
| **Hash Bucket** | Y | Y | Y | Complete |
| **Overflow Buckets** | Y | Y | Y | Complete |
| **Hybrid Log** | Y | Y | Y | Complete |
| **Page Allocator** | Y | Y | Y | Complete |
| **Record/RecordInfo** | Y | Y | Y | Complete |
| **Key/Value Traits** | Y | Y | Y | Complete |
| **FasterKV (CRUD)** | Y | Y | Y | Complete |
| **Session** | Y | Y | Y | Complete |
| **Thread Context** | Y | Y | Y | Complete |
| **FasterLog** | Y | Y | Y | Complete |

### Device Layer

| Feature | C++ | C# | Rust | Status |
|---------|:---:|:---:|:----:|--------|
| **NullDisk** | Y | Y | Y | Complete |
| **FileSystemDisk** | Y | Y | Y | Complete |
| **io_uring (Linux)** | Y | - | P | Mock implementation (API standardized) |
| **IOCP (Windows)** | Y | Y | N | Not implemented |
| **Azure Blob Storage** | Y | Y | N | Not implemented |
| **Tiered Storage** | - | Y | N | Not implemented |
| **Sharded Storage** | - | Y | N | Not implemented |

### Persistence and Recovery

| Feature | C++ | C# | Rust | Status |
|---------|:---:|:---:|:----:|--------|
| **Index Checkpoint** | Y | Y | Y | Complete |
| **HybridLog Checkpoint** | Y | Y | Y | Complete |
| **Full Checkpoint** | Y | Y | Y | Complete (with CPR state machine) |
| **CPR Protocol** | Y | Y | Y | Complete (full state machine integration) |
| **Snapshot Files** | Y | Y | Y | Complete (JSON + bincode) |
| **Session Persistence** | Y | Y | Y | Complete (GUID + serial_num) |
| **Incremental Checkpoint** | - | Y | N | Not implemented |
| **Delta Log** | - | Y | N | Not implemented |

### Performance Optimization

| Feature | C++ | C# | Rust | Status |
|---------|:---:|:---:|:----:|--------|
| **Read Cache** | Y | - | Y | Complete (integrated into read path) |
| **Cache Eviction** | Y | - | Y | Complete (LRU eviction policy) |
| **Log Compaction** | Y | Y | Y | Complete |
| **Auto Compaction** | Y | Y | Y | Complete (background thread) |
| **Concurrent Compaction** | Y | Y | Y | Complete (multi-threaded page distribution) |
| **Index Growth** | Y | Y | Y | Complete (with rehash callback) |
| **Log Scan Iterator** | Y | Y | Y | Complete (supports StorageDevice) |

### Advanced Features

| Feature | C++ | C# | Rust | Status |
|---------|:---:|:---:|:----:|--------|
| **F2 Architecture** | Y | - | Y | Complete (with Checkpoint/Recovery) |
| **Cold Index** | Y | - | Y | Complete (disk-based secondary index) |
| **Checkpoint Locks** | Y | Y | Y | Complete (CPR protocol lock protection) |
| **Statistics** | Y | Y | Y | Complete (integrated into all operations) |
| **Variable Length Records** | - | Y | N | Not implemented |
| **Async API** | Y | Y | P | Partial support |

### C# Exclusive Features

| Feature | Rust Plan |
|---------|-----------|
| **Remote Client/Server** | P3 - Optional |
| **Locking Context** | P2 |
| **Generic Allocator** | P3 - Optional |
| **Blittable/VarLen Allocator** | P2 |

**Legend**: Y=Complete | P=Partial | N=Not implemented

---

### Completed Features (Phase 1 - Core)

| Module | Feature | File | Status |
|--------|---------|------|:------:|
| **address** | 48-bit logical address system | `src/address.rs` | :white_check_mark: |
| **epoch** | LightEpoch concurrent protection framework | `src/epoch/light_epoch.rs` | :white_check_mark: |
| **index** | MemHashIndex in-memory hash index | `src/index/mem_index.rs` | :white_check_mark: |
| **index** | HashBucket hash bucket | `src/index/hash_bucket.rs` | :white_check_mark: |
| **index** | HashTable hash table | `src/index/hash_table.rs` | :white_check_mark: |
| **index** | GrowState index extension state | `src/index/grow.rs` | :white_check_mark: |
| **allocator** | PersistentMemoryMalloc hybrid log | `src/allocator/hybrid_log.rs` | :white_check_mark: |
| **allocator** | PageAllocator page allocator | `src/allocator/page_allocator.rs` | :white_check_mark: |
| **device** | StorageDevice trait | `src/device/traits.rs` | :white_check_mark: |
| **device** | NullDisk memory device | `src/device/null_device.rs` | :white_check_mark: |
| **device** | FileSystemDisk file device | `src/device/file_device.rs` | :white_check_mark: |
| **device** | IoUringDevice Mock implementation | `src/device/io_uring.rs` | :construction: |
| **store** | FasterKV core (Read/Upsert/RMW/Delete) | `src/store/faster_kv.rs` | :white_check_mark: |
| **store** | Session management (with GUID + serial_num) | `src/store/session.rs` | :white_check_mark: |
| **store** | ThreadContext thread context | `src/store/contexts.rs` | :white_check_mark: |
| **store** | StateTransitions CPR state machine | `src/store/state_transitions.rs` | :white_check_mark: |
| **record** | Record/RecordInfo record structure | `src/record.rs` | :white_check_mark: |
| **record** | Key/Value trait generic support | `src/record.rs` | :white_check_mark: |
| **log** | FasterLog basic log | `src/log/faster_log.rs` | :white_check_mark: |
| **cache** | ReadCache read cache | `src/cache/read_cache.rs` | :white_check_mark: |
| **cache** | ReadCacheConfig configuration | `src/cache/config.rs` | :white_check_mark: |
| **compaction** | Compactor compaction engine | `src/compaction/compact.rs` | :white_check_mark: |
| **compaction** | AutoCompactionWorker | `src/compaction/auto_compact.rs` | :white_check_mark: |
| **compaction** | CompactionConfig configuration | `src/compaction/compact.rs` | :white_check_mark: |
| **f2** | F2Kv hot-cold storage | `src/f2/store.rs` | :white_check_mark: |
| **f2** | F2Config configuration | `src/f2/config.rs` | :white_check_mark: |
| **f2** | F2 Checkpoint/Recovery | `src/f2/store.rs` | :white_check_mark: |
| **scan** | LogScanIterator log scan | `src/scan/log_iterator.rs` | :white_check_mark: |
| **scan** | DoubleBufferedLogIterator | `src/scan/log_iterator.rs` | :white_check_mark: |
| **stats** | StatsCollector statistics collector | `src/stats/collector.rs` | :white_check_mark: |
| **checkpoint** | Checkpoint state structure | `src/checkpoint/state.rs` | :white_check_mark: |
| **checkpoint** | Recovery structure | `src/checkpoint/recovery.rs` | :white_check_mark: |
| **checkpoint** | Serialization | `src/checkpoint/serialization.rs` | :white_check_mark: |
| **checkpoint** | Checkpoint Locks CPR locks | `src/checkpoint/locks.rs` | :white_check_mark: |
| **index** | Cold Index disk cold index | `src/index/cold_index.rs` | :white_check_mark: |
| **compaction** | Concurrent Compaction | `src/compaction/concurrent.rs` | :white_check_mark: |

**Legend**: :white_check_mark: Complete | :construction: Partial | :x: Not implemented

---

### Phase 2: Persistence and Recovery (Durability) - P0 :white_check_mark: Complete

| Feature | Description | File | Status |
|---------|-------------|------|:------:|
| **Checkpoint Metadata Serialization** | Index + HybridLog metadata persistence | `checkpoint/serialization.rs` | :white_check_mark: |
| **Index Checkpoint** | Hash index checkpoint write | `checkpoint/state.rs` | :white_check_mark: |
| **HybridLog Checkpoint** | Hybrid log checkpoint write | `checkpoint/state.rs` | :white_check_mark: |
| **Recovery Full Implementation** | Restore complete state from checkpoint (with RecoveryState) | `checkpoint/recovery.rs` | :white_check_mark: |
| **CPR Protocol** | Concurrent Prefix Recovery full state machine | `store/state_transitions.rs` | :white_check_mark: |
| **Session Persistence** | GUID + serial_num save and restore | `store/session.rs` | :white_check_mark: |
| **Snapshot File Format** | Snapshot file read/write (JSON + bincode) | `checkpoint/serialization.rs` | :white_check_mark: |

```rust
// Implemented API
let token = store.checkpoint(checkpoint_dir)?;      // Full checkpoint (with CPR state machine)
let store = FasterKv::recover(dir, token, config, device)?;  // Recovery (with session state)

// Session persistence
let states = store.get_recovered_sessions();        // Get recovered session states
let session = store.continue_session(state);        // Restore session from state
```

### Phase 3: Performance Optimization - P1 :white_check_mark: Complete

| Feature | Description | File | Status |
|---------|-------------|------|:------:|
| **Read Cache Full Integration** | Hot data memory cache integrated into read path | `cache/read_cache.rs` | :white_check_mark: |
| **Cache Eviction** | LRU-like cache eviction policy | `cache/read_cache.rs` | :white_check_mark: |
| **Auto Compaction** | Automatic background compaction thread | `compaction/auto_compact.rs` | :white_check_mark: |
| **Concurrent Compaction** | Multi-threaded concurrent compaction | `compaction/concurrent.rs` | :white_check_mark: |
| **Index Growth** | Dynamic hash table expansion (with rehash callback) | `index/grow.rs`, `index/mem_index.rs` | :white_check_mark: |
| **Log Scan Iterator** | Log scan iterator (supports StorageDevice) | `scan/log_iterator.rs` | :white_check_mark: |
| **Statistics Integration** | Statistics collector integrated into all operations | `stats/collector.rs` | :white_check_mark: |

```rust
// Implemented API
// Read Cache automatically integrated into read path
let value = session.read(&key)?;  // Automatically checks cache

// Compaction
store.log_compact_until(until_address)?;

// Auto Compaction (background thread)
let worker = AutoCompactionWorker::new(config);
worker.start(Arc::downgrade(&store));

// Index Growth (with rehash callback)
index.grow_with_rehash(|addr| {
    // Read key and compute hash
    Some(compute_hash(read_key(addr)))
})?;

// Statistics
let stats = store.stats();
println!("Read ops: {}", stats.operations.reads);
```

### Phase 4: Advanced Features - P2 :white_check_mark: Complete

| Feature | Description | File | Status |
|---------|-------------|------|:------:|
| **F2 Hot-Cold Full Implementation** | Hot-cold data separation | `f2/store.rs` | :white_check_mark: |
| **F2 Checkpoint/Recovery** | Hot-cold storage checkpoint and recovery | `f2/store.rs`, `f2/state.rs` | :white_check_mark: |
| **F2 Background Migration** | Automatic data migration thread | `f2/store.rs` | :white_check_mark: |
| **Cold Index** | Disk-based secondary hash index | `index/cold_index.rs` | :white_check_mark: |
| **Checkpoint Locks** | Lock protection during CPR protocol | `checkpoint/locks.rs` | :white_check_mark: |

```rust
// Implemented API: F2 Hot-Cold Architecture
let f2_store = F2Kv::new(config, hot_device, cold_device);

// F2 Checkpoint
let token = f2_store.checkpoint(checkpoint_dir)?;

// F2 Recovery
let version = f2_store.recover(checkpoint_dir, token)?;

// Cold Index (disk secondary index)
use oxifaster::index::{ColdIndex, ColdIndexConfig};
let cold_config = ColdIndexConfig::new(table_size, in_mem_size, mutable_fraction);
let mut cold_index = ColdIndex::new(cold_config);
cold_index.initialize()?;
cold_index.find_entry(hash);
cold_index.update_entry(hash, new_address);

// Checkpoint Locks (CPR protocol protection)
use oxifaster::checkpoint::{CheckpointLocks, CheckpointLockGuard};
let locks = CheckpointLocks::with_size(1024);
let mut guard = CheckpointLockGuard::new(&locks, key_hash);
guard.try_lock_old();  // Lock old version record
// ... perform operations ...
// guard automatically releases lock

// Concurrent Compaction (multi-threaded compaction)
use oxifaster::compaction::{ConcurrentCompactor, ConcurrentCompactionConfig};
let compactor = ConcurrentCompactor::new(ConcurrentCompactionConfig::new(4)); // 4 threads
let result = compactor.compact_range(scan_range, |chunk| {
    // Process each page chunk
    process_chunk(chunk)
});
```

### Phase 5: Platform and Ecosystem - P3

| Feature | Description | File | Reference |
|---------|-------------|------|-----------|
| **io_uring Full Implementation** | Linux high-performance async I/O | `device/io_uring.rs` | `file_linux.h` |
| **Azure Blob Storage** | Azure storage backend | `device/azure.rs` | C# `Devices.cs` |
| **Statistics Enhancement** | Performance metrics collection and reporting | `stats/collector.rs` | `faster.h` |
| **TOML Configuration** | Configuration file support | `config.rs` | - |

```rust
// Target API: io_uring
let device = IoUringDevice::new(path)?;

// Target API: Azure
let device = AzureBlobDevice::new(connection_string, container)?;
```

---

### Estimated Timeline

| Phase | Description | Effort | Cumulative | Status |
|-------|-------------|:------:|:----------:|:------:|
| Phase 2 | Persistence and Recovery | 45 days | 45 days | :white_check_mark: Complete |
| Phase 3 | Performance Optimization | 32 days | 77 days | :white_check_mark: Complete |
| Phase 4 | Advanced Features | 29 days | 106 days | :white_check_mark: Complete |
| Phase 5 | Platform and Ecosystem | 31 days | 137 days | Pending |

**Remaining: ~31 working days (~1.5 months)**

---

### Key Differences from C++/C# FASTER

| Aspect | C++ FASTER | C# FASTER | oxifaster |
|--------|-----------|-----------|-----------|
| **Memory Management** | Manual + RAII | GC Managed | Rust ownership system |
| **Concurrency Model** | std::atomic | Interlocked | std::sync::atomic |
| **Async Runtime** | Callbacks | async/await | Tokio async/await |
| **Generic System** | C++ templates | .NET generics | Rust generics + Traits |
| **Error Handling** | Return codes/Exceptions | Exception | Result<T, E> |
| **Cache Alignment** | Compiler macros | StructLayout | `#[repr(align(64))]` |
| **Cross-platform** | Conditional compilation | .NET Runtime | cfg attributes |

---

## Module Structure

```
oxifaster/
├── src/
│   ├── lib.rs              # Library entry
│   ├── address.rs          # Address system (Address, AtomicAddress)
│   ├── record.rs           # Record structure (RecordInfo, Record, Key, Value)
│   ├── status.rs           # Status codes (Status, OperationStatus)
│   ├── utility.rs          # Utility functions
│   │
│   ├── epoch/              # Epoch protection mechanism
│   │   ├── mod.rs
│   │   └── light_epoch.rs
│   │
│   ├── index/              # Hash index
│   │   ├── mod.rs
│   │   ├── hash_bucket.rs
│   │   ├── hash_table.rs
│   │   ├── mem_index.rs
│   │   ├── grow.rs         # Index growth
│   │   └── cold_index.rs   # Disk cold index
│   │
│   ├── allocator/          # Memory allocator
│   │   ├── mod.rs
│   │   ├── page_allocator.rs
│   │   └── hybrid_log.rs
│   │
│   ├── device/             # Storage devices
│   │   ├── mod.rs
│   │   ├── traits.rs
│   │   ├── file_device.rs
│   │   ├── null_device.rs
│   │   └── io_uring.rs     # Linux io_uring
│   │
│   ├── store/              # FasterKV store
│   │   ├── mod.rs
│   │   ├── faster_kv.rs
│   │   ├── session.rs
│   │   ├── contexts.rs
│   │   └── state_transitions.rs
│   │
│   ├── checkpoint/         # Checkpoint and recovery
│   │   ├── mod.rs
│   │   ├── state.rs
│   │   ├── locks.rs        # CPR checkpoint locks
│   │   ├── recovery.rs
│   │   └── serialization.rs
│   │
│   ├── log/                # FASTER Log
│   │   ├── mod.rs
│   │   └── faster_log.rs
│   │
│   ├── cache/              # Read Cache
│   │   ├── mod.rs
│   │   ├── config.rs
│   │   ├── read_cache.rs
│   │   ├── record_info.rs
│   │   └── stats.rs
│   │
│   ├── compaction/         # Log compaction
│   │   ├── mod.rs
│   │   ├── compact.rs
│   │   ├── concurrent.rs    # Concurrent compaction
│   │   ├── auto_compact.rs  # Auto compaction background thread
│   │   └── contexts.rs
│   │
│   ├── f2/                 # F2 hot-cold architecture
│   │   ├── mod.rs
│   │   ├── config.rs
│   │   ├── store.rs
│   │   └── state.rs
│   │
│   ├── scan/               # Log scanning
│   │   ├── mod.rs
│   │   └── log_iterator.rs
│   │
│   └── stats/              # Statistics collection
│       ├── mod.rs
│       ├── collector.rs
│       ├── metrics.rs
│       └── reporter.rs
│
├── examples/               # Example code
│   ├── async_operations.rs
│   ├── basic_kv.rs
│   ├── cold_index.rs
│   ├── compaction.rs
│   ├── concurrent_access.rs
│   ├── custom_types.rs
│   ├── epoch_protection.rs
│   ├── f2_hot_cold.rs
│   ├── faster_log.rs
│   ├── index_growth.rs
│   ├── log_scan.rs
│   ├── read_cache.rs
│   ├── statistics.rs
│   └── variable_length.rs
│
├── benches/
│   └── ycsb.rs             # YCSB benchmark
│
└── tests/
    ├── async_session.rs    # Async session tests
    ├── basic_ops.rs        # Basic operations tests
    ├── checkpoint_locks.rs # Checkpoint locks tests
    ├── checkpoint.rs       # Checkpoint tests
    ├── cold_index.rs       # Cold index tests
    ├── compaction.rs       # Compaction tests
    ├── f2.rs               # F2 tests
    ├── incremental_checkpoint.rs # Incremental checkpoint tests
    ├── index_growth.rs     # Index growth tests
    ├── log_scan.rs         # Log scan tests
    ├── read_cache.rs       # Read cache tests
    ├── recovery.rs         # Recovery tests
    ├── statistics.rs       # Statistics tests
    └── varlen.rs           # Variable length tests
```

---

## Core Concepts

### Address

48-bit logical address used to identify record positions in the hybrid log:
- 25-bit page offset (32 MB per page)
- 23-bit page number (~8 million pages)

### Epoch Protection

Lightweight epoch protection mechanism for:
- Safe memory reclamation
- Lock-free concurrent control
- Delayed operation execution

### Hybrid Log

Hybrid log allocator managing memory and disk storage:
- **Mutable Region**: Latest pages supporting in-place updates
- **Read-Only Region**: Older pages in memory
- **Disk Region**: Cold data

### Hash Index

High-performance in-memory hash index:
- Cache-line aligned hash buckets (64 bytes)
- 14-bit tags for fast comparison
- Overflow bucket support
- Dynamic expansion support

### Read Cache

Hot data cache:
- In-memory storage for frequently read records
- LRU-like eviction policy
- Transparently integrated into read path

### Log Compaction

Space reclamation mechanism:
- Scan old record regions
- Retain latest version records
- Release expired space

---

## API Reference

### FasterKv

Main KV store class:

```rust
use std::sync::Arc;
use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

// Create store
let config = FasterKvConfig::default();
let device = NullDisk::new();
let store = Arc::new(FasterKv::new(config, device));

// Start session
let mut session = store.start_session();

// Basic operations
session.upsert(key, value);                    // Insert/Update
let value = session.read(&key)?;               // Read
session.delete(&key);                          // Delete
session.rmw(key, |v| { *v += 1; true });       // Read-Modify-Write

// Checkpoint and Recovery (Phase 2 complete)
let token = store.checkpoint(checkpoint_dir)?; // Create checkpoint
let recovered = FasterKv::recover(            // Recover from checkpoint
    checkpoint_dir, token, config, device
)?;

// Session persistence
let states = store.get_recovered_sessions();   // Get recovered session states
let session = store.continue_session(state);   // Restore session from saved state
```

### FasterKv with Read Cache

Enable read cache:

```rust
use oxifaster::cache::ReadCacheConfig;

let cache_config = ReadCacheConfig::default()
    .with_mem_size(256 * 1024 * 1024)  // 256 MB
    .with_mutable_fraction(0.5);

let store = FasterKv::with_read_cache(config, device, cache_config);
```

### FasterKv with Compaction

Enable compaction:

```rust
use oxifaster::compaction::CompactionConfig;

let compaction_config = CompactionConfig::default()
    .with_target_utilization(0.5)
    .with_num_threads(2);

let store = FasterKv::with_compaction_config(config, device, compaction_config);
```

### FasterLog

Standalone high-performance log:

```rust
use oxifaster::log::faster_log::{FasterLog, FasterLogConfig};
use oxifaster::device::NullDisk;

let config = FasterLogConfig::default();
let device = NullDisk::new();
let log = FasterLog::new(config, device);

// Append data
let addr = log.append(b"data")?;

// Commit
log.commit()?;

// Read
let data = log.read_entry(addr);

// Scan all entries
for (addr, data) in log.scan_all() {
    println!("{}: {:?}", addr, data);
}
```

### F2Kv (Hot-Cold Architecture)

Two-tier storage architecture:

```rust
use oxifaster::f2::{F2Kv, F2Config, HotStoreConfig, ColdStoreConfig};

let config = F2Config {
    hot: HotStoreConfig::default(),
    cold: ColdStoreConfig::default(),
    ..Default::default()
};

let f2_store = F2Kv::new(config, hot_device, cold_device);
```

### Statistics

Collect performance statistics:

```rust
use oxifaster::stats::{StatsCollector, StatsConfig};

let stats = store.stats();
println!("Read hits: {}", stats.read_hits);
println!("Read misses: {}", stats.read_misses);
println!("Cache hit rate: {:.2}%", stats.cache_hit_rate() * 100.0);
```

---

## Running Examples

```bash
# Async operations
cargo run --example async_operations

# Basic KV operations
cargo run --example basic_kv

# Cold index
cargo run --example cold_index

# Compaction
cargo run --example compaction

# Concurrent access
cargo run --example concurrent_access

# Custom types
cargo run --example custom_types

# Epoch protection
cargo run --example epoch_protection

# F2 hot-cold architecture
cargo run --example f2_hot_cold

# FasterLog
cargo run --example faster_log

# Index growth
cargo run --example index_growth

# Log scan
cargo run --example log_scan

# Read cache
cargo run --example read_cache

# Statistics
cargo run --example statistics

# Variable length
cargo run --example variable_length
```

## Running Tests

```bash
cargo test
```

## Running Benchmarks

```bash
cargo bench
```

---

## Contributing

Contributions are welcome! Please check the **Feature Comparison Table** and **Development Roadmap** above to select features you're interested in developing.

### Priorities

- **P0**: ~~Checkpoint/Recovery - Production essential~~ :white_check_mark: Complete
- **P1**: ~~Read Cache, Compaction, Index Growth - Performance critical~~ :white_check_mark: Complete
- **P2**: ~~F2 Checkpoint/Recovery, Statistics integration, Cold Index, Checkpoint Locks, Concurrent Compaction~~ :white_check_mark: Complete
- **P3**: io_uring full implementation, Azure Storage, Configuration file - Ecosystem expansion

### Development Workflow

1. Fork this repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit changes: `git commit -am 'Add new feature'`
4. Push branch: `git push origin feature/your-feature`
5. Create Pull Request

### Code Standards

- Use `cargo fmt` to format code
- Use `cargo clippy` for code quality checks
- Add unit tests for new features
- Update relevant documentation

---

## References

- [FASTER Official Documentation](https://microsoft.github.io/FASTER/)
- [FASTER C++ Source](https://github.com/microsoft/FASTER/tree/main/cc)
- [FASTER C# Source](https://github.com/microsoft/FASTER/tree/main/cs)
- [FASTER Paper](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/)

## License

MIT License

## Acknowledgments

Thanks to the Microsoft FASTER team for their open-source contributions.
