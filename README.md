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
- **Prometheus Metrics**: Render statistics snapshots as Prometheus text exposition for external scraping (feature = `prometheus`)

## Quick Start

### Installation

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
oxifaster = { path = "oxifaster" }
```

Enable the Linux `io_uring` backend (feature-gated):

```toml
[dependencies]
oxifaster = { path = "oxifaster", features = ["io_uring"] }
```

Note: the real `io_uring` backend is only available on Linux. On non-Linux platforms (or when the feature is disabled), `IoUringDevice` falls back to a portable file-backed implementation.

Enable Prometheus text exposition (for statistics snapshots):

```toml
[dependencies]
oxifaster = { path = "oxifaster", features = ["prometheus"] }
```

With `prometheus` enabled, you can also run a minimal HTTP `/metrics` server (Tokio required):

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use oxifaster::device::NullDisk;
use oxifaster::stats::prometheus::{MetricsHttpServer, PrometheusRenderer};
use oxifaster::store::{FasterKv, FasterKvConfig};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let store = Arc::new(FasterKv::<u64, u64, _>::new(
        FasterKvConfig::default(),
        NullDisk::new(),
    ));

    let store_for_server = store.clone();
    let render = Arc::new(move || {
        PrometheusRenderer::new().render_snapshot(&store_for_server.stats_snapshot())
    });

    let addr: SocketAddr = "127.0.0.1:9898".parse().unwrap();
    let server = MetricsHttpServer::bind(addr, render).await?;
    println!("metrics: http://{}/metrics", server.local_addr());

    tokio::signal::ctrl_c().await.ok();
    server.shutdown().await?;
    Ok(())
}
```

### Basic Usage

```rust
use std::sync::Arc;
use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn main() -> std::io::Result<()> {
    // Create configuration
    let config = FasterKvConfig::default();
    
    // Create storage device (FileSystemDisk is the default persistent device)
    let device = FileSystemDisk::single_file("oxifaster.db")?;
    
    // Create store
    let store = Arc::new(FasterKv::new(config, device));
    
    // Start session
    let mut session = store.start_session().expect("failed to start session");
    
    // Insert data
    session.upsert(42u64, 100u64);
    
    // Read data
    if let Ok(Some(value)) = session.read(&42u64) {
        println!("Value: {}", value);
    }
    
    // Delete data
    session.delete(&42u64);

    Ok(())
}
```

---

## Configuration (TOML + Environment)

Use `oxifaster::config::OxifasterConfig` to load a TOML file and apply environment overrides.

Example `oxifaster.toml`:

```toml
[store]
table_size = 1048576
log_memory_size = 536870912
page_size_bits = 22
mutable_fraction = 0.9

[compaction]
target_utilization = 0.5
min_compact_bytes = 1048576
max_compact_bytes = 1073741824
num_threads = 1
compact_tombstones = true

[cache]
enabled = true
mem_size = 268435456
mutable_fraction = 0.9
pre_allocate = false
copy_to_tail = true

[device]
kind = "single_file"
path = "oxifaster.db"
```

Load from `OXIFASTER_CONFIG` and apply overrides such as `OXIFASTER__store__table_size=2097152`:

```rust
use oxifaster::config::OxifasterConfig;

let config = OxifasterConfig::load_from_env()?;
let store_config = config.to_faster_kv_config();
let compaction_config = config.to_compaction_config();
let cache_config = config.to_read_cache_config();
let device = config.open_device()?;
```

---

## Development Roadmap

Please refer to [DEV.md](DEV.md) for the detailed development roadmap, implementation status, and technical debt analysis.

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
│   │   ├── io_uring.rs        # io_uring entrypoint (Linux + mock)
│   │   ├── io_uring_common.rs # Shared types
│   │   ├── io_uring_linux.rs  # Linux backend (feature = "io_uring")
│   │   └── io_uring_mock.rs   # Non-Linux / feature off fallback
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
use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

// Create store
let config = FasterKvConfig::default();
let store_device = FileSystemDisk::single_file("oxifaster.db")?;
let store = Arc::new(FasterKv::new(config, store_device));

// Start session
let mut session = store.start_session().expect("failed to start session");

// Basic operations
session.upsert(key, value);                    // Insert/Update
let value = session.read(&key)?;               // Read
session.delete(&key);                          // Delete
session.rmw(key, |v| { *v += 1; true });       // Read-Modify-Write

// Checkpoint and Recovery (Phase 2 complete)
let token = store.checkpoint(checkpoint_dir)?; // Create checkpoint
let recovered = FasterKv::recover(            // Recover from checkpoint
    checkpoint_dir,
    token,
    config,
    FileSystemDisk::single_file("oxifaster.db")?
)?;

// Session persistence
let states = store.get_recovered_sessions();   // Get recovered session states
let session = store
    .continue_session(state)
    .expect("failed to continue session");     // Restore session from saved state
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

### Automatic Background Compaction

Start a background compaction worker and manage its lifecycle via the returned handle:

```rust
use oxifaster::compaction::AutoCompactionConfig;

let handle = store.start_auto_compaction(AutoCompactionConfig::new());
drop(handle); // Stops and joins the worker thread
```

### FasterLog

Standalone high-performance log:

```rust
use oxifaster::log::{FasterLog, FasterLogConfig};
use oxifaster::device::FileSystemDisk;

let config = FasterLogConfig::default();
let device = FileSystemDisk::single_file("oxifaster.log")?;
let log = FasterLog::open(config, device)?;

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

Note: most examples will run twice to compare devices — first with `NullDisk` (in-memory), then with `FileSystemDisk` (temp file).

```bash
# Async operations
cargo run --example async_operations

# Basic KV operations
cargo run --example basic_kv

# io_uring (Linux, enable feature for real backend)
cargo run --example io_uring --features io_uring

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

Contributions are welcome! Please check [DEV.md](DEV.md) for the development roadmap and technical debt analysis.

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
