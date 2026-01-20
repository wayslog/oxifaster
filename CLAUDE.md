# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Principle

- use chinese simplified communicate with me.
- Never use any emoji

## Project Overview

oxifaster is a Rust port of Microsoft FASTER - a high-performance concurrent key-value store and log engine. The system uses:
- **Hybrid Log Architecture**: Tiered storage with hot data in memory and cold data on disk
- **Epoch Protection**: Lock-free concurrency with epoch-based memory reclamation
- **CPR Protocol**: Concurrent Prefix Recovery for non-blocking checkpoints
- **Two Main Components**: FasterKV (key-value store) and FasterLog (append-only log)

## Build & Development Commands

### Building & Testing
```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run tests
./scripts/check-test.sh      # Run tests

# Run benchmarks
cargo bench
```

### Code Quality Checks
```bash
# Format code (required before committing)
cargo fmt --all

# Run clippy (must pass with zero warnings)
cargo clippy --all-targets --all-features -- -D warnings

# Run all checks (CI gates)
./scripts/check-fmt.sh       # Format check
./scripts/check-clippy.sh    # Clippy with -D warnings
./scripts/check-test-all-features.sh  # Test all feature combinations
./scripts/check-build.sh     # Build check
./scripts/check-bench.sh     # Bench compile check
```

### Running Examples
```bash
# Most examples run twice: first with NullDisk (in-memory), then FileSystemDisk (temp file)
cargo run --example basic_kv
cargo run --example async_operations
cargo run --example checkpoint
cargo run --example compaction
cargo run --example faster_log
cargo run --example read_cache
cargo run --example f2_hot_cold
cargo run --example statistics
cargo run --example variable_length
cargo run --example config_store  # Configuration system demo
```

### Single Test Execution
```bash
# Run a specific test
cargo test test_name

# Run tests in a specific file
cargo test --test checkpoint

# Run with verbose output
cargo test test_name -- --nocapture

# Run with specific features
cargo test --features statistics test_name
```

## Architecture & Core Concepts

### Address System (src/address.rs)
48-bit logical addresses identify record positions in the hybrid log:
- 25-bit page offset (32 MB per page maximum)
- 23-bit page number (~8 million pages)
- Supports `AtomicAddress` for lock-free updates

### Epoch Protection (src/epoch/)
Lightweight epoch-based memory reclamation:
- Threads enter/exit epochs via `EpochGuard`
- Safe memory reclamation without traditional locks
- Delayed operation execution through epoch callbacks
- Critical for all concurrent operations

### Hybrid Log (src/allocator/hybrid_log.rs)
Three-region log architecture:
- **Mutable Region**: Latest pages supporting in-place updates
- **Read-Only Region**: Older pages in memory (immutable)
- **On-Disk Region**: Cold data persisted to storage devices

### Hash Index (src/index/)
High-performance concurrent hash table:
- Cache-line aligned buckets (64 bytes)
- 14-bit tags for fast key comparison
- Overflow bucket chains for collision handling
- Dynamic growth support (`src/index/grow.rs`)
- Optional cold index for disk-based lookups

### Persistence & Type Model (CRITICAL)

The persistence boundary is **explicit and type-safe** via the codec system:

#### Supported Persistence Modes:
1. **Blittable/POD Mode** (`BlittableCodec<T>`):
   - Types must implement `bytemuck::Pod`
   - Byte-wise copy to/from disk (fixed-size records)
   - No serialization overhead
   - Example: `u64`, `i32`, `[u8; 16]`, POD structs

2. **Variable-Length Mode**:
   - `RawBytes`: raw byte slices (no SpanByte envelope)
   - `Utf8`: UTF-8 strings (no SpanByte envelope)
   - `Bincode<T>`: serde-compatible types in SpanByte envelope (FASTER-style)

#### Key Constraints:
- Keys must implement `PersistKey` (which binds to a `KeyCodec`)
- Values must implement `PersistValue` (which binds to a `ValueCodec`)
- Non-POD types CANNOT silently work in persistent mode - they must use explicit codecs
- Hash stability is critical: uses xxHash (xxh3 by default, xxh64 optional feature)

#### Codec System (src/codec/):
- `KeyCodec<K>` / `ValueCodec<V>` traits define encode/decode
- `RecordView<'a>` provides zero-copy access to encoded bytes
- `Session::read_view()` returns borrowed encoded data without deserialization

### Checkpoint & Recovery (src/checkpoint/, src/store/faster_kv/checkpoint.rs)

#### Workstream B: CPR Integration (IN PROGRESS)
The CPR (Concurrent Prefix Recovery) protocol is **not yet fully integrated** into the operational path:
- `ThreadContext.version` exists but not updated during normal operations
- Checkpoint phases (PrepIndexChkpt, WaitPending, WaitFlush) are partially implemented
- Multi-thread checkpoint-under-load is **not production-ready**

When working with checkpoints:
- Checkpoint creates: metadata snapshot + index snapshot + log snapshot + delta log
- Recovery loads these artifacts but lacks corruption detection/checksums
- Session state persistence exists but version consistency needs validation

#### Workstream C: Hybrid Log Durability (IN PROGRESS)
Background flushing is **not fully automated**:
- Page transitions (mutable → read-only → on-disk) don't automatically trigger flush
- `flush_until` writes pages but doesn't enforce durable semantics
- `StorageDevice::flush()` is not integrated into flush path

### FasterLog (src/log/)

#### Workstream D: Durable Log (IN PROGRESS)
Current FasterLog is **mostly in-memory**:
- `commit()` only advances in-memory pointers, doesn't guarantee durability
- No real `StorageDevice` integration for durable commit
- Recovery/scanning incomplete for crash consistency

### Storage Devices (src/device/)
- `FileSystemDisk`: Standard file-backed storage
- `NullDisk`: In-memory only (no persistence)
- `IoUringDevice`: Linux io_uring backend (feature-gated)
  - Real io_uring only on Linux with `io_uring` feature
  - Falls back to portable implementation otherwise

### Configuration System (src/config.rs)

Load from TOML + environment variable overrides:

```rust
use oxifaster::config::OxifasterConfig;

// Load from OXIFASTER_CONFIG env var (if set) + apply overrides
let config = OxifasterConfig::load_from_env()?;
let store_config = config.to_faster_kv_config();
let compaction_config = config.to_compaction_config();
let cache_config = config.to_read_cache_config();
let device = config.open_device()?;
```

Environment overrides format: `OXIFASTER__section__field`
Example: `OXIFASTER__store__table_size=2097152`

See `examples/config_store.rs` for complete example.

### Read Cache (src/cache/)
Optional hot data cache:
- LRU-like eviction
- Transparently integrated into read path
- Configured via `ReadCacheConfig`
- Enable with `FasterKv::with_read_cache()`

### Compaction (src/compaction/)
Space reclamation for obsolete records:
- Manual: `store.compact()`
- Automatic: `store.start_auto_compaction()` spawns background worker
- Configured via `CompactionConfig`
- Concurrency under checkpoint/compaction needs validation

### Statistics (src/stats/)
Performance metrics collection (feature-gated with `statistics` feature):
- Read/write hits/misses
- Cache hit rates
- Operation counters
- Integrated into all CRUD paths

## Feature Flags

```toml
default = ["hash-xxh3"]          # xxHash3 for key hashing
statistics = []                   # Enable statistics collection
io_uring = ["dep:io-uring"]      # Linux io_uring backend (Linux only)
hash-xxh3 = [...]                # xxHash3 (default, recommended)
hash-xxh64 = [...]               # xxHash64 (alternative)
f2 = []                          # F2 two-tier hot-cold architecture
```

## Development Workflow & Standards

### MSRV & Tooling
- **MSRV**: Rust 1.92.0 (enforced in `Cargo.toml`)
- **CI must stay green**: All PRs must pass fmt, clippy (with `-D warnings`), tests, and feature matrix
- **No warnings allowed**: `cargo clippy` configured with `-D warnings`

### Code Standards
- Run `cargo fmt --all` before committing (CI enforces this)
- Fix all clippy warnings before submitting PR
- Add tests for new functionality
- Maintain MSRV compatibility

### Testing Priorities
Current gaps (from DEV.md):
- Multi-thread checkpoint-under-load tests
- Crash consistency tests (kill -9 during checkpoint/flush)
- Concurrency correctness (Loom, Miri for core primitives)
- Fuzzing for checkpoint metadata/delta log parsing
- Platform matrix (Linux/macOS/Windows)

### Unsafe Code
The codebase contains ~180 `unsafe` blocks. When adding/modifying unsafe code:
- Add explicit "Safety:" comment explaining invariants
- Document what enforces the invariants
- Reference core invariants: epoch ownership, page lifetime, address regions, record layout

### Error Handling
- Library code should minimize panics
- Classify panics: "programmer error" vs "runtime error" (use `Result`/`Status`)
- Use `Status`/`OperationStatus` for operation results

## Production Readiness Notes

This is a research-grade port moving toward production readiness. Key gaps (see DEV.md):

1. **Workstream A** (Type Model): ✅ Implemented - Codec system enforces safe persistence
2. **Workstream B** (CPR Integration): 🚧 In Progress - Checkpoint not fully integrated into ops path
3. **Workstream C** (Hybrid Log Durability): 🚧 In Progress - Background flush not automated
4. **Workstream D** (Durable FasterLog): 🚧 In Progress - Not yet crash-consistent
5. **Workstream E** (Observability): 🚧 Partial - Statistics exist, tracing integration incomplete
6. **Workstream F** (CI Expansion): 🚧 Ongoing - Need crash tests, Loom, Miri, fuzzing

When implementing features:
- Prioritize correctness over performance initially
- Add comprehensive tests (especially for concurrent scenarios)
- Document durability guarantees explicitly
- Consider checkpoint/compaction interactions

## Key Files to Understand

### Store Operations
- `src/store/faster_kv.rs`: Main FasterKV implementation
- `src/store/session.rs`: Synchronous session (CRUD + RMW)
- `src/store/async_session.rs`: Async session wrapper
- `src/store/contexts.rs`: Thread/execution contexts
- `src/store/pending_io.rs`: Pending I/O manager for disk reads

### Persistence
- `src/codec/`: Type-safe persistence boundary (CRITICAL)
- `src/store/record_format.rs`: Record layout (fixed + varlen)
- `src/checkpoint/`: Checkpoint/recovery state machine
- `src/allocator/hybrid_log/checkpoint.rs`: Log snapshot logic

### Core Infrastructure
- `src/allocator/hybrid_log.rs`: Hybrid log allocator
- `src/index/mem_index/`: In-memory hash index implementation
- `src/epoch/light_epoch.rs`: Epoch protection
- `src/device/`: Storage device abstractions

### Advanced Features
- `src/compaction/`: Log compaction
- `src/cache/`: Read cache
- `src/f2/`: F2 hot-cold architecture
- `src/log/faster_log.rs`: FasterLog standalone log
- `src/stats/`: Statistics collection

## References

- [FASTER Official Docs](https://microsoft.github.io/FASTER/)
- [FASTER C++ Source](https://github.com/microsoft/FASTER/tree/main/cc)
- [FASTER Paper](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/)
- See `DEV.md` for detailed production readiness roadmap
