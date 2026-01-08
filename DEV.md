# Development & Commercialization Guide

This document outlines the roadmap and requirements for transitioning `oxifaster` from an experimental project to a commercially viable, production-ready system.

## 1. Commercialization Roadmap

To achieve production readiness, the following areas must be addressed.

### 1.1 Code Quality & Reliability

*   **Error Handling**:
    *   **Goal**: Eliminate `unwrap()` and `expect()` in core library code (non-test/non-example).
    *   **Action**: Refactor `src/store/state_transitions.rs`, `src/index/mem_index/overflow.rs`, and `src/allocator/hybrid_log.rs` to return `Result<T, Status>` instead of panicking.
    *   **Specific Focus**: The CPR state machine (`state_transitions.rs`) relies heavily on `unwrap()`, assuming valid state transitions. Invalid transitions should likely be fatal errors or handled gracefully, but not unchecked panics.

*   **Concurrency & Safety**:
    *   **Goal**: robust thread safety and memory safety.
    *   **Action**: Audit all `unsafe` blocks (approx. 100+ occurrences). Each `unsafe` block must have a comment explaining *why* it is safe (invariants checked).
    *   **Action**: Review `std::sync::RwLock` usage. Current usage (`read().unwrap()`, `write().unwrap()`) will propagate panics if a thread crashes while holding a lock (poisoning). Determine if this "crash-whole-process" strategy is acceptable or if lock poisoning needs handling.

*   **Logging & Observability**:
    *   **Goal**: Structured, configurable logging instead of stdout/stderr.
    *   **Action**: Replace `println!` and `eprintln!` in library code (e.g., `src/index/cold_index.rs`, `src/epoch/light_epoch.rs`) with the `tracing` or `log` crate.
    *   **Action**: Ensure `src/stats/reporter.rs` writes to a configurable output, not just standard output.

### 1.2 Features & Completeness

*   **Platform Support**:
    *   **Action**: Finalize `io_uring` support for Linux. Currently marked as "Partial".
    *   **Action**: Verify fallback mechanisms (Mock IO) for non-Linux platforms work correctly under load.

*   **Configuration**:
    *   **Action**: Implement TOML/YAML configuration file support (currently "Pending" in Phase 5).

### 1.3 Testing & Validation

*   **Goal**: Confidence in data integrity and recovery.
    *   **Action**: Add **Fuzz Testing** (e.g., `cargo-fuzz`) to test the limits of the parser and allocator.
    *   **Action**: Implement **Chaos Engineering** tests (simulating disk failures, random crashes) to verify Checkpoint/Recovery (CPR) under adverse conditions.
    *   **Action**: Benchmark against the original C# FASTER to ensure performance parity or improvements.

---

## 2. Technical Debt & Known Issues

The following items were identified during code analysis:

*   **Hardcoded Panics**: `src/store/state_transitions.rs` contains multiple `unwrap()` calls on state transitions.
*   **Stderr Logging**: `src/index/cold_index.rs` and `src/epoch/light_epoch.rs` print warnings to stderr. This breaks the principle of "libraries should not print to stdout/stderr".
*   **Temporary Implementations**:
    *   `src/device/io_uring.rs`: Feature-gated, with a mock fallback. Needs rigorous testing to ensure the abstraction layer doesn't leak details or performance penalties.

---

## 3. Original Development Milestones

### Phase 1: Core (Completed :white_check_mark:)
*   Address System (48-bit)
*   Epoch Protection
*   Hash Index (MemHashIndex, HashBucket)
*   Hybrid Log (PersistentMemoryMalloc)
*   Storage Device Layer (NullDisk, FileSystemDisk)
*   FasterKV Core (CRUD)

### Phase 2: Persistence and Recovery (Completed :white_check_mark:)
*   Checkpoint Metadata Serialization
*   Full Checkpoint & Recovery (CPR Protocol)
*   Session Persistence
*   Incremental Checkpoint & Delta Log

### Phase 3: Performance Optimization (Completed :white_check_mark:)
*   Read Cache Integration
*   Log Compaction (Auto & Concurrent)
*   Index Growth with Rehash

### Phase 4: Advanced Features (Completed :white_check_mark:)
*   F2 Architecture (Hot-Cold Separation)
*   Cold Index (Disk-based)
*   Checkpoint Locks

### Phase 5: Platform and Ecosystem (Pending :construction:)
*   **io_uring Enhancement**: Advanced features (SQPOLL, registered files) and performance tuning.
*   **Configuration**: TOML file support.
*   **Statistics**: Enhanced metrics collection and reporting.

### Future / Optional (from C# FASTER)
*   **Priority 2**: Locking Context, Blittable/VarLen Allocator.
*   **Priority 3**: Remote Client/Server, Generic Allocator.
