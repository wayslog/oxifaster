# Production Readiness & FASTER Parity Guide

This document tracks the gaps and the implementation plan to move `oxifaster` from a research-grade port to a production-ready system, with an explicit parity mapping against Microsoft FASTER.

Scope notes:

- “FASTER” has multiple implementations and feature sets (C# and C++ variants). This guide focuses on the typical FASTER surface area: `FasterKV` + HybridLog + sessions + `Pending + CompletePending` flow + checkpoint/recovery + compaction, plus the standalone `FasterLog` concept.
- Production readiness here means: crash-consistent durability, well-defined API contracts, safe and documented type constraints, multi-thread correctness under checkpoint/compaction, observability, and CI gates that catch regressions.

## Tooling Gates (Must Stay Green)

- MSRV: Rust 1.92.0 (`Cargo.toml` `rust-version`)
- Local preflight (before merging changes): `./scripts/check-fmt.sh`, `./scripts/check-clippy.sh`, `./scripts/check-test.sh`
- CI gates (GitHub Actions): `./scripts/check-fmt.sh`, `./scripts/check-clippy.sh`, `./scripts/check-build.sh`, `./scripts/check-test.sh`, `./scripts/check-test-all-features.sh`, `cargo package`, and bench compile check

## Current State Summary (Repository Reality)

Implemented building blocks already in-tree:

- Core primitives: 48-bit `Address`, record header (`RecordInfo`), epoch protection (`LightEpoch`), in-memory hash index, hybrid log allocator, device abstraction (file, null, Linux `io_uring` with fallback)
- Store surface: `FasterKv` + `Session`/`AsyncSession`, CRUD + RMW, read cache, compaction, index growth API, incremental checkpoint (delta log) artifacts
- Tests: integration tests for CRUD, checkpoint/recovery, compaction, cold index, read cache, incremental checkpoint, pending I/O, scanning, async session, statistics
- CI: fmt/clippy/test/all-features/MSRV/coverage/package/bench compile check

Where the implementation is still not production-equivalent to FASTER is mostly about correctness semantics (CPR integration), durability guarantees, and type-safety boundaries for persistence.

## FASTER Parity Matrix (High-Level)

Legend:

- Implemented: present with production-grade semantics and tests
- Partial: present but semantics incomplete / limited / not hardened
- Missing: not present in a meaningful way

### FasterKV Core

- CRUD + RMW: Implemented (sync session); tests present
- Session model (per-thread session, serial numbers): Implemented, but CPR/version integration is Partial (see “Critical Gaps”)
- Pending + CompletePending flow: Partial
  - Background disk readback exists (`PendingIoManager`), but the model is limited (see “Type Model” and async integration notes)
- Async API: Partial
  - `AsyncSession` exists, but is currently a cooperative wrapper rather than a fully integrated async completion model (no per-request waker/notification)

### Hybrid Log & Tiering

- In-memory ring with mutable/read-only/on-disk regions: Partial
  - Region boundaries exist, but the automatic background flushing and stable “on-disk” semantics are not fully wired (see “Critical Gaps”)
- Read cache: Implemented (feature-level); tests present
- Compaction: Implemented (manual + auto); concurrency hardening still needs validation under real CPR semantics

### Checkpoint & Recovery (CPR)

- Checkpoint artifacts: Implemented (metadata + index snapshot + log snapshot + incremental delta files)
- CPR protocol (concurrent prefix semantics): Partial
  - The state machine exists, but it is not integrated into the normal operation path and currently behaves more like a single-threaded checkpoint driver
- Incremental checkpoint (delta log): Partial
  - Artifacts exist; correctness under concurrent updates and crash scenarios needs dedicated verification
- Recovery: Partial
  - Loads snapshots/deltas; missing corruption hardening (format versioning/checksums) and rigorous crash-consistency tests

### FasterLog (Standalone Log)

- Append/commit/scan surface: Partial
  - Current `commit()` only advances an in-memory pointer; it does not make data durable on the `StorageDevice`
- Durable commit groups, recovery, file format hardening: Missing

### Devices & Platform

- File-backed device: Implemented
- Linux `io_uring`: Partial (feature-gated); requires correctness/performance hardening and feature-matrix testing
- Non-Linux fallback: Implemented (portable backend), but needs stress testing under load

## Critical Gaps Blocking “Production-Ready”

This is the short list of issues that must be resolved before treating the system as production-grade.

### 1) Persistence Type Model Is Not Explicit (and Not Safe by Default)

Today, the storage format is effectively “blittable-only”:

- Records are written by moving `K`/`V` into raw log memory (via `ptr::write`), and log pages are persisted as raw bytes.
- Disk readback parsing explicitly refuses types that `needs_drop()` (e.g. `String`, `Vec`, `SpanByte`), so reads from disk will stay `Pending` forever for non-POD types.
- Variable-length utilities (`SpanByte`, varlen traits) exist but are not integrated into the core log allocator/store record format; using them with persistence is not meaningful yet.

Production implications:

- Without a defined serialization strategy, any non-POD key/value type cannot be safely persisted or recovered.
- Even for in-memory usage, non-POD types stored in raw log memory require a clear destruction/reclamation strategy; otherwise the system accumulates leaked heap allocations over time.

### 2) CPR Is Not Integrated into the Operational Fast Path

The checkpoint state machine exists, but the “FASTER-style cooperative protocol” is not wired into `Session::refresh()` / operation entry points.

Symptoms in current code structure:

- `ThreadContext.version` exists but is not updated in normal operations.
- Checkpoint phases such as `PrepIndexChkpt`, `WaitPending`, and the “threads help advance state machine” behavior are stubbed/no-op in the store checkpoint driver.

Production implications:

- Checkpoint artifacts can be inconsistent under concurrent operations.
- Incremental checkpoints are especially sensitive: the “prefix” guarantee needs clear boundaries and tests.

### 3) Hybrid Log Durability & On-Disk Semantics Are Not Fully Implemented

Although `flush_until` exists, critical pieces are still incomplete:

- Page transitions (mutable -> read-only -> on-disk) do not automatically trigger real background flush and eviction.
- `flush_until` writes pages but does not clearly define and enforce “durable” vs “buffered” semantics (`StorageDevice::flush()` is not part of the allocator flush path).

Production implications:

- “Cold data on disk” is not a reliable invariant.
- Crash-consistent durability claims cannot be made without explicit fsync/flush semantics.

### 4) FasterLog Is Not Yet a Persistent Log

The current `FasterLog` implementation is primarily an in-memory ring with metadata pointers. It needs real `StorageDevice` integration for durable commit and recovery.

## Code Quality & Engineering Gaps

### Unsafe & Invariants

- The codebase contains a large amount of `unsafe` usage (currently ~180 occurrences). Many blocks have rationale, but production readiness requires consistent, auditable invariants:
  - Every `unsafe` block should have a localized “Safety:” justification that states required invariants and what enforces them.
  - A small set of core invariants should be documented centrally (epoch ownership, page lifetime, address region rules, record layout rules).

### Panics and Error Modeling

- Library code should minimize panics under valid API use. Remaining `expect()`/panic sites should be classified:
  - “Programmer error” (documented precondition) vs “runtime error” (must return `Result`/`Status`).

### Config Coherence

- Configuration fields must reflect actual behavior:
  - `FasterKvConfig.mutable_fraction` is currently not reflected in `HybridLogConfig` (the allocator uses a fixed `memory_pages / 4` mutable region).

### Observability

- `tracing` is a dependency, but end-to-end structured tracing is not yet a first-class observability story.
- Statistics exist; align them with feature flags and ensure output is suitable for production (no direct stdout/stderr in library paths).

## Testing / CI Gaps (What to Add)

Existing integration tests are strong for “happy path” functionality; production readiness needs adversarial and semantic tests:

- CPR semantics tests:
  - Multi-thread “checkpoint while writes/reads ongoing” with invariants checked (prefix guarantee, session serial correctness)
  - “help advance state machine” behavior in the presence of stalled threads
- Crash consistency tests:
  - Kill -9 style termination during checkpoint / flush / incremental delta write; verify recovery behavior and corruption detection
  - Fault injection device (short write, partial read, delayed flush, IO error) for deterministic coverage
- Concurrency correctness:
  - Loom tests for key lock-free structures / state transitions (or a minimal set around checkpoint + index growth)
  - Miri runs (where feasible) for UB detection in core record/page primitives
- Fuzzing:
  - Fuzz delta log entry parsing, snapshot parsing, metadata parsing
- Platform matrix:
  - Linux/macOS/Windows compilation and core tests
  - Linux `io_uring` workflow that actually exercises the backend
- Performance regression gates:
  - Baseline YCSB + a minimal “ops/sec + p99 latency” regression guard (even if only as a periodic job)

## Documentation Gaps (What to Write/Update)

Before production adoption, the following must be explicit and discoverable:

- Type model: what is safe to persist/recover; what is “blittable-only”; how varlen types are supported (or not yet supported)
- Durability contract: what operations are durable, when, and what `flush`/fsync means across devices
- Checkpoint operational guide: checkpoint lifecycle, incremental chains, retention/cleanup, validation tooling
- On-disk formats: snapshot/delta/index formats with versioning and corruption detection strategy
- Performance tuning guide: sizing knobs, page sizes, cache sizing, compaction tuning

## Implementation Plan (Detailed, Production-Oriented)

This plan is organized into workstreams with clear deliverables and acceptance criteria.

### Workstream A: Define and Enforce a Safe Persistence Type Model

Goal: make persistence semantics correct and explicit before expanding features.

Deliverables:

1. Define two supported persistence modes:
   - Blittable/POD mode: `K` and `V` must be safe for byte-wise persistence and recovery
   - VarLen/serialized mode: explicit serialization into the log format (SpanByte-style), no raw pointers on disk
2. Enforce the rules at compile-time where possible (preferred), otherwise at runtime with clear errors.
3. Define destruction/reclamation semantics for in-memory records (no silent unbounded leaks for non-POD types).

Acceptance criteria:

- The public docs state the type constraints clearly.
- Non-POD types cannot silently “appear to work” in persistent mode; they should be rejected or routed through serialization.
- Recovery tests cover both success and expected-failure cases.

### Workstream B: Wire CPR into the Operational Path (True Cooperative Checkpointing)

Goal: implement the FASTER-style “threads cooperate to progress checkpoint/recovery” semantics.

Key tasks:

1. Implement a `refresh()`/operation-entry hook that:
   - Observes `system_state`
   - Updates `ThreadContext.version` appropriately
   - Helps advance checkpoint phases when required
2. Implement real behaviors for checkpoint phases:
   - `PrepIndexChkpt`: thread rendezvous / safe points
   - `WaitPending`: ensure pending operations are drained consistently
   - `WaitFlush`: ensure durability boundary is met before finalizing metadata
3. Ensure checkpoint metadata reflects a coherent view:
   - Versioning is consistent across index/log metadata and record headers
   - Session serial numbers are meaningful and used during recovery validation

Acceptance criteria:

- Multi-thread checkpoint-under-load tests pass consistently.
- Checkpoint artifacts are validated on recovery; corrupted/incomplete checkpoints are detected and rejected.

### Workstream C: Make Hybrid Log “On-Disk Region” Real

Goal: background flushing and eviction consistent with FASTER hybrid log semantics.

Key tasks:

1. Implement the read-only transition logic to actually schedule flushes.
2. Track page flush status correctly (use `PageInfo` / flush status) and advance `safe_read_only_address` only after flush completes.
3. Define durability:
   - When a page is considered persisted
   - When `StorageDevice::flush()` is required and who calls it
4. Provide a controlled API for forcing flush and shifting head (needed for tests, compaction, ops tooling).

Acceptance criteria:

- “read from disk” path is exercised in tests without manual hacks.
- Crash consistency tests around flush + checkpoint behave as documented.

### Workstream D: Implement Durable FasterLog

Goal: bring `FasterLog` to parity with the “persistent log” concept, not just an in-memory ring.

Key tasks:

1. Implement append that writes to device pages (or a buffered staging area that flushes on commit).
2. Implement `commit()` that guarantees durability (including `StorageDevice::flush()` semantics).
3. Implement recovery: load metadata, rebuild tail/commit pointers, and support scanning committed entries after restart.
4. Add corruption detection and format versioning for log files.

Acceptance criteria:

- End-to-end tests: append -> commit -> drop -> reopen -> scan/read -> data matches.

### Workstream E: Observability, Configuration, and Operational Tooling

Goal: make the system operable in production environments.

Key tasks:

1. Structured tracing:
   - Add `tracing` spans/events around checkpoint, flush, compaction, growth, recovery, pending I/O
2. Metrics:
   - Decide on an export model (pull/push) and keep the internal collector consistent
3. Configuration:
   - Add a TOML config file loader (and a stable schema) that maps to `FasterKvConfig`/device config/compaction config/cache config
4. Operational tooling:
   - Checkpoint listing/validation CLI (optional but high leverage for production ops)

Acceptance criteria:

- A minimal “ops story” exists: configure, run, checkpoint, validate, recover, observe.

### Workstream F: CI Expansion (Regressions Must Be Caught Automatically)

Goal: raise confidence with adversarial tests and platform coverage.

Key tasks:

- Add workflows for:
  - Cross-platform build/test matrix
  - `io_uring` backend exercise on Linux
  - Miri (best-effort subset), sanitizer (best-effort), loom (targeted)
  - Fuzz jobs (nightly/cron)
  - Performance smoke regression (cron)

Acceptance criteria:

- A PR that breaks durability semantics, checkpoint correctness, or introduces UB has a high chance of being caught pre-merge.

## Suggested Priority Order (Recommended)

1. Workstream A (type model) + Workstream B (CPR integration): establishes correctness boundaries
2. Workstream C (hybrid log on-disk semantics): makes “bigger than memory” real
3. Workstream D (durable FasterLog): completes the second flagship component
4. Workstreams E/F: operability and sustained quality
