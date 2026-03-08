# F2 Engine Improvement Design

Date: 2026-03-09

## Background

F2 (Fast & Fair) is the two-tier hot-cold storage architecture ported from Microsoft FASTER C++.
The current Rust implementation has a functional foundation but significant gaps compared to the
C++ reference implementation (`cc/src/core/f2.h`).

This design covers a comprehensive improvement plan organized in three batches, prioritized as:
correctness > functionality > performance.

Compatibility target: strict binary/API compatibility with C++ FASTER F2.

## Current Gap Summary

| Gap | Severity | Description |
|-----|----------|-------------|
| Multi-thread session (per-thread epoch) | P0 | Hardcoded thread_id=0, epoch protection ineffective |
| ConditionalInsert + RMW retry | P0 | Concurrent RMW may lose updates (TOCTOU race) |
| Async/Pending IO path | P1 | Cannot read records flushed to disk |
| Read Cache integration | P1 | Config exists but code not implemented |
| Checkpoint coordination | P1 | heavy_enter is stub, no lazy checkpoint |
| Multi-threaded compaction | P1 | Single-threaded, bottleneck on large datasets |
| Cold store record-level compaction | P2 | Only address truncation, no record scanning |
| Variable-length key/value | P2 | Pod types only |
| TOML config integration | P3 | F2Config lacks serde derives |
| Statistics integration | P3 | Operation-level stats not implemented |
| Test coverage | Global | No concurrency, checkpoint roundtrip, large data, or FileSystemDisk tests |

## Batch 1: Correctness Fixes

### 1.1 Multi-thread Session (per-thread epoch)

**Problem**: `start_session`/`stop_session` hardcode `epoch.protect(0)`/`epoch.unprotect(0)`.
All threads share thread_id=0, epoch protection is completely ineffective under concurrency.

**Solution**:
- F2Kv holds shared `LightEpoch` instances for hot and cold stores
- Each session acquires an independent thread_id via `epoch.acquire_thread_id()`
- `start_session` returns session_id (= thread_id), `stop_session` takes session_id and calls `release_thread_id`
- Follow FasterKV's `Session` pattern: session holds `EpochGuard`, automatic protection during lifetime
- Both store epochs are bumped in `refresh()` but managed independently (no `SetOtherStore` mutual reference; Rust's borrow checker prevents circular references, external coordination is safer)

**Files affected**: `src/f2/store.rs`, `src/f2/store/kv_ops.rs`

### 1.2 ConditionalInsert + RMW Retry Queue

**Problem**: RMW uses `read -> modify -> upsert` path. Between read and upsert there is a
TOCTOU race -- other threads may modify the same key in the gap.

**Solution**:
- Add `conditional_insert(key, value, expected_address) -> Status` at FasterKV layer
  - Atomically check if index entry address equals `expected_address`, insert if so, return `Status::Aborted` otherwise
  - Semantically identical to C++ `ConditionalInsert`
- F2 RMW flow becomes:
  1. Try in-place RMW on hot store (if mutable region has matching record)
  2. On failure, record current `expected_hlog_address`
  3. Read current value from hot/cold
  4. Apply modify function
  5. Call `conditional_insert(key, new_value, expected_hlog_address)`
  6. If Aborted, enqueue into retry queue
- Add `retry_rmw_requests: ConcurrentQueue<RmwRetryContext>` field
- `complete_pending()` drains retry queue and re-executes

**Files affected**: `src/store/faster_kv.rs` (ConditionalInsert), `src/f2/store/kv_ops.rs`, `src/f2/store.rs`

### 1.3 Fix Unused Config Fields

**Problem**: `mutable_fraction`, `read_cache`, `log_path` configs exist but are not passed to underlying components.

**Solution**:
- `InternalStore::new` uses config's `mutable_fraction` when creating `HybridLogConfig`
- `log_path` reserved for future auto-device creation, devices still passed by caller
- `read_cache` wiring deferred to Batch 2, but ensure config is correctly propagated

**Files affected**: `src/f2/store/internal_store.rs`, `src/f2/config.rs`

### 1.4 Test Additions (Batch 1)

- **Concurrency tests**: Multi-thread simultaneous upsert/read/delete/rmw, verify epoch protection and CAS correctness
- **Checkpoint roundtrip**: Write data -> save_checkpoint -> new F2Kv -> recover -> verify all data readable
- **CAS contention tests**: Multi-thread concurrent RMW on same key, verify final value correctness (linearizability)
- **ConditionalInsert tests**: Verify expected_address mismatch returns Aborted

## Batch 2: Core Feature Alignment

### 2.1 Async/Pending IO Path

**Problem**: `internal_read` only searches in-memory. Records flushed to disk cannot be read.
`complete_pending` is a no-op.

**Solution**:
- Follow FasterKV's `PendingIoManager` and `AsyncSession` patterns
- Define F2-specific pending context types:
  - `PendingF2Read { stage: ReadOperationStage, key, callback }` -- tracks hot-read pending continuation to cold-read
  - `PendingF2Rmw { stage: RmwOperationStage, key, expected_address, modify_fn, callback }` -- tracks multi-stage RMW state
- When `internal_read` finds record on disk (address < begin_address but above safe_head_address):
  - Issue async IO request (`hlog.async_read_pages`)
  - Return `Status::Pending`
  - IO completion callback continues to next stage
- Synchronous session's `complete_pending(wait=true)` polls all pending IO until completion
- Add `AsyncF2Session` wrapper, similar to FasterKV's `AsyncSession`, based on `YieldOnce` future (no tokio dependency)

**Files affected**: `src/f2/store/kv_ops.rs`, `src/f2/store.rs`, new file `src/f2/store/async_session.rs`, new file `src/f2/store/pending_contexts.rs`

### 2.2 Read Cache Integration

**Problem**: `HotStoreConfig` has `read_cache` config but code never uses it.

**Solution**:
- Mount Read Cache on hot store's `InternalStore` (if `read_cache` configured)
- `internal_read` flow adjustment:
  1. Check hot store read cache
  2. Check hot store hlog
  3. Check cold store
  4. On cold hit, call `read_cache.try_insert(key, value)` to backfill into hot store's read cache
- Read cache eviction uses existing LRU mechanism
- RMW path must skip read cache entries (they are copies of cold data, should not be updated in-place)

**Files affected**: `src/f2/store/kv_ops.rs`, `src/f2/store/internal_store.rs`

### 2.3 Checkpoint Coordination

**Problem**: `heavy_enter` is a stub, lazy checkpoint not implemented, compaction+checkpoint not merged.

**Solution**:
- **`heavy_enter` implementation**: When checkpoint phase is not Rest, thread calls `heavy_enter`:
  - `HotStoreCheckpoint` phase: thread decrements `threads_pending_hot_store`, last thread advances to `ColdStoreCheckpoint`
  - `ColdStoreCheckpoint` phase: thread decrements `threads_pending_callback`, last thread advances to `Rest`
- **Lazy checkpoint**: After checkpoint request, wait up to 2 seconds. If hot-cold compaction triggers during wait, piggyback cold store checkpoint onto the compaction (saves one independent cold flush)
- **Compaction + Checkpoint merge**: `compact_hot_log_and_migrate` accepts optional `checkpoint_token` parameter. When compaction completes, it does cold store checkpoint as well

**Files affected**: `src/f2/store.rs`, `src/f2/state.rs`, `src/f2/store/log_compaction.rs`

### 2.4 Cold Store Record-Level Compaction

**Problem**: Cold log compaction only does `flush_until` + `shift_begin_address`, doesn't scan for expired/tombstone records.

**Solution**:
- Add `compact_cold_log_with_scan()` method:
  - Scan cold log for live records
  - Skip tombstones and superseded old versions
  - Copy surviving records to cold log tail
  - Update cold index
  - Shift begin address
- Migration policy: cold store compaction does not involve cross-store migration (cold data does not move back to hot), only internal garbage collection

**Files affected**: `src/f2/store/log_compaction.rs`

### 2.5 Test Additions (Batch 2)

- **Pending IO tests**: Use FileSystemDisk, insert enough data to flush hot log to disk, verify async read correctness
- **Read Cache tests**: Verify cold-read backfill into read cache, verify eviction behavior
- **Checkpoint under load**: Continuous read/write during checkpoint, verify data consistency
- **Large data tests**: 100K+ keys, trigger multiple compactions and migrations

## Batch 3: Performance and Usability

### 3.1 Multi-threaded Compaction

**Problem**: Compaction is single-threaded sequential scan, bottleneck on large datasets.

**Solution**:
- Follow C++ `ConcurrentLogPageIterator` pattern:
  - Divide log scan range into page-aligned segments
  - Each compaction thread claims a page range, scans and processes independently
  - Use `num_threads` config (already exists but unused)
- Thread coordination:
  - Main thread computes `begin_address` to `until_address` range and splits by page
  - Use `std::thread::scope` to spawn compaction threads
  - Each thread independently: scan pages -> decide migrate/copy per strategy -> write to target store
  - Index updates via CAS operations (existing hash index CAS is thread-safe)
  - After all threads complete, main thread executes `shift_begin_address`

**Files affected**: `src/f2/store/log_compaction.rs`

### 3.2 Compaction + Checkpoint Merge Execution

**Solution**:
- When cold store checkpoint is requested and hot-cold compaction is also pending:
  - Compaction threads write to cold store, then piggyback cold store index + hlog snapshot
  - Avoids separate IO pass for cold checkpoint after compaction
- This is an optimization only, does not affect correctness

**Files affected**: `src/f2/store/log_compaction.rs`, `src/f2/store.rs`

### 3.3 Variable-Length Key/Value Support (Codec Integration)

**Problem**: F2Kv type constraint is `K: Pod, V: Pod`, does not support variable-length data.

**Solution**:
- Extend F2Kv generic constraints from `K: Pod, V: Pod` to `K: PersistKey, V: PersistValue`
- Reuse existing Codec system (`BlittableCodec`, `RawBytes`, `Utf8`, `Bincode`)
- `internal_read`/`upsert_into_store`/`tombstone_into_store` use `KeyCodec`/`ValueCodec` for encode/decode
- Record size changes from compile-time fixed (`Record::<K,V>::size()`) to runtime computed
- log_compaction page traversal uses record header length field for per-record advancing

**Files affected**: `src/f2/store/kv_ops.rs`, `src/f2/store/log_compaction.rs`, `src/f2/store/internal_store.rs`, `src/f2/config.rs`

### 3.4 TOML Config Integration

**Solution**:
- Add `serde::Deserialize` derives to `F2Config`, `HotStoreConfig`, `ColdStoreConfig`, `F2CompactionConfig`
- Add `[f2]` section to `OxifasterConfig`:
  ```toml
  [f2.hot]
  index_size = 1048576
  log_mem_size = 536870912
  mutable_fraction = 0.6

  [f2.cold]
  index_size = 4194304
  log_mem_size = 268435456

  [f2.compaction]
  hot_log_size_budget = 1073741824
  cold_log_size_budget = 10737418240
  trigger_percentage = 90
  ```
- Environment variable override: `OXIFASTER__f2__hot__index_size=2097152`

**Files affected**: `src/f2/config.rs`, `src/config.rs`

### 3.5 Statistics (Operation-Level)

**Solution**:
- Under `#[cfg(feature = "statistics")]`, add F2-specific counters:
  - `hot_read_hits` / `hot_read_misses`
  - `cold_read_hits` / `cold_read_misses`
  - `read_cache_hits` / `read_cache_misses`
  - `hot_to_cold_migrations` / `cold_compacted_records`
  - `conditional_insert_success` / `conditional_insert_aborted`
  - `rmw_retry_count`
- Provide `print_stats()` / `reset_stats()` methods

**Files affected**: `src/f2/store.rs`, `src/f2/store/kv_ops.rs`, new or existing `src/f2/store/stats.rs`

### 3.6 Performance Benchmarks

**Solution**:
- Add F2-specific benchmarks:
  - Hot data read/write throughput (compare with single-tier FasterKV)
  - Hot-cold mixed read/write (zipfian distribution)
  - Compaction throughput (single vs multi-threaded)
  - Checkpoint latency

**Files affected**: `benches/f2_bench.rs` (new)

## Key Design Decisions

1. **No `SetOtherStore` mutual reference**: C++ uses raw pointers for cross-store references. In Rust, this would require `unsafe` or `Arc` with potential cycles. We coordinate externally in F2Kv instead.

2. **Preserve Rust-unique features**: `AccessFrequency` migration strategy and `KeyAccessTracker` are not in C++ but provide value. We keep them as additional options alongside the C++ default `AddressAging`.

3. **No tokio dependency for async**: Follow FasterKV's `YieldOnce` pattern for async session. No external runtime required.

4. **Codec integration is additive**: Batch 3's variable-length support extends (not replaces) the existing Pod type path. `BlittableCodec<T>` provides zero-cost Pod support.

## C++ Reference Files

- `/Users/wayslog/repo/cpp/FASTER/cc/src/core/f2.h` -- F2Kv main implementation
- `/Users/wayslog/repo/cpp/FASTER/cc/src/core/checkpoint_state_f2.h` -- HotColdCheckpointState
- `/Users/wayslog/repo/cpp/FASTER/cc/src/core/internal_contexts_f2.h` -- Async contexts
- `/Users/wayslog/repo/cpp/FASTER/cc/src/index/cold_index.h` -- ColdIndex
- `/Users/wayslog/repo/cpp/FASTER/cc/src/core/config.h` -- Config

## Verification Criteria

### Batch 1 Exit Criteria
- All existing tests pass
- Multi-thread concurrent CRUD tests pass
- Checkpoint roundtrip test passes
- ConditionalInsert + RMW retry under contention passes
- Coverage >= 80%

### Batch 2 Exit Criteria
- FileSystemDisk-backed read tests pass (records survive flush to disk)
- Read cache backfill verified
- Checkpoint under load test passes
- 100K+ key large data test passes
- Coverage >= 80%

### Batch 3 Exit Criteria
- Multi-threaded compaction throughput improves over single-threaded
- Variable-length key/value roundtrip test passes
- TOML config loading test passes
- F2 benchmark suite runs without errors
- Coverage >= 80%
