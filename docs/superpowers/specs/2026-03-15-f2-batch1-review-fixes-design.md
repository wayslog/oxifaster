# F2 Batch1 Correctness Review Fixes

Date: 2026-03-15
Branch: f2/batch1-correctness
Scope: Full sweep -- all CRITICAL, HIGH, MEDIUM issues + simplification + test coverage gaps

## Background

Four specialized review agents analyzed the f2/batch1-correctness branch (24 commits, +1644/-37 lines). They identified 3 CRITICAL, 4 HIGH, 5 MEDIUM issues, plus simplification opportunities and test coverage gaps. This spec describes all fixes.

## CRITICAL Fixes

### C1. RMW fast path missing read cache invalidation

**Problem**: `rmw()` in-place update path does not call `invalidate_read_cache()`. A key previously backfilled from cold store remains in cache with stale value after RMW.

**File**: `src/f2/store/kv_ops.rs` -- `rmw()` method

**Fix**: Add `self.invalidate_read_cache(hash.hash())` at the top of `rmw()`, before the fast path check. Also add invalidation in `conditional_upsert_into_hot` on the success path.

**Test**: `test_f2_rmw_invalidates_read_cache` -- write key, flush to cold, read (populates cache), RMW, read again, assert updated value.

### C2. Tombstone CAS failure counted as compacted -- dangling index pointer

**Problem**: In `compact_cold_log_with_scan`, when a tombstone's index CAS fails (concurrent upsert), the record is still counted as compacted and the address range is reclaimed. The index still points to the reclaimed address.

**File**: `src/f2/store/log_compaction.rs` -- tombstone handling block (~line 304)

**Fix**:
- If `try_update_entry` returns failure (CAS lost), do NOT increment `records_compacted`. Instead increment `records_skipped` and set `new_begin_address` to prevent reclaiming this range.
- If `atomic_entry` is `None`, also skip the record (do not count as compacted).

**Test**: Verify tombstone compaction correctness -- after compaction with concurrent upsert on a tombstoned key, the new value is still accessible.

### C3. RMW retry exhaustion silent fallback loses concurrent updates

**Problem**: After 256 CAS retries, RMW falls back to plain `upsert`, silently violating atomicity. Returns `Ok(())`.

**File**: `src/f2/store/kv_ops.rs` -- RMW retry loop fallback (~line 124)

**Fix**: Replace the fallback `upsert` with `return Err(Status::Aborted)`. Let callers decide retry strategy.

**Test**: Validate that under extreme contention (if retry exhaustion occurs), the error is properly returned.

## HIGH Fixes

### H1. Recovery silently ignores log metadata read failure

**Problem**: `recover()` defaults version to 0 when `log.meta` read fails. Partial checkpoint recovery produces inconsistent state.

**File**: `src/f2/store.rs` -- `recover()` method (~line 393)

**Fix**: If index and log recovered successfully but metadata read fails, return `Err(Status::Corruption)`.

### H2. Background compaction errors completely discarded

**Problem**: `compact_hot_log` and `compact_cold_log` results discarded with `let _ = ...`.

**File**: `src/f2/store.rs` -- background worker (~line 681)

**Fix**: Replace with `if let Err(e) = ...` and `tracing::warn!("hot/cold compaction failed: {e:?}")`. Do not change control flow.

### H3. CAS failure log space leakage -- document and track

**Problem**: `conditional_upsert_into_hot`, `copy_record_in_cold_store`, `copy_record_to_hot_tail` all allocate log space before CAS. On CAS failure, space is leaked.

**Files**: `src/f2/store/kv_ops.rs`, `src/f2/store/log_compaction.rs`

**Fix**: This is inherent to FASTER architecture. Add `bytes_leaked: u64` to `CompactionStats`. Increment on CAS failure paths. Add code comments documenting the tradeoff.

### H5. `compact_log` cold path returns wrong `new_begin_address`

**Problem**: `compact_log` forces `until_address` for the cold path's `CompactionResult.new_begin_address`, ignoring the adjusted `new_begin_address` set by `compact_cold_log_with_scan` when CAS failures occur. The hot path correctly uses `new_begin_address`. This is inconsistent and causes callers to receive incorrect compaction results.

**File**: `src/f2/store/log_compaction.rs` -- `compact_log` (~line 113)

**Fix**: Use `*new_begin_address` for both hot and cold paths, matching the existing pattern.

### H4. Read cache backfill race with concurrent invalidation -- document

**Problem**: Thread A reads cold V1, Thread B upserts V2 and invalidates cache, Thread A backfills stale V1.

**File**: `src/f2/store/kv_ops.rs` -- read cache backfill (~line 41)

**Fix**: Add doc comment explaining this is a known limitation. Read cache provides eventual consistency, not strong consistency.

## MEDIUM Fixes

### M1-M4. Error information loss -- unified tracing

**Strategy**: Use `tracing::warn!` / `tracing::debug!` to record original errors before mapping to generic Status variants. Do not change the `Status` enum.

- `log_compaction.rs` flush: `tracing::warn!("flush failed: {e:?}")` before returning `Status::Corruption`
- `store.rs` `checkpoint_store`: Change return type from `bool` to `Result<(), Status>`. Propagate errors.
- `kv_ops.rs` disk read: `tracing::debug!("disk read failed: {e:?}")` in `map_err`
- `store.rs` `save_checkpoint`: `tracing::warn!` before `map_err`

### M5. Read cache backfill failure metrics

**File**: `src/f2/store/kv_ops.rs`

**Fix**: On `try_insert` failure, `tracing::debug!("read cache backfill failed")`.

## Simplification

### S1. Merge `copy_record_to_hot_tail` and `copy_record_in_cold_store`

Both functions have identical logic (allocate, write header/key/value, CAS update index). Extract `copy_record_in_store(store, ...)` and call from both paths. Must preserve H3's `bytes_leaked` tracking on CAS failure paths.

### S2. Merge `upsert_into_store` and `tombstone_into_store`

Share 90% code. Add `value: Option<&V>` parameter (None = tombstone). Single function `write_record_into_store`.

### S3. Extract test helpers

- `make_f2() -> F2Kv<TestKey, TestValue, NullDisk>` -- default construction
- `with_session(f2, f)` -- start session, run closure, stop session
- Eliminates ~100 lines of duplication across 25+ tests

### S4. Remove dead code branch in `compact_log`

Third `else if shift_begin_address` is unreachable (StoreType is exhaustive with Hot and Cold). Replace with `unreachable!("StoreType is exhaustive")` as a safety net.

### S5. Unify `recover` phase reset

Extract phase reset to closure pattern (already used in `save_checkpoint`). Eliminates 4 duplicated reset-and-return blocks.

### S6. Extract `enter_session` from `start_session` / `continue_session`

Share checkpoint phase check, epoch protect, session count logic.

## Test Coverage

### T1. Concurrent compaction + read/write interleaving

Spawn writer threads alongside compaction. Verify no data corruption after compaction completes.

### T2. Fix `test_f2_heavy_enter_multi_thread_countdown`

Make it truly multi-threaded: spawn N threads that call `heavy_enter()` concurrently. Verify countdown correctness under actual concurrency.

## Commit Strategy

One commit per fix, ordered by priority:
1. C1, C2, C3 (critical correctness)
2. H1, H2, H3, H4, H5 (high importance)
3. M1-M5 (medium -- error tracing)
4. S1-S6 (simplification)
5. T1-T2 (test coverage)

After all fixes: create PR with review report in description.

## Out of Scope

- Changing `Status` enum to carry inner errors (would be a breaking change)
- Adding RMW retry with backoff (design choice for callers)
- Implementing version-tagged cache invalidation protocol (significant complexity)
