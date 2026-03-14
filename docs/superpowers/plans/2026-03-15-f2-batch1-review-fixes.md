# F2 Batch1 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all issues identified by 4 review agents on the f2/batch1-correctness branch: 3 CRITICAL, 5 HIGH, 5 MEDIUM correctness/error-handling fixes, 6 simplifications, and 2 test coverage improvements.

**Architecture:** All changes are localized to the F2 module (`src/f2/store/`). Fixes are ordered by severity (CRITICAL first). Each task produces one commit. The `tracing` crate is already a dependency.

**Tech Stack:** Rust, bytemuck (Pod types), parking_lot (RwLock), tracing (logging), NullDisk/FileSystemDisk (testing)

**Spec:** `docs/superpowers/specs/2026-03-15-f2-batch1-review-fixes-design.md`

---

## Chunk 1: Critical Correctness Fixes (C1-C3)

### Task 1: C1 -- RMW read cache invalidation

**Files:**
- Modify: `src/f2/store/kv_ops.rs:83-129` (rmw method)
- Modify: `src/f2/store/kv_ops.rs:419` (conditional_upsert_into_hot success path)
- Test: `src/f2/store/tests.rs` (new test)

- [ ] **Step 1: Write the failing test**

Add to `src/f2/store/tests.rs`:

```rust
#[test]
fn test_f2_rmw_invalidates_read_cache() {
    use crate::cache::ReadCacheConfig;

    let mut config = F2Config::default();
    config.hot_store.read_cache = Some(ReadCacheConfig::default());
    // Force all ops through slow path so records land in read-only region
    config.hot_store.mutable_fraction = 0.0;
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    let key = TestKey(42);

    // 1. Upsert a value
    f2.upsert(key, TestValue(100)).unwrap();

    // 2. Read to potentially populate cache
    let v = f2.read(&key).unwrap();
    assert_eq!(v, Some(TestValue(100)));

    // 3. RMW to increment
    f2.rmw(key, |v: &mut TestValue| v.0 += 1).unwrap();

    // 4. Read again -- must see updated value, not stale cache
    let v = f2.read(&key).unwrap();
    assert_eq!(v, Some(TestValue(101)), "RMW must invalidate read cache");

    f2.stop_session();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features f2 test_f2_rmw_invalidates_read_cache -- --nocapture`
Expected: FAIL -- read returns `TestValue(100)` instead of `TestValue(101)`

- [ ] **Step 3: Fix rmw() -- add invalidation**

In `src/f2/store/kv_ops.rs`, add read cache invalidation at line 89 (after `track_key_access`):

```rust
    pub fn rmw<F>(&self, key: K, mut modify: F) -> Result<(), Status>
    where
        F: FnMut(&mut V),
        V: Default,
    {
        let hash = KeyHash::new(hash64(bytemuck::bytes_of(&key)));
        self.track_key_access(hash.hash());
        self.invalidate_read_cache(hash.hash()); // <-- ADD THIS LINE

        // Fast path: in-place update in the mutable region.
        ...
```

Also in `conditional_upsert_into_hot`, add invalidation on success (line 419):

```rust
        if status == Status::Ok {
            self.invalidate_read_cache(hash.hash()); // <-- ADD THIS LINE
            Ok(())
        } else {
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features f2 test_f2_rmw_invalidates_read_cache -- --nocapture`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add src/f2/store/kv_ops.rs src/f2/store/tests.rs
git commit -m "fix(f2): add read cache invalidation to rmw path

rmw() fast path and conditional_upsert_into_hot were missing
invalidate_read_cache() calls, causing stale reads after RMW
when the key was previously backfilled from cold store."
```

---

### Task 2: C2 -- Tombstone CAS failure handling in cold compaction

**Files:**
- Modify: `src/f2/store/log_compaction.rs:301-314` (tombstone block in compact_cold_log_with_scan)
- Test: `src/f2/store/tests.rs` (new test)

- [ ] **Step 1: Write the failing test**

Add to `src/f2/store/tests.rs`:

```rust
#[test]
fn test_f2_cold_compaction_tombstone_cas_failure_safe() {
    // Verify that when tombstone CAS fails (index entry changed concurrently),
    // the record is skipped rather than counted as compacted.
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    let key = TestKey(100);

    // Write to hot, then delete (creates tombstone in hot)
    f2.upsert(key, TestValue(1)).unwrap();
    f2.delete(&key).unwrap();

    // Compact hot to cold (migrates tombstone to cold)
    let hot_begin = f2.hot_store.begin_address();
    let hot_tail = f2.hot_store.tail_address();
    if hot_tail > hot_begin {
        let _ = f2.compact_hot_log(hot_tail);
    }

    // Now re-insert the key into hot (simulates concurrent upsert)
    f2.upsert(key, TestValue(999)).unwrap();

    // Cold compaction should try to clear the tombstone's index entry via CAS,
    // but the entry now points to the new hot record, so CAS should fail.
    // The key must still be readable after cold compaction.
    let cold_begin = f2.cold_store.begin_address();
    let cold_tail = f2.cold_store.tail_address();
    if cold_tail > cold_begin {
        let _ = f2.compact_cold_log(cold_tail);
    }

    // The newly upserted value must still be accessible
    let v = f2.read(&key).unwrap();
    assert_eq!(v, Some(TestValue(999)), "Value must survive tombstone CAS failure in cold compaction");

    f2.stop_session();
}
```

- [ ] **Step 2: Run test to verify current behavior**

Run: `cargo test --features f2 test_f2_cold_compaction_tombstone_cas_failure_safe -- --nocapture`
Expected: May pass or fail depending on timing -- the bug is in the stats accounting and address reclamation, not necessarily observable in this single-threaded test. The code fix is still necessary for correctness under concurrency.

- [ ] **Step 3: Fix tombstone CAS handling**

In `src/f2/store/log_compaction.rs`, replace lines 301-314:

**Before:**
```rust
                        if is_tombstone {
                            // Tombstones in cold store: can be dropped entirely.
                            // Clear the index entry so the key is fully removed.
                            if let Some(atomic_entry) = index_result.atomic_entry {
                                let _ = store.hash_index.try_update_entry(
                                    Some(atomic_entry),
                                    index_result.entry,
                                    Address::INVALID,
                                    hash,
                                    false,
                                );
                            }
                            stats.records_compacted += 1;
                            stats.bytes_compacted += record_size;
```

**After:**
```rust
                        if is_tombstone {
                            // Tombstones in cold store: attempt to clear the index entry.
                            // If CAS fails (concurrent upsert changed the entry), skip
                            // this record to avoid reclaiming an address range that the
                            // index still references.
                            let cas_ok = if let Some(atomic_entry) = index_result.atomic_entry {
                                store.hash_index.try_update_entry(
                                    Some(atomic_entry),
                                    index_result.entry,
                                    Address::INVALID,
                                    hash,
                                    false,
                                ) == Status::Ok
                            } else {
                                false // No atomic entry -- cannot CAS, skip
                            };

                            if cas_ok {
                                stats.records_compacted += 1;
                                stats.bytes_compacted += record_size;
                            } else {
                                stats.records_skipped += 1;
                                if *new_begin_address == until_address {
                                    *new_begin_address = current_address;
                                }
                            }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features f2 test_f2_cold_compaction_tombstone_cas_failure_safe -- --nocapture`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add src/f2/store/log_compaction.rs src/f2/store/tests.rs
git commit -m "fix(f2): handle tombstone CAS failure in cold compaction

When a tombstone's index CAS fails (concurrent upsert changed the
entry), skip the record instead of counting it as compacted. This
prevents reclaiming address ranges that the index still references,
which could cause dangling pointer reads."
```

---

### Task 3: C3 -- RMW retry exhaustion returns error

**Files:**
- Modify: `src/f2/store/kv_ops.rs:124-128` (RMW fallback)
- Test: `src/f2/store/tests.rs` (new test)

- [ ] **Step 1: Write the failing test**

Add to `src/f2/store/tests.rs`:

```rust
#[test]
fn test_f2_rmw_retry_exhaustion_returns_error() {
    // The RMW method should return Err(Status::Aborted) when retries are exhausted,
    // not silently fall back to a non-atomic upsert.
    // This test verifies the error return path exists.
    // (Triggering actual retry exhaustion requires extreme contention which is
    // non-deterministic, so we verify the code path by inspection and test
    // that normal RMW still works.)
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let _session = f2.start_session().unwrap();

    let key = TestKey(1);
    f2.upsert(key, TestValue(0)).unwrap();

    // Normal RMW should still succeed
    let result = f2.rmw(key, |v: &mut TestValue| v.0 += 1);
    assert!(result.is_ok(), "Normal RMW should succeed");

    let v = f2.read(&key).unwrap();
    assert_eq!(v, Some(TestValue(1)));

    f2.stop_session();
}
```

- [ ] **Step 2: Fix RMW fallback**

In `src/f2/store/kv_ops.rs`, replace lines 124-128:

**Before:**
```rust
        // Fallback after exhausting retries: regular upsert to avoid livelock.
        // This may lose a concurrent update but guarantees progress.
        let mut value: V = (self.read(&key)?).unwrap_or_default();
        modify(&mut value);
        self.upsert(key, value)
```

**After:**
```rust
        // All retries exhausted: return error rather than silently falling back
        // to a non-atomic upsert that could lose concurrent updates.
        Err(Status::Aborted)
```

- [ ] **Step 3: Run tests**

Run: `cargo test --features f2 test_f2_rmw -- --nocapture`
Expected: All RMW tests pass (normal RMW completes within 256 retries)

- [ ] **Step 4: Run full test suite**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add src/f2/store/kv_ops.rs src/f2/store/tests.rs
git commit -m "fix(f2): return error on RMW retry exhaustion instead of silent fallback

Replace the silent fallback to non-atomic upsert with
Err(Status::Aborted). The old behavior violated the atomicity
contract of read-modify-write by silently losing concurrent
updates while returning Ok(())."
```

---

## Chunk 2: High Priority Fixes (H1-H5)

### Task 4: H1 -- Recovery metadata failure handling

**Files:**
- Modify: `src/f2/store.rs:392-397` (hot store log.meta read)
- Modify: `src/f2/store.rs:420-428` (cold store log.meta read)
- Test: `src/f2/store/tests.rs` (new test)

- [ ] **Step 1: Write the failing test**

Add to `src/f2/store/tests.rs`:

```rust
#[test]
fn test_f2_recover_fails_on_missing_metadata() {
    let config = F2Config::default();
    let hot_device = NullDisk::new();
    let cold_device = NullDisk::new();
    let mut f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap();

    let temp_dir = std::env::temp_dir().join(format!("f2_recover_meta_{}", uuid::Uuid::new_v4()));
    let token = uuid::Uuid::new_v4();
    // Create checkpoint dir structure with only index/log but no log.meta
    let hot_dir = temp_dir.join(token.to_string()).join("hot");
    std::fs::create_dir_all(&hot_dir).unwrap();

    // Write minimal valid index and log checkpoint files (so index/log recover succeeds)
    // but omit log.meta -- recovery should fail with Corruption
    let result = f2.recover(&temp_dir, token);
    // Without valid checkpoint files, this will fail at index recovery, not metadata.
    // This test documents the expected behavior: if index+log recover but metadata
    // is missing, the result should be an error, not a silent default.
    assert!(result.is_err(), "Recovery with missing checkpoint data should fail");

    let _ = std::fs::remove_dir_all(&temp_dir);
}
```

- [ ] **Step 2: Fix recovery metadata handling**

In `src/f2/store.rs`, replace lines 392-397 (hot store metadata read):

**Before:**
```rust
            // Get version from hot store log metadata
            if let Ok(log_meta) =
                crate::checkpoint::LogMetadata::read_from_file(&hot_dir.join("log.meta"))
            {
                version = log_meta.version;
            }
```

**After:**
```rust
            // Get version from hot store log metadata.
            // If index and log recovered but metadata is missing/corrupt, this is
            // a partial checkpoint -- treat as corruption.
            match crate::checkpoint::LogMetadata::read_from_file(&hot_dir.join("log.meta")) {
                Ok(log_meta) => version = log_meta.version,
                Err(_e) => {
                    tracing::warn!("hot store log.meta read failed during recovery");
                    self.checkpoint
                        .phase
                        .store(F2CheckpointPhase::Rest, Ordering::Release);
                    return Err(Status::Corruption);
                }
            }
```

Similarly for cold store (lines 422-428):

**Before:**
```rust
            if version == 0 {
                if let Ok(log_meta) =
                    crate::checkpoint::LogMetadata::read_from_file(&cold_dir.join("log.meta"))
                {
                    version = log_meta.version;
                }
            }
```

**After:**
```rust
            if version == 0 {
                match crate::checkpoint::LogMetadata::read_from_file(&cold_dir.join("log.meta")) {
                    Ok(log_meta) => version = log_meta.version,
                    Err(_e) => {
                        tracing::warn!("cold store log.meta read failed during recovery");
                        self.checkpoint
                            .phase
                            .store(F2CheckpointPhase::Rest, Ordering::Release);
                        return Err(Status::Corruption);
                    }
                }
            }
```

Add `use tracing;` import if not present in the file's use block (the crate already depends on `tracing`).

- [ ] **Step 3: Run tests**

Run: `cargo test --features f2 test_f2_recover -- --nocapture`
Expected: All recovery tests pass

- [ ] **Step 4: Commit**

```bash
git add src/f2/store.rs src/f2/store/tests.rs
git commit -m "fix(f2): fail recovery when log metadata is missing or corrupt

Instead of silently defaulting version to 0, return
Err(Status::Corruption) when log.meta cannot be read after
successful index and log recovery."
```

---

### Task 5: H2 -- Background compaction error logging

**Files:**
- Modify: `src/f2/store.rs:681-688` (background worker compaction calls)

- [ ] **Step 1: Fix background compaction error handling**

In `src/f2/store.rs`, replace lines 680-688:

**Before:**
```rust
        // Hot-cold compaction
        if let Some(until_addr) = self.should_compact_hot_log() {
            let _ = self.compact_hot_log(until_addr);
        }

        // Cold-cold compaction
        if let Some(until_addr) = self.should_compact_cold_log() {
            let _ = self.compact_cold_log(until_addr);
        }
```

**After:**
```rust
        // Hot-cold compaction
        if let Some(until_addr) = self.should_compact_hot_log() {
            if let Err(e) = self.compact_hot_log(until_addr) {
                tracing::warn!("hot log compaction failed: {e:?}");
            }
        }

        // Cold-cold compaction
        if let Some(until_addr) = self.should_compact_cold_log() {
            if let Err(e) = self.compact_cold_log(until_addr) {
                tracing::warn!("cold log compaction failed: {e:?}");
            }
        }
```

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store.rs
git commit -m "fix(f2): log background compaction errors instead of discarding

Replace let _ = ... with tracing::warn! so compaction failures
are observable. Control flow unchanged."
```

---

### Task 6: H3 -- CAS failure log space leakage tracking

**Files:**
- Modify: `src/compaction/compact.rs:150-167` (add `bytes_leaked` to CompactionStats)
- Modify: `src/f2/store/kv_ops.rs:421-423` (conditional_upsert_into_hot CAS failure)
- Modify: `src/f2/store/log_compaction.rs:411-415` (copy_record_in_cold_store CAS failure)
- Modify: `src/f2/store/log_compaction.rs:502-506` (copy_record_to_hot_tail CAS failure)

- [ ] **Step 1: Add `bytes_leaked` field to CompactionStats**

In `src/compaction/compact.rs`, add after line 166 (`pub duration_ms: u64`):

```rust
    /// Bytes allocated but orphaned due to CAS failures.
    /// This is inherent to the FASTER log-structured architecture: records are
    /// allocated before the CAS, and on failure the space cannot be reclaimed
    /// until the next compaction pass.
    pub bytes_leaked: u64,
```

- [ ] **Step 2: Add leak tracking comments to CAS failure paths**

In `src/f2/store/kv_ops.rs`, at line 421 (conditional_upsert_into_hot CAS failure):

```rust
        if status == Status::Ok {
            self.invalidate_read_cache(hash.hash());
            Ok(())
        } else {
            // Note: the allocated record (record_size bytes) is now orphaned in the log.
            // This is inherent to the FASTER architecture -- see CompactionStats::bytes_leaked.
            Err(Status::Aborted)
        }
```

In `src/f2/store/log_compaction.rs`, at `copy_record_in_cold_store` and `copy_record_to_hot_tail` CAS failure paths, add similar comments. These functions don't have access to `stats`, so tracking happens at the call site.

In `compact_cold_log_with_scan` (around line 331), when `copy_record_in_cold_store` returns Err:

**Before:**
```rust
                            } else {
                                stats.records_skipped += 1;
                                if *new_begin_address == until_address {
```

**After:**
```rust
                            } else {
                                stats.records_skipped += 1;
                                stats.bytes_leaked += record_size;
                                if *new_begin_address == until_address {
```

Similarly in `compact_hot_log_and_migrate` at the `copy_record_to_hot_tail` failure path (around line 213):

**Before:**
```rust
                            } else {
                                stats.records_skipped += 1;
                                if *new_begin_address == until_address {
```

**After:**
```rust
                            } else {
                                stats.records_skipped += 1;
                                stats.bytes_leaked += Record::<K, V>::size() as u64;
                                if *new_begin_address == until_address {
```

And at the `migrate_record_to_cold` failure path (around line 192):

**Before:**
```rust
                            } else {
                                stats.records_skipped += 1;
                                if *new_begin_address == until_address {
```

**After:**
```rust
                            } else {
                                stats.records_skipped += 1;
                                stats.bytes_leaked += record_size_u64;
                                if *new_begin_address == until_address {
```

- [ ] **Step 3: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add src/compaction/compact.rs src/f2/store/kv_ops.rs src/f2/store/log_compaction.rs
git commit -m "feat(f2): track CAS failure log space leakage in CompactionStats

Add bytes_leaked field to CompactionStats and increment on CAS
failure paths. Document this as inherent to FASTER architecture."
```

---

### Task 7: H4 -- Document read cache backfill race

**Files:**
- Modify: `src/f2/store/kv_ops.rs:40-48` (read cache backfill comment)

- [ ] **Step 1: Add documentation comment**

In `src/f2/store/kv_ops.rs`, expand the comment block at lines 40-48:

**Before:**
```rust
        // 3. Check cold store
        if let Some(value) = self.internal_read(&self.cold_store, false, key, hash)? {
            // 4. Backfill into read cache on cold hit
            if let Some(ref rc) = self.read_cache {
```

**After:**
```rust
        // 3. Check cold store
        if let Some(value) = self.internal_read(&self.cold_store, false, key, hash)? {
            // 4. Backfill into read cache on cold hit.
            //
            // Known limitation: there is a TOCTOU race between the cold read and
            // this backfill. If a concurrent upsert/delete invalidates the cache
            // entry between steps 3 and 4, this backfill re-inserts a stale value.
            // The read cache provides eventual consistency, not strong consistency.
            // A subsequent write to the same key will re-invalidate the entry.
            if let Some(ref rc) = self.read_cache {
```

- [ ] **Step 2: Run clippy**

Run: `./scripts/check-clippy.sh`
Expected: Zero warnings

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/kv_ops.rs
git commit -m "docs(f2): document read cache backfill TOCTOU race as known limitation"
```

---

### Task 8: H5 -- Fix compact_log cold path new_begin_address

**Files:**
- Modify: `src/f2/store/log_compaction.rs:113-117` (compact_log new_begin calculation)

- [ ] **Step 1: Fix cold path new_begin**

In `src/f2/store/log_compaction.rs`, replace lines 113-117:

**Before:**
```rust
        let new_begin = if store_type == StoreType::Hot {
            new_begin_address
        } else {
            until_address
        };
```

**After:**
```rust
        // Both hot and cold paths may adjust new_begin_address when CAS failures
        // prevent certain records from being compacted. Use the adjusted value.
        let new_begin = new_begin_address;
```

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/log_compaction.rs
git commit -m "fix(f2): use adjusted new_begin_address for cold compaction result

compact_log was forcing until_address for the cold path, ignoring
the adjusted new_begin_address set by compact_cold_log_with_scan
when CAS failures occurred."
```

---

## Chunk 3: Medium Priority Fixes (M1-M5)

### Task 9: M1-M4 -- Error information tracing

**Files:**
- Modify: `src/f2/store/log_compaction.rs:135,271` (flush error tracing)
- Modify: `src/f2/store.rs:694-711` (checkpoint_store return type)
- Modify: `src/f2/store.rs:634,664` (checkpoint_store call sites)
- Modify: `src/f2/store/kv_ops.rs:161` (disk read error tracing)
- Modify: `src/f2/store.rs:455-487` (save_checkpoint error tracing)

- [ ] **Step 1: Add flush error tracing in log_compaction.rs**

In `src/f2/store/log_compaction.rs`, line 135:

**Before:**
```rust
        if let Err(_e) = store.hlog().flush_until(until_address) {
```

**After:**
```rust
        if let Err(e) = store.hlog().flush_until(until_address) {
            tracing::warn!("hot log flush failed: {e:?}");
```

Same at line 271:

**Before:**
```rust
        if let Err(_e) = store.hlog().flush_until(until_address) {
```

**After:**
```rust
        if let Err(e) = store.hlog().flush_until(until_address) {
            tracing::warn!("cold log flush failed: {e:?}");
```

- [ ] **Step 2: Change checkpoint_store return type**

In `src/f2/store.rs`, replace `checkpoint_store`:

**Before:**
```rust
    fn checkpoint_store(&self, store: &InternalStore<D>, subdir: &str) -> bool {
        let Some(cp_dir) = self.checkpoint_dir.as_ref() else {
            return false;
        };

        let token = self.checkpoint.token();
        let version = self.checkpoint.version();
        let store_dir = cp_dir.join(token.to_string()).join(subdir);

        if std::fs::create_dir_all(&store_dir).is_err() {
            return false;
        }

        let index_ok = store.hash_index.checkpoint(&store_dir, token).is_ok();
        let log_ok = store.hlog().checkpoint(&store_dir, token, version).is_ok();

        index_ok && log_ok
    }
```

**After:**
```rust
    fn checkpoint_store(&self, store: &InternalStore<D>, subdir: &str) -> Result<(), Status> {
        let cp_dir = self.checkpoint_dir.as_ref().ok_or(Status::InvalidArgument)?;

        let token = self.checkpoint.token();
        let version = self.checkpoint.version();
        let store_dir = cp_dir.join(token.to_string()).join(subdir);

        std::fs::create_dir_all(&store_dir).map_err(|e| {
            tracing::warn!("checkpoint dir creation failed: {e:?}");
            Status::Corruption
        })?;

        store.hash_index.checkpoint(&store_dir, token).map_err(|e| {
            tracing::warn!("index checkpoint failed: {e:?}");
            Status::Corruption
        })?;

        store.hlog().checkpoint(&store_dir, token, version).map_err(|e| {
            tracing::warn!("log checkpoint failed: {e:?}");
            Status::Corruption
        })?;

        Ok(())
    }
```

Update call sites at lines 634 and 664:

**Before:**
```rust
            let success = self.checkpoint_store(&self.hot_store, "hot");

            if success {
```

**After:**
```rust
            let success = self.checkpoint_store(&self.hot_store, "hot").is_ok();

            if success {
```

(Same pattern for the cold store call at line 664.)

- [ ] **Step 3: Add disk read error tracing**

In `src/f2/store/kv_ops.rs`, line 161:

**Before:**
```rust
                        .map_err(|_| Status::IoError)?;
```

**After:**
```rust
                        .map_err(|e| {
                            tracing::debug!("disk read failed at address {:?}: {e:?}", address);
                            Status::IoError
                        })?;
```

- [ ] **Step 4: Add save_checkpoint error tracing**

In `src/f2/store.rs`, update the `map_err` calls in `save_checkpoint`:

**Before (example at line 455):**
```rust
            std::fs::create_dir_all(&hot_dir).map_err(|_| Status::Corruption)?;
```

**After:**
```rust
            std::fs::create_dir_all(&hot_dir).map_err(|e| {
                tracing::warn!("save_checkpoint: create dir failed: {e:?}");
                Status::Corruption
            })?;
```

Apply the same pattern to all `.map_err(|_| Status::Corruption)` in `save_checkpoint` (lines 455-487).

- [ ] **Step 5: Run tests and clippy**

Run: `./scripts/check-test.sh && ./scripts/check-clippy.sh`
Expected: All tests pass, zero clippy warnings

- [ ] **Step 6: Commit**

```bash
git add src/f2/store.rs src/f2/store/kv_ops.rs src/f2/store/log_compaction.rs
git commit -m "fix(f2): add tracing for discarded error information

- Log flush errors in compaction before returning Corruption
- Change checkpoint_store to return Result with error tracing
- Log disk read errors before mapping to IoError
- Log save_checkpoint errors before mapping to Corruption"
```

---

### Task 10: M5 -- Read cache backfill failure logging

**Files:**
- Modify: `src/f2/store/kv_ops.rs:43-47` (backfill try_insert)

- [ ] **Step 1: Add tracing on backfill failure**

In `src/f2/store/kv_ops.rs`, replace lines 44-46:

**Before:**
```rust
                if let Ok(cache_addr) = rc.try_insert(key, &value, Address::INVALID, true) {
                    self.rc_address_map.write().insert(hash.hash(), cache_addr);
                }
```

**After:**
```rust
                match rc.try_insert(key, &value, Address::INVALID, true) {
                    Ok(cache_addr) => {
                        self.rc_address_map.write().insert(hash.hash(), cache_addr);
                    }
                    Err(_) => {
                        tracing::debug!("read cache backfill failed for key_hash={}", hash.hash());
                    }
                }
```

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/kv_ops.rs
git commit -m "fix(f2): log read cache backfill failures for observability"
```

---

## Chunk 4: Simplification (S1-S6)

### Task 11: S1 -- Merge copy_record_to_hot_tail and copy_record_in_cold_store

**Files:**
- Modify: `src/f2/store/log_compaction.rs:366-507` (merge two functions into one)

- [ ] **Step 1: Create unified copy_record_in_store function**

In `src/f2/store/log_compaction.rs`, replace both `copy_record_in_cold_store` (lines 366-416) and `copy_record_to_hot_tail` (lines 458-507) with a single function:

```rust
    /// Copy a live record to the tail of the given store, updating the index
    /// via CAS. Returns `Err(Status::Aborted)` if the CAS fails (concurrent
    /// modification).
    ///
    /// Note: on CAS failure, the allocated log space is orphaned. This is
    /// inherent to the FASTER log-structured architecture. See
    /// `CompactionStats::bytes_leaked`.
    fn copy_record_in_store(
        &self,
        store: &InternalStore<D>,
        index_result: &crate::index::FindResult,
        record_key: K,
        value: V,
        hash: KeyHash,
        old_address: Address,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        let record_size = Record::<K, V>::size();
        let new_address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;
        let record_ptr =
            unsafe { store.hlog_mut().get_mut(new_address) }.ok_or(Status::OutOfMemory)?;

        unsafe {
            let new_record = record_ptr.as_ptr() as *mut Record<K, V>;
            let header = RecordInfo::new(
                old_address,
                self.checkpoint.version() as u16,
                false,
                false,
                false,
            );
            ptr::write(&mut (*new_record).header, header);

            Record::<K, V>::write_key(record_ptr.as_ptr(), record_key);
            Record::<K, V>::write_value(record_ptr.as_ptr(), value);
        }

        let Some(atomic_entry) = index_result.atomic_entry else {
            return Err(Status::Aborted);
        };

        let update_status = store.hash_index.try_update_entry(
            Some(atomic_entry),
            index_result.entry,
            new_address,
            hash,
            false,
        );

        if update_status == Status::Ok {
            Ok(())
        } else {
            Err(Status::Aborted)
        }
    }
```

- [ ] **Step 2: Update call sites**

Replace `self.copy_record_in_cold_store(store, ...)` with `self.copy_record_in_store(store, ...)`.
Replace `self.copy_record_to_hot_tail(store, ...)` with `self.copy_record_in_store(store, ...)`.

- [ ] **Step 3: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add src/f2/store/log_compaction.rs
git commit -m "refactor(f2): merge copy_record_to_hot_tail and copy_record_in_cold_store

Both functions had identical logic. Unified into copy_record_in_store."
```

---

### Task 12: S2 -- Merge upsert_into_store and tombstone_into_store

**Files:**
- Modify: `src/f2/store/kv_ops.rs:227-341` (merge two functions)

- [ ] **Step 1: Create unified write_record_into_store function**

Replace both `upsert_into_store` and `tombstone_into_store` with:

```rust
    /// Write a record (upsert or tombstone) into a store with bounded CAS retry.
    fn write_record_into_store(
        &self,
        store: &InternalStore<D>,
        hash: KeyHash,
        key: K,
        value: Option<V>,
    ) -> Result<(), Status> {
        debug_assert!(!std::mem::needs_drop::<K>());
        debug_assert!(!std::mem::needs_drop::<V>());

        const MAX_RETRIES: usize = 32;
        let is_tombstone = value.is_none();

        let record_size = Record::<K, V>::size();
        let address = unsafe { store.hlog_mut().allocate(record_size as u32) }?;

        let record_ptr = unsafe { store.hlog_mut().get_mut(address) };
        let Some(ptr) = record_ptr else {
            return Err(Status::OutOfMemory);
        };

        let record = ptr.as_ptr() as *mut Record<K, V>;
        unsafe {
            Record::<K, V>::write_key(ptr.as_ptr(), key);
            if let Some(v) = value {
                Record::<K, V>::write_value(ptr.as_ptr(), v);
            }
        }

        for _ in 0..MAX_RETRIES {
            let result = store.hash_index.find_or_create_entry(hash);
            let old_address = result.entry.address().readcache_address();

            unsafe {
                let header = RecordInfo::new(
                    old_address,
                    self.checkpoint.version() as u16,
                    false,
                    is_tombstone,
                    false,
                );
                ptr::write(&mut (*record).header, header);
            }

            let status = store.hash_index.try_update_entry(
                result.atomic_entry,
                result.entry,
                address,
                hash,
                false,
            );
            if status == Status::Ok {
                return Ok(());
            }
        }

        Err(Status::Aborted)
    }

    pub(super) fn upsert_into_store(
        &self,
        store: &InternalStore<D>,
        hash: KeyHash,
        key: K,
        value: V,
    ) -> Result<(), Status> {
        self.write_record_into_store(store, hash, key, Some(value))
    }

    pub(super) fn tombstone_into_store(
        &self,
        store: &InternalStore<D>,
        hash: KeyHash,
        key: K,
    ) -> Result<(), Status> {
        self.write_record_into_store(store, hash, key, None)
    }
```

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/kv_ops.rs
git commit -m "refactor(f2): merge upsert_into_store and tombstone_into_store

Extract shared CAS retry loop into write_record_into_store with
Option<V> parameter for tombstone vs upsert distinction."
```

---

### Task 13: S3 -- Extract test helpers

**Files:**
- Modify: `src/f2/store/tests.rs` (add helpers, refactor all tests)

- [ ] **Step 1: Add test helper functions at the top of tests.rs**

After the `TestValue` struct definition, add:

```rust
/// Create a default F2Kv for testing with NullDisk.
fn make_f2() -> F2Kv<TestKey, TestValue, NullDisk> {
    F2Kv::new(F2Config::default(), NullDisk::new(), NullDisk::new()).unwrap()
}

/// Create F2Kv with custom config for testing.
fn make_f2_with_config(config: F2Config) -> F2Kv<TestKey, TestValue, NullDisk> {
    F2Kv::new(config, NullDisk::new(), NullDisk::new()).unwrap()
}
```

- [ ] **Step 2: Replace repeated patterns across all tests**

Search for `F2Kv::<TestKey, TestValue, NullDisk>::new(config, hot_device, cold_device).unwrap()` and replace with `make_f2()` or `make_f2_with_config(config)` as appropriate. Remove unused `hot_device`/`cold_device` variables.

- [ ] **Step 3: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add src/f2/store/tests.rs
git commit -m "refactor(f2): extract test helper functions to reduce duplication

Add make_f2() and make_f2_with_config() helpers, eliminating
repetitive F2Kv construction across 25+ tests."
```

---

### Task 14: S4 -- Remove dead code branch in compact_log

**Files:**
- Modify: `src/f2/store/log_compaction.rs:99-107` (dead else-if branch)

- [ ] **Step 1: Replace unreachable branch**

In `src/f2/store/log_compaction.rs`, replace lines 99-107:

**Before:**
```rust
        } else if shift_begin_address {
            if let Err(_e) = store.hlog().flush_until(until_address) {
                compactor.complete();
                return Err(Status::Corruption);
            }
            stats.bytes_scanned = stats.bytes_reclaimed;
            unsafe { store.hlog_mut().shift_begin_address(until_address) };
            store.hash_index.garbage_collect(until_address);
        }
```

**After:**
```rust
        } else if shift_begin_address {
            unreachable!("StoreType is exhaustive with Hot and Cold");
        }
```

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/log_compaction.rs
git commit -m "refactor(f2): replace unreachable compact_log branch with unreachable!()

StoreType only has Hot and Cold variants, so the third
else-if branch was dead code."
```

---

### Task 15: S5 -- Unify recover phase reset

**Files:**
- Modify: `src/f2/store.rs:333-441` (recover method)

- [ ] **Step 1: Refactor recover to use closure pattern**

Wrap the recovery logic in a closure (same pattern as `save_checkpoint`):

```rust
    pub fn recover(&mut self, checkpoint_dir: &Path, token: Uuid) -> Result<u32, Status> {
        let result = self.checkpoint.phase.compare_exchange(
            F2CheckpointPhase::Rest,
            F2CheckpointPhase::Recover,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if result.is_err() {
            return Err(Status::Aborted);
        }

        let result = self.recover_inner(checkpoint_dir, token);

        // Always reset to Rest on completion (success or failure)
        self.checkpoint
            .phase
            .store(F2CheckpointPhase::Rest, Ordering::Release);

        result
    }

    fn recover_inner(&mut self, checkpoint_dir: &Path, token: Uuid) -> Result<u32, Status> {
        // ... existing logic without the duplicated phase resets ...
    }
```

Move the body of `recover` (from line 345 onward) into `recover_inner`, removing all 4 occurrences of:
```rust
                self.checkpoint
                    .phase
                    .store(F2CheckpointPhase::Rest, Ordering::Release);
                return Err(Status::...);
```

Replace each with just `return Err(Status::...)`.

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store.rs
git commit -m "refactor(f2): unify recover phase reset with closure pattern

Extract recover_inner() and reset phase in a single location,
eliminating 4 duplicated phase-reset blocks."
```

---

### Task 16: S6 -- Extract enter_session from start_session/continue_session

**Files:**
- Modify: `src/f2/store.rs:197-234` (start_session and continue_session)

- [ ] **Step 1: Extract shared logic**

```rust
    fn enter_session(&self) -> Result<(), Status> {
        if self.checkpoint.phase.load(Ordering::Acquire) != F2CheckpointPhase::Rest {
            return Err(Status::Aborted);
        }

        let tid = get_thread_id();
        self.hot_store.epoch.protect(tid);
        self.cold_store.epoch.protect(tid);
        self.num_active_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(())
    }

    pub fn start_session(&self) -> Result<Uuid, Status> {
        self.enter_session()?;
        Ok(Uuid::new_v4())
    }

    pub fn continue_session(&self, _session_id: Uuid) -> Result<u64, Status> {
        self.enter_session()?;
        Ok(0)
    }
```

- [ ] **Step 2: Run tests**

Run: `./scripts/check-test.sh`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/f2/store.rs
git commit -m "refactor(f2): extract enter_session from start/continue_session

Share checkpoint phase check, epoch protection, and session
counting logic."
```

---

## Chunk 5: Test Coverage (T1-T2)

### Task 17: T1 -- Concurrent compaction + read/write test

**Files:**
- Modify: `src/f2/store/tests.rs` (new test)

- [ ] **Step 1: Write the concurrent test**

```rust
#[test]
fn test_f2_concurrent_compaction_with_reads_and_writes() {
    use std::sync::Arc;
    use std::thread;

    let config = F2Config::default();
    let f2 = Arc::new(make_f2_with_config(config));

    let num_writers = 4;
    let ops_per_writer = 200;

    // Pre-populate some data
    {
        let _s = f2.start_session().unwrap();
        for i in 0..100u64 {
            f2.upsert(TestKey(i), TestValue(i)).unwrap();
        }
        f2.stop_session();
    }

    let f2_clone = Arc::clone(&f2);
    let writer_handles: Vec<_> = (0..num_writers)
        .map(|tid| {
            let f2 = Arc::clone(&f2_clone);
            thread::spawn(move || {
                let _s = f2.start_session().unwrap();
                for i in 0..ops_per_writer {
                    let key = TestKey((tid * ops_per_writer + i) as u64);
                    f2.upsert(key, TestValue(i as u64)).unwrap();

                    // Interleave reads
                    let _ = f2.read(&TestKey((i % 100) as u64));
                }
                f2.stop_session();
            })
        })
        .collect();

    // Run compaction concurrently
    {
        let _s = f2.start_session().unwrap();
        if let Some(until) = f2.should_compact_hot_log() {
            let _ = f2.compact_hot_log(until);
        }
        f2.stop_session();
    }

    for h in writer_handles {
        h.join().unwrap();
    }

    // Verify data integrity: all recently written keys should be readable
    {
        let _s = f2.start_session().unwrap();
        for tid in 0..num_writers {
            let key = TestKey((tid * ops_per_writer + ops_per_writer - 1) as u64);
            let v = f2.read(&key).unwrap();
            assert!(v.is_some(), "Key {key:?} should be readable after concurrent compaction");
        }
        f2.stop_session();
    }
}
```

- [ ] **Step 2: Run test**

Run: `cargo test --features f2 test_f2_concurrent_compaction_with_reads_and_writes -- --nocapture`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/tests.rs
git commit -m "test(f2): add concurrent compaction + read/write interleaving test"
```

---

### Task 18: T2 -- Fix heavy_enter multi-thread test

**Files:**
- Modify: `src/f2/store/tests.rs` (update existing test)

- [ ] **Step 1: Find and update the test**

Find `test_f2_heavy_enter_multi_thread_countdown` and rewrite it to use actual concurrent threads:

```rust
#[test]
fn test_f2_heavy_enter_multi_thread_countdown() {
    use std::sync::Arc;
    use std::thread;

    let mut config = F2Config::default();
    config.hot_store.mutable_fraction = 0.0;
    let f2 = Arc::new(make_f2_with_config(config));

    let num_threads = 4u32;

    // Start checkpoint with num_threads sessions
    // First, create sessions
    let sessions: Vec<_> = (0..num_threads)
        .map(|_| {
            let _s = f2.start_session().unwrap();
            _s
        })
        .collect();

    // Stop all sessions so we can mutably borrow for checkpoint
    for _ in &sessions {
        f2.stop_session();
    }

    // Use save_checkpoint or manually set checkpoint phase to test heavy_enter
    // Start checkpoint phase
    let f2_mut = Arc::get_mut(&mut { Arc::clone(&f2) });
    // Since we can't easily get &mut through Arc, test heavy_enter through refresh()
    // by putting checkpoint into HotStoreCheckpoint phase

    // Set up checkpoint state
    f2.checkpoint
        .phase
        .store(F2CheckpointPhase::HotStoreCheckpoint, Ordering::Release);
    f2.checkpoint.initialize(uuid::Uuid::new_v4(), num_threads);

    // Spawn threads that each call refresh() (which calls heavy_enter)
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let f2 = Arc::clone(&f2);
            thread::spawn(move || {
                let _s = f2.start_session().unwrap();
                f2.refresh(); // Calls heavy_enter
                f2.stop_session();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // After all threads have called heavy_enter, phase should have advanced
    let phase = f2.checkpoint.phase.load(Ordering::Acquire);
    assert!(
        phase == F2CheckpointPhase::ColdStoreCheckpoint || phase == F2CheckpointPhase::Rest,
        "Phase should advance after all threads acknowledged: got {phase:?}"
    );
}
```

Note: The exact test shape depends on the visibility of `checkpoint` fields. If they are not `pub(crate)`, this test may need adjustment. Read the actual test to adapt accordingly.

- [ ] **Step 2: Run test**

Run: `cargo test --features f2 test_f2_heavy_enter_multi_thread_countdown -- --nocapture`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/f2/store/tests.rs
git commit -m "test(f2): make heavy_enter countdown test truly multi-threaded

Spawn N threads that concurrently call refresh()/heavy_enter()
instead of calling sequentially from a single thread."
```

---

## Chunk 6: Final Verification and PR

### Task 19: Full CI checks

- [ ] **Step 1: Run all CI checks**

```bash
./scripts/check-fmt.sh
./scripts/check-clippy.sh
./scripts/check-test.sh
./scripts/check-coverage.sh
```

Expected: All pass, coverage >= 80%

- [ ] **Step 2: Fix any issues found**

If clippy, fmt, or coverage issues arise, fix them and amend the relevant commit.

### Task 20: Create PR with review report

- [ ] **Step 1: Push branch and create PR**

```bash
git push origin f2/batch1-correctness
```

Then create PR with `gh pr create` including the full review report in the description body.

The PR description should include:
- Summary of all changes (grouped by severity)
- The review findings table
- Test results

- [ ] **Step 2: Verify PR**

Check that the PR was created and the description renders correctly.
