# F2 Batch 2: Core Feature Alignment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable F2 to read records from disk, integrate read cache for cold-read acceleration, implement checkpoint coordination (heavy_enter), and add record-level cold store compaction.

**Architecture:** We take a pragmatic approach: synchronous disk reads (not full async callbacks) to unblock disk-backed datasets, read cache backfill after cold hits, checkpoint phase transitions via heavy_enter, and record-scanning cold compaction. Full async IO with PendingIoManager deferred to Batch 3.

**Tech Stack:** Rust, bytemuck (Pod), tempfile (tests), existing ReadCache/PendingIoManager/Compactor infrastructure

---

## Reference: Key File Locations

| File | Purpose |
|------|---------|
| `src/f2/store.rs` | F2Kv struct, session lifecycle, checkpoint, background worker |
| `src/f2/store/kv_ops.rs` | read/upsert/delete/rmw, internal_read |
| `src/f2/store/internal_store.rs` | InternalStore: HybridLog + HashIndex wrapper |
| `src/f2/store/log_compaction.rs` | Hot-cold migration, cold compaction |
| `src/f2/state.rs` | F2CheckpointState, phases, atomic wrappers |
| `src/cache/read_cache.rs` | ReadCache struct (try_insert, read, skip, invalidate) |
| `src/cache/config.rs` | ReadCacheConfig |
| `src/allocator/hybrid_log.rs` | PersistentMemoryMalloc, disk page reads |
| `src/store/faster_kv.rs` | FasterKv read_sync (reference for disk read + RC integration) |
| `src/device/` | StorageDevice trait, FileSystemDisk, NullDisk |

## Reference: C++ FASTER Source

| File | Purpose |
|------|---------|
| `/Users/wayslog/repo/cpp/FASTER/cc/src/core/f2.h` | C++ F2Kv: Read, CompactLog, CheckSystemState, lazy checkpoint |
| `/Users/wayslog/repo/cpp/FASTER/cc/src/core/internal_contexts_f2.h` | AsyncF2ReadContext stages |

---

### Task 1: Synchronous disk read in internal_read

Currently `internal_read` (kv_ops.rs:109-153) only reads records from memory. When
`hlog().get(address)` returns None (record on disk), it just breaks out of the chain
traversal. We need to read from disk synchronously.

**Files:**
- Modify: `src/f2/store/kv_ops.rs:109-153` (internal_read)
- Modify: `src/f2/store/internal_store.rs` (add sync disk read helper)
- Modify: `src/allocator/hybrid_log.rs` (expose page-aligned read if not already)
- Test: `src/f2/store/tests.rs`

**What to implement:**

In `internal_read`, when `hlog().get(address)` returns None and address >= begin_address
(record is on disk, not garbage-collected), read the record from the device:

```rust
// In internal_read, replace the None/break with:
let record_ptr = unsafe { store.hlog().get(address) };
if record_ptr.is_none() {
    // Record is on disk -- try synchronous read via device
    if address >= store.hlog().get_head_address() {
        // Record was evicted but within valid range, try disk read
        if let Some(value) = self.sync_disk_read(store, address, key)? {
            return Ok(Some(value));
        }
    }
    // Record is below head_address (garbage collected) or read failed
    break;
}
```

The `sync_disk_read` helper:
1. Calculate page number and offset from address
2. Read the page (or record bytes) from the device using `device.read_sync()`
3. Parse the record from the read bytes
4. Check key match, tombstone, return value

Check how `PersistentMemoryMalloc` reads from disk -- look at `read_page()` or similar
methods. The device has `read()` / `read_sync()` / `read_async()` methods.

For NullDisk, `read()` will return empty/error since there's no actual disk. The test
should use FileSystemDisk with a temp directory.

**Test:**

```rust
#[test]
fn test_f2_disk_read_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let hot_path = dir.path().join("hot");
    let cold_path = dir.path().join("cold");
    std::fs::create_dir_all(&hot_path).unwrap();
    std::fs::create_dir_all(&cold_path).unwrap();

    // Use FileSystemDisk with small log to force flush to disk
    let config = F2Config::default()
        .with_hot_store(HotStoreConfig::new()
            .with_log_mem_size(1024 * 1024)); // 1MB -- small to force flush

    let hot_device = FileSystemDisk::new(&hot_path).unwrap();
    let cold_device = FileSystemDisk::new(&cold_path).unwrap();
    let f2 = F2Kv::<TestKey, TestValue, FileSystemDisk>::new(config, hot_device, cold_device).unwrap();

    f2.start_session().unwrap();

    // Write enough data to push some pages to disk
    for i in 0..10_000u64 {
        f2.upsert(TestKey(i), TestValue(i * 10)).unwrap();
    }

    // Force flush (may need to trigger compaction or manual flush)
    // ...

    // Read back -- some keys should require disk read
    for i in 0..10_000u64 {
        let v = f2.read(&TestKey(i)).unwrap();
        assert_eq!(v, Some(TestValue(i * 10)), "Key {} not found after flush", i);
    }

    f2.stop_session();
}
```

Note: This test may need adjustment based on how flushing actually works. The key point is
to write enough data to push old pages past head_address, then read them back.

**Commit:** `feat(f2): implement synchronous disk read path for records below head_address`

---

### Task 2: Add ReadCache field to F2Kv and InternalStore

Mount a ReadCache on the hot store when config.hot_store.read_cache is Some.

**Files:**
- Modify: `src/f2/store.rs` (F2Kv struct, new() method)
- Modify: `src/f2/store/internal_store.rs` (add read_cache field)
- Test: `src/f2/store/tests.rs`

**What to implement:**

1. Add to InternalStore:
```rust
pub(super) read_cache: Option<Arc<ReadCache<K, V>>>,
```

Wait -- InternalStore is not generic over K, V. It uses raw byte manipulation.
ReadCache IS generic. Two approaches:
- Make InternalStore generic over K, V (big change)
- Store ReadCache at F2Kv level instead

**Use the F2Kv level approach** (simpler, matches C++ where F2Kv holds the read cache):

Add to F2Kv struct:
```rust
read_cache: Option<ReadCache<K, V>>,
```

In F2Kv::new(), if config.hot_store.read_cache is Some:
```rust
let read_cache = config.hot_store.read_cache.as_ref().map(|rc_config| {
    ReadCache::<K, V>::new(rc_config.clone())
});
```

**Test:**
```rust
#[test]
fn test_f2_read_cache_created_when_configured() {
    let config = F2Config::default(); // default has read_cache = Some(default)
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, NullDisk::new(), NullDisk::new()).unwrap();
    assert!(f2.has_read_cache());
}

#[test]
fn test_f2_no_read_cache_when_not_configured() {
    let config = F2Config::default()
        .with_hot_store(HotStoreConfig::new().with_read_cache(None));
    let f2 = F2Kv::<TestKey, TestValue, NullDisk>::new(config, NullDisk::new(), NullDisk::new()).unwrap();
    assert!(!f2.has_read_cache());
}
```

**Commit:** `feat(f2): mount ReadCache on F2Kv when configured`

---

### Task 3: Integrate ReadCache into F2 read path

When a cold-store read hits, backfill the result into the read cache. Also check
the read cache before traversing the cold store.

**Files:**
- Modify: `src/f2/store/kv_ops.rs` (read method, internal_read)
- Test: `src/f2/store/tests.rs`

**What to implement:**

Modify `read()` (kv_ops.rs:22-34):

```rust
pub fn read(&self, key: &K) -> Result<Option<V>, Status> {
    let hash = KeyHash::new(hash64(bytemuck::bytes_of(key)));

    // 1. Check hot store read cache first (if enabled)
    if let Some(ref rc) = self.read_cache {
        // Check if hash index entry has readcache bit
        // If so, try reading from cache
        if let Some(value) = rc.read_for_key(key, hash)? {
            return Ok(Some(value));
        }
    }

    // 2. Check hot store hlog
    if let Some(value) = self.internal_read(&self.hot_store, true, key, hash)? {
        return Ok(Some(value));
    }

    // 3. Check cold store
    if let Some(value) = self.internal_read(&self.cold_store, false, key, hash)? {
        // 4. Backfill into read cache on cold hit
        if let Some(ref rc) = self.read_cache {
            let _ = rc.try_insert(key, &value, Address::INVALID, true);
        }
        return Ok(Some(value));
    }

    Ok(None)
}
```

Note: The ReadCache API uses `PersistKey`/`PersistValue` traits, but F2 uses `Pod`.
Check if `Pod` types implement `PersistKey`/`PersistValue` via `BlittableCodec`.
If not, we may need to use a simpler cache (a concurrent HashMap) instead of the
full ReadCache infrastructure. Investigate before implementing.

**Tests:**
```rust
#[test]
fn test_f2_read_cache_backfill_on_cold_hit() {
    // Write key to hot, compact to cold, read should hit cold then cache
    // Second read should hit cache (faster)
}
```

**Commit:** `feat(f2): integrate ReadCache into F2 read path with cold-hit backfill`

---

### Task 4: Implement heavy_enter for checkpoint phase transitions

`heavy_enter()` (store.rs:524-534) is currently a stub. It needs to implement the
per-thread checkpoint acknowledgment pattern.

**Files:**
- Modify: `src/f2/store.rs:524-534` (heavy_enter)
- Modify: `src/f2/state.rs` (may need to update F2CheckpointState methods)
- Test: `src/f2/store/tests.rs`

**What to implement:**

```rust
fn heavy_enter(&self) {
    let phase = self.checkpoint.phase();

    match phase {
        F2CheckpointPhase::HotStoreCheckpoint => {
            // Thread acknowledges hot store checkpoint.
            // Decrement pending count; last thread advances to ColdStoreCheckpoint.
            if self.checkpoint.decrement_pending_hot_store() == 0 {
                // Last thread: advance phase
                self.checkpoint.set_cold_store_status(StoreCheckpointStatus::Requested);
                self.checkpoint.phase.store(
                    F2CheckpointPhase::ColdStoreCheckpoint,
                    Ordering::Release,
                );
            }
        }
        F2CheckpointPhase::ColdStoreCheckpoint => {
            // Thread acknowledges cold store checkpoint completion.
            if self.checkpoint.decrement_pending_callback() == 0 {
                // Last thread: advance to Rest
                self.checkpoint.phase.store(
                    F2CheckpointPhase::Rest,
                    Ordering::Release,
                );
            }
        }
        _ => {}
    }
}
```

Also update `checkpoint()` to properly initialize thread counts:
```rust
self.checkpoint.initialize(
    num_active_sessions,  // threads_pending_hot_store
    num_active_sessions,  // threads_pending_callback
);
```

**Test:**
```rust
#[test]
fn test_f2_heavy_enter_advances_checkpoint_phase() {
    // Start checkpoint, simulate heavy_enter calls, verify phase transitions
}
```

**Commit:** `feat(f2): implement heavy_enter for checkpoint phase transitions`

---

### Task 5: Cold store record-level compaction

Currently cold log compaction (log_compaction.rs:91-98) only does flush + shift_begin_address.
It needs to scan records and remove dead/tombstone entries.

**Files:**
- Modify: `src/f2/store/log_compaction.rs` (add compact_cold_log_with_scan)
- Test: `src/f2/store/tests.rs`

**What to implement:**

Add `compact_cold_log_with_scan()` following the pattern of `compact_hot_log_and_migrate()`
but simpler (no cross-store migration):

```rust
fn compact_cold_log_with_scan(
    &self,
    until_address: Address,
    shift_begin: bool,
) -> CompactionResult {
    let store = &self.cold_store;
    let begin_address = store.begin_address();

    // Flush pages that will be scanned
    unsafe { store.hlog_mut().flush_until(until_address) };

    let mut stats = CompactionStats::default();
    let mut address = begin_address;

    while address < until_address {
        let record_ptr = unsafe { store.hlog().get(address) };
        let Some(ptr) = record_ptr else {
            // Skip to next page boundary
            address = address.next_page_boundary();
            continue;
        };

        let record: &Record<K, V> = unsafe { &*(ptr.as_ptr() as *const _) };
        let key = unsafe { Record::<K, V>::read_key(ptr.as_ptr()) };
        let hash = KeyHash::new(hash64(bytemuck::bytes_of(&key)));

        // Check if this record is the latest version in the cold index
        if self.cold_compactor.should_compact_record(
            address,
            store.hash_index.find_entry(hash).entry.address(),
            record.header.is_tombstone(),
        ) {
            if !record.header.is_tombstone() {
                // Live record: copy to cold log tail
                self.copy_record_in_store(store, key, unsafe { Record::<K, V>::read_value(ptr.as_ptr()) }, hash)?;
                stats.records_copied += 1;
            } else {
                // Tombstone: can be dropped during cold compaction
                stats.tombstones_removed += 1;
            }
        } else {
            stats.records_skipped += 1;
        }

        address = address.offset(Record::<K, V>::size() as u64);
    }

    if shift_begin {
        store.hlog().shift_begin_address(until_address);
        store.hash_index.garbage_collect(until_address);
    }

    stats
}
```

Then update `compact_log()` to call this instead of the current flush-only path for cold:

```rust
StoreType::Cold if shift_begin => {
    self.compact_cold_log_with_scan(until_address, true);
}
```

**Test:**
```rust
#[test]
fn test_f2_cold_compaction_removes_tombstones() {
    // Write keys, delete some, compact hot to cold, compact cold
    // Verify tombstones are removed and live records survive
}
```

**Commit:** `feat(f2): implement record-level cold store compaction with tombstone removal`

---

### Task 6: Large data test with FileSystemDisk

**Files:**
- Test: `src/f2/store/tests.rs` or `tests/f2.rs`

**What to implement:**

A comprehensive test with 100K+ keys that exercises:
- Hot store filling and flushing to disk
- Hot-cold compaction and migration
- Cold compaction
- Read cache (if enabled)
- Checkpoint under load

```rust
#[test]
fn test_f2_large_dataset_with_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    // ... create F2Kv with FileSystemDisk and small log sizes
    // Write 100K keys
    // Trigger compactions
    // Verify all keys readable
    // Checkpoint and recover
    // Verify again
}
```

**Commit:** `test(f2): add large dataset test with FileSystemDisk`

---

### Task 7: Full quality checks

Run: `./scripts/check-fmt.sh`, `./scripts/check-clippy.sh`, `./scripts/check-test.sh`, `./scripts/check-coverage.sh`

Fix any issues. Commit.

---

## Summary of Batch 2 Tasks

| Task | Description | Complexity |
|------|-------------|------------|
| 1 | Synchronous disk read in internal_read | High |
| 2 | Mount ReadCache on F2Kv | Low |
| 3 | Read cache integration in read path | Medium |
| 4 | heavy_enter checkpoint coordination | Medium |
| 5 | Cold store record-level compaction | Medium |
| 6 | Large data test with FileSystemDisk | Medium |
| 7 | Quality checks | Low |

## Dependencies

```
Task 1 (disk read) -- independent
Task 2 (mount RC) -- independent
Task 3 (RC integration) -- depends on Task 2
Task 4 (heavy_enter) -- independent
Task 5 (cold compaction) -- independent
Task 6 (large test) -- depends on Task 1
Task 7 (quality) -- depends on all
```

Tasks 1, 2, 4, 5 can be implemented in parallel.
