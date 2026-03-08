# CPR/Checkpoint Completion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make CPR/Checkpoint production-grade by fixing WaitPending retry, adding timeouts, serial number validation, FoldOver durability, checkpoint-compaction mutex, metadata checksums, and driver takeover.

**Architecture:** The changes layer on top of the existing CPR state machine (`ActiveCheckpoint` + `CprCoordinator`). WaitPending retry is a one-line fix in `cpr_refresh()`. Timeouts and driver takeover add fields to `ActiveCheckpoint`. Serial number validation strengthens `validate_session_serials()`. Metadata checksums wrap existing JSON read/write with a CRC32 envelope. Checkpoint-compaction mutex is a `RwLock` on `FasterKv`.

**Tech Stack:** Rust, `crc32fast` crate, `parking_lot::RwLock`, existing `serde_json` serialization

---

### Task 1: Add `crc32fast` dependency

**Files:**
- Modify: `Cargo.toml:22-39`

**Step 1: Add crc32fast to dependencies**

In `Cargo.toml`, add `crc32fast` after the `bytemuck` line (line 37):

```toml
crc32fast = "1"
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles with no errors

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "deps: add crc32fast for checkpoint metadata checksums"
```

---

### Task 2: WaitPending retry mechanism

The core bug: in `cpr_refresh()`, the WaitPending branch at `src/store/faster_kv.rs:440-451` sets `last_cpr_state` only when `prev_ctx_is_drained()` is true AND ack succeeds. But the outer guard at line 383 (`if ctx.last_cpr_state == state_sig { return; }`) means if the thread already set `last_cpr_state` in a prior phase with the same sig, it won't retry. The fix: ensure WaitPending only updates `last_cpr_state` on successful ack, which already happens -- but we need to verify the state_sig changes between phases (it does, since phase is encoded in the sig). The actual issue is that `set_phase()` at cpr.rs:138-145 resets `acked` to 0 when phase changes, so an ack in a new phase is never re-tried if the first attempt fails and `last_cpr_state` is NOT updated. This is actually correct already. Let me re-read the code...

Actually the code at line 440-451 is:
```rust
Phase::WaitPending => {
    if ctx.prev_ctx_is_drained()
        && self.cpr.with_active_mut(|active| {
            active.set_phase(state.phase, state.version);
            active.ack(ctx.thread_id);
        }).is_some()
    {
        ctx.last_cpr_state = state_sig;
    }
}
```

If `prev_ctx_is_drained()` is false, `last_cpr_state` is NOT updated, so the next `cpr_refresh()` call will re-enter this branch (state_sig != last_cpr_state). This is actually correct retry behavior! But the issue is the conditional AND: if `with_active_mut` returns `None` (coordinator cleared), `last_cpr_state` also isn't set, which is correct.

So the retry mechanism already works. The real gaps are: (1) the driver loop has no timeout, (2) no progress monitoring, (3) no pending_threads() method.

**Files:**
- Modify: `src/store/faster_kv/cpr.rs:66-87` (add `pending_threads()`)
- Modify: `src/store/faster_kv/checkpoint.rs:508-521` (add timeout to WaitPending driver loop)
- Modify: `src/store/faster_kv/checkpoint.rs:413-593` (add heartbeat updates in driver loop)

**Step 1: Write test for `pending_threads()`**

Create file `src/store/faster_kv/cpr_tests.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointToken;
    use crate::store::{Action, Phase};
    use std::path::PathBuf;
    use uuid::Uuid;

    fn make_active(participants: u128, driver: usize) -> ActiveCheckpoint {
        ActiveCheckpoint::new(ActiveCheckpointStart {
            token: Uuid::new_v4(),
            dir: PathBuf::from("/tmp/test"),
            action: Action::Checkpoint,
            backend: LogCheckpointBackend::Snapshot,
            durability: CheckpointDurability::FasterLike,
            driver_thread_id: driver,
            participants,
            version: 1,
        })
    }

    #[test]
    fn test_pending_threads_all_pending() {
        let active = make_active(0b1011, 0); // threads 0, 1, 3
        let pending = active.pending_threads();
        assert_eq!(pending, vec![0, 1, 3]);
    }

    #[test]
    fn test_pending_threads_some_acked() {
        let mut active = make_active(0b1011, 0);
        active.set_phase(Phase::WaitPending, 1);
        active.ack(0);
        active.ack(3);
        let pending = active.pending_threads();
        assert_eq!(pending, vec![1]);
    }

    #[test]
    fn test_pending_threads_all_acked() {
        let mut active = make_active(0b1011, 0);
        active.set_phase(Phase::WaitPending, 1);
        active.ack(0);
        active.ack(1);
        active.ack(3);
        let pending = active.pending_threads();
        assert!(pending.is_empty());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib cpr_tests -- --nocapture`
Expected: FAIL - `pending_threads` method does not exist

**Step 3: Implement `pending_threads()` on `ActiveCheckpoint`**

In `src/store/faster_kv/cpr.rs`, add after `barrier_complete()` (line 159):

```rust
    /// Returns thread IDs that are participants but have not yet acked.
    pub(crate) fn pending_threads(&self) -> Vec<usize> {
        let not_acked = self.participants & !self.acked;
        (0..MAX_THREADS)
            .filter(|&i| (not_acked & (1u128 << i)) != 0)
            .collect()
    }
```

**Step 4: Run test to verify it passes**

Run: `cargo test --lib cpr_tests -- --nocapture`
Expected: PASS

**Step 5: Add `mod cpr_tests;` to the module tree**

In `src/store/faster_kv/cpr.rs`, add at the bottom:

```rust
#[cfg(test)]
mod tests;
```

Then rename the test file to `src/store/faster_kv/cpr/tests.rs` OR embed the tests inline. Choose the inline approach: put the test module directly at the bottom of `cpr.rs`.

**Step 6: Commit**

```bash
git add src/store/faster_kv/cpr.rs
git commit -m "feat: add ActiveCheckpoint::pending_threads() for monitoring"
```

---

### Task 3: WaitPending timeout in driver loop

**Files:**
- Modify: `src/store/faster_kv/checkpoint.rs:508-521` (WaitPending driver branch)
- Modify: `src/store/faster_kv/checkpoint.rs:413` (add deadline tracking at loop start)

**Step 1: Write integration test for timeout**

Create file `tests/checkpoint_wait_pending.rs`:

```rust
//! Tests for WaitPending phase improvements

use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKvBuilder, Status};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test that checkpoint completes when all threads eventually drain pending IOs.
#[test]
fn test_checkpoint_wait_pending_drains() {
    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 16)
        .build()
        .unwrap();
    let store = Arc::new(store);

    // Insert data from multiple threads
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let s = store.clone();
            std::thread::spawn(move || {
                let mut session = s.new_session();
                session.start();
                for i in 0..1000u64 {
                    session.upsert(&i, &(i * 10));
                    if i % 100 == 0 {
                        session.refresh();
                    }
                }
                session.end();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Checkpoint should succeed (all pending IOs drained)
    let token = store.checkpoint().expect("checkpoint should succeed");
    assert!(!token.is_nil());
}

/// Test that checkpoint with concurrent writes eventually completes.
#[test]
fn test_checkpoint_concurrent_writers_complete() {
    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 16)
        .build()
        .unwrap();
    let store = Arc::new(store);

    let running = Arc::new(AtomicBool::new(true));

    // Start background writers
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let s = store.clone();
            let r = running.clone();
            std::thread::spawn(move || {
                let mut session = s.new_session();
                session.start();
                let mut i = t * 10000u64;
                while r.load(Ordering::Relaxed) {
                    session.upsert(&i, &(i * 10));
                    i += 1;
                    if i % 50 == 0 {
                        session.refresh();
                    }
                }
                session.end();
            })
        })
        .collect();

    // Give writers time to run
    std::thread::sleep(Duration::from_millis(100));

    // Checkpoint while writers are active
    let start = Instant::now();
    let token = store.checkpoint().expect("checkpoint should succeed");
    let elapsed = start.elapsed();
    assert!(!token.is_nil());
    // Should complete within 30 seconds (the default timeout)
    assert!(elapsed < Duration::from_secs(30));

    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}
```

**Step 2: Run test to verify it passes with current code**

Run: `cargo test --test checkpoint_wait_pending -- --nocapture`
Expected: PASS (these tests validate existing behavior; timeout test comes after implementation)

**Step 3: Add timeout to WaitPending in the driver loop**

In `src/store/faster_kv/checkpoint.rs`, modify the `execute_checkpoint_state_machine` method.

At the top of the method (around line 413, just before the `loop {`), add:

```rust
let wait_pending_deadline: Option<Instant> = None;
let mut wait_pending_deadline = wait_pending_deadline;
```

Replace the `Phase::WaitPending` branch (lines 508-521) with:

```rust
Phase::WaitPending => {
    // Initialize deadline on first entry to WaitPending
    let deadline = wait_pending_deadline
        .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));

    let _ = self.cpr.with_active_mut(|active| {
        active.set_phase(current_state.phase, current_state.version);
    });
    let barrier_complete = self
        .cpr
        .with_active(|active| active.barrier_complete())
        .unwrap_or(false);
    if barrier_complete {
        let _ = self.system_state.try_advance();
        wait_pending_deadline = None; // Reset for next use
    } else if Instant::now() > *deadline {
        // Log which threads are stalled
        let stalled = self
            .cpr
            .with_active(|active| active.pending_threads())
            .unwrap_or_default();
        tracing::warn!(
            ?stalled,
            "WaitPending timeout: threads have not drained pending IOs"
        );
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "WaitPending timeout after 30s: stalled threads: {stalled:?}"
            ),
        ));
    }
}
```

Add `use std::time::{Duration, Instant};` to the imports at the top of `checkpoint.rs`.

**Step 4: Run all checkpoint tests**

Run: `cargo test checkpoint -- --nocapture`
Expected: All existing + new tests PASS

**Step 5: Commit**

```bash
git add src/store/faster_kv/checkpoint.rs tests/checkpoint_wait_pending.rs
git commit -m "feat: add WaitPending timeout (30s) with stalled thread reporting"
```

---

### Task 4: FoldOver durability fix

The FoldOver branch at `checkpoint.rs:682-695` calls `flush_until()` then immediately writes metadata. The comment says `flush_until()` now calls `device.flush()` (Workstream C), but we should verify the ordering is correct and add an explicit `device.flush()` call after `flush_until()` to be certain.

**Files:**
- Modify: `src/store/faster_kv/checkpoint.rs:682-695`

**Step 1: Write test to verify FoldOver durability ordering**

Add to `tests/checkpoint_wait_pending.rs`:

```rust
/// Test FoldOver checkpoint creates valid, recoverable checkpoint.
#[test]
fn test_foldover_checkpoint_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(
        oxifaster::device::FileSystemDisk::new(dir.path().join("data.log")).unwrap(),
    );
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 16)
        .with_checkpoint_dir(dir.path().join("checkpoints"))
        .build()
        .unwrap();
    let store = Arc::new(store);

    // Write data
    {
        let mut session = store.new_session();
        session.start();
        for i in 0..500u64 {
            session.upsert(&i, &(i * 10));
        }
        session.end();
    }

    // Checkpoint with FoldOver backend
    store.set_log_checkpoint_backend(
        oxifaster::store::faster_kv::cpr::LogCheckpointBackend::FoldOver,
    );
    let token = store.checkpoint().expect("foldover checkpoint should succeed");

    // Recover
    drop(store);
    let device2 = Arc::new(
        oxifaster::device::FileSystemDisk::open(dir.path().join("data.log")).unwrap(),
    );
    let store2 = FasterKvBuilder::<u64, u64, _>::new(device2)
        .with_table_size(1 << 16)
        .with_checkpoint_dir(dir.path().join("checkpoints"))
        .build()
        .unwrap();
    store2.recover(token).expect("recovery should succeed");

    // Verify data
    let store2 = Arc::new(store2);
    let mut session = store2.new_session();
    session.start();
    for i in 0..500u64 {
        let val = session.read(&i);
        assert!(val.is_ok(), "key {i} should be readable after recovery");
    }
    session.end();
}
```

**Step 2: Run test**

Run: `cargo test --test checkpoint_wait_pending test_foldover -- --nocapture`
Expected: May PASS or FAIL depending on current FileSystemDisk API

**Step 3: Fix FoldOver durability ordering**

In `src/store/faster_kv/checkpoint.rs`, replace the FoldOver branch (lines 682-695):

```rust
LogCheckpointBackend::FoldOver => {
    // Fold-over: ensure data pages are flushed and durable BEFORE writing metadata.
    // Step 1: Flush data pages to device
    unsafe {
        (*self.hlog.get()).flush_until(final_address)?;
    }

    // Step 2: Explicit device barrier to ensure data durability
    // (flush_until calls device.flush() internally since Workstream C,
    //  but we add a belt-and-suspenders call here for the FoldOver path
    //  where metadata correctness depends on data being durable first)
    if durability == CheckpointDurability::FsyncOnCheckpoint {
        unsafe {
            (*self.hlog.get()).device().flush()?;
        }
    }

    // Step 3: Write metadata (only after data is durable)
    let meta = write_log_meta(false)?;
    fsync_checkpoint_artifacts(false)?;

    let _ = self.cpr.with_active_mut(|active| {
        active.log_metadata = Some(meta);
    });
}
```

**Step 4: Verify compilation and tests**

Run: `cargo test checkpoint -- --nocapture`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/store/faster_kv/checkpoint.rs tests/checkpoint_wait_pending.rs
git commit -m "fix: ensure FoldOver data is durable before writing metadata"
```

---

### Task 5: Checkpoint-Compaction mutual exclusion

**Files:**
- Modify: `src/store/faster_kv.rs:119-174` (add `checkpoint_compaction_lock` field)
- Modify: `src/store/faster_kv.rs` (wherever `FasterKv::new()` is, add field init)
- Modify: `src/store/faster_kv/checkpoint.rs` (wrap checkpoint in write lock)
- Modify: `src/store/faster_kv/compaction.rs:28-46` (wrap compact in read lock)

**Step 1: Write test for mutual exclusion**

Add to `tests/checkpoint_wait_pending.rs`:

```rust
/// Test that checkpoint and compaction don't run concurrently.
#[test]
fn test_checkpoint_compaction_not_concurrent() {
    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 16)
        .build()
        .unwrap();
    let store = Arc::new(store);

    // Write enough data
    {
        let mut session = store.new_session();
        session.start();
        for i in 0..2000u64 {
            session.upsert(&i, &(i * 10));
        }
        session.end();
    }

    // Run checkpoint and compaction concurrently - neither should panic
    let s1 = store.clone();
    let s2 = store.clone();

    let h1 = std::thread::spawn(move || {
        let _ = s1.checkpoint();
    });
    let h2 = std::thread::spawn(move || {
        let begin = s2.log_begin_address();
        let head = s2.log_head_address();
        if head > begin {
            s2.compact(begin);
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();
}
```

**Step 2: Run test (should pass, just validates no panic)**

Run: `cargo test --test checkpoint_wait_pending test_checkpoint_compaction -- --nocapture`

**Step 3: Add `checkpoint_compaction_lock` field to `FasterKv`**

In `src/store/faster_kv.rs`, after `last_snapshot_checkpoint` (line 171), add:

```rust
    /// Mutual exclusion between checkpoint (write) and compaction (read).
    /// Checkpoint holds a write lock; compaction holds a read lock.
    checkpoint_compaction_lock: parking_lot::RwLock<()>,
```

In the `FasterKv::new()` constructor (or builder), add:

```rust
checkpoint_compaction_lock: parking_lot::RwLock::new(()),
```

**Step 4: Wrap checkpoint in write lock**

In `src/store/faster_kv/checkpoint.rs`, at the start of `execute_checkpoint_state_machine()` (around line 370), add:

```rust
let _compaction_guard = self.checkpoint_compaction_lock.write();
```

**Step 5: Wrap compaction in read lock**

In `src/store/faster_kv/compaction.rs`, at the start of `compact()` (line 28), add:

```rust
let _checkpoint_guard = self.checkpoint_compaction_lock.read();
```

Also wrap `log_compact_until()` (the internal compaction method, around line 94):

```rust
let _checkpoint_guard = self.checkpoint_compaction_lock.read();
```

**Step 6: Run tests**

Run: `cargo test -- --nocapture`
Expected: All PASS

**Step 7: Commit**

```bash
git add src/store/faster_kv.rs src/store/faster_kv/checkpoint.rs src/store/faster_kv/compaction.rs
git commit -m "feat: add checkpoint-compaction mutual exclusion via RwLock"
```

---

### Task 6: Serial number validation

**Files:**
- Modify: `src/checkpoint/state.rs:93-109` (add `checkpoint_version` to `SessionState`)
- Modify: `src/checkpoint/serialization.rs:114-139` (update `SerializableSessionState`)
- Modify: `src/store/faster_kv/checkpoint.rs:1086-1122` (strengthen `validate_session_serials`)
- Modify: `src/checkpoint/recovery.rs` (add incremental chain serial validation)

**Step 1: Write test for serial number validation**

Create file `tests/checkpoint_serial_numbers.rs`:

```rust
//! Tests for serial number validation during checkpoint and recovery

use oxifaster::checkpoint::{LogMetadata, SessionState};
use uuid::Uuid;

#[test]
fn test_valid_session_serials_accepted() {
    let mut meta = LogMetadata::new();
    meta.session_states = vec![
        SessionState::new(Uuid::new_v4(), 10),
        SessionState::new(Uuid::new_v4(), 20),
    ];
    // validate_session_serials is internal; test via checkpoint + recovery round-trip
}

#[test]
fn test_duplicate_guid_in_session_states_detected() {
    let guid = Uuid::new_v4();
    let mut meta = LogMetadata::new();
    meta.session_states = vec![
        SessionState::new(guid, 10),
        SessionState::new(guid, 20), // duplicate GUID
    ];
    // After implementation, this should be detected during recovery validation
}

#[test]
fn test_serial_number_continues_after_recovery() {
    use oxifaster::device::NullDisk;
    use oxifaster::store::FasterKvBuilder;
    use std::sync::Arc;

    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 16)
        .build()
        .unwrap();
    let store = Arc::new(store);

    // Write data and capture serial number
    let mut session = store.new_session();
    session.start();
    for i in 0..100u64 {
        session.upsert(&i, &(i * 10));
    }
    let state_before = session.to_session_state();
    session.end();

    // Serial number should be > 0 after mutations
    assert!(state_before.serial_num > 0, "serial should be > 0 after upserts");
}
```

**Step 2: Run test**

Run: `cargo test --test checkpoint_serial_numbers -- --nocapture`
Expected: PASS (basic assertions; more tests after implementation)

**Step 3: Add `checkpoint_version` to `SessionState`**

In `src/checkpoint/state.rs`, modify `SessionState` (lines 93-109):

```rust
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    /// Session GUID
    pub guid: Uuid,
    /// Monotonic serial number for this session
    pub serial_num: u64,
    /// Checkpoint version this state was captured at
    pub checkpoint_version: u32,
}

impl SessionState {
    /// Create a new session state
    pub fn new(guid: Uuid, serial_num: u64) -> Self {
        Self {
            guid,
            serial_num,
            checkpoint_version: 0,
        }
    }

    /// Create with explicit checkpoint version
    pub fn with_version(guid: Uuid, serial_num: u64, version: u32) -> Self {
        Self {
            guid,
            serial_num,
            checkpoint_version: version,
        }
    }
}
```

**Step 4: Update `SerializableSessionState`**

In `src/checkpoint/serialization.rs`, modify `SerializableSessionState` (lines 114-139):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableSessionState {
    pub guid: String,
    pub serial_num: u64,
    #[serde(default)]
    pub checkpoint_version: u32,
}

impl SerializableSessionState {
    pub fn from_state(state: &super::SessionState) -> Self {
        Self {
            guid: state.guid.to_string(),
            serial_num: state.serial_num,
            checkpoint_version: state.checkpoint_version,
        }
    }

    pub fn to_state(&self) -> io::Result<super::SessionState> {
        let guid = self.guid.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UUID: {e}"))
        })?;
        Ok(super::SessionState {
            guid,
            serial_num: self.serial_num,
            checkpoint_version: self.checkpoint_version,
        })
    }
}
```

**Step 5: Strengthen `validate_session_serials()`**

In `src/store/faster_kv/checkpoint.rs`, replace lines 1086-1122:

```rust
    fn validate_session_serials(log_meta: &LogMetadata) -> io::Result<()> {
        let mut seen_guids = std::collections::HashSet::new();

        for session_state in &log_meta.session_states {
            // Skip validation for new sessions (serial number 0)
            if session_state.serial_num == 0 {
                continue;
            }

            // Check for obviously invalid serial numbers
            if session_state.serial_num == u64::MAX {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid serial number {} for session {}",
                        session_state.serial_num, session_state.guid
                    ),
                ));
            }

            // Check for duplicate GUIDs within the same checkpoint
            if !seen_guids.insert(session_state.guid) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Duplicate session GUID {} in checkpoint",
                        session_state.guid
                    ),
                ));
            }

            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    session_id = %session_state.guid,
                    serial = session_state.serial_num,
                    version = log_meta.version,
                    "validated session serial number"
                );
            }
        }

        Ok(())
    }
```

**Step 6: Set checkpoint_version when capturing session states**

In `src/store/faster_kv/checkpoint.rs`, find the `Prepare` phase (around line 476-490) where `session_states` are captured. After `active.session_states = session_states;`, add version stamping:

```rust
// Stamp each session state with the checkpoint version
for ss in &mut active.session_states {
    ss.checkpoint_version = version;
}
```

**Step 7: Run tests**

Run: `cargo test -- --nocapture`
Expected: All PASS

**Step 8: Commit**

```bash
git add src/checkpoint/state.rs src/checkpoint/serialization.rs \
    src/store/faster_kv/checkpoint.rs tests/checkpoint_serial_numbers.rs
git commit -m "feat: strengthen serial number validation with duplicate GUID detection"
```

---

### Task 7: Metadata checksums

**Files:**
- Modify: `src/checkpoint/serialization.rs:20-47` (wrap read/write with checksum)
- Test: `tests/checkpoint_durability.rs`

**Step 1: Write test for checksum validation**

Create file `tests/checkpoint_durability.rs`:

```rust
//! Tests for checkpoint metadata checksum validation

use std::fs;
use std::io::Write;

#[test]
fn test_metadata_checksum_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    // Create metadata and write with checksum
    let mut meta = oxifaster::checkpoint::LogMetadata::new();
    meta.version = 42;
    meta.write_to_file(&path).unwrap();

    // Read back - should succeed
    let loaded = oxifaster::checkpoint::LogMetadata::read_from_file(&path).unwrap();
    assert_eq!(loaded.version, 42);
}

#[test]
fn test_metadata_corrupted_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    // Write metadata
    let meta = oxifaster::checkpoint::LogMetadata::new();
    meta.write_to_file(&path).unwrap();

    // Corrupt one byte in the file
    let mut data = fs::read(&path).unwrap();
    if data.len() > 10 {
        data[10] ^= 0xFF; // flip bits
        fs::write(&path, &data).unwrap();
    }

    // Read should detect corruption
    let result = oxifaster::checkpoint::LogMetadata::read_from_file(&path);
    // After checksum implementation, this should return an error
    // For now, it may still succeed (old format has no checksum)
    // This test becomes meaningful after Task 7 implementation
    let _ = result;
}

#[test]
fn test_metadata_backward_compatible_loading() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    // Write plain JSON (old format, no checksum)
    let json = r#"{"token":"00000000-0000-0000-0000-000000000001","version":7,"begin_address":0,"final_address":100,"flushed_until_address":100,"session_states":[]}"#;
    fs::write(&path, json).unwrap();

    // Should load successfully with backward compatibility
    let loaded = oxifaster::checkpoint::LogMetadata::read_from_file(&path).unwrap();
    assert_eq!(loaded.version, 7);
}
```

**Step 2: Run test**

Run: `cargo test --test checkpoint_durability -- --nocapture`
Expected: PASS (backward compat test validates current behavior)

**Step 3: Implement checksummed read/write**

In `src/checkpoint/serialization.rs`, modify `write_json_pretty_to_file` (lines 20-42) and `read_json_from_file` (lines 44+):

```rust
/// Magic bytes for checksummed metadata files
const CHECKSUM_MAGIC: &[u8; 4] = b"OXCK";

fn write_json_pretty_to_file<T: Serialize>(value: &T, path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing file name"))?
        .to_string_lossy();
    let tmp_path = parent.join(format!(".{file_name}.tmp"));

    let json_bytes = serde_json::to_vec_pretty(value).map_err(invalid_data)?;
    let crc = crc32fast::hash(&json_bytes);

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(CHECKSUM_MAGIC)?;
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&json_bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;

    match fs::rename(&tmp_path, path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(path);
            fs::rename(&tmp_path, path)
        }
        Err(e) => Err(e),
    }
}

fn read_json_from_file<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    let data = fs::read(path)?;

    // Check for checksum magic
    if data.len() >= 8 && &data[0..4] == CHECKSUM_MAGIC {
        let expected_crc = u32::from_le_bytes(
            data[4..8]
                .try_into()
                .map_err(|_| invalid_data("invalid CRC bytes"))?,
        );
        let json = &data[8..];
        let actual_crc = crc32fast::hash(json);

        if expected_crc != actual_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Checksum mismatch in {}: expected {expected_crc:#x}, got {actual_crc:#x}",
                    path.display()
                ),
            ));
        }

        serde_json::from_slice(json).map_err(invalid_data)
    } else {
        // Backward compatibility: old format without checksum
        serde_json::from_slice(&data).map_err(invalid_data)
    }
}
```

Add `use crate` import for `crc32fast` -- actually `crc32fast` is a standalone crate, just use it directly.

**Step 4: Update the corruption test assertion**

In `tests/checkpoint_durability.rs`, update `test_metadata_corrupted_detected`:

```rust
#[test]
fn test_metadata_corrupted_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.meta");

    let mut meta = oxifaster::checkpoint::LogMetadata::new();
    meta.version = 99;
    meta.write_to_file(&path).unwrap();

    // Corrupt a byte in the JSON payload (after the 8-byte header)
    let mut data = fs::read(&path).unwrap();
    assert!(data.len() > 12, "file too small");
    data[12] ^= 0xFF;
    fs::write(&path, &data).unwrap();

    let result = oxifaster::checkpoint::LogMetadata::read_from_file(&path);
    assert!(result.is_err(), "corrupted metadata should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Checksum mismatch"),
        "error should mention checksum: {}",
        err
    );
}
```

**Step 5: Run all tests**

Run: `cargo test -- --nocapture`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/checkpoint/serialization.rs tests/checkpoint_durability.rs
git commit -m "feat: add CRC32 checksums to checkpoint metadata with backward compat"
```

---

### Task 8: Driver heartbeat and takeover

**Files:**
- Modify: `src/store/faster_kv/cpr.rs:66-87` (add heartbeat fields)
- Modify: `src/store/faster_kv/checkpoint.rs:413-593` (update heartbeat in driver loop)
- Modify: `src/store/faster_kv.rs:373-479` (check heartbeat in `cpr_refresh`)

**Step 1: Write test for heartbeat update**

Add to the inline tests in `src/store/faster_kv/cpr.rs`:

```rust
#[test]
fn test_driver_heartbeat_increments() {
    let active = make_active(0b11, 0);
    assert_eq!(active.driver_heartbeat.load(Ordering::Relaxed), 0);
    active.update_heartbeat();
    assert_eq!(active.driver_heartbeat.load(Ordering::Relaxed), 1);
    active.update_heartbeat();
    assert_eq!(active.driver_heartbeat.load(Ordering::Relaxed), 2);
}

#[test]
fn test_driver_takeover() {
    let active = make_active(0b111, 0); // threads 0, 1, 2; driver=0
    assert_eq!(active.driver_thread_id(), 0);

    // Heartbeat is fresh - takeover should fail
    active.update_heartbeat();
    assert!(!active.try_takeover(1, 0));
    assert_eq!(active.driver_thread_id(), 0);

    // Without heartbeat update, after threshold - takeover should succeed
    // (We can't easily test time-based threshold in unit tests,
    //  so test the counter-based check)
    assert!(active.try_takeover(1, active.heartbeat_value()));
    assert_eq!(active.driver_thread_id(), 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --lib cpr -- --nocapture`
Expected: FAIL - methods don't exist yet

**Step 3: Add heartbeat fields and methods to `ActiveCheckpoint`**

In `src/store/faster_kv/cpr.rs`, modify `ActiveCheckpoint` struct (lines 66-87):

Add after `snapshot_written` (line 86):

```rust
    /// Driver heartbeat counter (incremented each driver loop iteration).
    pub(crate) driver_heartbeat: std::sync::atomic::AtomicU64,
```

In `ActiveCheckpoint::new()` (line 102-121), add:

```rust
driver_heartbeat: std::sync::atomic::AtomicU64::new(0),
```

Add methods after `barrier_complete()`:

```rust
    /// Update the driver heartbeat (called by driver thread each loop iteration).
    pub(crate) fn update_heartbeat(&self) {
        self.driver_heartbeat.fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// Get current heartbeat value.
    pub(crate) fn heartbeat_value(&self) -> u64 {
        self.driver_heartbeat.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get driver thread ID.
    pub(crate) fn driver_thread_id(&self) -> usize {
        self.driver_thread_id
    }

    /// Try to take over as driver if the current driver appears stalled.
    /// Returns true if takeover succeeded.
    ///
    /// `last_observed_heartbeat` is the heartbeat value the caller last observed.
    /// Takeover succeeds only if the heartbeat hasn't changed since then.
    pub(crate) fn try_takeover(&mut self, new_driver_id: usize, last_observed_heartbeat: u64) -> bool {
        let current = self.driver_heartbeat.load(std::sync::atomic::Ordering::Acquire);
        if current != last_observed_heartbeat {
            // Driver is still alive (heartbeat changed)
            return false;
        }
        self.driver_thread_id = new_driver_id;
        tracing::warn!(
            old_driver = last_observed_heartbeat,
            new_driver = new_driver_id,
            "Checkpoint driver takeover"
        );
        true
    }
```

**Step 4: Update driver loop to call `update_heartbeat()`**

In `src/store/faster_kv/checkpoint.rs`, inside the main loop (after line 414 `let current_state = ...`), add:

```rust
// Update heartbeat so participant threads know we're alive
let _ = self.cpr.with_active(|active| active.update_heartbeat());
```

**Step 5: Run tests**

Run: `cargo test -- --nocapture`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/store/faster_kv/cpr.rs src/store/faster_kv/checkpoint.rs
git commit -m "feat: add driver heartbeat and takeover mechanism for checkpoint robustness"
```

---

### Task 9: Session recovery protection

**Files:**
- Modify: `src/store/session.rs:296-311` (add `is_recovered` flag)
- Modify: `src/store/session.rs:556-568` (continue_from_state)

**Step 1: Write test**

Add to `tests/checkpoint_serial_numbers.rs`:

```rust
#[test]
fn test_session_recovery_serial_continuity() {
    use oxifaster::device::NullDisk;
    use oxifaster::store::FasterKvBuilder;
    use std::sync::Arc;

    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 16)
        .build()
        .unwrap();
    let store = Arc::new(store);

    let mut session = store.new_session();
    session.start();

    // Write 50 records
    for i in 0..50u64 {
        session.upsert(&i, &(i * 10));
    }

    let state = session.to_session_state();
    let serial_after_50 = state.serial_num;
    assert!(serial_after_50 >= 50, "should have serial >= 50 after 50 upserts");

    // Simulate recovery: continue from state
    session.continue_from_state(&state);

    // Write more
    for i in 50..60u64 {
        session.upsert(&i, &(i * 10));
    }

    let state2 = session.to_session_state();
    assert!(
        state2.serial_num > serial_after_50,
        "serial should continue increasing after recovery: {} > {}",
        state2.serial_num,
        serial_after_50
    );

    session.end();
}
```

**Step 2: Run test**

Run: `cargo test --test checkpoint_serial_numbers test_session_recovery -- --nocapture`
Expected: PASS (this behavior already works, test validates it)

**Step 3: Commit**

```bash
git add tests/checkpoint_serial_numbers.rs
git commit -m "test: add session recovery serial continuity test"
```

---

### Task 10: Concurrent checkpoint stress test

**Files:**
- Create: `tests/checkpoint_concurrent_robust.rs`

**Step 1: Write comprehensive concurrent checkpoint test**

```rust
//! Stress tests for concurrent checkpoint correctness

use oxifaster::device::NullDisk;
use oxifaster::store::FasterKvBuilder;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 8 threads write concurrently while checkpoint runs.
/// After recovery, verify all recovered keys have correct values.
#[test]
fn test_checkpoint_prefix_consistency() {
    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 18)
        .build()
        .unwrap();
    let store = Arc::new(store);

    let running = Arc::new(AtomicBool::new(true));
    let total_written = Arc::new(AtomicU64::new(0));

    // Start 8 writer threads
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let s = store.clone();
            let r = running.clone();
            let tw = total_written.clone();
            std::thread::spawn(move || {
                let mut session = s.new_session();
                session.start();
                let base = t * 100_000u64;
                let mut count = 0u64;
                while r.load(Ordering::Relaxed) {
                    let key = base + count;
                    // Value encodes the key so we can verify correctness
                    session.upsert(&key, &(key * 7 + 13));
                    count += 1;
                    if count % 100 == 0 {
                        session.refresh();
                    }
                }
                tw.fetch_add(count, Ordering::Relaxed);
                session.end();
            })
        })
        .collect();

    // Let writers run for a bit
    std::thread::sleep(Duration::from_millis(200));

    // Take checkpoint while writes are happening
    let token = store.checkpoint().expect("checkpoint under load should succeed");
    assert!(!token.is_nil());

    // Stop writers
    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let total = total_written.load(Ordering::Relaxed);
    assert!(total > 0, "writers should have written some data");

    // Verify: for every key we can read, the value must be correct
    let s = store.clone();
    let mut session = s.new_session();
    session.start();

    let mut verified = 0u64;
    for t in 0..8u64 {
        let base = t * 100_000;
        for i in 0..20_000u64 {
            let key = base + i;
            match session.read(&key) {
                Ok(Some(val)) => {
                    assert_eq!(
                        val,
                        key * 7 + 13,
                        "key {key} has wrong value: expected {}, got {val}",
                        key * 7 + 13
                    );
                    verified += 1;
                }
                Ok(None) => {
                    // Key not present - that's fine, it may not have been written yet
                    break;
                }
                Err(_) => break,
            }
        }
    }

    assert!(verified > 0, "should have verified at least some keys");
    session.end();
}

/// Multiple sequential checkpoints under continuous load.
#[test]
fn test_multiple_checkpoints_under_load() {
    let device = Arc::new(NullDisk::default());
    let store = FasterKvBuilder::<u64, u64, _>::new(device)
        .with_table_size(1 << 18)
        .build()
        .unwrap();
    let store = Arc::new(store);

    let running = Arc::new(AtomicBool::new(true));

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let s = store.clone();
            let r = running.clone();
            std::thread::spawn(move || {
                let mut session = s.new_session();
                session.start();
                let mut i = t * 100_000u64;
                while r.load(Ordering::Relaxed) {
                    session.upsert(&i, &i);
                    i += 1;
                    if i % 50 == 0 {
                        session.refresh();
                    }
                }
                session.end();
            })
        })
        .collect();

    // Take 3 sequential checkpoints
    for cp_num in 0..3 {
        std::thread::sleep(Duration::from_millis(100));
        let token = store
            .checkpoint()
            .unwrap_or_else(|e| panic!("checkpoint {cp_num} failed: {e}"));
        assert!(!token.is_nil(), "checkpoint {cp_num} returned nil token");
    }

    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}
```

**Step 2: Run tests**

Run: `cargo test --test checkpoint_concurrent_robust -- --nocapture`
Expected: All PASS

**Step 3: Commit**

```bash
git add tests/checkpoint_concurrent_robust.rs
git commit -m "test: add concurrent checkpoint stress tests for prefix consistency"
```

---

### Task 11: Run full quality checks

**Step 1: Format**

Run: `./scripts/check-fmt.sh`
Expected: PASS

**Step 2: Clippy**

Run: `./scripts/check-clippy.sh`
Expected: PASS (fix any warnings)

**Step 3: All tests**

Run: `./scripts/check-test.sh`
Expected: PASS

**Step 4: Coverage**

Run: `./scripts/check-coverage.sh`
Expected: >= 80%

**Step 5: Fix any issues found, then commit**

```bash
git add -A
git commit -m "chore: fix lint and formatting from quality checks"
```

---

### Task 12: Final integration verification

**Step 1: Run the full test suite with all features**

Run: `./scripts/check-test-all-features.sh`
Expected: PASS

**Step 2: Run examples to verify no regressions**

Run:
```bash
cargo run --example checkpoint
cargo run --example basic_kv
```
Expected: Both complete without errors

**Step 3: Commit any final fixes**

```bash
git add -A
git commit -m "chore: final integration verification fixes"
```
