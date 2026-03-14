# PR #11 Code Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 10 issues identified in PR #11 code review to improve checkpoint robustness and correctness.

**Architecture:** Minimal invasive fixes on existing architecture. Changes are isolated to CPR coordinator, serialization, and checkpoint modules. TDD approach with tests first.

**Tech Stack:** Rust, std::sync::atomic, crc32fast, serde_json

**Spec:** `docs/superpowers/specs/2026-03-15-pr11-review-fixes-design.md`

---

## File Structure

| File | Responsibility | Changes |
|------|----------------|---------|
| `src/store/faster_kv/cpr.rs` | CPR coordinator, ActiveCheckpoint | Issues 1, 3, 5, 6, 10 |
| `src/checkpoint/serialization.rs` | Metadata serialization with checksums | Issues 2, 8 |
| `src/store/faster_kv/checkpoint.rs` | Checkpoint state machine | Issues 3, 4, 7 |
| `src/store/faster_kv.rs` | FasterKv struct, config, cpr_refresh | Issues 3, 4, 9 |

---

## Chunk 1: Critical Fixes

### Task 1: Atomic driver_thread_id (Issue #1)

**Files:**
- Modify: `src/store/faster_kv/cpr.rs:74` (driver_thread_id field)
- Modify: `src/store/faster_kv/cpr.rs:106-126` (ActiveCheckpoint::new)
- Modify: `src/store/faster_kv/cpr.rs:184-209` (try_takeover)
- Test: `src/store/faster_kv/cpr.rs` (existing tests)

- [ ] **Step 1.1: Update imports in cpr.rs**

Add `AtomicUsize` to imports:

```rust
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
```

- [ ] **Step 1.2: Change driver_thread_id to AtomicUsize**

In `ActiveCheckpoint` struct (around line 74):

```rust
pub(crate) driver_thread_id: AtomicUsize,
```

- [ ] **Step 1.3: Update ActiveCheckpoint::new()**

Change line 113:

```rust
driver_thread_id: AtomicUsize::new(start.driver_thread_id),
```

- [ ] **Step 1.4: Update current_driver_thread_id()**

Change method (around line 185-187):

```rust
pub(crate) fn current_driver_thread_id(&self) -> usize {
    self.driver_thread_id.load(Ordering::Acquire)
}
```

- [ ] **Step 1.5: Update try_takeover() to use atomic CAS**

Replace entire method (around line 192-209):

```rust
/// Try to take over as driver if the current driver appears stalled.
///
/// This method is reserved for future driver failover functionality.
/// Currently, if a driver thread stalls, the checkpoint will timeout
/// via WaitPending. In the future, participant threads may use this
/// to elect a new driver and continue the checkpoint.
///
/// # Arguments
/// * `new_driver_id` - Thread ID of the candidate new driver
/// * `last_observed_heartbeat` - Heartbeat value the caller last observed
///
/// # Returns
/// `true` if takeover succeeded, `false` if driver is still active
#[allow(dead_code)]  // Reserved for future driver failover feature
pub(crate) fn try_takeover(
    &self,
    new_driver_id: usize,
    last_observed_heartbeat: u64,
) -> bool {
    let current = self.driver_heartbeat.load(Ordering::Acquire);
    if current != last_observed_heartbeat {
        return false;
    }
    let old_driver = self.driver_thread_id.load(Ordering::Acquire);
    match self.driver_thread_id.compare_exchange(
        old_driver,
        new_driver_id,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(prev) => {
            tracing::warn!(
                old_driver = prev,
                new_driver = new_driver_id,
                "Checkpoint driver takeover"
            );
            true
        }
        Err(_) => false,
    }
}
```

- [ ] **Step 1.6: Fix checkpoint.rs driver_thread_id access**

In `src/store/faster_kv/checkpoint.rs`, find all reads of `active.driver_thread_id` and change to `active.driver_thread_id.load(Ordering::Acquire)` or use `active.current_driver_thread_id()`.

Search pattern: `driver_thread_id ==` or `driver_thread_id !=`

Around line 427-430:

```rust
let is_driver = self
    .cpr
    .with_active(|active| active.current_driver_thread_id() == driver_thread_id)
    .unwrap_or(false);
```

- [ ] **Step 1.7: Run tests to verify**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests -v
```

Expected: All tests pass

- [ ] **Step 1.8: Commit**

```bash
git add src/store/faster_kv/cpr.rs src/store/faster_kv/checkpoint.rs
git commit -m "fix(cpr): use AtomicUsize for driver_thread_id to prevent race condition

- Convert driver_thread_id from usize to AtomicUsize
- Use compare_exchange in try_takeover for atomic takeover
- Add #[allow(dead_code)] annotation for future driver failover feature
- Update all accesses to use atomic load/store"
```

---

### Task 2: Checksum V1 Truncation Check (Issue #2)

**Files:**
- Modify: `src/checkpoint/serialization.rs:52-79` (read_json_from_file)
- Test: `src/checkpoint/serialization.rs` (add new test)

- [ ] **Step 2.1: Write failing test for V1 truncation**

Add test in `src/checkpoint/serialization.rs` tests module:

```rust
#[test]
fn test_truncated_checksum_v1_header_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("truncated.meta");

    // Write a file that starts with OXCK but is only 6 bytes (< 8 required)
    let mut truncated = Vec::new();
    truncated.extend_from_slice(b"OXCK");
    truncated.extend_from_slice(&[0u8, 0u8]); // Only 2 more bytes, not 4 for CRC
    std::fs::write(&file_path, &truncated).unwrap();

    let result = IndexMetadata::read_from_file(&file_path);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("Truncated"));
}
```

- [ ] **Step 2.2: Run test to verify it fails**

```bash
cargo test --package oxifaster --lib -- checkpoint::serialization::tests::test_truncated_checksum_v1_header_rejected -v
```

Expected: FAIL (currently falls through to JSON parsing which fails differently)

- [ ] **Step 2.3: Implement truncation check**

In `read_json_from_file()` (around line 52), add check before the main V1 detection:

```rust
fn read_json_from_file<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    let data = fs::read(path)?;

    // Check for truncated V1 checksum header
    if data.len() >= 4 && data.len() < 8 && &data[0..4] == CHECKSUM_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Truncated checksum header in {}: expected at least 8 bytes, got {}",
                path.display(),
                data.len()
            ),
        ));
    }

    if data.len() >= 8 && &data[0..4] == CHECKSUM_MAGIC {
        // ... existing V1 logic
    }
    // ... rest of function
}
```

- [ ] **Step 2.4: Run test to verify it passes**

```bash
cargo test --package oxifaster --lib -- checkpoint::serialization::tests::test_truncated_checksum_v1_header_rejected -v
```

Expected: PASS

- [ ] **Step 2.5: Run all serialization tests**

```bash
cargo test --package oxifaster --lib -- checkpoint::serialization::tests -v
```

Expected: All tests pass

- [ ] **Step 2.6: Commit**

```bash
git add src/checkpoint/serialization.rs
git commit -m "fix(checkpoint): reject truncated V1 checksum headers

Files starting with OXCK magic but < 8 bytes are now properly
rejected as truncated instead of falling through to JSON parsing."
```

---

## Chunk 2: High Priority Fixes

### Task 3: Aborted Flag for WaitPending Cleanup (Issue #3)

**Files:**
- Modify: `src/store/faster_kv/cpr.rs` (add aborted field and methods)
- Modify: `src/store/faster_kv/checkpoint.rs` (mark aborted on timeout)
- Modify: `src/store/faster_kv.rs` (check aborted in cpr_refresh)
- Test: `src/store/faster_kv/cpr.rs` (add new test)

- [ ] **Step 3.1: Write failing test for aborted flag**

Add test in `src/store/faster_kv/cpr.rs` tests module:

```rust
#[test]
fn test_checkpoint_aborted_flag() {
    let active = make_active(0b111, 0);
    assert!(!active.is_aborted());

    active.mark_aborted();
    assert!(active.is_aborted());

    // Marking aborted multiple times is idempotent
    active.mark_aborted();
    assert!(active.is_aborted());
}
```

- [ ] **Step 3.2: Run test to verify it fails**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests::test_checkpoint_aborted_flag -v
```

Expected: FAIL (methods don't exist)

- [ ] **Step 3.3: Add AtomicBool import**

```rust
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
```

- [ ] **Step 3.4: Add aborted field to ActiveCheckpoint**

After `driver_heartbeat` field (around line 90):

```rust
/// Flag indicating the checkpoint has been aborted (e.g., due to timeout).
/// Participant threads check this to exit early from cpr_refresh.
aborted: AtomicBool,
```

- [ ] **Step 3.5: Initialize aborted in new()**

In `ActiveCheckpoint::new()`:

```rust
aborted: AtomicBool::new(false),
```

- [ ] **Step 3.6: Add mark_aborted() and is_aborted() methods**

After `try_takeover()`:

```rust
/// Mark the checkpoint as aborted.
/// Called by driver thread when WaitPending times out.
pub(crate) fn mark_aborted(&self) {
    self.aborted.store(true, Ordering::Release);
}

/// Check if the checkpoint has been aborted.
pub(crate) fn is_aborted(&self) -> bool {
    self.aborted.load(Ordering::Acquire)
}
```

- [ ] **Step 3.7: Run test to verify it passes**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests::test_checkpoint_aborted_flag -v
```

Expected: PASS

- [ ] **Step 3.8: Update WaitPending timeout handler in checkpoint.rs**

In `src/store/faster_kv/checkpoint.rs`, Phase::WaitPending block (around line 517-546).

After line 531 (`wait_pending_deadline = None;`), before the `else if` branch, add the aborted marking in the timeout branch:

```rust
} else if Instant::now() > *deadline {
    // Mark checkpoint as aborted so participant threads can exit cleanly
    let _ = self.cpr.with_active(|active| active.mark_aborted());

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
        format!("WaitPending timeout after 30s: stalled threads: {stalled:?}"),
    ));
}
```

- [ ] **Step 3.9: Add aborted check in cpr_refresh()**

In `src/store/faster_kv.rs`, `cpr_refresh()` method starts at line 376.

Insert the aborted check after the `is_participant` check block (after line 399, before line 401 `if !is_participant`):

```rust
// Check if checkpoint was aborted (e.g., due to timeout)
if self.cpr.with_active(|active| active.is_aborted()).unwrap_or(false) {
    // Checkpoint failed, don't participate
    ctx.version = state.version;
    ctx.current.version = state.version;
    return;
}
```

- [ ] **Step 3.10: Run all cpr tests**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests -v
```

Expected: All tests pass

- [ ] **Step 3.11: Commit**

```bash
git add src/store/faster_kv/cpr.rs src/store/faster_kv/checkpoint.rs src/store/faster_kv.rs
git commit -m "fix(checkpoint): add aborted flag for clean WaitPending timeout handling

- Add aborted AtomicBool to ActiveCheckpoint
- Mark checkpoint aborted before returning timeout error
- Check aborted flag in cpr_refresh to exit early
- Prevents stalled threads from interacting with cleared CPR state"
```

---

### Task 4: Configurable WaitPending Timeout (Issue #4)

**Files:**
- Modify: `src/store/faster_kv.rs:47-78` (FasterKvConfig)
- Modify: `src/store/faster_kv.rs:159-190` (FasterKv struct)
- Modify: `src/store/faster_kv.rs:288-337` (with_full_config_impl)
- Modify: `src/store/faster_kv/checkpoint.rs:1066-1092` (recover_with_full_config struct init)
- Modify: `src/store/faster_kv/checkpoint.rs:1243-1269` (recover_index_only struct init)
- Modify: `src/store/faster_kv/checkpoint.rs:1312-1340` (recover_log_only struct init)
- Modify: `src/store/faster_kv/checkpoint.rs:519-545` (use configured timeout)

- [ ] **Step 4.1: Add wait_pending_timeout_secs to FasterKvConfig**

In `src/store/faster_kv.rs`, add field to `FasterKvConfig` (around line 54):

```rust
/// WaitPending phase timeout in seconds (default: 30)
pub wait_pending_timeout_secs: u64,
```

- [ ] **Step 4.2: Update FasterKvConfig::new()**

```rust
pub fn new(table_size: u64, log_memory_size: u64) -> Self {
    Self {
        table_size,
        log_memory_size,
        page_size_bits: 22,
        mutable_fraction: 0.9,
        wait_pending_timeout_secs: 30,
    }
}
```

- [ ] **Step 4.3: Update FasterKvConfig Default impl**

```rust
impl Default for FasterKvConfig {
    fn default() -> Self {
        Self {
            table_size: 1 << 20,
            log_memory_size: 1 << 29,
            page_size_bits: 22,
            mutable_fraction: 0.9,
            wait_pending_timeout_secs: 30,
        }
    }
}
```

- [ ] **Step 4.4: Add timeout field to FasterKv struct**

In `FasterKv` struct definition (around line 170, after `checkpoint_compaction_lock`):

```rust
/// WaitPending timeout in seconds (from config)
wait_pending_timeout_secs: u64,
```

- [ ] **Step 4.5: Initialize timeout in with_full_config_impl()**

In `src/store/faster_kv.rs`, in `with_full_config_impl()` struct initialization (around line 314-336), add field:

```rust
wait_pending_timeout_secs: config.wait_pending_timeout_secs,
```

- [ ] **Step 4.6: Initialize timeout in recover_with_full_config()**

In `src/store/faster_kv/checkpoint.rs`, in `recover_with_full_config()` struct initialization (around line 1066-1092), add field:

```rust
wait_pending_timeout_secs: config.wait_pending_timeout_secs,
```

- [ ] **Step 4.7: Initialize timeout in recover_index_only()**

In `src/store/faster_kv/checkpoint.rs`, in `recover_index_only()` struct initialization (around line 1243-1269), add field:

```rust
wait_pending_timeout_secs: config.wait_pending_timeout_secs,
```

- [ ] **Step 4.8: Initialize timeout in recover_log_only()**

In `src/store/faster_kv/checkpoint.rs`, in `recover_log_only()` struct initialization (around line 1312-1340), add field:

```rust
wait_pending_timeout_secs: config.wait_pending_timeout_secs,
```

- [ ] **Step 4.9: Add getter method**

In `src/store/faster_kv.rs`, add method after other getters:

```rust
/// Get the configured WaitPending timeout in seconds
pub fn wait_pending_timeout_secs(&self) -> u64 {
    self.wait_pending_timeout_secs
}
```

- [ ] **Step 4.10: Update checkpoint.rs to use configured timeout**

In `src/store/faster_kv/checkpoint.rs`, Phase::WaitPending block (around line 517-546).

Replace lines 519-520:

```rust
// Initialize deadline on first entry to WaitPending
let deadline = wait_pending_deadline
    .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));
```

With:

```rust
// Initialize deadline on first entry to WaitPending
let timeout_secs = self.wait_pending_timeout_secs();
let deadline = wait_pending_deadline
    .get_or_insert_with(|| Instant::now() + Duration::from_secs(timeout_secs));
```

Also update the error message (around line 543). Note: `timeout_secs` is in scope here because we define it at the start of the WaitPending block:

```rust
format!("WaitPending timeout after {}s: stalled threads: {stalled:?}", timeout_secs),
```

- [ ] **Step 4.11: Run build check**

```bash
cargo build --package oxifaster
```

Expected: Build succeeds

- [ ] **Step 4.12: Run checkpoint tests**

```bash
cargo test --package oxifaster checkpoint -v
```

Expected: All tests pass

- [ ] **Step 4.13: Commit**

```bash
git add src/store/faster_kv.rs src/store/faster_kv/checkpoint.rs
git commit -m "feat(config): add configurable WaitPending timeout

- Add wait_pending_timeout_secs to FasterKvConfig (default: 30)
- Store timeout in FasterKv struct
- Initialize in all constructors and recovery functions
- Use configured timeout in checkpoint state machine
- Allows tuning timeout for different workload characteristics"
```

---

### Task 5: Heartbeat Initial Value (Issue #5)

**Files:**
- Modify: `src/store/faster_kv/cpr.rs:124` (change initial value)
- Modify: `src/store/faster_kv/cpr.rs` (update tests)

- [ ] **Step 5.1: Change heartbeat initial value**

In `ActiveCheckpoint::new()` (around line 124):

```rust
driver_heartbeat: AtomicU64::new(1),  // Start at 1, not 0
```

- [ ] **Step 5.2: Update test_driver_heartbeat_increments**

Find the existing test and update expected values:

```rust
#[test]
fn test_driver_heartbeat_increments() {
    let active = make_active(0b11, 0);
    // Initial value is now 1 (not 0) to prevent false takeover detection
    assert_eq!(active.heartbeat_value(), 1);
    active.update_heartbeat();
    assert_eq!(active.heartbeat_value(), 2);
    active.update_heartbeat();
    assert_eq!(active.heartbeat_value(), 3);
}
```

- [ ] **Step 5.3: Update test_driver_takeover_stale_heartbeat**

Find the existing test and update to use initial value 1:

```rust
#[test]
fn test_driver_takeover_stale_heartbeat() {
    let active = make_active(0b111, 0);
    assert_eq!(active.current_driver_thread_id(), 0);

    // Initial heartbeat is 1 and we pass 1 as last observed -> stale, takeover succeeds
    assert!(active.try_takeover(1, 1));
    assert_eq!(active.current_driver_thread_id(), 1);
}
```

- [ ] **Step 5.4: Update test_driver_takeover_fresh_heartbeat_fails**

Find the existing test and update to use initial value 1:

```rust
#[test]
fn test_driver_takeover_fresh_heartbeat_fails() {
    let active = make_active(0b111, 0);
    // Initial heartbeat is 1, update to 2
    active.update_heartbeat();

    // Caller observed 1, but heartbeat is now 2 -> driver is active, takeover fails
    assert!(!active.try_takeover(1, 1));
    assert_eq!(active.current_driver_thread_id(), 0);
}
```

- [ ] **Step 5.5: Run tests**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests -v
```

Expected: All tests pass

- [ ] **Step 5.6: Commit**

```bash
git add src/store/faster_kv/cpr.rs
git commit -m "fix(cpr): start heartbeat at 1 to prevent false takeover detection

Starting at 0 could cause a newly started checkpoint to be
incorrectly detected as stalled. Starting at 1 provides a
safe initial state."
```

---

## Chunk 3: Medium Priority Fixes

### Task 6: PendingThreadsIter Performance (Issue #6)

**Files:**
- Modify: `src/store/faster_kv/cpr.rs` (add iterator)
- Test: `src/store/faster_kv/cpr.rs` (add comparison test)

- [ ] **Step 6.1: Write test for iterator equivalence**

Add test:

```rust
#[test]
fn test_pending_threads_iter_matches_vec() {
    let mut active = make_active(0b10110101, 0);
    active.set_phase(Phase::WaitPending, 1);
    active.ack(0);
    active.ack(2);

    let vec_result: Vec<usize> = active.pending_threads();
    let iter_result: Vec<usize> = active.pending_threads_iter().collect();

    assert_eq!(vec_result, iter_result);
}
```

- [ ] **Step 6.2: Run test to verify it fails**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests::test_pending_threads_iter_matches_vec -v
```

Expected: FAIL (method doesn't exist)

- [ ] **Step 6.3: Add PendingThreadsIter struct**

Before `impl ActiveCheckpoint`:

```rust
/// Iterator over pending thread IDs using efficient bit manipulation.
struct PendingThreadsIter {
    bits: u128,
}

impl Iterator for PendingThreadsIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bits == 0 {
            return None;
        }
        let idx = self.bits.trailing_zeros() as usize;
        self.bits &= !(1u128 << idx);
        Some(idx)
    }
}
```

- [ ] **Step 6.4: Add pending_threads_iter() method**

In `impl ActiveCheckpoint`:

```rust
/// Returns an iterator over thread IDs that have not yet acked.
/// More efficient than pending_threads() as it avoids allocation.
pub(crate) fn pending_threads_iter(&self) -> impl Iterator<Item = usize> {
    let not_acked = self.participants & !self.acked;
    PendingThreadsIter { bits: not_acked }
}
```

- [ ] **Step 6.5: Refactor pending_threads() to use iterator**

```rust
/// Returns thread IDs of participants that have not yet acked.
pub(crate) fn pending_threads(&self) -> Vec<usize> {
    self.pending_threads_iter().collect()
}
```

- [ ] **Step 6.6: Run test**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests::test_pending_threads_iter_matches_vec -v
```

Expected: PASS

- [ ] **Step 6.7: Run all cpr tests**

```bash
cargo test --package oxifaster --lib -- store::faster_kv::cpr::tests -v
```

Expected: All tests pass

- [ ] **Step 6.8: Commit**

```bash
git add src/store/faster_kv/cpr.rs
git commit -m "perf(cpr): add iterator-based pending_threads for zero allocation

- Add PendingThreadsIter using trailing_zeros() for O(k) iteration
- Refactor pending_threads() to use new iterator
- Avoids scanning all 96 thread bits when only few are pending"
```

---

### Task 7: Duplicate GUID Detection Fix (Issue #7)

**Files:**
- Modify: `src/store/faster_kv/checkpoint.rs:1123-1150` (validate_session_serials)
- Test: add new test

- [ ] **Step 7.1: Write failing test**

Add test in `tests/` or as integration test:

```rust
#[test]
fn test_duplicate_guid_with_zero_serial() {
    use oxifaster::checkpoint::LogMetadata;
    use uuid::Uuid;

    let mut meta = LogMetadata::with_token(Uuid::new_v4());
    let dup_guid = Uuid::new_v4();

    // Add two sessions with same GUID but serial_num = 0
    meta.add_session(dup_guid, 0);
    meta.add_session(dup_guid, 0);

    // This should fail validation
    // (need to expose validate_session_serials or test through recovery)
}
```

Note: `validate_session_serials` is private. Test through `recover()` or make it `pub(crate)` for testing.

- [ ] **Step 7.2: Fix the validation order**

In `src/store/faster_kv/checkpoint.rs`, `validate_session_serials()` (around line 1123-1150):

```rust
fn validate_session_serials(log_meta: &LogMetadata) -> io::Result<()> {
    let mut seen_guids = std::collections::HashSet::new();

    for session_state in &log_meta.session_states {
        // Check for duplicate GUID FIRST, before any continue
        if !seen_guids.insert(session_state.guid) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Duplicate session GUID {} in checkpoint",
                    session_state.guid
                ),
            ));
        }

        // Skip value range checks for serial_num == 0
        if session_state.serial_num == 0 {
            continue;
        }

        if session_state.serial_num == u64::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid serial number {} for session {}",
                    session_state.serial_num, session_state.guid
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

- [ ] **Step 7.3: Run checkpoint tests**

```bash
cargo test --package oxifaster checkpoint -v
```

Expected: All tests pass

- [ ] **Step 7.4: Commit**

```bash
git add src/store/faster_kv/checkpoint.rs
git commit -m "fix(checkpoint): detect duplicate GUIDs even with serial_num=0

Moved duplicate GUID check before the continue statement to ensure
all sessions are checked regardless of serial number value."
```

---

### Task 8: CRC32 V2 Format (Issue #8)

**Files:**
- Modify: `src/checkpoint/serialization.rs` (add V2 format)
- Test: `src/checkpoint/serialization.rs` (add V2 tests)

- [ ] **Step 8.1: Add V2 magic constant**

After `CHECKSUM_MAGIC`:

```rust
/// Magic bytes for V2 checksummed metadata files (CRC covers header)
const CHECKSUM_MAGIC_V2: &[u8; 4] = b"OXC2";

/// V2 format version number
const CHECKSUM_VERSION_V2: u8 = 2;
```

- [ ] **Step 8.2: Write failing test for V2 roundtrip**

```rust
#[test]
fn test_checksum_v2_format_roundtrip() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("v2.meta");

    let mut meta = IndexMetadata::with_token(Uuid::new_v4());
    meta.table_size = 1024;
    meta.num_buckets = 100;

    meta.write_to_file(&file_path).unwrap();

    // Verify file starts with V2 magic
    let data = std::fs::read(&file_path).unwrap();
    assert_eq!(&data[0..4], b"OXC2");

    let restored = IndexMetadata::read_from_file(&file_path).unwrap();
    assert_eq!(meta.token, restored.token);
    assert_eq!(meta.table_size, restored.table_size);
}
```

- [ ] **Step 8.3: Write test for V2 truncation detection**

```rust
#[test]
fn test_truncated_checksum_v2_header_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("truncated_v2.meta");

    // Write file with V2 magic but only 10 bytes (< 13 required)
    let mut truncated = Vec::new();
    truncated.extend_from_slice(b"OXC2");
    truncated.extend_from_slice(&[0u8; 6]); // Only 6 more bytes
    std::fs::write(&file_path, &truncated).unwrap();

    let result = IndexMetadata::read_from_file(&file_path);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}
```

- [ ] **Step 8.4: Write test for V2 corruption detection**

```rust
#[test]
fn test_checksum_v2_detects_header_corruption() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("corrupted_v2.meta");

    let mut meta = IndexMetadata::with_token(Uuid::new_v4());
    meta.table_size = 1024;
    meta.write_to_file(&file_path).unwrap();

    // Corrupt the VERSION byte (byte 8)
    let mut data = std::fs::read(&file_path).unwrap();
    data[8] ^= 0xFF;
    std::fs::write(&file_path, &data).unwrap();

    let result = IndexMetadata::read_from_file(&file_path);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Checksum mismatch"));
}
```

- [ ] **Step 8.5: Update write_json_pretty_to_file() for V2**

```rust
fn write_json_pretty_to_file<T: Serialize>(value: &T, path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing file name"))?
        .to_string_lossy();
    let tmp_path = parent.join(format!(".{file_name}.tmp"));

    let json_bytes = serde_json::to_vec_pretty(value).map_err(invalid_data)?;
    let length = json_bytes.len() as u32;

    // V2: CRC covers [VERSION][LENGTH][JSON]
    let mut to_hash = Vec::with_capacity(1 + 4 + json_bytes.len());
    to_hash.push(CHECKSUM_VERSION_V2);
    to_hash.extend_from_slice(&length.to_le_bytes());
    to_hash.extend_from_slice(&json_bytes);
    let crc = crc32fast::hash(&to_hash);

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(CHECKSUM_MAGIC_V2)?;
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&to_hash)?;  // VERSION + LENGTH + JSON
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
```

- [ ] **Step 8.6: Update read_json_from_file() for V2**

```rust
fn read_json_from_file<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    let data = fs::read(path)?;

    // V2 format: [MAGIC_V2: 4][CRC: 4][VERSION: 1][LENGTH: 4][JSON]
    // Minimum size: 13 bytes (4 + 4 + 1 + 4 + 0)
    if data.len() >= 4 && &data[0..4] == CHECKSUM_MAGIC_V2 {
        if data.len() < 13 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Truncated V2 checksum header in {}: expected at least 13 bytes, got {}",
                    path.display(),
                    data.len()
                ),
            ));
        }

        let expected_crc = u32::from_le_bytes(
            data[4..8].try_into().map_err(|_| invalid_data("invalid CRC bytes"))?,
        );
        let to_verify = &data[8..];  // VERSION + LENGTH + JSON
        let actual_crc = crc32fast::hash(to_verify);

        if expected_crc != actual_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Checksum mismatch in {}: expected {expected_crc:#x}, got {actual_crc:#x}",
                    path.display()
                ),
            ));
        }

        let _version = data[8];  // Currently always 2, reserved for future
        let length = u32::from_le_bytes(
            data[9..13].try_into().map_err(|_| invalid_data("invalid length bytes"))?,
        ) as usize;

        if data.len() != 13 + length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Length mismatch in {}: header says {} bytes, file has {}",
                    path.display(),
                    length,
                    data.len() - 13
                ),
            ));
        }

        let json = &data[13..];
        return serde_json::from_slice(json).map_err(invalid_data);
    }

    // V1 format: [MAGIC: 4][CRC: 4][JSON]
    if data.len() >= 4 && data.len() < 8 && &data[0..4] == CHECKSUM_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Truncated checksum header in {}: expected at least 8 bytes, got {}",
                path.display(),
                data.len()
            ),
        ));
    }

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

        return serde_json::from_slice(json).map_err(invalid_data);
    }

    // Backward compatibility: old format without checksum header
    serde_json::from_slice(&data).map_err(invalid_data)
}
```

- [ ] **Step 8.7: Run all serialization tests**

```bash
cargo test --package oxifaster --lib -- checkpoint::serialization::tests -v
```

Expected: All tests pass

- [ ] **Step 8.8: Run full test suite**

```bash
cargo test --package oxifaster
```

Expected: All tests pass (existing checkpoints still readable via V1/plain fallback)

- [ ] **Step 8.9: Commit**

```bash
git add src/checkpoint/serialization.rs
git commit -m "feat(checkpoint): add V2 checksum format with header coverage

V2 format: [OXC2][CRC32][VERSION][LENGTH][JSON]
- CRC now covers VERSION and LENGTH fields
- Detects header tampering and truncation
- Backward compatible: reads V1 (OXCK) and plain JSON
- Forward incompatible: old code cannot read V2 files"
```

---

## Chunk 4: Low Priority Fixes & Verification

### Task 9: checkpoint_compaction_lock Documentation (Issue #9)

**Files:**
- Modify: `src/store/faster_kv.rs:172-173`

- [ ] **Step 9.1: Add documentation comment**

Replace the existing comment (around line 171-173):

```rust
/// Mutual exclusion between checkpoint (write lock) and compaction (read lock).
///
/// # Design Notes
/// - Checkpoint acquires write lock; compaction acquires read lock
/// - Write lock is held for the entire checkpoint duration (including WaitPending timeout)
/// - This is intentional: checkpoint requires a consistent view of the log
/// - Compaction can wait up to `wait_pending_timeout_secs` in worst case
/// - Multiple compactions can run concurrently (all hold read locks)
/// - Future optimization: split into checkpoint_init_lock + checkpoint_data_lock
///   to allow compaction during WaitFlush phase
checkpoint_compaction_lock: RwLock<()>,
```

- [ ] **Step 9.2: Verify build**

```bash
cargo build --package oxifaster
```

Expected: Build succeeds

- [ ] **Step 9.3: Commit**

```bash
git add src/store/faster_kv.rs
git commit -m "docs(faster_kv): document checkpoint_compaction_lock design rationale

Explains why checkpoint holds the lock for entire duration and
mentions future optimization path for finer-grained locking."
```

---

### Task 10: Final Verification

- [ ] **Step 10.1: Run format check**

```bash
./scripts/check-fmt.sh
```

Expected: PASS

- [ ] **Step 10.2: Run clippy**

```bash
./scripts/check-clippy.sh
```

Expected: PASS (zero warnings)

- [ ] **Step 10.3: Run all tests**

```bash
./scripts/check-test.sh
```

Expected: All tests pass

- [ ] **Step 10.4: Run coverage check**

```bash
./scripts/check-coverage.sh
```

Expected: Coverage >= 80%

- [ ] **Step 10.5: Run all features test**

```bash
./scripts/check-test-all-features.sh
```

Expected: All tests pass

- [ ] **Step 10.6: Final commit (if any fixups needed)**

```bash
git status
# If changes needed:
git add -A
git commit -m "fix: address CI check feedback"
```

---

## Summary

| Task | Issue | Files Modified | Tests Added |
|------|-------|----------------|-------------|
| 1 | try_takeover race | cpr.rs, checkpoint.rs | Updated existing |
| 2 | V1 truncation | serialization.rs | 1 new |
| 3 | Aborted flag | cpr.rs, checkpoint.rs, faster_kv.rs | 1 new |
| 4 | Timeout config | faster_kv.rs, checkpoint.rs (4 struct inits) | - |
| 5 | Heartbeat init | cpr.rs | Updated existing |
| 6 | Iterator perf | cpr.rs | 1 new |
| 7 | GUID detection | checkpoint.rs | - |
| 8 | V2 format | serialization.rs | 3 new |
| 9 | Lock docs | faster_kv.rs | - |
| 10 | Verification | - | - |
