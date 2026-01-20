# Phase 2: Complete Workstream B (CPR Integration)

**Priority**: Important
**Estimated Effort**: 2-3 days
**Status**: Ready for implementation

## Overview

This phase completes the CPR (Concurrent Prefix Recovery) integration by implementing proper pending I/O draining, adding concurrent checkpoint tests, and validating serial numbers during recovery.

---

## Task 2.1: Implement WaitPending Phase

**File**: `src/store/faster_kv/checkpoint.rs`
**Estimated Time**: 4-6 hours
**Difficulty**: Medium

### Current State

The WaitPending phase exists but only has a comment:

```rust
Phase::WaitPending => {
    // WAIT_PENDING semantics: wait only for the previous execution context to drain
    // (pending I/Os + retry_requests).
    let _ = self.cpr.with_active_mut(|active| {
        active.set_phase(current_state.phase, current_state.version);
    });
    let barrier_complete = self
        .cpr
        .with_active(|active| active.barrier_complete())
        .unwrap_or(false);
    if barrier_complete {
        let _ = self.system_state.try_advance();
    }
}
```

### What Needs to Be Done

1. **Add thread pending I/O tracking**
   - Each thread needs to report when its `prev` context is drained
   - Use `ThreadContext::prev_ctx_is_drained()`

2. **Implement cooperative draining**
   - Driver thread should poll all participant threads
   - Wait until all threads report their prev context is drained
   - Add timeout protection (e.g., 30 seconds)

### Implementation

```rust
// TODO: Replace the WaitPending phase handler in src/store/faster_kv/checkpoint.rs

Phase::WaitPending => {
    // WAIT_PENDING: Ensure all threads drain their previous execution context
    // before taking the checkpoint snapshot.

    let _ = self.cpr.with_active_mut(|active| {
        active.set_phase(current_state.phase, current_state.version);
    });

    // TODO: Add this method to FasterKv
    if self.all_threads_drained() {
        let _ = self.cpr.with_active_mut(|active| {
            active.ack(driver_thread_id);
        });
    }

    let barrier_complete = self
        .cpr
        .with_active(|active| active.barrier_complete())
        .unwrap_or(false);
    if barrier_complete {
        let _ = self.system_state.try_advance();
    }
}
```

3. **Add helper method to FasterKv**

```rust
// TODO: Add to src/store/faster_kv.rs

/// Check if all participant threads have drained their previous execution context.
fn all_threads_drained(&self) -> bool {
    self.cpr.with_active(|active| {
        let participants = active.participants;
        for thread_id in 0..MAX_THREADS {
            if (participants & (1u128 << thread_id)) == 0 {
                continue; // Not a participant
            }

            // Check if this thread's prev context is drained
            // This requires accessing thread contexts, which may need additional infrastructure
            // For now, we can use a timeout-based approach or add a registry
        }
        true // Placeholder
    }).unwrap_or(true)
}
```

### Alternative Simpler Approach

Since tracking all threads is complex, use a timeout-based approach:

```rust
Phase::WaitPending => {
    let _ = self.cpr.with_active_mut(|active| {
        active.set_phase(current_state.phase, current_state.version);
    });

    // Give threads time to drain (cooperative approach)
    // In practice, most pending I/Os complete quickly
    std::thread::sleep(std::time::Duration::from_millis(100));

    let barrier_complete = self
        .cpr
        .with_active(|active| active.barrier_complete())
        .unwrap_or(false);
    if barrier_complete {
        let _ = self.system_state.try_advance();
    }
}
```

### Testing

Create `tests/checkpoint_wait_pending.rs`:

```rust
// TODO: Create this test file

#[test]
fn test_wait_pending_drains_operations() {
    // Start many pending read operations
    // Trigger checkpoint
    // Verify all pending operations complete before snapshot
    // Verify no operations are lost
}

#[test]
fn test_wait_pending_timeout() {
    // Simulate stuck pending I/O
    // Verify checkpoint doesn't hang forever
    // Verify appropriate error handling
}
```

---

## Task 2.2: Add Concurrent Checkpoint Tests

**File**: `tests/checkpoint_concurrent.rs` (NEW)
**Estimated Time**: 4-6 hours
**Difficulty**: Medium

### Purpose

Verify that checkpoints work correctly under heavy concurrent load and validate the CPR prefix guarantee.

### Tests to Implement

```rust
// TODO: Create tests/checkpoint_concurrent.rs

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

#[test]
fn test_concurrent_checkpoint_under_write_load() {
    // TODO: Implement
    // 1. Start 10 writer threads continuously writing data
    // 2. Trigger checkpoint while writes are ongoing
    // 3. Continue writes after checkpoint
    // 4. Stop all writers
    // 5. Recover from checkpoint
    // 6. Verify: All data written before checkpoint is present
    // 7. Verify: Data written after checkpoint may or may not be present (CPR prefix)
}

#[test]
fn test_checkpoint_cpr_prefix_guarantee() {
    // TODO: Implement
    // 1. Multiple threads writing with serial number tracking
    // 2. Checkpoint at specific point
    // 3. Track max serial number in checkpoint
    // 4. Recover and verify exact prefix up to that serial
    // 5. Verify no operations beyond the prefix are present
}

#[test]
fn test_multiple_concurrent_checkpoints() {
    // TODO: Implement
    // 1. Start continuous write load
    // 2. Create checkpoint 1
    // 3. Continue writes
    // 4. Create checkpoint 2
    // 5. Continue writes
    // 6. Create checkpoint 3
    // 7. Verify all checkpoints are valid
    // 8. Verify recovery from each checkpoint works
}

#[test]
fn test_checkpoint_with_concurrent_reads() {
    // TODO: Implement
    // 1. Write initial data
    // 2. Start many reader threads
    // 3. Trigger checkpoint during reads
    // 4. Verify reads continue to work
    // 5. Verify checkpoint completes successfully
}

#[test]
fn test_checkpoint_thread_coordination() {
    // TODO: Implement
    // Verify "help advance state machine" behavior
    // 1. Multiple threads performing operations
    // 2. Some threads help checkpoint progress
    // 3. Verify all threads acknowledge phases correctly
    // 4. Verify barriers complete properly
}
```

### Key Validation Points

For each test, verify:
- ✅ Checkpoint completes without errors
- ✅ Recovery succeeds
- ✅ Data consistency maintained
- ✅ No deadlocks or hangs
- ✅ CPR prefix guarantee upheld

---

## Task 2.3: Add Recovery Serial Number Validation

**File**: `src/checkpoint/recovery.rs`
**Estimated Time**: 2-4 hours
**Difficulty**: Low-Medium

### Current State

Recovery loads session states but doesn't validate serial numbers against the checkpoint version.

### What Needs to Be Done

1. **Add serial number validation during recovery**

```rust
// TODO: Add to src/checkpoint/recovery.rs or src/store/faster_kv/checkpoint.rs

/// Validate that recovered session serial numbers are consistent with checkpoint version
fn validate_session_serials(
    checkpoint_version: u32,
    log_final_address: Address,
    session_states: &[SessionState],
) -> io::Result<()> {
    for session in session_states {
        // Verify serial numbers are reasonable
        if session.serial_number == 0 {
            continue; // New session, OK
        }

        // In a real implementation, you'd verify:
        // - Serial numbers are monotonic within a session
        // - Operations beyond the checkpoint boundary have higher serials
        // - No operations are "lost" in the serial sequence

        tracing::debug!(
            session_id = %session.session_id,
            serial = session.serial_number,
            version = checkpoint_version,
            "validating session serial"
        );
    }

    Ok(())
}
```

2. **Call validation during recovery**

```rust
// TODO: Update the recover() method in src/store/faster_kv/checkpoint.rs

pub fn recover(
    checkpoint_dir: &Path,
    token: CheckpointToken,
    config: FasterKvConfig,
    device: D,
) -> io::Result<Self> {
    // ... existing recovery code ...

    // After loading log metadata and session states:
    validate_session_serials(
        log_meta.version,
        log_meta.final_log_address,
        &log_meta.session_states,
    )?;

    // ... continue recovery ...
}
```

3. **Add test for serial validation**

```rust
// TODO: Add to tests/recovery.rs or create tests/recovery_validation.rs

#[test]
fn test_recovery_validates_serial_numbers() {
    // 1. Create store with operations
    // 2. Track serial numbers
    // 3. Checkpoint
    // 4. More operations (different serials)
    // 5. Recover
    // 6. Verify only operations <= checkpoint serial are present
}

#[test]
fn test_recovery_rejects_corrupted_serials() {
    // 1. Create checkpoint
    // 2. Manually corrupt session serial in checkpoint metadata
    // 3. Attempt recovery
    // 4. Should fail or warn (depending on policy)
}
```

---

## Task 2.4: Add CPR Integration Tests

**File**: `tests/cpr_integration.rs` (NEW)
**Estimated Time**: 2-3 hours
**Difficulty**: Low

### Purpose

Test the complete CPR flow from operation to checkpoint to recovery.

```rust
// TODO: Create tests/cpr_integration.rs

#[test]
fn test_cpr_version_tracking() {
    // Verify version numbers increment correctly
    // Verify operations track correct version
}

#[test]
fn test_cpr_context_swapping() {
    // Verify execution contexts swap correctly during checkpoint
    // Verify prev context is properly tracked
}

#[test]
fn test_cpr_phase_transitions() {
    // Verify all checkpoint phases are entered
    // Verify phase order is correct
    // Verify no phases are skipped
}
```

---

## Success Criteria

Phase 2 is complete when:

✅ WaitPending phase drains pending I/O
✅ All concurrent checkpoint tests pass
✅ Recovery validates serial numbers
✅ CPR prefix guarantee is verified
✅ No regressions in existing tests
✅ All code is documented and formatted

---

## Notes for Implementer

- The infrastructure is already in place (`ThreadContext`, `ExecutionContext`, CPR coordinator)
- Most work is wiring existing pieces together
- Focus on correctness over performance initially
- Add comprehensive tracing for debugging
- Test with `RUST_LOG=debug` to see phase transitions

---

## Estimated Breakdown

| Task | Time | Difficulty |
|------|------|-----------|
| 2.1 WaitPending | 4-6h | Medium |
| 2.2 Concurrent tests | 4-6h | Medium |
| 2.3 Serial validation | 2-4h | Low-Medium |
| 2.4 Integration tests | 2-3h | Low |
| **Total** | **12-19h** | **~2-3 days** |

---

## References

- See `docs/workstreams_review.md` for analysis
- See `src/store/session.rs` for context tracking
- See `src/store/faster_kv/cpr.rs` for coordinator
- Microsoft FASTER paper for CPR semantics
