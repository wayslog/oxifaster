//! Integration tests to increase coverage for the epoch and delta_log modules.
//!
//! Covers:
//! - LightEpoch: protect_and_drain, reentrant_protect_and_drain,
//!   bump_current_epoch_with_action (multi-thread), spin_wait_for_safe_to_reclaim,
//!   reset_phase_finished / finish_thread_phase / has_thread_finished_phase,
//!   EpochGuard::thread_id, get_thread_id, get_thread_tag
//! - DeltaLog: create, write entries (delta / metadata / checkpoint_metadata),
//!   read back, flush, init_for_writes / init_for_reads
//! - DeltaLogIterator: create, iterate entries, handle empty log, get_next
//!
//! Note: DeltaLog flush has a known limitation where WriteBuffer.offset is never
//! updated, so partial-page flushes are effectively no-ops. Tests that need
//! read-back verification write enough data to cross page boundaries, which
//! triggers the seal path that calls flush_current_page.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use oxifaster::delta_log::{
    DeltaLog, DeltaLogConfig, DeltaLogEntry, DeltaLogEntryType, DeltaLogIntoIterator,
    DeltaLogIterator, IteratorState,
};
use oxifaster::device::NullDisk;
use oxifaster::epoch::{get_thread_id, get_thread_tag, EpochGuard, LightEpoch};

// ============================================================
// LightEpoch tests
// ============================================================

include!("coverage_epoch_delta/part1.rs");
include!("coverage_epoch_delta/part2.rs");
