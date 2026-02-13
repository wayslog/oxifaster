//! Optional profiling/instrumentation for hot paths.
//!
//! This module is only compiled when the `index-profile` feature is enabled.
//! It is intended for local performance investigation and should not be used as a stable API.

#![allow(missing_docs)]

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[inline]
fn dur_to_ns(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Snapshot of insert-path counters and timings.
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexInsertProfileSnapshot {
    pub find_or_create_calls: u64,
    pub find_or_create_retries: u64,

    pub scan_calls: u64,
    pub scan_ns: u64,
    pub scan_slots: u64,
    pub scan_tag_matches: u64,
    pub scan_preferred_tag_matches: u64,
    pub scan_overflow_summary_skips: u64,

    pub lookup_calls: u64,
    pub lookup_slots: u64,
    pub lookup_base_slots: u64,
    pub lookup_overflow_slots: u64,
    pub lookup_tag_matches: u64,
    pub lookup_preferred_tag_matches: u64,
    pub lookup_overflow_summary_skips: u64,

    pub append_overflow_calls: u64,
    pub append_overflow_ns: u64,
    pub append_link_attempts: u64,
    pub append_link_failures: u64,
    pub append_link_race_deallocs: u64,
    pub append_reused_free_slots: u64,
    pub append_chain_depth_total: u64,
    pub append_chain_depth_max: u64,

    pub cas_attempts: u64,
    pub cas_success: u64,
    pub cas_fail: u64,

    pub conflict_checks: u64,
    pub conflict_ns: u64,
    pub conflicts_found: u64,
    pub tentative_clears: u64,

    pub scan_chain_depth_total: u64,
    pub scan_chain_depth_max: u64,
}

impl IndexInsertProfileSnapshot {
    fn pct(part: u64, total: u64) -> f64 {
        if total == 0 {
            0.0
        } else {
            (part as f64) * 100.0 / (total as f64)
        }
    }
}

impl fmt::Display for IndexInsertProfileSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scan_ms = self.scan_ns as f64 / 1_000_000.0;
        let overflow_ms = self.append_overflow_ns as f64 / 1_000_000.0;
        let conflict_ms = self.conflict_ns as f64 / 1_000_000.0;
        let total_ns = self.scan_ns + self.append_overflow_ns + self.conflict_ns;
        let total_ms = total_ns as f64 / 1_000_000.0;

        writeln!(
            f,
            "Index Insert Profile (MemHashIndex::find_or_create_entry)"
        )?;
        writeln!(f, "  calls: {}", self.find_or_create_calls)?;
        writeln!(f, "  retries: {}", self.find_or_create_retries)?;
        writeln!(
            f,
            "  cas: attempts={} success={} fail={}",
            self.cas_attempts, self.cas_success, self.cas_fail
        )?;
        writeln!(f, "  tentative_clears: {}", self.tentative_clears)?;
        writeln!(
            f,
            "  conflicts: checks={} found={}",
            self.conflict_checks, self.conflicts_found
        )?;
        writeln!(f, "  timings (ms): total={total_ms:.3}")?;
        writeln!(
            f,
            "    scan: {scan_ms:.3} ({:.1}%)",
            Self::pct(self.scan_ns, total_ns)
        )?;
        let avg_slots = if self.scan_calls == 0 {
            0.0
        } else {
            self.scan_slots as f64 / self.scan_calls as f64
        };
        writeln!(
            f,
            "    scan_detail: slots={} avg_slots_per_scan={avg_slots:.2} tag_matches={} preferred_tag_matches={} overflow_summary_skips={}",
            self.scan_slots,
            self.scan_tag_matches,
            self.scan_preferred_tag_matches,
            self.scan_overflow_summary_skips,
        )?;
        let avg_scan_chain_depth = if self.scan_calls == 0 {
            0.0
        } else {
            self.scan_chain_depth_total as f64 / self.scan_calls as f64
        };
        writeln!(
            f,
            "    scan_chain: avg_depth={avg_scan_chain_depth:.2} max_depth={}",
            self.scan_chain_depth_max
        )?;
        let avg_lookup_slots = if self.lookup_calls == 0 {
            0.0
        } else {
            self.lookup_slots as f64 / self.lookup_calls as f64
        };
        writeln!(
            f,
            "    lookup_detail: calls={} slots={} avg_slots_per_lookup={avg_lookup_slots:.2} base_slots={} overflow_slots={} tag_matches={} preferred_tag_matches={} overflow_summary_skips={}",
            self.lookup_calls,
            self.lookup_slots,
            self.lookup_base_slots,
            self.lookup_overflow_slots,
            self.lookup_tag_matches,
            self.lookup_preferred_tag_matches,
            self.lookup_overflow_summary_skips,
        )?;
        writeln!(
            f,
            "    append_overflow: {overflow_ms:.3} ({:.1}%)",
            Self::pct(self.append_overflow_ns, total_ns)
        )?;
        let avg_append_chain_depth = if self.append_overflow_calls == 0 {
            0.0
        } else {
            self.append_chain_depth_total as f64 / self.append_overflow_calls as f64
        };
        writeln!(
            f,
            "    append_detail: link_attempts={} link_failures={} race_deallocs={} reused_free_slots={} avg_chain_depth={avg_append_chain_depth:.2} max_chain_depth={}",
            self.append_link_attempts,
            self.append_link_failures,
            self.append_link_race_deallocs,
            self.append_reused_free_slots,
            self.append_chain_depth_max,
        )?;
        writeln!(
            f,
            "    conflict_check: {conflict_ms:.3} ({:.1}%)",
            Self::pct(self.conflict_ns, total_ns)
        )?;
        Ok(())
    }
}

/// Global counters for insert-path profiling.
///
/// All fields use relaxed atomics because the values are informational only.
pub struct IndexInsertProfile {
    find_or_create_calls: AtomicU64,
    find_or_create_retries: AtomicU64,

    scan_calls: AtomicU64,
    scan_ns: AtomicU64,
    scan_slots: AtomicU64,
    scan_tag_matches: AtomicU64,
    scan_preferred_tag_matches: AtomicU64,
    scan_overflow_summary_skips: AtomicU64,

    lookup_calls: AtomicU64,
    lookup_slots: AtomicU64,
    lookup_base_slots: AtomicU64,
    lookup_overflow_slots: AtomicU64,
    lookup_tag_matches: AtomicU64,
    lookup_preferred_tag_matches: AtomicU64,
    lookup_overflow_summary_skips: AtomicU64,

    append_overflow_calls: AtomicU64,
    append_overflow_ns: AtomicU64,
    append_link_attempts: AtomicU64,
    append_link_failures: AtomicU64,
    append_link_race_deallocs: AtomicU64,
    append_reused_free_slots: AtomicU64,
    append_chain_depth_total: AtomicU64,
    append_chain_depth_max: AtomicU64,

    cas_attempts: AtomicU64,
    cas_success: AtomicU64,
    cas_fail: AtomicU64,

    conflict_checks: AtomicU64,
    conflict_ns: AtomicU64,
    conflicts_found: AtomicU64,
    tentative_clears: AtomicU64,

    scan_chain_depth_total: AtomicU64,
    scan_chain_depth_max: AtomicU64,
}

impl IndexInsertProfile {
    pub const fn new() -> Self {
        Self {
            find_or_create_calls: AtomicU64::new(0),
            find_or_create_retries: AtomicU64::new(0),
            scan_calls: AtomicU64::new(0),
            scan_ns: AtomicU64::new(0),
            scan_slots: AtomicU64::new(0),
            scan_tag_matches: AtomicU64::new(0),
            scan_preferred_tag_matches: AtomicU64::new(0),
            scan_overflow_summary_skips: AtomicU64::new(0),
            lookup_calls: AtomicU64::new(0),
            lookup_slots: AtomicU64::new(0),
            lookup_base_slots: AtomicU64::new(0),
            lookup_overflow_slots: AtomicU64::new(0),
            lookup_tag_matches: AtomicU64::new(0),
            lookup_preferred_tag_matches: AtomicU64::new(0),
            lookup_overflow_summary_skips: AtomicU64::new(0),
            append_overflow_calls: AtomicU64::new(0),
            append_overflow_ns: AtomicU64::new(0),
            append_link_attempts: AtomicU64::new(0),
            append_link_failures: AtomicU64::new(0),
            append_link_race_deallocs: AtomicU64::new(0),
            append_reused_free_slots: AtomicU64::new(0),
            append_chain_depth_total: AtomicU64::new(0),
            append_chain_depth_max: AtomicU64::new(0),
            cas_attempts: AtomicU64::new(0),
            cas_success: AtomicU64::new(0),
            cas_fail: AtomicU64::new(0),
            conflict_checks: AtomicU64::new(0),
            conflict_ns: AtomicU64::new(0),
            conflicts_found: AtomicU64::new(0),
            tentative_clears: AtomicU64::new(0),
            scan_chain_depth_total: AtomicU64::new(0),
            scan_chain_depth_max: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.find_or_create_calls.store(0, Ordering::Relaxed);
        self.find_or_create_retries.store(0, Ordering::Relaxed);
        self.scan_calls.store(0, Ordering::Relaxed);
        self.scan_ns.store(0, Ordering::Relaxed);
        self.scan_slots.store(0, Ordering::Relaxed);
        self.scan_tag_matches.store(0, Ordering::Relaxed);
        self.scan_preferred_tag_matches.store(0, Ordering::Relaxed);
        self.scan_overflow_summary_skips.store(0, Ordering::Relaxed);
        self.lookup_calls.store(0, Ordering::Relaxed);
        self.lookup_slots.store(0, Ordering::Relaxed);
        self.lookup_base_slots.store(0, Ordering::Relaxed);
        self.lookup_overflow_slots.store(0, Ordering::Relaxed);
        self.lookup_tag_matches.store(0, Ordering::Relaxed);
        self.lookup_preferred_tag_matches
            .store(0, Ordering::Relaxed);
        self.lookup_overflow_summary_skips
            .store(0, Ordering::Relaxed);
        self.append_overflow_calls.store(0, Ordering::Relaxed);
        self.append_overflow_ns.store(0, Ordering::Relaxed);
        self.append_link_attempts.store(0, Ordering::Relaxed);
        self.append_link_failures.store(0, Ordering::Relaxed);
        self.append_link_race_deallocs.store(0, Ordering::Relaxed);
        self.append_reused_free_slots.store(0, Ordering::Relaxed);
        self.append_chain_depth_total.store(0, Ordering::Relaxed);
        self.append_chain_depth_max.store(0, Ordering::Relaxed);
        self.cas_attempts.store(0, Ordering::Relaxed);
        self.cas_success.store(0, Ordering::Relaxed);
        self.cas_fail.store(0, Ordering::Relaxed);
        self.conflict_checks.store(0, Ordering::Relaxed);
        self.conflict_ns.store(0, Ordering::Relaxed);
        self.conflicts_found.store(0, Ordering::Relaxed);
        self.tentative_clears.store(0, Ordering::Relaxed);
        self.scan_chain_depth_total.store(0, Ordering::Relaxed);
        self.scan_chain_depth_max.store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IndexInsertProfileSnapshot {
        IndexInsertProfileSnapshot {
            find_or_create_calls: self.find_or_create_calls.load(Ordering::Relaxed),
            find_or_create_retries: self.find_or_create_retries.load(Ordering::Relaxed),
            scan_calls: self.scan_calls.load(Ordering::Relaxed),
            scan_ns: self.scan_ns.load(Ordering::Relaxed),
            scan_slots: self.scan_slots.load(Ordering::Relaxed),
            scan_tag_matches: self.scan_tag_matches.load(Ordering::Relaxed),
            scan_preferred_tag_matches: self.scan_preferred_tag_matches.load(Ordering::Relaxed),
            scan_overflow_summary_skips: self.scan_overflow_summary_skips.load(Ordering::Relaxed),
            lookup_calls: self.lookup_calls.load(Ordering::Relaxed),
            lookup_slots: self.lookup_slots.load(Ordering::Relaxed),
            lookup_base_slots: self.lookup_base_slots.load(Ordering::Relaxed),
            lookup_overflow_slots: self.lookup_overflow_slots.load(Ordering::Relaxed),
            lookup_tag_matches: self.lookup_tag_matches.load(Ordering::Relaxed),
            lookup_preferred_tag_matches: self.lookup_preferred_tag_matches.load(Ordering::Relaxed),
            lookup_overflow_summary_skips: self
                .lookup_overflow_summary_skips
                .load(Ordering::Relaxed),
            append_overflow_calls: self.append_overflow_calls.load(Ordering::Relaxed),
            append_overflow_ns: self.append_overflow_ns.load(Ordering::Relaxed),
            append_link_attempts: self.append_link_attempts.load(Ordering::Relaxed),
            append_link_failures: self.append_link_failures.load(Ordering::Relaxed),
            append_link_race_deallocs: self.append_link_race_deallocs.load(Ordering::Relaxed),
            append_reused_free_slots: self.append_reused_free_slots.load(Ordering::Relaxed),
            append_chain_depth_total: self.append_chain_depth_total.load(Ordering::Relaxed),
            append_chain_depth_max: self.append_chain_depth_max.load(Ordering::Relaxed),
            cas_attempts: self.cas_attempts.load(Ordering::Relaxed),
            cas_success: self.cas_success.load(Ordering::Relaxed),
            cas_fail: self.cas_fail.load(Ordering::Relaxed),
            conflict_checks: self.conflict_checks.load(Ordering::Relaxed),
            conflict_ns: self.conflict_ns.load(Ordering::Relaxed),
            conflicts_found: self.conflicts_found.load(Ordering::Relaxed),
            tentative_clears: self.tentative_clears.load(Ordering::Relaxed),
            scan_chain_depth_total: self.scan_chain_depth_total.load(Ordering::Relaxed),
            scan_chain_depth_max: self.scan_chain_depth_max.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn record_find_or_create_call(&self) {
        self.find_or_create_calls.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_retry(&self) {
        self.find_or_create_retries.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_scan(&self, elapsed: Duration) {
        self.scan_calls.fetch_add(1, Ordering::Relaxed);
        self.scan_ns
            .fetch_add(dur_to_ns(elapsed), Ordering::Relaxed);
    }

    #[inline]
    pub fn record_scan_observations(
        &self,
        scan_slots: u64,
        tag_matches: u64,
        preferred_tag_matches: u64,
        chain_depth: u64,
    ) {
        self.scan_slots.fetch_add(scan_slots, Ordering::Relaxed);
        self.scan_tag_matches
            .fetch_add(tag_matches, Ordering::Relaxed);
        self.scan_preferred_tag_matches
            .fetch_add(preferred_tag_matches, Ordering::Relaxed);
        self.scan_chain_depth_total
            .fetch_add(chain_depth, Ordering::Relaxed);
        self.scan_chain_depth_max
            .fetch_max(chain_depth, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_scan_overflow_summary_skip(&self) {
        self.scan_overflow_summary_skips
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_lookup_observations(
        &self,
        base_slots: u64,
        overflow_slots: u64,
        tag_matches: u64,
        preferred_tag_matches: u64,
    ) {
        self.lookup_calls.fetch_add(1, Ordering::Relaxed);
        self.lookup_slots
            .fetch_add(base_slots + overflow_slots, Ordering::Relaxed);
        self.lookup_base_slots
            .fetch_add(base_slots, Ordering::Relaxed);
        self.lookup_overflow_slots
            .fetch_add(overflow_slots, Ordering::Relaxed);
        self.lookup_tag_matches
            .fetch_add(tag_matches, Ordering::Relaxed);
        self.lookup_preferred_tag_matches
            .fetch_add(preferred_tag_matches, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_lookup_overflow_summary_skip(&self) {
        self.lookup_overflow_summary_skips
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_append_overflow(&self, elapsed: Duration) {
        self.append_overflow_calls.fetch_add(1, Ordering::Relaxed);
        self.append_overflow_ns
            .fetch_add(dur_to_ns(elapsed), Ordering::Relaxed);
    }

    #[inline]
    pub fn record_append_link_attempt(&self, success: bool) {
        self.append_link_attempts.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.append_link_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_append_link_race_deallocate(&self) {
        self.append_link_race_deallocs
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_append_reused_free_slot(&self) {
        self.append_reused_free_slots
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_append_chain_depth(&self, chain_depth: u64) {
        self.append_chain_depth_total
            .fetch_add(chain_depth, Ordering::Relaxed);
        self.append_chain_depth_max
            .fetch_max(chain_depth, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cas_attempt(&self, success: bool) {
        self.cas_attempts.fetch_add(1, Ordering::Relaxed);
        if success {
            self.cas_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cas_fail.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_conflict_check(&self, elapsed: Duration, conflict_found: bool) {
        self.conflict_checks.fetch_add(1, Ordering::Relaxed);
        self.conflict_ns
            .fetch_add(dur_to_ns(elapsed), Ordering::Relaxed);
        if conflict_found {
            self.conflicts_found.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_tentative_clear(&self) {
        self.tentative_clears.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for IndexInsertProfile {
    fn default() -> Self {
        Self::new()
    }
}

/// Global insert-path profiler state.
pub static INDEX_INSERT_PROFILE: IndexInsertProfile = IndexInsertProfile::new();
