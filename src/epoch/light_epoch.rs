//! LightEpoch - Lightweight epoch-based memory reclamation
//!
//! This module implements the epoch protection mechanism used by FASTER
//! to safely reclaim memory in lock-free data structures.

use std::cell::{RefCell, UnsafeCell};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;

use crate::constants::{CACHE_LINE_BYTES, MAX_THREADS};

// ============ Thread ID Allocation ============

/// Global counter for allocating thread-local IDs
static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

/// Recycled thread IDs (used when threads exit).
static FREE_THREAD_IDS: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();

/// Per-slot generation counter.
///
/// This allows other subsystems (e.g., async I/O routing) to safely distinguish
/// a reused `thread_id` slot across different thread lifetimes.
static THREAD_ID_GENERATIONS: OnceLock<Vec<AtomicU64>> = OnceLock::new();

fn free_thread_ids() -> &'static Mutex<Vec<usize>> {
    FREE_THREAD_IDS.get_or_init(|| Mutex::new(Vec::new()))
}

fn thread_id_generations() -> &'static [AtomicU64] {
    THREAD_ID_GENERATIONS
        .get_or_init(|| (0..MAX_THREADS).map(|_| AtomicU64::new(0)).collect())
        .as_slice()
}

#[derive(Debug)]
struct ThreadIdGuard {
    id: usize,
    generation: u64,
}

impl Drop for ThreadIdGuard {
    fn drop(&mut self) {
        if let Some(gens) = THREAD_ID_GENERATIONS.get() {
            if let Some(generation) = gens.get(self.id) {
                generation.fetch_add(1, Ordering::AcqRel);
            }
        }
        if let Some(free) = FREE_THREAD_IDS.get() {
            free.lock().push(self.id);
        }
    }
}

fn try_allocate_thread_id() -> Option<ThreadIdGuard> {
    let make_guard = |id: usize| {
        let generation = thread_id_generations()
            .get(id)
            .map(|g| g.load(Ordering::Acquire))
            .unwrap_or(0);
        ThreadIdGuard { id, generation }
    };

    if let Some(id) = free_thread_ids().lock().pop() {
        return Some(make_guard(id));
    }

    match NEXT_THREAD_ID.fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
        if next < MAX_THREADS {
            Some(next + 1)
        } else {
            None
        }
    }) {
        Ok(id) => Some(make_guard(id)),
        Err(_) => {
            // Another thread may have just returned an ID. Re-check before giving up.
            free_thread_ids().lock().pop().map(make_guard)
        }
    }
}

thread_local! {
    static THREAD_ID: RefCell<Option<ThreadIdGuard>> = const { RefCell::new(None) };
}

use crate::status::Status;

/// Get the current thread's ID for epoch protection
///
/// This returns a stable ID for the current thread that can be used
/// as an index into the epoch table. The ID is allocated on first call
/// and remains constant for the thread's lifetime.
///
/// # Errors
///
/// Returns `Status::TooManyThreads` if more than `MAX_THREADS` threads
/// concurrently use the library.
#[inline]
pub fn get_thread_id() -> Result<usize, Status> {
    try_get_thread_id().ok_or(Status::TooManyThreads)
}

/// Try to get the current thread's ID for epoch protection.
///
/// Returns `None` if more than `MAX_THREADS` threads concurrently use the library.
#[inline]
pub fn try_get_thread_id() -> Option<usize> {
    THREAD_ID.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(guard) = slot.as_ref() {
            return Some(guard.id);
        }

        let guard = try_allocate_thread_id()?;
        let id = guard.id;
        *slot = Some(guard);
        Some(id)
    })
}

/// Get the current thread's generation tag for its `thread_id`.
///
/// This value changes whenever a `thread_id` slot is recycled and can be used
/// to disambiguate stale events associated with a prior owner of the same slot.
#[inline]
pub fn get_thread_tag() -> u64 {
    THREAD_ID.with(|slot| slot.borrow().as_ref().map_or(0, |g| g.generation))
}

/// Return the current generation tag for a given `thread_id` slot.
///
/// This is primarily intended for internal filtering of stale events.
#[inline]
pub(crate) fn current_thread_tag_for(thread_id: usize) -> u64 {
    thread_id_generations()
        .get(thread_id)
        .map(|g| g.load(Ordering::Acquire))
        .unwrap_or(0)
}

/// Special epoch value indicating the thread is not protected
pub const UNPROTECTED: u64 = 0;

/// Size of the drain list for deferred actions
const DRAIN_LIST_SIZE: usize = 256;

/// Entry in the epoch table (one per thread)
#[repr(C, align(64))]
struct Entry {
    /// Local epoch value seen by this thread
    local_current_epoch: AtomicU64,
    /// Reentrant protection counter
    reentrant: AtomicU32,
    /// Phase finished flag for checkpointing
    phase_finished: AtomicU32,
    /// Padding to fill cache line
    _padding: [u8; CACHE_LINE_BYTES - 16],
}

impl Entry {
    const fn new() -> Self {
        Self {
            local_current_epoch: AtomicU64::new(UNPROTECTED),
            reentrant: AtomicU32::new(0),
            phase_finished: AtomicU32::new(0),
            _padding: [0; CACHE_LINE_BYTES - 16],
        }
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self::new()
    }
}

/// Action to be performed when an epoch becomes safe to reclaim
pub struct EpochAction {
    /// The epoch when this action was registered
    epoch: AtomicU64,
    /// The callback to invoke
    callback: UnsafeCell<Option<Box<dyn FnOnce() + Send + 'static>>>,
}

impl EpochAction {
    /// Epoch value indicating this slot is free
    const FREE: u64 = u64::MAX;
    /// Epoch value indicating this slot is locked
    const LOCKED: u64 = u64::MAX - 1;

    const fn new() -> Self {
        Self {
            epoch: AtomicU64::new(Self::FREE),
            callback: UnsafeCell::new(None),
        }
    }

    fn is_free(&self) -> bool {
        self.epoch.load(Ordering::Acquire) == Self::FREE
    }

    /// Try to pop the action if the epoch has been reached
    fn try_pop(&self, expected_epoch: u64) -> bool {
        match self.epoch.compare_exchange(
            expected_epoch,
            Self::LOCKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Take the callback
                let callback = unsafe { (*self.callback.get()).take() };
                // Release the lock
                self.epoch.store(Self::FREE, Ordering::Release);
                // Execute the callback
                if let Some(cb) = callback {
                    cb();
                }
                true
            }
            Err(_) => false,
        }
    }

    /// Try to push a new action
    ///
    /// Returns `Ok(())` if successful, `Err(callback)` if the CAS failed,
    /// allowing the caller to retry with the callback.
    fn try_push<F>(&self, prior_epoch: u64, callback: F) -> Result<(), F>
    where
        F: FnOnce() + Send + 'static,
    {
        match self.epoch.compare_exchange(
            Self::FREE,
            Self::LOCKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Store the callback
                unsafe {
                    *self.callback.get() = Some(Box::new(callback));
                }
                // Release the lock with the epoch value
                self.epoch.store(prior_epoch, Ordering::Release);
                Ok(())
            }
            Err(_) => Err(callback),
        }
    }

    /// Try to swap an existing action with a new one
    ///
    /// Returns `Ok(())` if successful, `Err(callback)` if the CAS failed,
    /// allowing the caller to retry with the callback.
    fn try_swap<F>(&self, expected_epoch: u64, prior_epoch: u64, new_callback: F) -> Result<(), F>
    where
        F: FnOnce() + Send + 'static,
    {
        match self.epoch.compare_exchange(
            expected_epoch,
            Self::LOCKED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Take the existing callback
                let existing_callback = unsafe { (*self.callback.get()).take() };
                // Store the new callback
                unsafe {
                    *self.callback.get() = Some(Box::new(new_callback));
                }
                // Release the lock with the new epoch
                self.epoch.store(prior_epoch, Ordering::Release);
                // Execute the existing callback
                if let Some(cb) = existing_callback {
                    cb();
                }
                Ok(())
            }
            Err(_) => Err(new_callback),
        }
    }
}

// Safety: EpochAction is protected by atomic operations
unsafe impl Send for EpochAction {}
unsafe impl Sync for EpochAction {}

/// Lightweight epoch protection framework
///
/// Provides safe memory reclamation for lock-free data structures by tracking
/// which threads are accessing shared data and deferring cleanup until all
/// threads have moved past a safe point.
pub struct LightEpoch {
    /// Per-thread epoch table
    table: Box<[Entry]>,
    /// List of deferred actions
    drain_list: Box<[EpochAction]>,
    /// Number of pending drain actions
    drain_count: AtomicU32,
    /// Current global epoch
    pub current_epoch: AtomicU64,
    /// Cached safe-to-reclaim epoch
    pub safe_to_reclaim_epoch: AtomicU64,
}

impl LightEpoch {
    /// Create a new LightEpoch instance
    pub fn new() -> Self {
        let table = (0..MAX_THREADS)
            .map(|_| Entry::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let drain_list = (0..DRAIN_LIST_SIZE)
            .map(|_| EpochAction::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            table,
            drain_list,
            drain_count: AtomicU32::new(0),
            current_epoch: AtomicU64::new(1),
            safe_to_reclaim_epoch: AtomicU64::new(0),
        }
    }

    /// Enter the protected region
    ///
    /// Returns the current epoch value.
    /// The thread should call `unprotect()` when done accessing shared data.
    #[inline]
    pub fn protect(&self, thread_id: usize) -> u64 {
        debug_assert!(thread_id < MAX_THREADS);
        let epoch = self.current_epoch.load(Ordering::Acquire);
        self.table[thread_id]
            .local_current_epoch
            .store(epoch, Ordering::Release);
        epoch
    }

    /// Enter the protected region and drain pending actions
    #[inline]
    pub fn protect_and_drain(&self, thread_id: usize) -> u64 {
        let epoch = self.protect(thread_id);
        if self.drain_count.load(Ordering::Acquire) > 0 {
            self.drain(epoch);
        }
        epoch
    }

    /// Reentrant protection with draining - supports nested protection calls.
    #[inline]
    pub fn reentrant_protect_and_drain(&self, thread_id: usize) -> u64 {
        let epoch = self.reentrant_protect(thread_id);
        if self.drain_count.load(Ordering::Acquire) > 0 {
            self.drain(epoch);
        }
        epoch
    }

    /// Reentrant protection - supports nested protection calls
    #[inline]
    pub fn reentrant_protect(&self, thread_id: usize) -> u64 {
        debug_assert!(thread_id < MAX_THREADS);
        // SAFETY: `thread_id` is owned by the calling thread (assigned via the thread-local ID
        // allocator). Only the owning thread mutates its `Entry`'s reentrancy counter and local
        // epoch, while other threads may read the published epoch concurrently.
        let entry = &self.table[thread_id];

        // Always increment the reentrant counter to track nesting depth
        let current_count = entry.reentrant.fetch_add(1, Ordering::AcqRel);

        // If this is the first protection call, set the epoch
        if current_count == 0 {
            let epoch = self.current_epoch.load(Ordering::Acquire);
            entry.local_current_epoch.store(epoch, Ordering::Release);
            epoch
        } else {
            // Already protected - return the current epoch
            entry.local_current_epoch.load(Ordering::Acquire)
        }
    }

    /// Check if the thread is currently protected
    #[inline]
    pub fn is_protected(&self, thread_id: usize) -> bool {
        debug_assert!(thread_id < MAX_THREADS);
        self.table[thread_id]
            .local_current_epoch
            .load(Ordering::Acquire)
            != UNPROTECTED
    }

    /// Exit the protected region
    #[inline]
    pub fn unprotect(&self, thread_id: usize) {
        debug_assert!(thread_id < MAX_THREADS);
        self.table[thread_id]
            .local_current_epoch
            .store(UNPROTECTED, Ordering::Release);
    }

    /// Exit reentrant protection
    #[inline]
    pub fn reentrant_unprotect(&self, thread_id: usize) {
        debug_assert!(thread_id < MAX_THREADS);
        let entry = &self.table[thread_id];

        if entry.reentrant.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        entry
            .local_current_epoch
            .store(UNPROTECTED, Ordering::Release);
    }

    /// Drain pending actions that are now safe to execute
    fn drain(&self, next_epoch: u64) {
        self.compute_new_safe_to_reclaim_epoch(next_epoch);
        let safe_epoch = self.safe_to_reclaim_epoch.load(Ordering::Acquire);

        for action in self.drain_list.iter() {
            let trigger_epoch = action.epoch.load(Ordering::Acquire);
            if trigger_epoch == EpochAction::FREE
                || trigger_epoch == EpochAction::LOCKED
                || trigger_epoch > safe_epoch
            {
                continue;
            }

            if action.try_pop(trigger_epoch) && self.drain_count.fetch_sub(1, Ordering::AcqRel) == 1
            {
                break;
            }
        }
    }

    /// Increment the current epoch
    pub fn bump_current_epoch(&self) -> u64 {
        let next_epoch = self.current_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        if self.drain_count.load(Ordering::Acquire) > 0 {
            self.drain(next_epoch);
        }
        next_epoch
    }

    /// Increment the epoch and register a callback for when the old epoch is safe
    pub fn bump_current_epoch_with_action<F>(&self, callback: F) -> u64
    where
        F: FnOnce() + Send + 'static,
    {
        const MAX_FULL_SCANS: usize = 500;

        let prior_epoch = self.bump_current_epoch() - 1;

        let mut callback = Some(callback);
        let mut i = 0;
        let mut full_scans = 0;
        loop {
            let action = &self.drain_list[i];
            let trigger_epoch = action.epoch.load(Ordering::Acquire);

            if trigger_epoch == EpochAction::FREE {
                if let Some(cb) = callback.take() {
                    match action.try_push(prior_epoch, cb) {
                        Ok(()) => {
                            self.drain_count.fetch_add(1, Ordering::AcqRel);
                            return prior_epoch + 1;
                        }
                        Err(returned_cb) => {
                            // CAS failed, restore the callback and try another slot
                            callback = Some(returned_cb);
                        }
                    }
                }
            } else if trigger_epoch != EpochAction::LOCKED
                && trigger_epoch <= self.safe_to_reclaim_epoch.load(Ordering::Acquire)
            {
                if let Some(cb) = callback.take() {
                    match action.try_swap(trigger_epoch, prior_epoch, cb) {
                        Ok(()) => return prior_epoch + 1,
                        Err(returned_cb) => {
                            // CAS failed, restore the callback and try another slot
                            callback = Some(returned_cb);
                        }
                    }
                }
            }

            i = (i + 1) % DRAIN_LIST_SIZE;
            if i == 0 {
                full_scans += 1;
                if full_scans >= MAX_FULL_SCANS {
                    thread::sleep(Duration::from_secs(1));
                    tracing::warn!("Unable to add trigger to epoch after many retries");
                    // Execute the callback directly since we couldn't defer it
                    if let Some(cb) = callback.take() {
                        cb();
                    }
                    return prior_epoch + 1;
                }
            }
        }
    }

    /// Compute the new safe-to-reclaim epoch by scanning all threads
    pub fn compute_new_safe_to_reclaim_epoch(&self, current_epoch: u64) -> u64 {
        let mut oldest_ongoing = current_epoch;

        for entry in self.table.iter() {
            let entry_epoch = entry.local_current_epoch.load(Ordering::Acquire);
            if entry_epoch != UNPROTECTED && entry_epoch < oldest_ongoing {
                oldest_ongoing = entry_epoch;
            }
        }

        let safe = oldest_ongoing.saturating_sub(1);
        self.safe_to_reclaim_epoch.store(safe, Ordering::Release);
        safe
    }

    /// Spin wait until the specified epoch is safe to reclaim
    pub fn spin_wait_for_safe_to_reclaim(&self, current_epoch: u64, target_safe_epoch: u64) {
        loop {
            self.compute_new_safe_to_reclaim_epoch(current_epoch);
            if self.safe_to_reclaim_epoch.load(Ordering::Acquire) >= target_safe_epoch {
                break;
            }
            thread::yield_now();
        }
    }

    /// Check if an epoch is safe to reclaim
    #[inline]
    pub fn is_safe_to_reclaim(&self, epoch: u64) -> bool {
        epoch <= self.safe_to_reclaim_epoch.load(Ordering::Acquire)
    }

    /// Reset phase finished flags for all threads
    pub fn reset_phase_finished(&self) {
        for entry in self.table.iter() {
            entry.phase_finished.store(0, Ordering::Release);
        }
    }

    /// Mark this thread as having finished the specified phase
    ///
    /// Returns true if all threads have finished the phase
    pub fn finish_thread_phase(&self, thread_id: usize, phase: u32) -> bool {
        debug_assert!(thread_id < MAX_THREADS);
        self.table[thread_id]
            .phase_finished
            .store(phase, Ordering::Release);

        // Check if all other threads have finished
        for (i, entry) in self.table.iter().enumerate() {
            let entry_phase = entry.phase_finished.load(Ordering::Acquire);
            let entry_epoch = entry.local_current_epoch.load(Ordering::Acquire);
            if entry_epoch != UNPROTECTED && entry_phase != phase && i != thread_id {
                return false;
            }
        }
        true
    }

    /// Check if this thread has finished the specified phase
    #[inline]
    pub fn has_thread_finished_phase(&self, thread_id: usize, phase: u32) -> bool {
        debug_assert!(thread_id < MAX_THREADS);
        self.table[thread_id].phase_finished.load(Ordering::Acquire) == phase
    }
}

impl Default for LightEpoch {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: LightEpoch uses only atomic operations for thread-safe access
unsafe impl Send for LightEpoch {}
unsafe impl Sync for LightEpoch {}

/// RAII guard for epoch protection
pub struct EpochGuard {
    epoch: Arc<LightEpoch>,
    thread_id: usize,
}

impl EpochGuard {
    /// Create a new epoch guard
    pub fn new(epoch: Arc<LightEpoch>, thread_id: usize) -> Self {
        epoch.reentrant_protect(thread_id);
        Self { epoch, thread_id }
    }

    /// Get the thread ID
    pub fn thread_id(&self) -> usize {
        self.thread_id
    }
}

impl Drop for EpochGuard {
    fn drop(&mut self) {
        self.epoch.reentrant_unprotect(self.thread_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_basic_protection() {
        let epoch = LightEpoch::new();

        assert!(!epoch.is_protected(0));

        let e = epoch.protect(0);
        assert!(epoch.is_protected(0));
        assert_eq!(e, 1);

        epoch.unprotect(0);
        assert!(!epoch.is_protected(0));
    }

    #[test]
    fn test_bump_epoch() {
        let epoch = LightEpoch::new();

        assert_eq!(epoch.current_epoch.load(Ordering::Relaxed), 1);

        let new_epoch = epoch.bump_current_epoch();
        assert_eq!(new_epoch, 2);
        assert_eq!(epoch.current_epoch.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_safe_to_reclaim() {
        let epoch = LightEpoch::new();

        // No threads protected - all epochs should be safe
        epoch.compute_new_safe_to_reclaim_epoch(10);
        assert!(epoch.is_safe_to_reclaim(9));

        // Protect thread 0 at epoch 5
        epoch.current_epoch.store(5, Ordering::Relaxed);
        epoch.protect(0);

        // Compute safe epoch
        epoch.compute_new_safe_to_reclaim_epoch(10);
        assert!(epoch.is_safe_to_reclaim(4));
        assert!(!epoch.is_safe_to_reclaim(5));

        epoch.unprotect(0);
    }

    #[test]
    fn test_epoch_with_action() {
        use std::sync::atomic::AtomicBool;

        let epoch = Arc::new(LightEpoch::new());
        let executed = Arc::new(AtomicBool::new(false));

        let executed_clone = executed.clone();
        epoch.bump_current_epoch_with_action(move || {
            executed_clone.store(true, Ordering::Release);
        });

        // Trigger drain by computing safe epoch with no protected threads
        epoch.compute_new_safe_to_reclaim_epoch(100);
        epoch.drain(100);

        assert!(executed.load(Ordering::Acquire));
    }

    #[test]
    fn test_epoch_guard() {
        let epoch = Arc::new(LightEpoch::new());

        {
            let _guard = EpochGuard::new(epoch.clone(), 0);
            assert!(epoch.is_protected(0));
        }

        assert!(!epoch.is_protected(0));
    }

    #[test]
    fn test_reentrant_protection() {
        let epoch = LightEpoch::new();

        // First protect
        epoch.reentrant_protect(0);
        assert!(epoch.is_protected(0));

        // Nested protect keeps the thread protected until the last unprotect.
        epoch.reentrant_protect(0);
        assert!(epoch.is_protected(0));

        epoch.reentrant_unprotect(0);
        assert!(epoch.is_protected(0));

        epoch.reentrant_unprotect(0);
        assert!(!epoch.is_protected(0));
    }

    #[test]
    fn test_thread_id_reuse_changes_generation() {
        use std::collections::{HashMap, HashSet};

        let mut generations_by_id: HashMap<usize, HashSet<u64>> = HashMap::new();

        for _ in 0..(MAX_THREADS * 2) {
            let (id, tag) = std::thread::spawn(|| {
                let id = get_thread_id().unwrap();
                let tag = get_thread_tag();
                (id, tag)
            })
            .join()
            .unwrap();

            assert!(id < MAX_THREADS);
            generations_by_id.entry(id).or_default().insert(tag);
        }

        assert!(generations_by_id.values().any(|tags| tags.len() >= 2));
    }

    #[test]
    fn test_thread_id_does_not_exhaust_under_sequential_churn() {
        // This would previously panic after MAX_THREADS spawns due to monotonically increasing IDs.
        for _ in 0..(MAX_THREADS * 2) {
            std::thread::spawn(|| {
                let id = get_thread_id().unwrap();
                assert!(id < MAX_THREADS);
            })
            .join()
            .unwrap();
        }
    }
}
