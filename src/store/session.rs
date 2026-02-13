//! Session management for FASTER
//!
//! This module provides session management for FasterKV operations.
//! Each thread should maintain its own session for optimal performance.
//!
//! ## Session Persistence
//!
//! Sessions support persistence through checkpoint/recovery:
//! - Each session has a unique GUID for identification
//! - A monotonically increasing serial number tracks operations
//! - Session state can be saved to and restored from checkpoints

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::address::Address;
use crate::checkpoint::SessionState;
use crate::codec::{KeyCodec, PersistKey, PersistValue};
use crate::device::StorageDevice;
use crate::epoch::{get_thread_tag, EpochGuard};
use crate::index::{AtomicHashBucketEntry, KeyHash};
use crate::status::Status;
use crate::store::{Action, Phase};
use crate::store::{FasterKv, RecordView};

use super::faster_kv::ChainReadOutcome;

/// Thread context for FASTER operations
///
/// Tracks per-thread state including the serial number for CPR (Concurrent Prefix Recovery).
/// The serial number is incremented with each operation and is used to determine
/// which operations have been persisted during recovery.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionContext {
    pub(crate) id: u64,
    pub(crate) version: u32,
    pub(crate) pending_ios: u32,
    pub(crate) retry_requests: u32,
}

const INDEX_ENTRY_CACHE_SIZE: usize = 64;

#[derive(Debug, Clone, Copy)]
struct IndexEntryCacheSlot {
    hash: u64,
    version: u8,
    atomic_entry: *const AtomicHashBucketEntry,
}

impl IndexEntryCacheSlot {
    const EMPTY: Self = Self {
        hash: 0,
        version: 0,
        atomic_entry: std::ptr::null(),
    };

    #[inline]
    const fn is_empty(self) -> bool {
        self.atomic_entry.is_null()
    }
}

impl ExecutionContext {
    fn new(id: u64, version: u32) -> Self {
        Self {
            id,
            version,
            pending_ios: 0,
            retry_requests: 0,
        }
    }
}

/// Thread context for FASTER operations.
///
/// Tracks per-thread state including the serial number for CPR (Concurrent Prefix Recovery).
/// The serial number is incremented with each successful mutating operation and is used to
/// validate recovery boundaries.
#[derive(Debug)]
pub struct ThreadContext {
    /// Thread ID
    pub thread_id: usize,
    /// Current version for CPR
    pub version: u32,
    /// Monotonically increasing serial number for this session
    /// Used for Concurrent Prefix Recovery (CPR) to track operation ordering
    pub serial_num: u64,
    /// Number of pending operations
    pub pending_count: u32,
    /// Retry counter
    pub retry_count: u32,

    // === CPR internal state ===
    pub(crate) current: ExecutionContext,
    pub(crate) prev: ExecutionContext,
    next_ctx_id: u64,
    /// Last CPR state (packed as: action(8) | phase(8) | version(32)).
    pub(crate) last_cpr_state: u64,
    /// Last on-disk address observed by `read()` when returning `Status::Pending`.
    pub(crate) last_pending_read: Option<Address>,
    index_entry_cache_store: usize,
    index_entry_cache: [IndexEntryCacheSlot; INDEX_ENTRY_CACHE_SIZE],
}

impl ThreadContext {
    /// Create a new thread context
    pub fn new(thread_id: usize) -> Self {
        let current = ExecutionContext::new(1, 0);
        let prev = ExecutionContext::new(0, 0);
        Self {
            thread_id,
            version: current.version,
            serial_num: 0,
            pending_count: current.pending_ios,
            retry_count: current.retry_requests,
            current,
            prev,
            next_ctx_id: 2,
            last_cpr_state: 0,
            last_pending_read: None,
            index_entry_cache_store: 0,
            index_entry_cache: [IndexEntryCacheSlot::EMPTY; INDEX_ENTRY_CACHE_SIZE],
        }
    }

    #[inline]
    fn index_cache_slot(hash: KeyHash) -> usize {
        (hash.hash() as usize) & (INDEX_ENTRY_CACHE_SIZE - 1)
    }

    #[inline]
    fn clear_index_entry_cache(&mut self) {
        self.index_entry_cache = [IndexEntryCacheSlot::EMPTY; INDEX_ENTRY_CACHE_SIZE];
    }

    #[inline]
    pub(crate) fn prepare_index_entry_cache_for_store(&mut self, store_id: usize) {
        if self.index_entry_cache_store != store_id {
            self.index_entry_cache_store = store_id;
            self.clear_index_entry_cache();
        }
    }

    #[inline]
    pub(crate) fn get_cached_index_entry(
        &self,
        hash: KeyHash,
        version: u8,
    ) -> Option<*const AtomicHashBucketEntry> {
        let slot = self.index_entry_cache[Self::index_cache_slot(hash)];
        if slot.is_empty() || slot.hash != hash.hash() || slot.version != version {
            return None;
        }
        Some(slot.atomic_entry)
    }

    #[inline]
    pub(crate) fn put_cached_index_entry(
        &mut self,
        hash: KeyHash,
        version: u8,
        atomic_entry: *const AtomicHashBucketEntry,
    ) {
        self.index_entry_cache[Self::index_cache_slot(hash)] = IndexEntryCacheSlot {
            hash: hash.hash(),
            version,
            atomic_entry,
        };
    }

    #[inline]
    pub(crate) fn invalidate_cached_index_entry(&mut self, hash: KeyHash) {
        let slot = Self::index_cache_slot(hash);
        let cached = self.index_entry_cache[slot];
        if !cached.is_empty() && cached.hash == hash.hash() {
            self.index_entry_cache[slot] = IndexEntryCacheSlot::EMPTY;
        }
    }

    /// Get the current serial number
    #[inline]
    pub fn serial_num(&self) -> u64 {
        self.serial_num
    }

    /// Increment and return the new serial number
    /// Called after each successful operation for CPR tracking
    #[inline]
    pub fn increment_serial(&mut self) -> u64 {
        self.serial_num += 1;
        self.serial_num
    }

    /// Set the serial number (used during recovery)
    #[inline]
    pub fn set_serial_num(&mut self, serial_num: u64) {
        self.serial_num = serial_num;
    }

    #[inline]
    pub(crate) fn current_ctx_id(&self) -> u64 {
        self.current.id
    }

    #[inline]
    pub(crate) fn prev_ctx_id(&self) -> u64 {
        self.prev.id
    }

    #[inline]
    pub(crate) fn on_pending_io_submitted(&mut self) {
        self.current.pending_ios = self.current.pending_ios.saturating_add(1);
        self.pending_count = self.current.pending_ios;
    }

    #[inline]
    pub(crate) fn on_pending_io_submitted_for_ctx(&mut self, ctx_id: u64) {
        if self.current.id == ctx_id {
            self.on_pending_io_submitted();
            return;
        }
        if self.prev.id == ctx_id {
            self.prev.pending_ios = self.prev.pending_ios.saturating_add(1);
        }
    }

    #[inline]
    pub(crate) fn on_pending_io_completed(&mut self, ctx_id: u64, n: u32) {
        if self.current.id == ctx_id {
            self.current.pending_ios = self.current.pending_ios.saturating_sub(n);
            self.pending_count = self.current.pending_ios;
            return;
        }
        if self.prev.id == ctx_id {
            self.prev.pending_ios = self.prev.pending_ios.saturating_sub(n);
        }
    }

    #[inline]
    pub(crate) fn on_retry_requests_ready(&mut self, ctx_id: u64, n: u32) {
        if self.current.id == ctx_id {
            self.current.retry_requests = self.current.retry_requests.saturating_add(n);
            self.retry_count = self.current.retry_requests;
            return;
        }
        if self.prev.id == ctx_id {
            self.prev.retry_requests = self.prev.retry_requests.saturating_add(n);
        }
    }

    #[inline]
    pub(crate) fn on_retry_request_consumed(&mut self, ctx_id: u64) {
        if self.current.id == ctx_id {
            self.current.retry_requests = self.current.retry_requests.saturating_sub(1);
            self.retry_count = self.current.retry_requests;
            return;
        }
        if self.prev.id == ctx_id {
            self.prev.retry_requests = self.prev.retry_requests.saturating_sub(1);
        }
    }

    #[inline]
    pub(crate) fn prev_ctx_is_drained(&self) -> bool {
        self.prev.pending_ios == 0 && self.prev.retry_requests == 0
    }

    #[inline]
    pub(crate) fn total_pending_ios(&self) -> u32 {
        self.current
            .pending_ios
            .saturating_add(self.prev.pending_ios)
    }

    #[inline]
    pub(crate) fn total_retry_requests(&self) -> u32 {
        self.current
            .retry_requests
            .saturating_add(self.prev.retry_requests)
    }

    pub(crate) fn cpr_state_sig(action: Action, phase: Phase, version: u32) -> u64 {
        (action as u64) | ((phase as u64) << 8) | ((version as u64) << 16)
    }

    pub(crate) fn swap_contexts_for_checkpoint(&mut self, new_version: u32) {
        std::mem::swap(&mut self.current, &mut self.prev);
        let id = self.next_ctx_id;
        self.next_ctx_id = self.next_ctx_id.saturating_add(1);
        self.current = ExecutionContext::new(id, new_version);
        self.version = self.current.version;
        self.pending_count = self.current.pending_ios;
        self.retry_count = self.current.retry_requests;
        self.clear_index_entry_cache();
    }
}

impl Clone for ThreadContext {
    fn clone(&self) -> Self {
        Self {
            thread_id: self.thread_id,
            version: self.version,
            serial_num: self.serial_num,
            pending_count: self.pending_count,
            retry_count: self.retry_count,
            current: self.current.clone(),
            prev: self.prev.clone(),
            next_ctx_id: self.next_ctx_id,
            last_cpr_state: self.last_cpr_state,
            last_pending_read: self.last_pending_read,
            index_entry_cache_store: 0,
            index_entry_cache: [IndexEntryCacheSlot::EMPTY; INDEX_ENTRY_CACHE_SIZE],
        }
    }
}

/// Session for FASTER operations
///
/// A session encapsulates a thread's interaction with a FasterKV store.
/// Sessions are not thread-safe and should not be shared between threads.
///
/// ## Persistence
///
/// Each session has a unique GUID that persists across checkpoint/recovery.
/// The serial number tracks operations for Concurrent Prefix Recovery (CPR).
/// Use `to_session_state()` to capture the session state for checkpointing,
/// and `continue_from_state()` to restore after recovery.
pub struct Session<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Reference to the store
    store: Arc<FasterKv<K, V, D>>,
    /// Unique session identifier (GUID)
    guid: Uuid,
    /// Thread context
    ctx: ThreadContext,
    /// Whether the session is active
    active: bool,
    /// Pending read requests grouped by execution context id.
    pending_reads: HashMap<u64, Vec<PendingReadRequest<K>>>,
    /// Completed read results grouped by stable key hash.
    completed_reads: HashMap<u64, Vec<CompletedRead<K, V>>>,
}

#[derive(Debug, Clone)]
struct PendingReadRequest<K: PersistKey> {
    key: K,
    hash: u64,
    address: Address,
    ctx_id: u64,
    cancelled: bool,
    retry_tracked: bool,
}

#[derive(Debug, Clone)]
struct CompletedRead<K: PersistKey, V: PersistValue> {
    key: K,
    result: Result<Option<V>, Status>,
}

impl<K, V, D> Session<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    fn bump_serial_on_ok(&mut self, status: Status) -> Status {
        if status == Status::Ok {
            self.ctx.increment_serial();
        }
        status
    }

    /// Create a new session with a fresh GUID
    pub(crate) fn new(store: Arc<FasterKv<K, V, D>>, thread_id: usize) -> Self {
        Self {
            store,
            guid: Uuid::new_v4(),
            ctx: ThreadContext::new(thread_id),
            active: false,
            pending_reads: HashMap::new(),
            completed_reads: HashMap::new(),
        }
    }

    /// Create a session from a saved state (for recovery)
    ///
    /// This restores a session from a previously checkpointed state,
    /// preserving the GUID and serial number.
    pub(crate) fn from_state(
        store: Arc<FasterKv<K, V, D>>,
        thread_id: usize,
        state: &SessionState,
    ) -> Self {
        let mut ctx = ThreadContext::new(thread_id);
        ctx.set_serial_num(state.serial_num);
        Self {
            store,
            guid: state.guid,
            ctx,
            active: false,
            pending_reads: HashMap::new(),
            completed_reads: HashMap::new(),
        }
    }

    fn try_take_completed_read(&mut self, hash: u64, key: &K) -> Option<Result<Option<V>, Status>> {
        let entries = self.completed_reads.get_mut(&hash)?;
        let pos = entries.iter().position(|e| e.key == *key)?;
        let entry = entries.swap_remove(pos);
        if entries.is_empty() {
            self.completed_reads.remove(&hash);
        }
        Some(entry.result)
    }

    fn enqueue_pending_read(&mut self, ctx_id: u64, key: K, hash: u64, address: Address) {
        let requests = self.pending_reads.entry(ctx_id).or_default();

        // Avoid unbounded growth when a caller retries `read()` while the same request is still
        // pending (or the I/O is inflight). Without de-duplication, `retry_count` can grow without
        // bound, making `complete_pending(wait=true)` unable to drain.
        if let Some(existing) = requests
            .iter_mut()
            .find(|r| !r.cancelled && r.hash == hash && r.key == key)
        {
            // Keep the newest observed on-disk address so the request can make forward progress
            // even if the caller retries `read()` while the chain traversal advances.
            existing.address = address;
            return;
        }

        requests.push(PendingReadRequest {
            key,
            hash,
            address,
            ctx_id,
            cancelled: false,
            retry_tracked: true,
        });
        self.ctx.on_retry_requests_ready(ctx_id, 1);
    }

    fn cancel_one_pending_read(&mut self, hash: u64, key: &K) {
        let current_id = self.ctx.current_ctx_id();
        let prev_id = self.ctx.prev_ctx_id();

        for ctx_id in [current_id, prev_id] {
            if ctx_id == 0 {
                continue;
            }
            let Some(requests) = self.pending_reads.get_mut(&ctx_id) else {
                continue;
            };
            let Some(req) = requests
                .iter_mut()
                .find(|r| r.hash == hash && r.key == *key)
            else {
                continue;
            };
            if req.retry_tracked {
                self.ctx.on_retry_request_consumed(req.ctx_id);
                req.retry_tracked = false;
            }
            req.cancelled = true;
            break;
        }
    }

    fn drive_pending_reads_for_ctx(&mut self, ctx_id: u64, thread_tag: u64) {
        let head_address = self.store.head_address();
        let thread_id = self.ctx.thread_id;

        let Some(requests) = self.pending_reads.get_mut(&ctx_id) else {
            return;
        };

        let mut i = 0usize;
        while i < requests.len() {
            let address = requests[i].address;

            // Handle cancelled requests
            if requests[i].cancelled {
                if self.store.peek_disk_read_result(address).is_some() {
                    let _ = requests.swap_remove(i);
                } else {
                    i += 1;
                }
                continue;
            }

            // Check if disk read completed - need to extract result to avoid borrow conflicts
            let parsed = match self.store.peek_disk_read_result(address) {
                Some(p) => p,
                None => {
                    i += 1;
                    continue;
                }
            };

            match parsed {
                Err(status) => {
                    let req = requests.swap_remove(i);
                    self.completed_reads
                        .entry(req.hash)
                        .or_default()
                        .push(CompletedRead {
                            key: req.key,
                            result: Err(status),
                        });
                    if req.retry_tracked {
                        self.ctx.on_retry_request_consumed(req.ctx_id);
                    }
                }
                Ok(disk) => {
                    // Check if this is the key we're looking for
                    if disk.key.as_ref() == Some(&requests[i].key) {
                        let req = requests.swap_remove(i);
                        self.completed_reads
                            .entry(req.hash)
                            .or_default()
                            .push(CompletedRead {
                                key: req.key,
                                result: Ok(disk.value),
                            });
                        if req.retry_tracked {
                            self.ctx.on_retry_request_consumed(req.ctx_id);
                        }
                        continue;
                    }

                    // Key mismatch - need to follow the chain
                    let next = disk.previous_address;
                    if !next.is_valid() {
                        let req = requests.swap_remove(i);
                        self.completed_reads
                            .entry(req.hash)
                            .or_default()
                            .push(CompletedRead {
                                key: req.key,
                                result: Ok(None),
                            });
                        if req.retry_tracked {
                            self.ctx.on_retry_request_consumed(req.ctx_id);
                        }
                        continue;
                    }

                    // Continue searching the chain
                    if !next.in_readcache() && next < head_address {
                        // Next record is on disk
                        let submitted = self
                            .store
                            .submit_disk_read_for_context(thread_id, thread_tag, ctx_id, next);
                        if submitted {
                            self.ctx.on_pending_io_submitted_for_ctx(ctx_id);
                        }
                        requests[i].address = next;
                        i += 1;
                        continue;
                    }

                    // Try to read from memory
                    match self.store.read_chain_from_memory(&requests[i].key, next) {
                        Ok(ChainReadOutcome::Completed(value)) => {
                            let req = requests.swap_remove(i);
                            self.completed_reads
                                .entry(req.hash)
                                .or_default()
                                .push(CompletedRead {
                                    key: req.key,
                                    result: Ok(value),
                                });
                            if req.retry_tracked {
                                self.ctx.on_retry_request_consumed(req.ctx_id);
                            }
                        }
                        Ok(ChainReadOutcome::NeedDiskRead(disk_addr)) => {
                            let submitted = self.store.submit_disk_read_for_context(
                                thread_id, thread_tag, ctx_id, disk_addr,
                            );
                            if submitted {
                                self.ctx.on_pending_io_submitted_for_ctx(ctx_id);
                            }
                            requests[i].address = disk_addr;
                            i += 1;
                        }
                        Err(status) => {
                            let req = requests.swap_remove(i);
                            self.completed_reads
                                .entry(req.hash)
                                .or_default()
                                .push(CompletedRead {
                                    key: req.key,
                                    result: Err(status),
                                });
                            if req.retry_tracked {
                                self.ctx.on_retry_request_consumed(req.ctx_id);
                            }
                        }
                    }
                }
            }
        }

        if requests.is_empty() {
            self.pending_reads.remove(&ctx_id);
        }
    }

    /// Get the session GUID
    #[inline]
    pub fn guid(&self) -> Uuid {
        self.guid
    }

    /// Get the thread ID
    #[inline]
    pub fn thread_id(&self) -> usize {
        self.ctx.thread_id
    }

    /// Get the current version
    #[inline]
    pub fn version(&self) -> u32 {
        self.ctx.version
    }

    /// Get the current serial number
    #[inline]
    pub fn serial_num(&self) -> u64 {
        self.ctx.serial_num()
    }

    /// Convert to SessionState for checkpointing
    ///
    /// Returns a snapshot of the session's persistent state (GUID + serial number)
    /// that can be saved during checkpoint and restored during recovery.
    pub fn to_session_state(&self) -> SessionState {
        SessionState::new(self.guid, self.ctx.serial_num())
    }

    /// Continue session from a restored state
    ///
    /// Called after recovery to update the session's serial number
    /// to match the last checkpointed state.
    pub fn continue_from_state(&mut self, state: &SessionState) {
        if self.guid == state.guid {
            self.ctx.set_serial_num(state.serial_num);
        }
    }

    /// Start the session
    pub fn start(&mut self) {
        if !self.active {
            self.store.epoch().reentrant_protect(self.ctx.thread_id);
            self.active = true;
            self.store.register_thread(self.ctx.thread_id);
            // Register session when starting
            self.store.register_session(&self.to_session_state());
        }
    }

    /// End the session
    pub fn end(&mut self) {
        if self.active {
            // Update final session state before unregistering
            self.store.update_session(&self.to_session_state());
            self.store.epoch().reentrant_unprotect(self.ctx.thread_id);
            self.active = false;
            // Note: We don't unregister on end() to allow checkpoint to capture
            // sessions that have ended but whose operations should be persisted.
            // Sessions are only fully removed on drop.
        }
    }

    /// Check if the session is active
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Refresh the epoch protection
    ///
    /// This also updates the session state in the registry to keep
    /// checkpoint information current.
    pub fn refresh(&mut self) {
        if self.active {
            self.store.cpr_refresh(&mut self.ctx);
            self.store
                .epoch()
                .reentrant_protect_and_drain(self.ctx.thread_id);
            // Periodically update session state in registry
            self.store.update_session(&self.to_session_state());
        }
    }

    /// Read a value from the store
    ///
    /// Note: Read operations do NOT increment the serial number because they
    /// don't modify state or write to the log. The CPR serial number only tracks
    /// operations that are persisted and need to be recovered.
    pub fn read(&mut self, key: &K) -> Result<Option<V>, Status> {
        self.start();
        self.store.cpr_refresh(&mut self.ctx);

        // Fast path: skip completed/pending map lookups when no async I/O is outstanding.
        let has_pending = !self.completed_reads.is_empty() || !self.pending_reads.is_empty();

        if has_pending {
            let hash = <K as PersistKey>::Codec::hash(key)?;
            if let Some(result) = self.try_take_completed_read(hash, key) {
                return result;
            }

            match self.store.read_sync(&mut self.ctx, key) {
                Ok(value) => {
                    self.cancel_one_pending_read(hash, key);
                    Ok(value)
                }
                Err(Status::Pending) => {
                    if let Some(address) = self.ctx.last_pending_read.take() {
                        self.enqueue_pending_read(
                            self.ctx.current_ctx_id(),
                            key.clone(),
                            hash,
                            address,
                        );
                    }
                    Err(Status::Pending)
                }
                Err(status) => Err(status),
            }
        } else {
            // Common fast path: no pending I/O, skip hash + HashMap lookups entirely.
            match self.store.read_sync(&mut self.ctx, key) {
                Ok(value) => Ok(value),
                Err(Status::Pending) => {
                    if let Some(address) = self.ctx.last_pending_read.take() {
                        let hash = <K as PersistKey>::Codec::hash(key)?;
                        self.enqueue_pending_read(
                            self.ctx.current_ctx_id(),
                            key.clone(),
                            hash,
                            address,
                        );
                    }
                    Err(Status::Pending)
                }
                Err(status) => Err(status),
            }
        }
    }

    /// Read a record as a zero-copy view.
    ///
    /// The returned `RecordView` borrows from the hybrid log; the caller must hold `guard`
    /// for the entire duration of view usage.
    ///
    /// Note: This API only supports records that are still resident in the in-memory portion of
    /// the hybrid log. If the latest record for `key` is on disk, this returns `Status::NotSupported`.
    pub fn read_view<'g>(
        &'g mut self,
        guard: &'g EpochGuard,
        key: &K,
    ) -> Result<Option<RecordView<'g>>, Status> {
        self.start();
        self.store.cpr_refresh(&mut self.ctx);
        if guard.thread_id() != self.ctx.thread_id {
            return Err(Status::InvalidArgument);
        }
        self.store.read_view_sync(&mut self.ctx, guard, key)
    }

    /// Convenience helper to create an epoch guard for this session's thread.
    pub fn pin(&self) -> EpochGuard {
        EpochGuard::new(self.store.epoch_arc(), self.ctx.thread_id)
    }

    /// Upsert a key-value pair
    pub fn upsert(&mut self, key: K, value: V) -> Status {
        self.start();
        self.store.cpr_refresh(&mut self.ctx);

        let status = self.store.upsert_sync(&mut self.ctx, key, value);
        self.bump_serial_on_ok(status)
    }

    /// Delete a key
    ///
    /// Only increments the serial number when the delete actually modifies state
    /// (i.e., when the key exists and is deleted). Deleting a non-existent key
    /// doesn't write to the log and shouldn't affect CPR recovery.
    pub fn delete(&mut self, key: &K) -> Status {
        self.start();
        self.store.cpr_refresh(&mut self.ctx);

        let status = self.store.delete_sync(&mut self.ctx, key);
        self.bump_serial_on_ok(status)
    }

    /// Perform a read-modify-write operation
    pub fn rmw<F>(&mut self, key: K, modifier: F) -> Status
    where
        F: FnMut(&mut V) -> bool,
    {
        self.start();
        self.store.cpr_refresh(&mut self.ctx);

        let status = self.store.rmw_sync(&mut self.ctx, key, modifier);
        self.bump_serial_on_ok(status)
    }

    /// Conditionally insert a key-value pair
    ///
    /// Inserts the key-value pair only if the key does not already exist in the store.
    /// A key that has been deleted (i.e. whose latest record is a tombstone) is treated as absent.
    /// Returns `Status::Ok` if the insertion succeeded, `Status::Aborted` if the key
    /// already exists.
    ///
    /// This corresponds to FASTER C++ ConditionalInsert operation.
    pub fn conditional_insert(&mut self, key: K, value: V) -> Status {
        self.start();
        self.store.cpr_refresh(&mut self.ctx);

        let status = self
            .store
            .conditional_insert_sync(&mut self.ctx, key, value);
        self.bump_serial_on_ok(status)
    }

    /// Complete all pending operations
    ///
    /// # Arguments
    ///
    /// * `wait` - If true, blocks until all pending operations complete (with timeout protection)
    ///
    /// # Returns
    ///
    /// Returns `true` if all pending operations have completed, `false` otherwise.
    ///
    /// # Timeout
    ///
    /// When `wait=true`, this method will timeout after 30 seconds to prevent infinite blocking
    /// in case of I/O worker thread failure. Use `complete_pending_with_timeout` for custom timeout.
    pub fn complete_pending(&mut self, wait: bool) -> bool {
        self.complete_pending_with_timeout(wait, Duration::from_secs(30))
    }

    /// Complete all pending operations with custom timeout
    ///
    /// # Arguments
    ///
    /// * `wait` - If true, blocks until all pending operations complete or timeout
    /// * `timeout` - Maximum duration to wait when `wait=true`
    ///
    /// # Returns
    ///
    /// Returns `true` if all pending operations have completed, `false` if timeout or still pending.
    pub fn complete_pending_with_timeout(&mut self, wait: bool, timeout: Duration) -> bool {
        let start = Instant::now();

        loop {
            let thread_tag = get_thread_tag();

            let current_id = self.ctx.current_ctx_id();
            let completed_current = self.store.take_completed_io_for_context(
                self.ctx.thread_id,
                thread_tag,
                current_id,
            );
            if completed_current > 0 {
                self.ctx
                    .on_pending_io_completed(current_id, completed_current);
            }

            let prev_id = self.ctx.prev_ctx_id();
            let completed_prev =
                self.store
                    .take_completed_io_for_context(self.ctx.thread_id, thread_tag, prev_id);
            if completed_prev > 0 {
                self.ctx.on_pending_io_completed(prev_id, completed_prev);
            }

            self.drive_pending_reads_for_ctx(current_id, thread_tag);
            if prev_id != current_id {
                self.drive_pending_reads_for_ctx(prev_id, thread_tag);
            }

            if !wait || (self.ctx.total_pending_ios() == 0 && self.ctx.total_retry_requests() == 0)
            {
                break;
            }

            // Timeout guard: prevent infinite blocking if the I/O worker thread fails.
            if start.elapsed() >= timeout {
                break;
            }

            // wait=true: yield the thread while keeping epoch protection fresh.
            if self.active {
                self.refresh();
            }
            std::thread::yield_now();
        }

        self.ctx.total_pending_ios() == 0 && self.ctx.total_retry_requests() == 0
    }

    /// Get a reference to the thread context
    pub fn context(&self) -> &ThreadContext {
        &self.ctx
    }

    /// Get a mutable reference to the thread context
    pub fn context_mut(&mut self) -> &mut ThreadContext {
        &mut self.ctx
    }
}

impl<K, V, D> Drop for Session<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    fn drop(&mut self) {
        self.end();
        // Unregister from the session registry on drop
        self.store.unregister_session(self.guid);
        self.store.unregister_thread(self.ctx.thread_id);
    }
}

/// Session builder for configuring session options
pub struct SessionBuilder<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    store: Arc<FasterKv<K, V, D>>,
    thread_id: Option<usize>,
    session_state: Option<SessionState>,
}

impl<K, V, D> SessionBuilder<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    /// Create a new session builder
    pub fn new(store: Arc<FasterKv<K, V, D>>) -> Self {
        Self {
            store,
            thread_id: None,
            session_state: None,
        }
    }

    /// Set the thread ID
    pub fn thread_id(mut self, id: usize) -> Self {
        self.thread_id = Some(id);
        self
    }

    /// Restore from a saved session state (for recovery)
    ///
    /// When building with a session state, the session will use
    /// the same GUID and continue from the checkpointed serial number.
    pub fn from_state(mut self, state: SessionState) -> Self {
        self.session_state = Some(state);
        self
    }

    /// Build the session
    pub fn build(self) -> Session<K, V, D> {
        let thread_id = self.thread_id.unwrap_or(0);

        match self.session_state {
            Some(state) => Session::from_state(self.store, thread_id, &state),
            None => Session::new(self.store, thread_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_context() {
        let ctx = ThreadContext::new(5);
        assert_eq!(ctx.thread_id, 5);
        assert_eq!(ctx.version, 0);
        assert_eq!(ctx.serial_num, 0);
        assert_eq!(ctx.pending_count, 0);
    }

    #[test]
    fn test_thread_context_serial_num() {
        let mut ctx = ThreadContext::new(0);
        assert_eq!(ctx.serial_num(), 0);

        let new_serial = ctx.increment_serial();
        assert_eq!(new_serial, 1);
        assert_eq!(ctx.serial_num(), 1);

        ctx.set_serial_num(100);
        assert_eq!(ctx.serial_num(), 100);
    }

    #[test]
    fn test_session_state_conversion() {
        let guid = Uuid::new_v4();
        let state = SessionState::new(guid, 42);

        assert_eq!(state.guid, guid);
        assert_eq!(state.serial_num, 42);
    }
}
