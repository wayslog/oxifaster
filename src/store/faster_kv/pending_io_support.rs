use std::ptr;

use crate::device::StorageDevice;
use crate::epoch::current_thread_tag_for;
use crate::record::{Key, Record, RecordInfo, Value};

use super::{DiskReadResult, FasterKv, PendingIoKey};

impl<K, V, D> FasterKv<K, V, D>
where
    K: Key,
    V: Value,
    D: StorageDevice,
{
    /// Drive and process background I/O completions (global).
    fn process_pending_io_completions(&self) {
        let completions = self.pending_io.drain_completions();
        if completions.is_empty() {
            return;
        }

        let mut completed = self.pending_io_completed.lock();

        for c in completions {
            match c {
                crate::store::pending_io::IoCompletion::ReadBytesDone {
                    thread_id,
                    thread_tag,
                    address,
                    result,
                } => {
                    if let Ok(bytes) = result {
                        if let Some(parsed) = self.parse_disk_record(&bytes) {
                            self.disk_read_results
                                .lock()
                                .insert(address.control(), parsed);
                        }
                    }

                    if current_thread_tag_for(thread_id) != thread_tag {
                        continue;
                    }

                    *completed
                        .entry(PendingIoKey {
                            thread_id,
                            thread_tag,
                        })
                        .or_insert(0) += 1;
                }
            }
        }
    }

    /// Consume and return the number of completed I/Os for the given thread.
    pub(crate) fn take_completed_io_for_thread(&self, thread_id: usize, thread_tag: u64) -> u32 {
        self.process_pending_io_completions();
        self.pending_io_completed
            .lock()
            .remove(&PendingIoKey {
                thread_id,
                thread_tag,
            })
            .unwrap_or(0)
    }

    /// Parse key/value from disk-read record bytes (producing an owned result).
    ///
    /// # Safety Note
    ///
    /// This method is only safe for POD (Plain Old Data) types. For types that contain pointers
    /// (e.g. `String`, `Vec`), bytes read from disk would contain invalid pointers, and
    /// interpreting them directly would be undefined behavior.
    ///
    /// Therefore, this method checks at runtime whether `K` and `V` require drop:
    /// - If they do, it returns `None` and disables disk parsing for this store.
    /// - Only POD types (e.g. `u64`, `i64`, `[u8; N]`) are actually parsed.
    fn parse_disk_record(&self, bytes: &[u8]) -> Option<DiskReadResult<K, V>> {
        // Safety check: only POD types can be safely reconstructed from raw bytes.
        if std::mem::needs_drop::<K>() || std::mem::needs_drop::<V>() {
            // Non-POD types are not supported; the caller will keep returning `Pending`
            // because the cache will never be filled (expected behavior).
            return None;
        }

        let min_len = Record::<K, V>::disk_size() as usize;
        if bytes.len() < min_len {
            return None;
        }

        let header_control = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
        let header = RecordInfo::from_control(header_control);

        let key_offset = Record::<K, V>::key_offset();
        let value_offset = Record::<K, V>::value_offset();

        // SAFETY:
        // 1. `needs_drop` checks ensure K and V are POD (no internal pointers)
        // 2. bytes come from a read of `Record::<K,V>::disk_size()` and the length is checked
        // 3. offsets are derived from `Record` layout
        unsafe {
            let key_ptr = bytes.as_ptr().add(key_offset) as *const K;
            let value_ptr = bytes.as_ptr().add(value_offset) as *const V;

            // For POD types, a bitwise copy is sufficient.
            let key: K = ptr::read_unaligned(key_ptr);

            let value = if header.is_tombstone() {
                None
            } else {
                let value: V = ptr::read_unaligned(value_ptr);
                Some(value)
            };

            Some(DiskReadResult { key, value })
        }
    }
}
