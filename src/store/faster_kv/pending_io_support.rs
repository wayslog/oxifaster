use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::device::StorageDevice;
use crate::epoch::current_thread_tag_for;
use crate::record::RecordInfo;
use crate::status::Status;

use super::record_format;
use super::{DiskReadResult, FasterKv, PendingIoKey};

impl<K, V, D> FasterKv<K, V, D>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    const RECORD_INFO_SIZE: usize = std::mem::size_of::<RecordInfo>();
    const VARLEN_LENGTHS_SIZE: usize = 2 * std::mem::size_of::<u32>();
    const VARLEN_HEADER_SIZE: usize = Self::RECORD_INFO_SIZE + Self::VARLEN_LENGTHS_SIZE;

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
                    let parsed = match result {
                        Ok(bytes) => self.parse_disk_record(&bytes),
                        Err(_) => Err(Status::IoError),
                    };
                    self.disk_read_results
                        .lock()
                        .insert(address.control(), parsed);

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
    fn parse_disk_record(&self, bytes: &[u8]) -> Result<DiskReadResult<K, V>, Status> {
        if bytes.len() < Self::RECORD_INFO_SIZE {
            return Err(Status::Corruption);
        }

        let header_control =
            u64::from_le_bytes(bytes[0..Self::RECORD_INFO_SIZE].try_into().unwrap());
        let header = RecordInfo::from_control(header_control);
        let previous_address = header.previous_address();

        // Skip invalid records to avoid returning partially written/corrupt data.
        if header.is_invalid() {
            return Ok(DiskReadResult {
                key: None,
                value: None,
                previous_address,
            });
        }

        if record_format::is_fixed_record::<K, V>() {
            let disk_len = record_format::fixed_disk_len::<K, V>();
            if bytes.len() < disk_len {
                return Err(Status::Corruption);
            }

            let key_len = <K as PersistKey>::Codec::FIXED_LEN;
            let value_len = <V as PersistValue>::Codec::FIXED_LEN;

            let key_bytes = &bytes[Self::RECORD_INFO_SIZE..Self::RECORD_INFO_SIZE + key_len];
            let key = <K as PersistKey>::Codec::decode(key_bytes)?;

            let value = if header.is_tombstone() {
                None
            } else {
                let value_start = Self::RECORD_INFO_SIZE + key_len;
                let value_bytes = &bytes[value_start..value_start + value_len];
                Some(<V as PersistValue>::Codec::decode(value_bytes)?)
            };

            Ok(DiskReadResult {
                key: Some(key),
                value,
                previous_address,
            })
        } else {
            if bytes.len() < Self::VARLEN_HEADER_SIZE {
                return Err(Status::Corruption);
            }
            let key_len = u32::from_le_bytes(
                bytes[Self::RECORD_INFO_SIZE..Self::RECORD_INFO_SIZE + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let value_len = u32::from_le_bytes(
                bytes
                    [Self::RECORD_INFO_SIZE + std::mem::size_of::<u32>()..Self::VARLEN_HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let total = Self::VARLEN_HEADER_SIZE
                .checked_add(key_len)
                .and_then(|n| n.checked_add(value_len))
                .ok_or(Status::ResourceExhausted)?;
            if bytes.len() < total {
                return Err(Status::Corruption);
            }

            let key_bytes = &bytes[Self::VARLEN_HEADER_SIZE..Self::VARLEN_HEADER_SIZE + key_len];
            let key = <K as PersistKey>::Codec::decode(key_bytes)?;

            let value = if header.is_tombstone() {
                None
            } else {
                let value_start = Self::VARLEN_HEADER_SIZE + key_len;
                let value_bytes = &bytes[value_start..value_start + value_len];
                Some(<V as PersistValue>::Codec::decode(value_bytes)?)
            };

            Ok(DiskReadResult {
                key: Some(key),
                value,
                previous_address,
            })
        }
    }
}
