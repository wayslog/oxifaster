use std::io;
use std::sync::Arc;

use crate::address::Address;
use crate::checkpoint::LogMetadata;
use crate::delta_log::{DeltaLog, DeltaLogEntry, DeltaLogEntryType};
use crate::device::StorageDevice;

use super::PersistentMemoryMalloc;

impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    /// Flush delta records to the delta log for incremental checkpoint.
    pub fn flush_delta_to_device<DeltaDevice: StorageDevice>(
        &self,
        start_address: Address,
        end_address: Address,
        prev_end_address: Address,
        _version: u32,
        delta_log: &DeltaLog<DeltaDevice>,
    ) -> io::Result<u64> {
        let actual_start = if start_address < prev_end_address {
            prev_end_address
        } else {
            start_address
        };

        if actual_start >= end_address {
            return Ok(0);
        }

        delta_log.init_for_writes();

        let mut count = 0u64;
        let page_size = self.config.page_size;

        let start_page = actual_start.page();
        let end_page = end_address.page();

        for page in start_page..=end_page {
            if let Some(page_data) = self.pages.get_page(page) {
                let write_start = if page == start_page {
                    actual_start.offset() as usize
                } else {
                    0
                };

                let write_end = if page == end_page {
                    end_address.offset() as usize
                } else {
                    page_size
                };

                if write_start >= write_end {
                    continue;
                }

                let data_len = write_end - write_start;
                let entry_size = 8 + 4 + 4 + data_len;

                let mut entry_data = Vec::with_capacity(entry_size);
                entry_data.extend_from_slice(&(page as u64).to_le_bytes());
                entry_data.extend_from_slice(&(write_start as u32).to_le_bytes());
                entry_data.extend_from_slice(&(data_len as u32).to_le_bytes());
                entry_data.extend_from_slice(&page_data[write_start..write_end]);

                let entry = DeltaLogEntry::delta(entry_data);
                delta_log.write_entry(&entry)?;
                count += 1;
            }
        }

        delta_log.flush()?;

        Ok(count)
    }

    /// Asynchronously flush delta records to the delta log.
    pub async fn flush_delta_to_device_async<DeltaDevice: StorageDevice>(
        &self,
        start_address: Address,
        end_address: Address,
        prev_end_address: Address,
        version: u32,
        delta_log: &DeltaLog<DeltaDevice>,
    ) -> io::Result<u64> {
        self.flush_delta_to_device(
            start_address,
            end_address,
            prev_end_address,
            version,
            delta_log,
        )
    }

    /// Apply delta records from a delta log during recovery.
    pub fn apply_delta_records<DeltaDevice: StorageDevice>(
        &mut self,
        delta_log: Arc<DeltaLog<DeltaDevice>>,
    ) -> io::Result<u64> {
        use crate::delta_log::DeltaLogIterator;

        delta_log.init_for_reads();

        let mut count = 0u64;
        let mut iter = DeltaLogIterator::all(delta_log);

        while let Some((_addr, entry)) = iter.get_next()? {
            if entry.entry_type() != Some(DeltaLogEntryType::Delta) {
                continue;
            }

            let payload = &entry.payload;
            if payload.len() < 16 {
                continue;
            }

            let page_num = u64::from_le_bytes(payload[0..8].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid page_num in delta log")
            })?) as u32;
            let offset = u32::from_le_bytes(payload[8..12].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid offset in delta log")
            })?) as usize;
            let length = u32::from_le_bytes(payload[12..16].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid length in delta log")
            })?) as usize;

            if payload.len() < 16 + length {
                continue;
            }

            let data = &payload[16..16 + length];

            if !self.pages.allocate_page(page_num, self.config.page_size) {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("Failed to allocate page {page_num} during delta recovery"),
                ));
            }

            if let Some(page_data) = self.pages.get_page_mut(page_num) {
                if offset + length <= page_data.len() {
                    page_data[offset..offset + length].copy_from_slice(data);
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Write incremental checkpoint metadata to the delta log.
    pub fn write_incremental_metadata<DeltaDevice: StorageDevice>(
        &self,
        delta_log: &DeltaLog<DeltaDevice>,
        metadata: &LogMetadata,
    ) -> io::Result<()> {
        let metadata_bytes = metadata.serialize_json()?;
        let entry = DeltaLogEntry::checkpoint_metadata(metadata_bytes);
        delta_log.write_entry(&entry)?;
        delta_log.flush()?;
        Ok(())
    }
}
