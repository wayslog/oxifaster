use std::io;

use crate::address::Address;
use crate::allocator::page_allocator::FlushStatus;
use crate::device::StorageDevice;

use super::PersistentMemoryMalloc;

impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    /// Flush all pages up to (but not including) the specified address.
    ///
    /// This writes in-memory pages to the storage device. Callers that need durable
    /// persistence must invoke `StorageDevice::flush()` separately.
    pub fn flush_until(&self, until_address: Address) -> io::Result<()> {
        let current_flushed = self.get_flushed_until_address();

        let Some(pages_to_flush) = Self::pages_to_flush(current_flushed, until_address) else {
            return Ok(());
        };
        let start_page = *pages_to_flush.start();
        let end_page = *pages_to_flush.end();
        if tracing::enabled!(tracing::Level::DEBUG) {
            let pages = end_page.saturating_sub(start_page) + 1;
            tracing::debug!(
                from = %current_flushed,
                until = %until_address,
                pages,
                "hybrid log flush start"
            );
        }

        // Create a dedicated runtime for synchronous I/O.
        //
        // Do not use Handle::try_current() + block_on(), because:
        // - Handle::block_on() panics when called from a thread already running a runtime
        // - This can happen when flush_until/flush_and_shift_head is invoked from async code
        //
        // Creating a dedicated runtime is safe and predictable here. This is a synchronous,
        // blocking flush path, so the overhead is acceptable.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let write_page_sync = |page: u32, data: &[u8]| -> io::Result<()> {
            let expected_len = data.len();
            let page_offset = Address::new(page, 0).control();

            let written = rt.block_on(async { self.device.write(page_offset, data).await })?;
            if written != expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "partial write to page {page}: expected {expected_len} bytes, wrote {written} bytes"
                    ),
                ));
            }

            Ok(())
        };

        for page in pages_to_flush {
            match self.flush_shared.page_flush_status(page) {
                FlushStatus::Flushed => {}
                FlushStatus::Dirty => {
                    if !self.flush_shared.try_mark_flushing(page) {
                        while self.flush_shared.page_flush_status(page) != FlushStatus::Flushed {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        self.flush_shared.advance_safe_read_only(until_address);
                        continue;
                    }

                    if let Some(page_data) = self.pages.get_page(page) {
                        if let Err(e) = write_page_sync(page, page_data) {
                            self.flush_shared.mark_page_dirty_after_error(page);
                            if tracing::enabled!(tracing::Level::WARN) {
                                tracing::warn!(page, error = %e, "hybrid log flush failed");
                            }
                            return Err(e);
                        }
                        self.flush_shared.mark_page_flushed(page);
                    } else {
                        self.flush_shared.mark_page_dirty_after_error(page);
                        if tracing::enabled!(tracing::Level::WARN) {
                            tracing::warn!(page, "hybrid log flush failed: missing page");
                        }
                        return Err(io::Error::other(format!(
                            "missing in-memory page {page} during flush"
                        )));
                    }
                }
                FlushStatus::Flushing => {
                    while self.flush_shared.page_flush_status(page) != FlushStatus::Flushed {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }

            self.flush_shared.advance_safe_read_only(until_address);
        }

        self.flush_shared.advance_safe_read_only(until_address);

        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(until = %until_address, "hybrid log flush completed");
        }
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_to_disk(&self) -> io::Result<()> {
        let tail = self.get_tail_address();
        self.flush_until(tail)
    }

    fn pages_to_flush(
        current_flushed: Address,
        until_address: Address,
    ) -> Option<std::ops::RangeInclusive<u32>> {
        if until_address <= current_flushed {
            return None;
        }

        // flush_until writes full pages. To preserve the semantics of "up to but not including",
        // include the last page only when until_address is inside that page (offset > 0).
        let begin_page = current_flushed.page();
        let mut last_page = until_address.page();
        if until_address.offset() == 0 {
            if last_page == 0 {
                return None;
            }
            last_page -= 1;
        }

        if last_page < begin_page {
            return None;
        }

        Some(begin_page..=last_page)
    }
}
