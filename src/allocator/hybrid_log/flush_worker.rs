use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex;
use tracing::warn;

use crate::address::{Address, AtomicAddress};
use crate::allocator::page_allocator::{FlushStatus, PageInfo};
use crate::device::StorageDevice;

pub(crate) struct FlushShared<D: StorageDevice> {
    device: Arc<D>,
    page_info: Arc<Vec<PageInfo>>,
    buffer_size: u32,
    read_only_address: Arc<AtomicAddress>,
    safe_read_only_address: Arc<AtomicAddress>,
    flushed_until_address: Arc<AtomicAddress>,
    pending_flushes: Arc<AtomicU64>,
}

impl<D: StorageDevice> FlushShared<D> {
    pub(crate) fn new(
        device: Arc<D>,
        page_info: Arc<Vec<PageInfo>>,
        buffer_size: u32,
        read_only_address: Arc<AtomicAddress>,
        safe_read_only_address: Arc<AtomicAddress>,
        flushed_until_address: Arc<AtomicAddress>,
        pending_flushes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            device,
            page_info,
            buffer_size,
            read_only_address,
            safe_read_only_address,
            flushed_until_address,
            pending_flushes,
        }
    }

    pub(crate) fn device(&self) -> &Arc<D> {
        &self.device
    }

    pub(crate) fn increment_pending(&self) {
        self.pending_flushes.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn decrement_pending(&self) {
        self.pending_flushes.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn page_flush_status(&self, page: u32) -> FlushStatus {
        let info = self.page_info_for(page);
        info.status.load(Ordering::Acquire).flush()
    }

    pub(crate) fn mark_page_dirty(&self, page: u32) {
        let info = self.page_info_for(page);
        loop {
            let current = info.status.load(Ordering::Acquire);
            match current.flush() {
                FlushStatus::Flushed => {
                    let mut next = current;
                    next.flush_status = FlushStatus::Dirty as u8;
                    if info
                        .status
                        .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        break;
                    }
                }
                FlushStatus::Dirty | FlushStatus::Flushing => break,
            }
        }
    }

    pub(crate) fn try_mark_flushing(&self, page: u32) -> bool {
        let info = self.page_info_for(page);
        loop {
            let current = info.status.load(Ordering::Acquire);
            match current.flush() {
                FlushStatus::Dirty => {
                    let mut next = current;
                    next.flush_status = FlushStatus::Flushing as u8;
                    if info
                        .status
                        .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return true;
                    }
                }
                FlushStatus::Flushed | FlushStatus::Flushing => return false,
            }
        }
    }

    pub(crate) fn mark_page_flushed(&self, page: u32) {
        let info = self.page_info_for(page);
        Self::set_flush_status(info, FlushStatus::Flushed);
    }

    pub(crate) fn mark_page_dirty_after_error(&self, page: u32) {
        let info = self.page_info_for(page);
        Self::set_flush_status(info, FlushStatus::Dirty);
    }

    pub(crate) fn read_only_address(&self) -> Address {
        self.read_only_address.load(Ordering::Acquire)
    }

    pub(crate) fn advance_safe_read_only(&self, limit: Address) {
        loop {
            let current = self.safe_read_only_address.load(Ordering::Acquire);
            if current >= limit {
                break;
            }

            let page = current.page();
            if self.page_flush_status(page) != FlushStatus::Flushed {
                break;
            }

            let mut next = Address::new(page.saturating_add(1), 0);
            if next > limit {
                next = limit;
            }

            if self
                .safe_read_only_address
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                Self::bump_max_address(&self.flushed_until_address, next);
                continue;
            }
        }
    }

    fn page_index(&self, page: u32) -> usize {
        (page % self.buffer_size) as usize
    }

    fn page_info_for(&self, page: u32) -> &PageInfo {
        &self.page_info[self.page_index(page)]
    }

    fn set_flush_status(info: &PageInfo, flush_status: FlushStatus) {
        loop {
            let current = info.status.load(Ordering::Acquire);
            if current.flush() == flush_status {
                break;
            }

            let mut next = current;
            next.flush_status = flush_status as u8;
            if info
                .status
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    fn bump_max_address(target: &AtomicAddress, value: Address) {
        loop {
            let current = target.load(Ordering::Acquire);
            if value <= current {
                break;
            }

            if target
                .compare_exchange(current, value, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }
}

pub(crate) enum FlushRequest {
    Page { page: u32, data: Vec<u8> },
    Shutdown,
}

pub(crate) struct FlushManager<D: StorageDevice> {
    tx: Sender<FlushRequest>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    _marker: std::marker::PhantomData<D>,
}

impl<D: StorageDevice> FlushManager<D> {
    pub(crate) fn new(shared: Arc<FlushShared<D>>) -> Self {
        let (tx, rx) = unbounded::<FlushRequest>();
        let worker = thread::spawn(move || worker_loop(shared, rx));

        Self {
            tx,
            worker: Mutex::new(Some(worker)),
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn submit_page(&self, page: u32, data: Vec<u8>) -> bool {
        self.tx.send(FlushRequest::Page { page, data }).is_ok()
    }

    pub(crate) fn stop(&self) {
        if let Some(worker) = self.worker.lock().take() {
            let _ = self.tx.send(FlushRequest::Shutdown);
            let _ = worker.join();
        }
    }
}

impl<D: StorageDevice> Drop for FlushManager<D> {
    fn drop(&mut self) {
        self.stop();
    }
}

fn worker_loop<D: StorageDevice>(shared: Arc<FlushShared<D>>, rx: Receiver<FlushRequest>) {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            warn!("Failed to start flush runtime: {e}");
            return;
        }
    };
    let handle = rt.handle().clone();

    while let Ok(req) = rx.recv() {
        match req {
            FlushRequest::Page { page, data } => {
                let shared = Arc::clone(&shared);
                handle.spawn(async move {
                    let result = async {
                        let expected_len = data.len();
                        let offset = Address::new(page, 0).control();
                        let written = shared.device().write(offset, &data).await?;
                        if written != expected_len {
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                format!(
                                    "partial write to page {page}: expected {expected_len} bytes, wrote {written} bytes"
                                ),
                            ));
                        }
                        Ok(())
                    }
                    .await;

                    match result {
                        Ok(()) => {
                            shared.mark_page_flushed(page);
                            shared.advance_safe_read_only(shared.read_only_address());
                        }
                        Err(e) => {
                            shared.mark_page_dirty_after_error(page);
                            warn!("Hybrid log page flush failed for page {page}: {e}");
                        }
                    }

                    shared.decrement_pending();
                });
            }
            FlushRequest::Shutdown => break,
        }
    }

    rt.shutdown_timeout(std::time::Duration::from_secs(1));
}
