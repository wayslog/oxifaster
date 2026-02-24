mod fuzz_util;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Arc as StdArc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use oxifaster::cache::ReadCacheConfig;
use oxifaster::device::SyncStorageDevice;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use rand::Rng;
use tempfile::tempdir;

/// A minimal file-based device for fuzz tests.
///
/// Unlike `FileSystemDisk`, this stores sparse writes in-memory (keyed by the `offset` passed by
/// the store) to avoid OS-specific stalls and huge sparse files when the hybrid log writes at
/// sparse offsets (see `Address::control()`).
#[derive(Clone)]
struct SparseMemDisk {
    page_size: usize,
    pages: StdArc<StdMutex<HashMap<u64, Vec<u8>>>>,
}

impl SparseMemDisk {
    fn new(page_size: usize) -> Self {
        Self {
            page_size,
            pages: StdArc::new(StdMutex::new(HashMap::new())),
        }
    }
}

impl SyncStorageDevice for SparseMemDisk {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let page_mask = (1u64 << oxifaster::Address::OFFSET_BITS) - 1;
        let pages = self.pages.lock().expect("SparseMemDisk lock poisoned");

        let mut remaining = buf.len();
        let mut dst_off = 0usize;
        let mut cur = offset;
        while remaining > 0 {
            let page_base = cur & !page_mask;
            let within = (cur & page_mask) as usize;

            let Some(page) = pages.get(&page_base) else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("missing page at base offset {page_base} (read at {cur})"),
                ));
            };
            if page.len() != self.page_size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "page size mismatch at base offset {page_base}: expected {} bytes, got {} bytes",
                        self.page_size,
                        page.len()
                    ),
                ));
            }
            if within >= page.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("read within-page offset out of bounds: {within}"),
                ));
            }

            let can = usize::min(remaining, page.len() - within);
            buf[dst_off..dst_off + can].copy_from_slice(&page[within..within + can]);
            remaining -= can;
            dst_off += can;
            cur = cur.saturating_add(can as u64);
        }

        Ok(buf.len())
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        let page_mask = (1u64 << oxifaster::Address::OFFSET_BITS) - 1;
        let mut pages = self.pages.lock().expect("SparseMemDisk lock poisoned");

        let mut remaining = buf.len();
        let mut src_off = 0usize;
        let mut cur = offset;
        while remaining > 0 {
            let page_base = cur & !page_mask;
            let within = (cur & page_mask) as usize;

            let page = pages
                .entry(page_base)
                .or_insert_with(|| vec![0u8; self.page_size]);
            if page.len() != self.page_size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "page size mismatch at base offset {page_base}: expected {} bytes, got {} bytes",
                        self.page_size,
                        page.len()
                    ),
                ));
            }
            if within >= page.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("write within-page offset out of bounds: {within}"),
                ));
            }

            let can = usize::min(remaining, page.len() - within);
            page[within..within + can].copy_from_slice(&buf[src_off..src_off + can]);
            remaining -= can;
            src_off += can;
            cur = cur.saturating_add(can as u64);
        }

        Ok(buf.len())
    }

    fn flush_sync(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn truncate_sync(&self, size: u64) -> std::io::Result<()> {
        let mut pages = self.pages.lock().expect("SparseMemDisk lock poisoned");
        pages.retain(|&offset, data| offset.saturating_add(data.len() as u64) <= size);
        Ok(())
    }

    fn size_sync(&self) -> std::io::Result<u64> {
        let pages = self.pages.lock().expect("SparseMemDisk lock poisoned");
        let mut max_end = 0u64;
        for (offset, data) in pages.iter() {
            max_end = max_end.max(offset.saturating_add(data.len() as u64));
        }
        Ok(max_end)
    }
}

fn pending_store_config() -> FasterKvConfig {
    // Keep enough pages in memory to avoid wrapping the in-memory page ring during the run.
    // We still force the "on-disk" boundary by shifting head forward.
    FasterKvConfig {
        table_size: 1 << 12,
        log_memory_size: 1 << 24, // 16MiB
        page_size_bits: 14,       // 16KiB pages (reduce flush work per head shift)
        mutable_fraction: 0.2,
    }
}

fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
    match r {
        Ok(v) => v,
        Err(s) => panic!("{ctx} failed: {s:?}"),
    }
}

fn retry_read(
    session: &mut oxifaster::store::Session<u64, u64, SparseMemDisk>,
    key: &u64,
    max_retries: usize,
    report: &mut PendingReport,
) -> (Option<u64>, bool) {
    let timed_out = false;
    for _ in 0..=max_retries {
        match session.read(key) {
            Ok(v) => return (v, timed_out),
            Err(Status::Pending) => {
                let ok = complete_pending_step(session, report);
                if !ok {
                    return (None, true);
                }
                session.refresh();
            }
            Err(s) => panic!("read error: {s:?}"),
        }
    }
    (None, true)
}

fn retry_status<F>(
    session: &mut oxifaster::store::Session<u64, u64, SparseMemDisk>,
    mut f: F,
    max_retries: usize,
    report: &mut PendingReport,
) -> (Status, bool)
where
    F: FnMut(&mut oxifaster::store::Session<u64, u64, SparseMemDisk>) -> Status,
{
    let timed_out = false;
    for _ in 0..=max_retries {
        let s = f(session);
        if s != Status::Pending {
            return (s, timed_out);
        }
        let ok = complete_pending_step(session, report);
        if !ok {
            return (Status::Pending, true);
        }
        session.refresh();
    }
    (Status::Pending, true)
}

fn complete_pending_step(
    session: &mut oxifaster::store::Session<u64, u64, SparseMemDisk>,
    report: &mut PendingReport,
) -> bool {
    report.complete_pending_calls += 1;

    let started_at = Instant::now();
    let ok = session.complete_pending_with_timeout(true, pending_timeout());
    report.complete_pending_wait_micros += started_at.elapsed().as_micros() as u64;

    // Capture context counters for diagnosing "why did we fail to drain?"
    let ctx = session.context();
    report.max_ctx_pending_count = report.max_ctx_pending_count.max(ctx.pending_count);
    report.max_ctx_retry_count = report.max_ctx_retry_count.max(ctx.retry_count);

    if !ok {
        report.complete_pending_timeouts += 1;
        report.max_timeout_pending_count = report.max_timeout_pending_count.max(ctx.pending_count);
        report.max_timeout_retry_count = report.max_timeout_retry_count.max(ctx.retry_count);
        if ctx.pending_count > 0 {
            report.timeouts_with_pending_count += 1;
        }
        if ctx.retry_count > 0 {
            report.timeouts_with_retry_count += 1;
        }
    }

    ok
}

fn pending_timeout() -> Duration {
    // Keep the fuzz suite bounded by default. The library's `complete_pending(wait=true)` uses a
    // much larger timeout. For fuzz, we prefer short waits and tracking timeouts explicitly.
    let ms = std::env::var("OXIFASTER_FUZZ_PENDING_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(5);
    Duration::from_millis(ms.max(1))
}

#[derive(Debug, Default, Clone)]
struct PendingReport {
    elapsed: Duration,
    reads_ok: u64,
    reads_pending: u64,
    reads_timeout: u64,
    writes_ok: u64,
    writes_pending: u64,
    writes_timeout: u64,
    deletes_ok: u64,
    deletes_pending: u64,
    deletes_timeout: u64,
    compactions: u64,
    head_shifts: u64,
    complete_pending_calls: u64,
    complete_pending_timeouts: u64,
    complete_pending_wait_micros: u64,
    max_ctx_pending_count: u32,
    max_ctx_retry_count: u32,
    max_timeout_pending_count: u32,
    max_timeout_retry_count: u32,
    timeouts_with_pending_count: u64,
    timeouts_with_retry_count: u64,
}

fn run_pending_fuzz(
    store: &Arc<FasterKv<u64, u64, SparseMemDisk>>,
    steps: usize,
    key_space: u64,
    seed: u64,
) -> PendingReport {
    let mut rng = fuzz_util::rng(seed);
    let started_at = Instant::now();
    let strict = fuzz_util::strict();

    let mut report = PendingReport::default();
    let mut model = HashMap::<u64, u64>::new();
    let mut session = expect_ok("start_session", store.start_session());
    let keep_hot_pages: u32 = 1;
    let head_step_pages: u32 = 4;

    for i in 0..steps {
        let op = rng.gen_range(0u8..=99);
        let key = fuzz_util::choose_key(&mut rng, key_space);

        match op {
            0..=44 => {
                // read
                match session.read(&key) {
                    Ok(v) => {
                        if strict {
                            assert_eq!(v, model.get(&key).copied());
                        }
                        report.reads_ok += 1;
                    }
                    Err(Status::Pending) => {
                        report.reads_pending += 1;
                        let (got, to) = retry_read(&mut session, &key, 2, &mut report);
                        if to {
                            report.reads_timeout += 1;
                            if strict {
                                panic!("read pending timeout");
                            }
                        }
                        if strict {
                            assert_eq!(got, model.get(&key).copied());
                        }
                    }
                    Err(s) => panic!("read error: {s:?}"),
                }
            }
            45..=84 => {
                // upsert
                let value = fuzz_util::choose_value(&mut rng);
                let (s, to) = retry_status(&mut session, |s| s.upsert(key, value), 2, &mut report);
                match s {
                    Status::Ok => {
                        model.insert(key, value);
                        report.writes_ok += 1;
                    }
                    Status::Pending => {
                        report.writes_pending += 1;
                        if to {
                            report.writes_timeout += 1;
                            if strict {
                                panic!("upsert pending timeout");
                            }
                        }
                    }
                    Status::OutOfMemory => {}
                    other => panic!("unexpected upsert status: {other:?}"),
                }
            }
            _ => {
                // delete
                let (s, to) = retry_status(&mut session, |s| s.delete(&key), 2, &mut report);
                match s {
                    Status::Ok => {
                        model.remove(&key);
                        report.deletes_ok += 1;
                    }
                    Status::Pending => {
                        report.deletes_pending += 1;
                        if to {
                            report.deletes_timeout += 1;
                            if strict {
                                panic!("delete pending timeout");
                            }
                        }
                    }
                    Status::NotFound => {}
                    Status::OutOfMemory => {}
                    other => panic!("unexpected delete status: {other:?}"),
                }
            }
        }

        // Periodically compact to keep the log from growing without bound under small memory.
        if i % 2_000 == 0 && i != 0 && store.should_compact() && !store.is_compaction_in_progress()
        {
            let r = store.log_compact();
            assert!(matches!(r.status, Status::Ok | Status::Aborted));
            report.compactions += 1;
        }

        // Periodically advance the head address so a meaningful portion of the log becomes
        // "on disk" from the store's perspective. This is required to trigger the
        // `Pending + complete_pending` path via public APIs.
        if i % 2_000 == 0 && i != 0 {
            let head = store.log_head_address();
            let tail = store.log_tail_address();
            if tail.page() > keep_hot_pages {
                let limit = tail.page().saturating_sub(keep_hot_pages);
                let target_page = head.page().saturating_add(head_step_pages).min(limit);
                let new_head = oxifaster::Address::new(target_page, 0);
                if new_head > head {
                    let s = store.flush_and_shift_head(new_head);
                    assert!(matches!(
                        s,
                        Status::Ok | Status::Aborted | Status::Corruption
                    ));
                    if s == Status::Ok {
                        report.head_shifts += 1;
                    }
                }
            }
        }
    }

    report.elapsed = started_at.elapsed();
    report
}

#[test]
fn fuzz_kv_pending_smoke_disk_and_cache() {
    let p = fuzz_util::params("kv_pending_smoke", 3_000, 5_000);
    let dir = tempdir().expect("tempdir");
    let _data_path = dir.path().join("oxifaster_fuzz_pending_sparse_mem.dat");
    let config = pending_store_config();
    let device = SparseMemDisk::new(1usize << config.page_size_bits);

    let cache = ReadCacheConfig::new(1 << 20) // 1MiB
        .with_mutable_fraction(0.9)
        .with_pre_allocate(false)
        .with_copy_to_tail(true);

    let store = Arc::new(FasterKv::<u64, u64, _>::with_read_cache(config, device, cache).unwrap());

    let report = run_pending_fuzz(&store, p.steps, p.key_space, p.seed);
    if fuzz_util::verbose() {
        let snap = store.stats_snapshot();
        let head = store.log_head_address();
        let tail = store.log_tail_address();
        eprintln!(
            "fuzz_kv_pending_smoke: steps={} elapsed_ms={} reads_ok={} reads_pending={} reads_timeout={} writes_ok={} writes_pending={} writes_timeout={} deletes_ok={} deletes_pending={} deletes_timeout={} compactions={} head_shifts={} complete_pending={{calls={}, timeouts={}, wait_micros={}, max_pending={}, max_retry={}, max_timeout_pending={}, max_timeout_retry={}, timeouts_with_pending={}, timeouts_with_retry={}}} log={{head={}, tail={}}} snap={{pending_io_submitted={}, pending_io_completed={}, pending_io_failed={}, read_hits={}, pages_flushed={}}}",
            p.steps,
            report.elapsed.as_millis(),
            report.reads_ok,
            report.reads_pending,
            report.reads_timeout,
            report.writes_ok,
            report.writes_pending,
            report.writes_timeout,
            report.deletes_ok,
            report.deletes_pending,
            report.deletes_timeout,
            report.compactions,
            report.head_shifts,
            report.complete_pending_calls,
            report.complete_pending_timeouts,
            report.complete_pending_wait_micros,
            report.max_ctx_pending_count,
            report.max_ctx_retry_count,
            report.max_timeout_pending_count,
            report.max_timeout_retry_count,
            report.timeouts_with_pending_count,
            report.timeouts_with_retry_count,
            head,
            tail,
            snap.pending_io_submitted,
            snap.pending_io_completed,
            snap.pending_io_failed,
            snap.read_hits,
            snap.pages_flushed,
        );
    }
}

#[test]
#[ignore]
fn fuzz_kv_pending_stress_disk_and_cache() {
    let p = fuzz_util::params("kv_pending_stress", 50_000, 20_000);
    let dir = tempdir().expect("tempdir");
    let _data_path = dir
        .path()
        .join("oxifaster_fuzz_pending_stress_sparse_mem.dat");
    let config = pending_store_config();
    let device = SparseMemDisk::new(1usize << config.page_size_bits);

    let cache = ReadCacheConfig::new(8 << 20) // 8MiB
        .with_mutable_fraction(0.9)
        .with_pre_allocate(false)
        .with_copy_to_tail(true);

    let store = Arc::new(FasterKv::<u64, u64, _>::with_read_cache(config, device, cache).unwrap());

    let report = run_pending_fuzz(&store, p.steps, p.key_space, p.seed);
    let snap = store.stats_snapshot();
    let head = store.log_head_address();
    let tail = store.log_tail_address();
    eprintln!(
        "fuzz_kv_pending_stress: steps={} elapsed_ms={} reads_pending={} reads_timeout={} writes_pending={} writes_timeout={} deletes_pending={} deletes_timeout={} compactions={} head_shifts={} complete_pending={{calls={}, timeouts={}, wait_micros={}, max_pending={}, max_retry={}, max_timeout_pending={}, max_timeout_retry={}, timeouts_with_pending={}, timeouts_with_retry={}}} log={{head={}, tail={}}} snap={{pending_io_submitted={}, pending_io_completed={}, pending_io_failed={}, read_hits={}, read_misses={}, pages_flushed={}}}",
        p.steps,
        report.elapsed.as_millis(),
        report.reads_pending,
        report.reads_timeout,
        report.writes_pending,
        report.writes_timeout,
        report.deletes_pending,
        report.deletes_timeout,
        report.compactions,
        report.head_shifts,
        report.complete_pending_calls,
        report.complete_pending_timeouts,
        report.complete_pending_wait_micros,
        report.max_ctx_pending_count,
        report.max_ctx_retry_count,
        report.max_timeout_pending_count,
        report.max_timeout_retry_count,
        report.timeouts_with_pending_count,
        report.timeouts_with_retry_count,
        head,
        tail,
        snap.pending_io_submitted,
        snap.pending_io_completed,
        snap.pending_io_failed,
        snap.read_hits,
        snap.reads.saturating_sub(snap.read_hits),
        snap.pages_flushed,
    );
}
