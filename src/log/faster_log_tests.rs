use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use super::*;
use crate::device::NullDisk;

#[test]
fn test_wait_for_commit_blocks_until_padding_sealed() {
    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 2,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };

    let log = Arc::new(FasterLog::open(config, NullDisk::new()).unwrap());
    log.append(b"a").unwrap();

    let current = log.shared.tail.load(Ordering::Acquire);
    let page = current.page();
    let offset = current.offset();

    log.shared
        .tail
        .store(PageOffset::new(page + 1, 0), Ordering::Release);
    log.shared.sealed_page.store(page, Ordering::Release);

    let commit_addr = log.commit().unwrap();

    let started = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let wait_log = Arc::clone(&log);
    let started_wait = Arc::clone(&started);
    let handle = thread::spawn(move || {
        started_wait.store(true, Ordering::Release);
        let _ = wait_log.wait_for_commit(commit_addr);
        let _ = tx.send(());
    });

    while !started.load(Ordering::Acquire) {
        thread::yield_now();
    }

    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "wait_for_commit returned before padding was sealed"
    );

    log.write_padding(page, offset).unwrap();
    log.shared.advance_sealed_page(page + 1);

    rx.recv_timeout(Duration::from_secs(1))
        .expect("wait_for_commit should complete after padding is sealed");
    handle.join().unwrap();
    log.close();
}
