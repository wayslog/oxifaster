mod fuzz_util;

use oxifaster::device::FileSystemDisk;
use oxifaster::log::{FasterLog, FasterLogConfig, FasterLogOpenOptions};
use oxifaster::status::Status;
use rand::Rng;
use tempfile::tempdir;

fn expect_ok<T>(ctx: &'static str, r: Result<T, Status>) -> T {
    match r {
        Ok(v) => v,
        Err(s) => panic!("{ctx} failed: {s:?}"),
    }
}

fn small_log_config() -> FasterLogConfig {
    FasterLogConfig {
        page_size: 1 << 14,    // 16KiB
        memory_pages: 32,      // 512KiB
        segment_size: 1 << 20, // 1MiB
        auto_commit_ms: 0,
    }
}

#[test]
fn fuzz_log_smoke_append_commit_reopen_scan() {
    let p = fuzz_util::params("log_smoke", 2_000, 1);
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_fuzz_log.dat");

    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let log = expect_ok("open", FasterLog::open(small_log_config(), device));

    let mut rng = fuzz_util::rng(p.seed);
    let mut entries = Vec::<(oxifaster::Address, Vec<u8>)>::new();
    let mut committed_upto = 0usize;

    for _ in 0..p.steps {
        let len = rng.gen_range(1usize..=1024);
        let mut payload = vec![0u8; len];
        rng.fill(&mut payload[..]);

        let addr = log.append(&payload).expect("append");
        entries.push((addr, payload));

        if rng.gen_ratio(1, 16) {
            log.commit().expect("commit");
            committed_upto = entries.len();
        }

        if rng.gen_ratio(1, 64) && committed_upto > 0 {
            // Read a random committed entry.
            let idx = rng.gen_range(0..committed_upto);
            let (addr, expected) = &entries[idx];
            let got = log.read_entry(*addr).expect("read_entry");
            assert_eq!(&got, expected);
        }
    }

    // Final commit to make scan deterministic.
    log.commit().expect("final commit");
    committed_upto = entries.len();

    // Scan should yield at least as many records as we committed (addressing may include holes).
    let scan_count = log.scan_all().count();
    assert!(scan_count >= committed_upto);

    let stats = log.get_stats();
    assert!(stats.committed_address <= stats.tail_address);

    log.close();
    assert!(log.is_closed());

    // Reopen and scan again.
    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let reopened = expect_ok(
        "reopen",
        FasterLog::open_with_options(small_log_config(), device, FasterLogOpenOptions::default()),
    );
    let scan_count2 = reopened.scan_all().count();
    assert!(scan_count2 >= scan_count.saturating_sub(1));
    reopened.close();
}

#[test]
#[ignore]
fn fuzz_log_stress_append_commit_truncate() {
    let p = fuzz_util::params("log_stress", 50_000, 1);
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_fuzz_log_stress.dat");

    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let log = expect_ok("open", FasterLog::open(small_log_config(), device));

    let mut rng = fuzz_util::rng(p.seed);
    let mut addrs = Vec::new();

    for i in 0..p.steps {
        let len = rng.gen_range(1usize..=2048);
        let mut payload = vec![0u8; len];
        rng.fill(&mut payload[..]);
        let addr = log.append(&payload).expect("append");
        addrs.push(addr);

        if i % 128 == 0 {
            log.commit().expect("commit");
        }
        if i % 2048 == 0 && addrs.len() > 4096 {
            // Truncate to a recent address and ensure begin address advances.
            let idx = rng.gen_range(0..addrs.len() - 1024);
            let truncate_to = addrs[idx];
            log.truncate_until(truncate_to);
            assert!(log.get_begin_address() >= truncate_to);
        }
    }

    log.commit().expect("final commit");
    let _ = log.scan_all().count();
    log.close();
}
