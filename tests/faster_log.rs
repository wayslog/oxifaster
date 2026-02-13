use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::thread;

use oxifaster::Status;
use oxifaster::device::FileSystemDisk;
use oxifaster::log::{
    FasterLog, FasterLogConfig, FasterLogOpenOptions, FasterLogSelfCheckOptions, LogErrorKind,
};
use tempfile::tempdir;

fn create_config() -> FasterLogConfig {
    FasterLogConfig {
        page_size: 4096,
        memory_pages: 8,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    }
}

#[test]
fn test_reopen_scan_persists_entries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("faster_log.dat");

    let config = create_config();
    let device = FileSystemDisk::single_file(&path).unwrap();
    let log = FasterLog::open(config.clone(), device).unwrap();

    let entries = ["alpha", "beta", "gamma"];
    for entry in &entries {
        log.append(entry.as_bytes()).unwrap();
    }

    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();
    log.close();

    let device = FileSystemDisk::single_file(&path).unwrap();
    let reopened = FasterLog::open(config, device).unwrap();

    let read_entries: Vec<String> = reopened
        .scan_all()
        .map(|(_, data)| String::from_utf8(data).unwrap())
        .collect();

    assert_eq!(read_entries, entries);
}

#[test]
fn test_reopen_scan_across_pages() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("faster_log_pages.dat");

    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 4,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };

    let device = FileSystemDisk::single_file(&path).unwrap();
    let log = FasterLog::open(config.clone(), device).unwrap();

    let entries = vec!["entry-1", "entry-2", "entry-3", "entry-4"];
    for entry in &entries {
        let payload = vec![b'x'; 480];
        let mut data = entry.as_bytes().to_vec();
        data.extend_from_slice(&payload);
        log.append(&data).unwrap();
    }

    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();
    log.close();

    let device = FileSystemDisk::single_file(&path).unwrap();
    let reopened = FasterLog::open(config, device).unwrap();

    let read_entries: Vec<String> = reopened
        .scan_all()
        .map(|(_, data)| String::from_utf8(data[..7].to_vec()).unwrap())
        .collect();

    assert_eq!(read_entries, entries);
}

#[test]
fn test_reopen_scan_multithread_across_pages() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("faster_log_pages_mt.dat");

    let config = FasterLogConfig {
        page_size: 512,
        memory_pages: 64,
        segment_size: 1 << 20,
        auto_commit_ms: 0,
    };

    let device = FileSystemDisk::single_file(&path).unwrap();
    let log = Arc::new(FasterLog::open(config.clone(), device).unwrap());

    let threads = 4;
    let entries_per_thread = 4;
    let data_len = 480usize;
    let mut expected = HashSet::new();

    let mut handles = Vec::new();
    for thread_id in 0..threads {
        let log = Arc::clone(&log);
        for entry_id in 0..entries_per_thread {
            expected.insert(format!("t{thread_id}-e{entry_id}"));
        }
        handles.push(thread::spawn(move || {
            for entry_id in 0..entries_per_thread {
                let prefix = format!("t{thread_id}-e{entry_id}:");
                let padding_len = data_len.saturating_sub(prefix.len());
                let mut data = Vec::with_capacity(data_len);
                data.extend_from_slice(prefix.as_bytes());
                data.extend(std::iter::repeat_n(b'x', padding_len));
                log.append(&data).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();
    log.close();

    let device = FileSystemDisk::single_file(&path).unwrap();
    let reopened = FasterLog::open(config, device).unwrap();

    let read_entries: HashSet<String> = reopened
        .scan_all()
        .map(|(_, data)| {
            let text = String::from_utf8(data).unwrap();
            text.split(':').next().unwrap().to_string()
        })
        .collect();

    assert_eq!(read_entries, expected);
}

#[test]
fn test_recover_truncates_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("faster_log_corrupt.dat");

    let config = create_config();
    let device = FileSystemDisk::single_file(&path).unwrap();
    let log = FasterLog::open(config.clone(), device).unwrap();

    let addr1 = log.append(b"first").unwrap();
    let addr2 = log.append(b"second").unwrap();

    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();
    log.close();

    let entry_header_size = 16u64;
    let data_offset = config.page_size as u64;
    let entry2_offset = data_offset
        + (addr2.page() as u64 * config.page_size as u64)
        + addr2.offset() as u64
        + entry_header_size
        + 1;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(entry2_offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(entry2_offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();

    let device = FileSystemDisk::single_file(&path).unwrap();
    let reopened = FasterLog::open(config, device).unwrap();

    let read_entries: Vec<String> = reopened
        .scan_all()
        .map(|(_, data)| String::from_utf8(data).unwrap())
        .collect();

    assert_eq!(read_entries, vec!["first"]);
    let last_error = reopened.last_error().expect("expected recovery error");
    assert_eq!(last_error.kind, LogErrorKind::Corruption);
    assert!(addr1 < addr2);
}

#[test]
fn test_self_check_dry_run_reports_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("faster_log_self_check.dat");

    let config = create_config();
    let device = FileSystemDisk::single_file(&path).unwrap();
    let log = FasterLog::open(config.clone(), device).unwrap();

    let addr2 = log.append(b"second").unwrap();
    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();
    log.close();

    let entry_header_size = 16u64;
    let data_offset = config.page_size as u64;
    let entry2_offset = data_offset
        + (addr2.page() as u64 * config.page_size as u64)
        + addr2.offset() as u64
        + entry_header_size
        + 1;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(entry2_offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(entry2_offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();

    let device = FileSystemDisk::single_file(&path).unwrap();
    let report = FasterLog::self_check_with_options(
        config.clone(),
        device,
        FasterLogSelfCheckOptions {
            repair: true,
            dry_run: true,
            create_if_missing: false,
        },
    )
    .unwrap();

    assert!(report.had_corruption);
    assert!(report.truncate_to.is_some());
    assert!(report.dry_run);
    assert!(!report.repaired);

    let device = FileSystemDisk::single_file(&path).unwrap();
    let result = FasterLog::open_with_options(
        config,
        device,
        FasterLogOpenOptions {
            recover: true,
            create_if_missing: true,
            truncate_on_corruption: false,
        },
    );
    assert!(matches!(result, Err(Status::Corruption)));
}
