// DeltaLog tests (NullDisk)
// ============================================================

fn make_nulldisk_delta_log(page_size_bits: u32) -> DeltaLog<NullDisk> {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(page_size_bits);
    DeltaLog::new(device, config, 0)
}

#[test]
fn cov_delta_log_create_nulldisk() {
    let log = make_nulldisk_delta_log(12);
    assert_eq!(log.tail_address(), 0);
    assert_eq!(log.flushed_until_address(), 0);
    assert_eq!(log.end_address(), 0);
    assert!(!log.is_initialized_for_writes());
    assert!(!log.is_initialized_for_reads());
}

#[test]
fn cov_delta_log_init_for_writes_and_reads() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    assert!(log.is_initialized_for_writes());

    log.init_for_reads();
    assert!(log.is_initialized_for_reads());
}

#[test]
fn cov_delta_log_double_init_for_writes() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    // Second call should be a no-op.
    log.init_for_writes();
    assert!(log.is_initialized_for_writes());
}

#[test]
fn cov_delta_log_write_delta_entry() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let entry = DeltaLogEntry::delta(b"hello delta".to_vec());
    let addr = log.write_entry(&entry).unwrap();
    assert_eq!(addr, 0);
    assert!(log.tail_address() > 0);
}

#[test]
fn cov_delta_log_write_checkpoint_metadata_entry() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let entry = DeltaLogEntry::checkpoint_metadata(b"checkpoint-meta-v1".to_vec());
    let addr = log.write_entry(&entry).unwrap();
    assert_eq!(addr, 0);
    assert!(log.tail_address() > 0);
}

#[test]
fn cov_delta_log_write_multiple_entries() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let mut prev_tail = 0i64;
    for i in 0..20 {
        let payload = format!("entry-{i}");
        let entry = DeltaLogEntry::delta(payload.into_bytes());
        log.write_entry(&entry).unwrap();
        let new_tail = log.tail_address();
        assert!(new_tail > prev_tail, "tail should advance after each write");
        prev_tail = new_tail;
    }
}

#[test]
fn cov_delta_log_write_without_init_fails() {
    let log = make_nulldisk_delta_log(12);
    let entry = DeltaLogEntry::delta(b"fail".to_vec());
    let result = log.write_entry(&entry);
    assert!(result.is_err());
}

#[test]
fn cov_delta_log_allocate_without_init_fails() {
    let log = make_nulldisk_delta_log(12);
    let result = log.allocate();
    assert!(result.is_err());
}

#[test]
fn cov_delta_log_allocate_after_init() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let (max_len, ptr) = log.allocate().unwrap();
    assert!(max_len > 0);
    assert!(!ptr.is_null());
}

#[test]
fn cov_delta_log_flush_empty() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    assert!(log.flush().is_ok());
}

#[test]
fn cov_delta_log_flush_after_writes() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let entry = DeltaLogEntry::delta(b"data-to-flush".to_vec());
    log.write_entry(&entry).unwrap();

    // flush() should not error even though the internal buffer tracking
    // prevents actual device writes for partial pages.
    assert!(log.flush().is_ok());
}

#[test]
fn cov_delta_log_seal_zero_length_skips_page() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let _ = log.allocate().unwrap();
    log.seal(0, DeltaLogEntryType::Delta).unwrap();

    // Tail should have advanced to next page boundary.
    let page_size = log.config().page_size() as i64;
    assert!(log.tail_address() >= page_size);
}

#[test]
fn cov_delta_log_seal_with_data() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let (max_len, ptr) = log.allocate().unwrap();
    assert!(max_len >= 10);

    // Write some data into the allocated space.
    let data = b"seal-test!";
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
    }

    let old_tail = log.tail_address();
    log.seal(data.len(), DeltaLogEntryType::Delta).unwrap();
    assert!(log.tail_address() > old_tail);
}

#[test]
fn cov_delta_log_seal_checkpoint_metadata_type() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let (max_len, ptr) = log.allocate().unwrap();
    assert!(max_len >= 5);

    let data = b"meta!";
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
    }

    log.seal(data.len(), DeltaLogEntryType::CheckpointMetadata)
        .unwrap();
    assert!(log.tail_address() > 0);
}

#[test]
fn cov_delta_log_dispose() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    let entry = DeltaLogEntry::delta(b"dispose-test".to_vec());
    log.write_entry(&entry).unwrap();

    log.dispose();
    // Double dispose should not panic.
    log.dispose();
}

#[test]
fn cov_delta_log_negative_tail_uses_device_size() {
    let device = Arc::new(NullDisk::with_size(4096));
    let config = DeltaLogConfig::new(12);
    let log = DeltaLog::new(device, config, -1);
    assert_eq!(log.tail_address(), 4096);
}

#[test]
fn cov_delta_log_with_defaults() {
    let device = Arc::new(NullDisk::new());
    let log = DeltaLog::with_defaults(device, 0);
    assert_eq!(log.config().page_size_bits, 22);
}

#[test]
fn cov_delta_log_next_address() {
    let log = make_nulldisk_delta_log(12);
    assert_eq!(log.next_address(), log.tail_address());
}

#[test]
fn cov_delta_log_read_entry_negative_address() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    let result = log.read_entry(-1).unwrap();
    assert!(result.is_none());
}

#[test]
fn cov_delta_log_read_entry_past_tail() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    let result = log.read_entry(100).unwrap();
    assert!(result.is_none());
}

#[test]
fn cov_delta_log_read_entry_at_valid_address_nulldisk() {
    // NullDisk returns zeros, so the header will have length 0 -> returns None.
    // This exercises the read_entry code path beyond the bounds check.
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let entry = DeltaLogEntry::delta(b"some-data".to_vec());
    log.write_entry(&entry).unwrap();

    // Attempt to read at address 0 (within tail range).
    // NullDisk reads zeros, so header.length will be 0 -> returns None.
    let result = log.read_entry(0).unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn cov_delta_log_flush_async_empty() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    assert!(log.flush_async().await.is_ok());
}

#[tokio::test]
async fn cov_delta_log_flush_async_with_data() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    let entry = DeltaLogEntry::delta(b"async-flush-data".to_vec());
    log.write_entry(&entry).unwrap();
    assert!(log.flush_async().await.is_ok());
}

#[tokio::test]
async fn cov_delta_log_read_entry_async_negative() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    let result = log.read_entry_async(-1).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn cov_delta_log_read_entry_async_past_tail() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();
    let result = log.read_entry_async(999).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn cov_delta_log_read_entry_async_at_valid_address_nulldisk() {
    let log = make_nulldisk_delta_log(12);
    log.init_for_writes();

    let entry = DeltaLogEntry::delta(b"async-read".to_vec());
    log.write_entry(&entry).unwrap();

    // NullDisk reads zeros -> header.length == 0 -> None.
    let result = log.read_entry_async(0).await.unwrap();
    assert!(result.is_none());
}

// ============================================================
// DeltaLog: write enough to cross page boundary (exercises
// the page-boundary flush path in seal)
// ============================================================

#[test]
fn cov_delta_log_write_crosses_page_boundary() {
    let log = make_nulldisk_delta_log(12); // 4096-byte pages
    log.init_for_writes();

    let page_size = log.config().page_size();
    let mut count = 0;
    while (log.tail_address() as usize) < page_size * 2 {
        let payload = format!("boundary-test-{count:05}").into_bytes();
        let entry = DeltaLogEntry::delta(payload);
        log.write_entry(&entry).unwrap();
        count += 1;
    }

    assert!(log.tail_address() as usize >= page_size * 2);
    assert!(count > 1);
}

#[test]
fn cov_delta_log_write_entry_that_does_not_fit_current_page() {
    // Use a very small page (1 << 10 = 1024 bytes).
    let log = make_nulldisk_delta_log(10);
    log.init_for_writes();

    // Write a small entry first to consume part of the page.
    let small = DeltaLogEntry::delta(b"small".to_vec());
    log.write_entry(&small).unwrap();

    // Now write a large entry that won't fit in the remaining page space.
    // This should trigger the "entry doesn't fit -> seal(0) -> retry" path.
    let large_payload = vec![0xAA; 900]; // 900 bytes + 16 header > remaining ~1000 bytes
    let large = DeltaLogEntry::delta(large_payload);
    let addr = log.write_entry(&large).unwrap();
    // The large entry should have been written on the next page.
    assert!(addr > 0);
}

// ============================================================
// DeltaLogIterator tests (NullDisk)
// ============================================================

#[test]
fn cov_delta_log_iterator_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    assert_eq!(iter.state(), IteratorState::NotStarted);
    assert_eq!(iter.current_address(), 0);
    assert_eq!(iter.next_address(), 0);
    assert!(!iter.is_done());

    let result = iter.get_next().unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
    assert_eq!(iter.state(), IteratorState::Done);
}

#[test]
fn cov_delta_log_iterator_collect_all_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    let entries = iter.collect_all().unwrap();
    assert!(entries.is_empty());
}

#[test]
fn cov_delta_log_iterator_collect_by_type_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    let entries = iter.collect_by_type(DeltaLogEntryType::Delta).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn cov_delta_log_iterator_find_last_checkpoint_metadata_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    let result = iter.find_last_checkpoint_metadata().unwrap();
    assert!(result.is_none());
}

#[test]
fn cov_delta_log_into_iterator_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let iter = DeltaLogIntoIterator::new(delta_log);
    let items: Vec<_> = iter.collect();
    assert!(items.is_empty());
}

#[test]
fn cov_delta_log_iterator_custom_range() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let iter = DeltaLogIterator::new(delta_log, 100, 200);
    assert_eq!(iter.current_address(), 100);
    assert_eq!(iter.next_address(), 100);
}

#[test]
fn cov_delta_log_iterator_start_equals_end() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 100, 100);
    let result = iter.get_next().unwrap();
    assert!(result.is_none());
    assert_eq!(iter.state(), IteratorState::Done);
}

#[test]
fn cov_delta_log_iterator_start_past_end() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 200, 100);
    let result = iter.get_next().unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[test]
fn cov_delta_log_iterator_with_nonzero_range_nulldisk() {
    // NullDisk returns zeros, so iterator sees zero-length headers and stops.
    // This exercises the load_page + header-read path.
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 0, 4096);
    let result = iter.get_next().unwrap();
    // Zero-length header at offset 0 means end of delta log.
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[test]
fn cov_delta_log_iterator_with_range_mid_page() {
    // Start mid-page to exercise partial-page offset handling.
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 512, 4096);
    let result = iter.get_next().unwrap();
    // NullDisk returns zeros, zero-length header mid-page -> skip to next page.
    // Next page (4096) >= end_address (4096) -> Done.
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[test]
fn cov_delta_log_iterator_collect_all_with_range() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 0, 8192);
    let entries = iter.collect_all().unwrap();
    assert!(entries.is_empty());
}

#[test]
fn cov_delta_log_iterator_collect_by_type_with_range() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 0, 4096);
    let entries = iter
        .collect_by_type(DeltaLogEntryType::CheckpointMetadata)
        .unwrap();
    assert!(entries.is_empty());
}

#[test]
fn cov_delta_log_iterator_find_last_checkpoint_metadata_with_range() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 0, 4096);
    let result = iter.find_last_checkpoint_metadata().unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn cov_delta_log_iterator_get_next_async_empty() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    let result = iter.get_next_async().await.unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[tokio::test]
async fn cov_delta_log_iterator_get_next_async_with_range() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 0, 4096);
    let result = iter.get_next_async().await.unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[tokio::test]
async fn cov_delta_log_iterator_get_next_async_mid_page() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::new(delta_log, 512, 4096);
    let result = iter.get_next_async().await.unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
}

#[test]
fn cov_delta_log_iterator_sequential_get_next_calls() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let mut iter = DeltaLogIterator::all(delta_log);
    for _ in 0..5 {
        let result = iter.get_next().unwrap();
        assert!(result.is_none());
        assert!(iter.is_done());
    }
}

#[test]
fn cov_delta_log_into_iterator_with_range() {
    // DeltaLogIntoIterator uses Iterator trait. Exercise on non-empty range.
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    let iter = DeltaLogIntoIterator::new(delta_log);
    let items: Vec<_> = iter.collect();
    assert!(items.is_empty());
}

// ============================================================
// DeltaLogConfig edge cases
// ============================================================

#[test]
fn cov_delta_log_config_align_various() {
    let config = DeltaLogConfig::default();
    // Already aligned.
    assert_eq!(config.align(512), 512);
    assert_eq!(config.align(1024), 1024);
    assert_eq!(config.align(0), 0);
    // Needs alignment.
    assert_eq!(config.align(1), 512);
    assert_eq!(config.align(513), 1024);
}

#[test]
fn cov_delta_log_config_page_size_mask() {
    let config = DeltaLogConfig::new(12); // 4096 byte pages
    assert_eq!(config.page_size(), 4096);
    assert_eq!(config.page_size_mask(), 4095);
}

#[test]
fn cov_delta_log_config_default_values() {
    let config = DeltaLogConfig::default();
    assert_eq!(config.page_size_bits, 22);
    assert_eq!(config.sector_size, 512);
    assert_eq!(config.page_size(), 1 << 22);
}

#[test]
fn cov_delta_log_config_various_sizes() {
    for bits in [10, 12, 16, 20, 22] {
        let config = DeltaLogConfig::new(bits);
        assert_eq!(config.page_size(), 1 << bits);
        assert_eq!(config.page_size_mask(), (1 << bits) - 1);
    }
}

// ============================================================
// DeltaLogEntry edge cases
// ============================================================

#[test]
fn cov_delta_log_entry_verify() {
    let entry = DeltaLogEntry::delta(b"verify-me".to_vec());
    assert!(entry.verify());
    assert_eq!(entry.entry_type(), Some(DeltaLogEntryType::Delta));
}

#[test]
fn cov_delta_log_entry_checkpoint_metadata_verify() {
    let entry = DeltaLogEntry::checkpoint_metadata(b"meta-verify".to_vec());
    assert!(entry.verify());
    assert_eq!(
        entry.entry_type(),
        Some(DeltaLogEntryType::CheckpointMetadata)
    );
}

#[test]
fn cov_delta_log_entry_to_from_bytes() {
    let original = DeltaLogEntry::checkpoint_metadata(b"round-trip".to_vec());
    let bytes = original.to_bytes();
    let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.payload, original.payload);
    assert_eq!(restored.entry_type(), original.entry_type());
    assert!(restored.verify());
}

#[test]
fn cov_delta_log_entry_delta_to_from_bytes() {
    let original = DeltaLogEntry::delta(b"delta-round-trip".to_vec());
    let bytes = original.to_bytes();
    let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.payload, original.payload);
    assert_eq!(restored.entry_type(), Some(DeltaLogEntryType::Delta));
    assert!(restored.verify());
}

#[test]
fn cov_delta_log_entry_empty_payload() {
    let entry = DeltaLogEntry::delta(Vec::new());
    assert!(entry.verify());
    assert_eq!(entry.payload.len(), 0);

    let bytes = entry.to_bytes();
    let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.payload.len(), 0);
}

#[test]
fn cov_delta_log_entry_large_payload() {
    let payload = vec![0xBB; 4096];
    let entry = DeltaLogEntry::delta(payload.clone());
    assert!(entry.verify());

    let bytes = entry.to_bytes();
    let restored = DeltaLogEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.payload, payload);
    assert!(restored.verify());
}

#[test]
fn cov_delta_log_entry_type_as_str() {
    assert_eq!(DeltaLogEntryType::Delta.as_str(), "Delta");
    assert_eq!(
        DeltaLogEntryType::CheckpointMetadata.as_str(),
        "CheckpointMetadata"
    );
}

#[test]
fn cov_delta_log_entry_type_from_i32() {
    assert_eq!(
        DeltaLogEntryType::from_i32(0),
        Some(DeltaLogEntryType::Delta)
    );
    assert_eq!(
        DeltaLogEntryType::from_i32(1),
        Some(DeltaLogEntryType::CheckpointMetadata)
    );
    assert_eq!(DeltaLogEntryType::from_i32(-1), None);
    assert_eq!(DeltaLogEntryType::from_i32(2), None);
    assert_eq!(DeltaLogEntryType::from_i32(99), None);
}

#[test]
fn cov_delta_log_entry_type_to_i32() {
    assert_eq!(DeltaLogEntryType::Delta.to_i32(), 0);
    assert_eq!(DeltaLogEntryType::CheckpointMetadata.to_i32(), 1);
}

#[test]
fn cov_iterator_state_debug_and_equality() {
    let states = [
        IteratorState::NotStarted,
        IteratorState::Active,
        IteratorState::Done,
        IteratorState::Error,
    ];

    for state in &states {
        let debug = format!("{state:?}");
        assert!(!debug.is_empty());
    }

    assert_eq!(IteratorState::NotStarted, IteratorState::NotStarted);
    assert_ne!(IteratorState::NotStarted, IteratorState::Active);
    assert_ne!(IteratorState::Done, IteratorState::Error);
}

#[test]
fn cov_iterator_state_clone_copy() {
    let state = IteratorState::Active;
    let cloned = state;
    let copied = state;
    assert_eq!(state, cloned);
    assert_eq!(state, copied);
}

// ============================================================
// LightEpoch multi-thread stress: protect_and_drain
// ============================================================

#[test]
fn cov_light_epoch_protect_and_drain_multithread() {
    let epoch = Arc::new(LightEpoch::new());

    let mut handles = Vec::new();
    for _ in 0..4 {
        let ep = epoch.clone();
        let handle = std::thread::spawn(move || {
            let tid = get_thread_id();
            for _ in 0..100 {
                let e = ep.protect_and_drain(tid);
                assert!(e > 0);
                ep.unprotect(tid);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================
// LightEpoch: finish_thread_phase with multiple threads
// ============================================================

#[test]
fn cov_light_epoch_finish_thread_phase_multithread() {
    let epoch = Arc::new(LightEpoch::new());
    let phase = 42u32;

    // Protect 3 threads.
    epoch.protect(10);
    epoch.protect(11);
    epoch.protect(12);

    // Thread 10 finishes - not all done.
    assert!(!epoch.finish_thread_phase(10, phase));
    // Thread 11 finishes - not all done.
    assert!(!epoch.finish_thread_phase(11, phase));
    // Thread 12 finishes - all done.
    assert!(epoch.finish_thread_phase(12, phase));

    // Verify.
    assert!(epoch.has_thread_finished_phase(10, phase));
    assert!(epoch.has_thread_finished_phase(11, phase));
    assert!(epoch.has_thread_finished_phase(12, phase));

    epoch.unprotect(10);
    epoch.unprotect(11);
    epoch.unprotect(12);
}

// ============================================================
// LightEpoch: compute_new_safe_to_reclaim_epoch edge cases
// ============================================================

#[test]
fn cov_light_epoch_compute_safe_to_reclaim_no_protected() {
    let epoch = LightEpoch::new();
    // No threads protected: safe_to_reclaim should be current_epoch - 1.
    let safe = epoch.compute_new_safe_to_reclaim_epoch(10);
    assert_eq!(safe, 9);
}

#[test]
fn cov_light_epoch_compute_safe_to_reclaim_with_protected() {
    let epoch = LightEpoch::new();
    epoch.current_epoch.store(10, Ordering::Relaxed);
    epoch.protect(0);

    let safe = epoch.compute_new_safe_to_reclaim_epoch(20);
    // Thread 0 is at epoch 10, so safe should be 9.
    assert_eq!(safe, 9);
    assert!(epoch.is_safe_to_reclaim(9));
    assert!(!epoch.is_safe_to_reclaim(10));

    epoch.unprotect(0);
}

#[test]
fn cov_light_epoch_compute_safe_to_reclaim_multiple_protected() {
    let epoch = LightEpoch::new();
    epoch.current_epoch.store(10, Ordering::Relaxed);
    epoch.protect(0); // epoch 10

    epoch.current_epoch.store(15, Ordering::Relaxed);
    epoch.protect(1); // epoch 15

    // The oldest protected epoch is 10, so safe should be 9.
    let safe = epoch.compute_new_safe_to_reclaim_epoch(20);
    assert_eq!(safe, 9);

    epoch.unprotect(0);
    // Now the oldest is 15, safe should be 14.
    let safe = epoch.compute_new_safe_to_reclaim_epoch(20);
    assert_eq!(safe, 14);

    epoch.unprotect(1);
}

#[test]
fn cov_light_epoch_is_safe_to_reclaim() {
    let epoch = LightEpoch::new();
    epoch.safe_to_reclaim_epoch.store(5, Ordering::Release);
    assert!(epoch.is_safe_to_reclaim(5));
    assert!(epoch.is_safe_to_reclaim(4));
    assert!(epoch.is_safe_to_reclaim(0));
    assert!(!epoch.is_safe_to_reclaim(6));
}

// ============================================================
// LightEpoch: bump_current_epoch_with_action edge cases
// ============================================================

#[test]
fn cov_light_epoch_bump_with_action_single() {
    let epoch = Arc::new(LightEpoch::new());
    let executed = Arc::new(AtomicBool::new(false));
    let executed_clone = executed.clone();

    let new_epoch = epoch.bump_current_epoch_with_action(move || {
        executed_clone.store(true, Ordering::Release);
    });
    assert_eq!(new_epoch, 2);

    // Drain by bumping enough.
    for _ in 0..10 {
        epoch.bump_current_epoch();
    }

    assert!(executed.load(Ordering::Acquire));
}

#[test]
fn cov_light_epoch_bump_with_action_many_sequential() {
    let epoch = Arc::new(LightEpoch::new());
    let counter = Arc::new(AtomicU64::new(0));

    for _ in 0..50 {
        let c = counter.clone();
        epoch.bump_current_epoch_with_action(move || {
            c.fetch_add(1, Ordering::AcqRel);
        });
    }

    // Drain by bumping.
    for _ in 0..100 {
        epoch.bump_current_epoch();
    }

    assert_eq!(counter.load(Ordering::Acquire), 50);
}

// ============================================================
// LightEpoch: protect/unprotect basic variants
// ============================================================

#[test]
fn cov_light_epoch_protect_unprotect_cycle() {
    let epoch = LightEpoch::new();
    for i in 0..10 {
        let tid = i % 10;
        epoch.protect(tid);
        assert!(epoch.is_protected(tid));
        epoch.unprotect(tid);
        assert!(!epoch.is_protected(tid));
    }
}

#[test]
fn cov_light_epoch_reentrant_protect_unprotect_deep() {
    let epoch = LightEpoch::new();
    let depth = 10;

    // Nest protection to depth 10.
    for _ in 0..depth {
        epoch.reentrant_protect(0);
    }
    assert!(epoch.is_protected(0));

    // Unprotect all but one level.
    for _ in 0..(depth - 1) {
        epoch.reentrant_unprotect(0);
        assert!(epoch.is_protected(0));
    }

    // Last unprotect.
    epoch.reentrant_unprotect(0);
    assert!(!epoch.is_protected(0));
}

// ============================================================
// DeltaLog with positive tail_address constructor
// ============================================================

#[test]
fn cov_delta_log_positive_initial_tail() {
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let log = DeltaLog::new(device, config, 512);
    assert_eq!(log.tail_address(), 512);
    assert_eq!(log.flushed_until_address(), 512);
    assert_eq!(log.end_address(), 512);
}

// ============================================================
// DeltaLog: init_for_reads then check
// ============================================================

#[test]
fn cov_delta_log_init_for_reads_flag() {
    let log = make_nulldisk_delta_log(12);
    assert!(!log.is_initialized_for_reads());
    log.init_for_reads();
    assert!(log.is_initialized_for_reads());
    // Calling again should not panic.
    log.init_for_reads();
    assert!(log.is_initialized_for_reads());
}

// ============================================================
// DeltaLog: get config accessor
// ============================================================

#[test]
fn cov_delta_log_config_accessor() {
    let log = make_nulldisk_delta_log(14);
    let config = log.config();
    assert_eq!(config.page_size_bits, 14);
    assert_eq!(config.page_size(), 1 << 14);
}

// ============================================================
// DeltaLogIterator with different page configurations
// ============================================================

#[test]
fn cov_delta_log_iterator_various_page_sizes() {
    for bits in [10, 12, 14, 16] {
        let device = Arc::new(NullDisk::new());
        let config = DeltaLogConfig::new(bits);
        let delta_log = Arc::new(DeltaLog::new(device, config, 0));

        let mut iter = DeltaLogIterator::all(delta_log);
        let result = iter.get_next().unwrap();
        assert!(result.is_none());
        assert!(iter.is_done());
    }
}

#[test]
fn cov_delta_log_iterator_header_past_valid_data() {
    // Create an iterator with end_address that is less than a full header size
    // from the start. This exercises the "not enough bytes for header" path.
    let device = Arc::new(NullDisk::new());
    let config = DeltaLogConfig::new(12);
    let delta_log = Arc::new(DeltaLog::new(device, config, 0));

    // end_address = 10, which is less than DELTA_LOG_HEADER_SIZE (16).
    // The iterator loads 10 bytes, but can't read a full header -> skip to next page.
    let mut iter = DeltaLogIterator::new(delta_log, 0, 10);
    let result = iter.get_next().unwrap();
    assert!(result.is_none());
    assert!(iter.is_done());
}
