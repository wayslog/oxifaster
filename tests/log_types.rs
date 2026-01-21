//! 测试 log types 功能

use oxifaster::address::Address;
use oxifaster::log::{
    FasterLogConfig, FasterLogOpenOptions, FasterLogSelfCheckOptions, FasterLogSelfCheckReport,
    LogErrorKind, LogStats,
};

#[test]
fn test_faster_log_config_new() {
    let config = FasterLogConfig::new(1 << 20, 4096);

    assert_eq!(config.page_size, 4096);
    assert_eq!(config.memory_pages, (1 << 20) / 4096);
    assert_eq!(config.segment_size, 1 << 30);
    assert_eq!(config.auto_commit_ms, 0);
}

#[test]
fn test_faster_log_config_default() {
    let config = FasterLogConfig::default();

    assert_eq!(config.page_size, 1 << 22);
    assert_eq!(config.memory_pages, 64);
    assert_eq!(config.segment_size, 1 << 30);
    assert_eq!(config.auto_commit_ms, 0);
}

#[test]
fn test_faster_log_config_custom() {
    let config = FasterLogConfig {
        page_size: 8192,
        memory_pages: 128,
        segment_size: 1 << 28,
        auto_commit_ms: 100,
    };

    assert_eq!(config.page_size, 8192);
    assert_eq!(config.memory_pages, 128);
    assert_eq!(config.segment_size, 1 << 28);
    assert_eq!(config.auto_commit_ms, 100);
}

#[test]
fn test_faster_log_open_options_default() {
    let opts = FasterLogOpenOptions::default();

    assert!(opts.recover);
    assert!(opts.create_if_missing);
    assert!(opts.truncate_on_corruption);
}

#[test]
fn test_faster_log_open_options_with_self_check() {
    let opts = FasterLogOpenOptions::default().with_self_check(false);

    assert!(!opts.recover);
    assert!(opts.create_if_missing);
    assert!(opts.truncate_on_corruption);
}

#[test]
fn test_faster_log_open_options_with_self_repair() {
    let opts = FasterLogOpenOptions::default().with_self_repair(false);

    assert!(opts.recover);
    assert!(opts.create_if_missing);
    assert!(!opts.truncate_on_corruption);
}

#[test]
fn test_faster_log_open_options_with_create_if_missing() {
    let opts = FasterLogOpenOptions::default().with_create_if_missing(false);

    assert!(opts.recover);
    assert!(!opts.create_if_missing);
    assert!(opts.truncate_on_corruption);
}

#[test]
fn test_faster_log_open_options_chaining() {
    let opts = FasterLogOpenOptions::default()
        .with_self_check(false)
        .with_self_repair(false)
        .with_create_if_missing(false);

    assert!(!opts.recover);
    assert!(!opts.create_if_missing);
    assert!(!opts.truncate_on_corruption);
}

#[test]
fn test_faster_log_self_check_options_default() {
    let opts = FasterLogSelfCheckOptions::default();

    assert!(!opts.repair);
    assert!(!opts.dry_run);
    assert!(!opts.create_if_missing);
}

#[test]
fn test_faster_log_self_check_options_custom() {
    let opts = FasterLogSelfCheckOptions {
        repair: true,
        dry_run: false,
        create_if_missing: true,
    };

    assert!(opts.repair);
    assert!(!opts.dry_run);
    assert!(opts.create_if_missing);
}

#[test]
fn test_faster_log_self_check_report() {
    let report = FasterLogSelfCheckReport {
        metadata_present: true,
        metadata_created: false,
        scan_end: Address::from(1000),
        had_corruption: false,
        truncate_to: None,
        repaired: false,
        dry_run: false,
    };

    assert!(report.metadata_present);
    assert!(!report.metadata_created);
    assert_eq!(report.scan_end, Address::from(1000));
    assert!(!report.had_corruption);
    assert!(report.truncate_to.is_none());
    assert!(!report.repaired);
    assert!(!report.dry_run);
}

#[test]
fn test_faster_log_self_check_report_with_corruption() {
    let report = FasterLogSelfCheckReport {
        metadata_present: true,
        metadata_created: false,
        scan_end: Address::from(500),
        had_corruption: true,
        truncate_to: Some(Address::from(450)),
        repaired: true,
        dry_run: false,
    };

    assert!(report.metadata_present);
    assert!(report.had_corruption);
    assert_eq!(report.scan_end, Address::from(500));
    assert_eq!(report.truncate_to, Some(Address::from(450)));
    assert!(report.repaired);
}

#[test]
fn test_log_error_kind_as_str() {
    assert_eq!(LogErrorKind::Io.as_str(), "Io");
    assert_eq!(LogErrorKind::Metadata.as_str(), "Metadata");
    assert_eq!(LogErrorKind::Entry.as_str(), "Entry");
    assert_eq!(LogErrorKind::Config.as_str(), "Config");
    assert_eq!(LogErrorKind::Corruption.as_str(), "Corruption");
}

#[test]
fn test_log_error_kind_equality() {
    assert_eq!(LogErrorKind::Io, LogErrorKind::Io);
    assert_ne!(LogErrorKind::Io, LogErrorKind::Metadata);
    assert_ne!(LogErrorKind::Entry, LogErrorKind::Config);
}

#[test]
fn test_log_stats() {
    let stats = LogStats {
        tail_address: Address::from(1000),
        committed_address: Address::from(900),
        begin_address: Address::from(0),
        page_size: 4096,
        buffer_pages: 64,
    };

    assert_eq!(stats.tail_address, Address::from(1000));
    assert_eq!(stats.committed_address, Address::from(900));
    assert_eq!(stats.begin_address, Address::from(0));
    assert_eq!(stats.page_size, 4096);
    assert_eq!(stats.buffer_pages, 64);
}

#[test]
fn test_log_stats_display() {
    let stats = LogStats {
        tail_address: Address::from(1000),
        committed_address: Address::from(900),
        begin_address: Address::from(0),
        page_size: 4096,
        buffer_pages: 64,
    };

    let display = format!("{}", stats);
    assert!(display.contains("FASTER Log Statistics:"));
    assert!(display.contains("Tail:"));
    assert!(display.contains("Committed:"));
    assert!(display.contains("Begin:"));
    assert!(display.contains("Page size: 4096 bytes"));
    assert!(display.contains("Buffer pages: 64"));
}

#[test]
fn test_log_error_kind_copy() {
    let kind1 = LogErrorKind::Entry;
    let kind2 = kind1;

    assert_eq!(kind1, kind2);
}

#[test]
fn test_faster_log_config_clone() {
    let config1 = FasterLogConfig::new(1 << 20, 4096);
    let config2 = config1.clone();

    assert_eq!(config1.page_size, config2.page_size);
    assert_eq!(config1.memory_pages, config2.memory_pages);
    assert_eq!(config1.segment_size, config2.segment_size);
    assert_eq!(config1.auto_commit_ms, config2.auto_commit_ms);
}

#[test]
fn test_faster_log_open_options_clone() {
    let opts1 = FasterLogOpenOptions::default().with_self_check(false);
    let opts2 = opts1.clone();

    assert_eq!(opts1.recover, opts2.recover);
    assert_eq!(opts1.create_if_missing, opts2.create_if_missing);
    assert_eq!(opts1.truncate_on_corruption, opts2.truncate_on_corruption);
}

#[test]
fn test_faster_log_self_check_options_clone() {
    let opts1 = FasterLogSelfCheckOptions {
        repair: true,
        dry_run: false,
        create_if_missing: true,
    };
    let opts2 = opts1.clone();

    assert_eq!(opts1.repair, opts2.repair);
    assert_eq!(opts1.dry_run, opts2.dry_run);
    assert_eq!(opts1.create_if_missing, opts2.create_if_missing);
}

#[test]
fn test_faster_log_self_check_report_clone() {
    let report1 = FasterLogSelfCheckReport {
        metadata_present: true,
        metadata_created: false,
        scan_end: Address::from(1000),
        had_corruption: false,
        truncate_to: None,
        repaired: false,
        dry_run: false,
    };
    let report2 = report1.clone();

    assert_eq!(report1.metadata_present, report2.metadata_present);
    assert_eq!(report1.scan_end, report2.scan_end);
    assert_eq!(report1.had_corruption, report2.had_corruption);
}

#[test]
fn test_log_stats_clone() {
    let stats1 = LogStats {
        tail_address: Address::from(1000),
        committed_address: Address::from(900),
        begin_address: Address::from(0),
        page_size: 4096,
        buffer_pages: 64,
    };
    let stats2 = stats1.clone();

    assert_eq!(stats1.tail_address, stats2.tail_address);
    assert_eq!(stats1.committed_address, stats2.committed_address);
    assert_eq!(stats1.page_size, stats2.page_size);
}

#[test]
fn test_faster_log_config_large_memory() {
    let config = FasterLogConfig::new(1 << 30, 1 << 22);

    assert_eq!(config.page_size, 1 << 22);
    assert_eq!(config.memory_pages, (1 << 30) / (1 << 22));
}

#[test]
fn test_faster_log_config_small_page_size() {
    let config = FasterLogConfig::new(1 << 20, 1024);

    assert_eq!(config.page_size, 1024);
    assert_eq!(config.memory_pages, (1 << 20) / 1024);
}

#[test]
fn test_log_stats_boundary_values() {
    let stats = LogStats {
        tail_address: Address::INVALID,
        committed_address: Address::from(0),
        begin_address: Address::from(0),
        page_size: 0,
        buffer_pages: 0,
    };

    assert_eq!(stats.tail_address, Address::INVALID);
    assert_eq!(stats.committed_address, Address::from(0));
    assert_eq!(stats.page_size, 0);
    assert_eq!(stats.buffer_pages, 0);
}

#[test]
fn test_faster_log_open_options_all_disabled() {
    let opts = FasterLogOpenOptions {
        recover: false,
        create_if_missing: false,
        truncate_on_corruption: false,
    };

    assert!(!opts.recover);
    assert!(!opts.create_if_missing);
    assert!(!opts.truncate_on_corruption);
}

#[test]
fn test_faster_log_self_check_options_all_enabled() {
    let opts = FasterLogSelfCheckOptions {
        repair: true,
        dry_run: true,
        create_if_missing: true,
    };

    assert!(opts.repair);
    assert!(opts.dry_run);
    assert!(opts.create_if_missing);
}
