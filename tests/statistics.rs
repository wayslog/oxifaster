//! Statistics integration tests.
//!
//! Covers statistics collection and configuration APIs.

use std::sync::Arc;
use std::time::Duration;

use oxifaster::device::NullDisk;
use oxifaster::stats::{StatsCollector, StatsConfig};
use oxifaster::store::{FasterKv, FasterKvConfig};

// ============ StatsConfig Tests ============

#[test]
fn test_stats_config_default() {
    let config = StatsConfig::default();

    assert!(!config.enabled);
    assert_eq!(config.collection_interval, Duration::from_secs(1));
    assert!(!config.track_latency_histogram);
    assert!(config.track_per_operation);
}

#[test]
fn test_stats_config_builder() {
    let config = StatsConfig::new()
        .with_enabled(false)
        .with_collection_interval(Duration::from_secs(5))
        .with_latency_histogram(true);

    assert!(!config.enabled);
    assert_eq!(config.collection_interval, Duration::from_secs(5));
    assert!(config.track_latency_histogram);
}

#[test]
fn test_stats_config_new() {
    let config = StatsConfig::new();

    // Should be same as default
    assert!(!config.enabled);
}

// ============ StatsCollector Tests ============

#[test]
fn test_stats_collector_new() {
    let config = StatsConfig::default();
    let collector = StatsCollector::new(config);

    assert!(!collector.is_enabled());
}

#[test]
fn test_stats_collector_with_defaults() {
    let collector = StatsCollector::with_defaults();

    assert!(!collector.is_enabled());
}

#[test]
fn test_stats_collector_enable_disable() {
    let collector = StatsCollector::with_defaults();

    assert!(!collector.is_enabled());

    collector.enable();
    assert!(collector.is_enabled());

    collector.disable();
    assert!(!collector.is_enabled());
}

#[test]
fn test_stats_collector_config() {
    let config = StatsConfig::new().with_collection_interval(Duration::from_secs(10));
    let collector = StatsCollector::new(config);

    assert_eq!(
        collector.config().collection_interval,
        Duration::from_secs(10)
    );
}

#[test]
fn test_stats_collector_elapsed() {
    let collector = StatsCollector::with_defaults();

    // Elapsed time should exist
    let elapsed = collector.elapsed();
    let _ = elapsed.as_nanos();
}

#[test]
fn test_stats_collector_throughput_initial() {
    let collector = StatsCollector::with_defaults();

    // Initial throughput might be very low or zero
    let throughput = collector.throughput();
    assert!(throughput >= 0.0);
}

#[test]
fn test_stats_collector_reset() {
    let mut collector = StatsCollector::with_defaults();

    collector.reset();

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.total_operations, 0);
}

#[test]
fn test_stats_collector_snapshot() {
    let collector = StatsCollector::with_defaults();

    let snapshot = collector.snapshot();

    let _ = snapshot.elapsed.as_nanos();
    assert!(snapshot.throughput >= 0.0);
}

#[test]
fn test_stats_collector_disabled() {
    let config = StatsConfig::new().with_enabled(false);
    let collector = StatsCollector::new(config);

    assert!(!collector.is_enabled());
}

// ============ FasterKv Index Stats Integration ============

fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device).unwrap())
}

#[test]
fn test_index_stats_initial() {
    let store = create_test_store();

    let stats = store.index_stats();

    assert!(stats.table_size > 0);
    assert_eq!(stats.used_entries, 0);
    assert_eq!(stats.load_factor, 0.0);
}

#[test]
fn test_index_stats_after_inserts() {
    let store = create_test_store();

    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }
    }

    let stats = store.index_stats();

    assert!(stats.used_entries > 0);
    assert!(stats.load_factor > 0.0);
}

#[test]
fn test_log_stats_initial() {
    let store = create_test_store();

    let stats = store.log_stats();

    // Initial log should have valid addresses
    assert!(stats.tail_address.control() >= stats.begin_address.control());
}

#[test]
fn test_log_stats_after_inserts() {
    let store = create_test_store();

    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            session.upsert(i, i * 10);
        }
    }

    let stats = store.log_stats();

    // Tail should have advanced
    assert!(stats.tail_address.control() > stats.begin_address.control());
}

// ============ StatsSnapshot Tests ============

#[test]
fn test_stats_snapshot_fields() {
    let collector = StatsCollector::with_defaults();
    let snapshot = collector.snapshot();

    // All fields should be accessible
    let _ = snapshot.elapsed;
    let _ = snapshot.total_operations;
    let _ = snapshot.reads;
    let _ = snapshot.read_hits;
    let _ = snapshot.upserts;
    let _ = snapshot.rmws;
    let _ = snapshot.deletes;
    let _ = snapshot.pending;
    let _ = snapshot.hit_rate;
    let _ = snapshot.throughput;
    let _ = snapshot.avg_latency;
    let _ = snapshot.index_entries;
    let _ = snapshot.index_load_factor;
    let _ = snapshot.bytes_allocated;
}

#[test]
fn test_stats_snapshot_initial_values() {
    let collector = StatsCollector::with_defaults();
    let snapshot = collector.snapshot();

    assert_eq!(snapshot.total_operations, 0);
    assert_eq!(snapshot.reads, 0);
    assert_eq!(snapshot.upserts, 0);
    assert_eq!(snapshot.deletes, 0);
}

// ============ Edge Cases ============

#[test]
fn test_stats_collector_multiple_enable_disable() {
    let collector = StatsCollector::with_defaults();

    for _ in 0..10 {
        collector.disable();
        assert!(!collector.is_enabled());
        collector.enable();
        assert!(collector.is_enabled());
    }
}

#[test]
fn test_stats_config_zero_interval() {
    let config = StatsConfig::new().with_collection_interval(Duration::ZERO);

    assert_eq!(config.collection_interval, Duration::ZERO);
}

#[test]
fn test_stats_config_large_interval() {
    let config = StatsConfig::new().with_collection_interval(Duration::from_secs(3600));

    assert_eq!(config.collection_interval, Duration::from_secs(3600));
}

// ============ Thread Safety Tests ============

#[test]
fn test_stats_collector_concurrent_access() {
    use std::thread;

    let collector = Arc::new(StatsCollector::with_defaults());
    let num_threads = 4;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let collector = collector.clone();
            thread::spawn(move || {
                for _ in 0..100 {
                    let _ = collector.is_enabled();
                    let _ = collector.elapsed();
                    let _ = collector.throughput();
                    let _ = collector.snapshot();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_stats_collector_concurrent_enable_disable() {
    use std::thread;

    let collector = Arc::new(StatsCollector::with_defaults());
    let num_threads = 4;

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let collector = collector.clone();
            thread::spawn(move || {
                for _ in 0..50 {
                    if i % 2 == 0 {
                        collector.enable();
                    } else {
                        collector.disable();
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Collector should still be in valid state
    let _ = collector.is_enabled();
}
