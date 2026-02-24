//! Statistics 示例
//!
//! 演示 oxifaster 的统计收集功能。
//!
//! 运行: cargo run --example statistics

mod util;

use std::sync::Arc;
use std::time::Duration;

use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::stats::{StatsCollector, StatsConfig};
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

fn run_with_device<D: StorageDevice>(device_name: &str, device: D) {
    println!("=== oxifaster Statistics 示例（{device_name}） ===\n");
    // 1. StatsConfig 配置
    println!("--- 1. StatsConfig 配置 ---");
    let config = StatsConfig::new()
        .with_enabled(true)
        .with_collection_interval(Duration::from_secs(1))
        .with_latency_histogram(false);

    println!("  启用统计: {}", config.enabled);
    println!("  采集间隔: {:?}", config.collection_interval);
    println!("  延迟直方图: {}", config.track_latency_histogram);
    println!("  按操作统计: {}\n", config.track_per_operation);

    // 2. 默认配置
    println!("--- 2. 默认配置 ---");
    let default_config = StatsConfig::default();
    println!("  默认启用: {}", default_config.enabled);
    println!("  默认间隔: {:?}", default_config.collection_interval);
    println!("  默认直方图桶数: {}\n", default_config.latency_buckets);

    // 3. StatsCollector 基本使用
    println!("--- 3. StatsCollector 基本使用 ---");
    let collector = StatsCollector::with_defaults();
    println!("  是否启用: {}", collector.is_enabled());
    println!("  运行时间: {:?}", collector.elapsed());
    println!("  吞吐量: {:.2} ops/sec\n", collector.throughput());

    // 4. 创建存储并执行操作
    println!("--- 4. 执行存储操作 ---");
    let store_config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<u64, u64, _>::new(store_config, device).unwrap());

    // 插入数据
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=1000 {
            session.upsert(i, i * 10);
        }
    }
    println!("  插入 1000 条记录");

    // 读取数据
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=500 {
            let _ = session.read(&i);
        }
    }
    println!("  读取 500 条记录");

    // 更新数据
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=200 {
            session.upsert(i, i * 100);
        }
    }
    println!("  更新 200 条记录");

    // 删除数据
    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=100 {
            session.delete(&i);
        }
    }
    println!("  删除 100 条记录\n");

    let snapshot = store.stats_snapshot();
    util::assert_observable_activity(&snapshot);
    util::print_prometheus(&snapshot);

    // 5. 获取统计快照
    println!("--- 5. 统计快照 ---");
    // 注意: FasterKv 内部使用统计收集器
    // 这里展示如何使用独立的收集器

    let collector = StatsCollector::new(StatsConfig::new().with_enabled(true));
    // 模拟一些操作记录
    let snapshot = collector.snapshot();

    println!("  运行时间: {:?}", snapshot.elapsed);
    println!("  总操作数: {}", snapshot.total_operations);
    println!("  吞吐量: {:.2} ops/sec", snapshot.throughput);
    println!();

    // 6. 启用/禁用统计
    println!("--- 6. 启用/禁用统计 ---");
    let collector = StatsCollector::with_defaults();
    println!(
        "  初始状态: {}",
        if collector.is_enabled() {
            "启用"
        } else {
            "禁用"
        }
    );

    collector.disable();
    println!(
        "  禁用后: {}",
        if collector.is_enabled() {
            "启用"
        } else {
            "禁用"
        }
    );

    collector.enable();
    println!(
        "  重新启用: {}\n",
        if collector.is_enabled() {
            "启用"
        } else {
            "禁用"
        }
    );

    // 7. 索引统计
    println!("--- 7. 索引统计 ---");
    let index_stats = store.index_stats();
    println!("  表大小: {}", index_stats.table_size);
    println!("  总条目: {}", index_stats.total_entries);
    println!("  已用条目: {}", index_stats.used_entries);
    println!("  有条目的桶: {}", index_stats.buckets_with_entries);
    println!("  负载因子: {:.4}\n", index_stats.load_factor);

    // 8. 日志统计
    println!("--- 8. 日志统计 ---");
    let log_stats = store.log_stats();
    println!("  起始地址: {:?}", log_stats.begin_address);
    println!("  头部地址: {:?}", log_stats.head_address);
    println!("  尾部地址: {:?}", log_stats.tail_address);
    println!("  只读地址: {:?}", log_stats.read_only_address);
    println!();

    // 9. 重置统计
    println!("--- 9. 重置统计 ---");
    let mut collector = StatsCollector::with_defaults();
    // 记录一些操作后重置
    collector.reset();
    let snapshot_after_reset = collector.snapshot();
    println!(
        "  重置后总操作数: {}",
        snapshot_after_reset.total_operations
    );
    println!("  重置后运行时间: {:?}", snapshot_after_reset.elapsed);

    println!("\n=== 示例完成 ===");
}

fn main() {
    run_with_device("NullDisk（纯内存）", NullDisk::new());

    let dir = tempdir().expect("创建临时目录失败");
    let data_path = dir.path().join("oxifaster_statistics.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("创建数据文件失败");
    run_with_device(
        &format!("FileSystemDisk（文件持久化：{}）", data_path.display()),
        fs_device,
    );
}
