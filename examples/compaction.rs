//! Log Compaction 示例
//!
//! 演示 oxifaster 的日志压缩功能，包括基础压缩、自动压缩和并发压缩。
//!
//! 运行: cargo run --example compaction

use std::sync::Arc;

use oxifaster::compaction::{
    CompactionConfig, CompactionStats, Compactor, ConcurrentCompactionConfig, ConcurrentCompactor,
};
use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::scan::ScanRange;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::Address;
use tempfile::tempdir;

fn run_with_device<D: StorageDevice>(device_name: &str, device: D) {
    println!("=== oxifaster Log Compaction 示例（{device_name}） ===\n");
    // 1. 基础 Compaction 配置
    println!("--- 1. Compaction 配置 ---");
    let config = CompactionConfig::new()
        .with_target_utilization(0.5) // 目标 50% 空间利用率
        .with_min_compact_bytes(1024) // 最小压缩 1 KB
        .with_max_compact_bytes(1 << 20) // 最大压缩 1 MB
        .with_num_threads(2); // 2 线程

    println!("  目标利用率: {:.0}%", config.target_utilization * 100.0);
    println!("  最小压缩大小: {} bytes", config.min_compact_bytes);
    println!("  最大压缩大小: {} bytes", config.max_compact_bytes);
    println!("  线程数: {}\n", config.num_threads);

    // 2. 创建带压缩配置的存储
    println!("--- 2. 创建存储 ---");
    let store_config = FasterKvConfig {
        table_size: 1 << 10,      // 1K 哈希桶
        log_memory_size: 1 << 18, // 256 KB 日志内存 (小一点便于触发压缩)
        page_size_bits: 14,       // 16 KB 页面
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<u64, u64, _>::with_compaction_config(
        store_config,
        device,
        config.clone(),
    ));
    println!("  存储创建成功\n");

    // 3. 插入大量数据以创建可压缩的日志
    println!("--- 3. 插入和删除数据 ---");
    {
        let mut session = store.start_session();

        // 先插入一批数据
        for i in 1u64..=500 {
            session.upsert(i, i * 100);
        }
        println!("  插入 500 条记录");

        // 删除一半 (创建 tombstones)
        for i in 1u64..=250 {
            session.delete(&i);
        }
        println!("  删除 250 条记录 (创建 tombstones)");

        // 更新一部分 (创建旧版本)
        for i in 251u64..=400 {
            session.upsert(i, i * 200); // 更新值
        }
        println!("  更新 150 条记录\n");
    }

    // 4. 显示日志统计
    println!("--- 4. 压缩前日志统计 ---");
    let log_stats = store.log_stats();
    println!("  起始地址: {}", log_stats.begin_address);
    println!("  尾部地址: {}", log_stats.tail_address);
    println!(
        "  日志大小: {} bytes\n",
        log_stats.tail_address.control() - log_stats.begin_address.control()
    );

    // 5. 使用 Compactor 计算压缩范围
    println!("--- 5. Compactor 基本使用 ---");
    let compactor = Compactor::with_config(config.clone());
    println!("  Compactor 已创建");
    println!("  是否正在压缩: {}", compactor.is_in_progress());

    // 尝试启动压缩
    if compactor.try_start().is_ok() {
        println!("  成功获取压缩锁");

        // 计算压缩范围
        let begin = Address::from_control(64);
        let head = Address::new(1, 0);
        if let Some(range) = compactor.calculate_scan_range(begin, head, None) {
            println!(
                "  压缩范围: {} -> {}",
                range.begin.control(),
                range.end.control()
            );
        }

        compactor.complete();
        println!("  压缩锁已释放\n");
    }

    // 6. CompactionStats 演示
    println!("--- 6. CompactionStats 示例 ---");
    let stats = CompactionStats {
        records_scanned: 500,
        records_compacted: 200,
        records_skipped: 300,
        tombstones_found: 250,
        bytes_scanned: 50000,
        bytes_compacted: 20000,
        bytes_reclaimed: 30000,
        duration_ms: 50,
    };
    println!("  扫描记录数: {}", stats.records_scanned);
    println!("  压缩记录数: {}", stats.records_compacted);
    println!("  跳过记录数: {}", stats.records_skipped);
    println!("  发现 tombstones: {}", stats.tombstones_found);
    println!("  存活率: {:.1}%", stats.live_ratio() * 100.0);
    println!("  回收率: {:.1}%", stats.compaction_ratio() * 100.0);
    println!("  耗时: {} ms\n", stats.duration_ms);

    // 7. 并发压缩配置
    println!("--- 7. 并发压缩 (ConcurrentCompactor) ---");
    let concurrent_config = ConcurrentCompactionConfig::new(4) // 4 线程
        .with_pages_per_chunk(8) // 每块 8 页
        .with_page_size(1 << 14); // 16 KB 页面

    println!("  线程数: {}", concurrent_config.num_threads);
    println!("  每块页数: {}", concurrent_config.pages_per_chunk);
    println!("  页面大小: {} bytes", concurrent_config.page_size);

    let concurrent_compactor = ConcurrentCompactor::new(concurrent_config);
    println!("  ConcurrentCompactor 已创建");
    println!(
        "  是否正在压缩: {}\n",
        concurrent_compactor.is_in_progress()
    );

    // 8. ScanRange 演示
    println!("--- 8. ScanRange 使用 ---");
    let range = ScanRange::new(Address::new(0, 100), Address::new(1, 0));

    println!("  起始地址: {}", range.begin.control());
    println!("  结束地址: {}", range.end.control());
    println!("  是否为空: {}", range.is_empty());
    println!(
        "  范围大小: {} bytes\n",
        range.end.control() - range.begin.control()
    );

    // 9. 验证数据完整性 (压缩后未删除的数据应该仍然存在)
    println!("--- 9. 验证数据完整性 ---");
    {
        let mut session = store.start_session();
        let mut found = 0;
        let mut not_found = 0;

        for i in 1u64..=500 {
            match session.read(&i) {
                Ok(Some(_)) => found += 1,
                Ok(None) => not_found += 1,
                Err(_) => {}
            }
        }

        println!("  找到记录: {found}");
        println!("  未找到记录: {not_found} (已删除)\n");
    }

    // 10. should_compact_record 逻辑演示
    println!("--- 10. 记录压缩判断逻辑 ---");
    let compactor = Compactor::new();
    let addr1 = Address::new(1, 100);
    let addr2 = Address::new(2, 200);

    // 最新版本，非 tombstone -> 应该压缩
    let should1 = compactor.should_compact_record(addr1, addr1, false);
    println!(
        "  最新版本, 非 tombstone: {}",
        if should1 { "压缩" } else { "跳过" }
    );

    // 最新版本，是 tombstone -> 根据配置决定
    let should2 = compactor.should_compact_record(addr1, addr1, true);
    println!(
        "  最新版本, tombstone: {}",
        if should2 { "压缩" } else { "跳过" }
    );

    // 旧版本 -> 应该跳过
    let should3 = compactor.should_compact_record(addr1, addr2, false);
    println!("  旧版本: {}", if should3 { "压缩" } else { "跳过" });

    println!("\n=== 示例完成 ===");
}

fn main() {
    run_with_device("NullDisk（纯内存）", NullDisk::new());

    let dir = tempdir().expect("创建临时目录失败");
    let data_path = dir.path().join("oxifaster_compaction.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("创建数据文件失败");
    run_with_device(
        &format!("FileSystemDisk（文件持久化：{}）", data_path.display()),
        fs_device,
    );
}
