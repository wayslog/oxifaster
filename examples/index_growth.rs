//! Index Growth 示例
//!
//! 演示 oxifaster 的哈希索引动态扩容功能。
//!
//! 运行: cargo run --example index_growth

mod util;

use std::sync::Arc;

use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::index::{
    calculate_num_chunks, get_chunk_bounds, GrowConfig, GrowResult, GrowState, MemHashIndex,
    MemHashIndexConfig, HASH_TABLE_CHUNK_SIZE,
};
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::Status;
use tempfile::tempdir;

fn run_with_device<D: StorageDevice>(device_name: &str, device: D) {
    println!("=== oxifaster Index Growth 示例（{device_name}） ===\n");
    // 1. GrowConfig 配置
    println!("--- 1. GrowConfig 配置 ---");
    let grow_config = GrowConfig::new()
        .with_min_load_factor(0.5)
        .with_max_load_factor(0.9)
        .with_growth_factor(2)
        .with_auto_grow(false); // 手动触发

    println!(
        "  最小负载因子: {:.0}%",
        grow_config.min_load_factor * 100.0
    );
    println!(
        "  最大负载因子: {:.0}%",
        grow_config.max_load_factor * 100.0
    );
    println!("  扩容倍数: {}x", grow_config.growth_factor);
    println!(
        "  自动扩容: {}\n",
        if grow_config.auto_grow {
            "启用"
        } else {
            "禁用"
        }
    );

    // 2. 创建哈希索引
    println!("--- 2. 创建哈希索引 ---");
    let mut index = MemHashIndex::with_grow_config(grow_config.clone());
    let config = MemHashIndexConfig::new(1 << 10); // 1024 buckets
    index.initialize(&config);

    println!("  初始大小: {} 桶", index.size());
    println!("  当前版本: {}\n", index.version());

    // 3. 插入数据来增加负载因子
    println!("--- 3. 插入数据 ---");
    let store_config = FasterKvConfig {
        table_size: 1 << 10,      // 匹配索引大小
        log_memory_size: 1 << 22, // 4 MB
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<u64, u64, _>::new(store_config, device));

    {
        let mut session = store.start_session().unwrap();
        for i in 1u64..=500 {
            session.upsert(i, i * 100);
        }
    }

    let stats = store.index_stats();
    println!("  已插入: 500 条记录");
    println!("  已用条目: {}", stats.used_entries);
    println!("  负载因子: {:.2}%\n", stats.load_factor * 100.0);

    // Trigger store index growth (public API) and verify it is observed.
    let current_size = store.index_size();
    let new_size = current_size * 2;
    let status = store.grow_index_with_callback(new_size, None);
    assert_eq!(status, Status::Ok);
    let snapshot = store.stats_snapshot();
    assert!(snapshot.index_grows_started >= 1);
    assert!(snapshot.index_grows_completed + snapshot.index_grows_failed >= 1);

    // 4. GrowState 基本使用
    println!("--- 4. GrowState 演示 ---");
    let mut grow_state = GrowState::new();
    println!("  初始状态:");
    println!("    正在扩容: {}", grow_state.is_in_progress());

    // 模拟初始化扩容
    grow_state.initialize(0, 10);
    println!("  初始化后:");
    println!("    正在扩容: {}", grow_state.is_in_progress());
    println!("    旧版本: {}", grow_state.old_version());
    println!("    新版本: {}", grow_state.new_version());
    println!("    总块数: {}", grow_state.num_chunks());

    // 获取并完成一些块
    println!("  处理块:");
    for _ in 0..3 {
        if let Some(chunk) = grow_state.get_next_chunk() {
            println!("    获取块 {chunk}");
            grow_state.complete_chunk();
        }
    }
    let (completed, total) = grow_state.progress();
    println!(
        "  进度: {}/{} ({}%)\n",
        completed,
        total,
        completed * 100 / total
    );

    // 5. 计算扩容块数
    println!("--- 5. 扩容块计算 ---");
    let table_sizes = [1024u64, 16384, 65536, 262144];
    println!("  块大小: {HASH_TABLE_CHUNK_SIZE} 桶");
    println!("  不同表大小需要的块数:");
    for size in &table_sizes {
        let chunks = calculate_num_chunks(*size);
        println!("    {size} 桶 -> {chunks} 块");
    }
    println!();

    // 6. 块边界计算
    println!("--- 6. 块边界计算 ---");
    let total_buckets = 50000u64;
    println!("  总桶数: {total_buckets}");
    for chunk_idx in 0..3 {
        let (start, end) = get_chunk_bounds(chunk_idx, total_buckets);
        println!("  块 {chunk_idx}: 桶 {start} -> {end}");
    }
    let last_chunk = calculate_num_chunks(total_buckets) - 1;
    let (start, end) = get_chunk_bounds(last_chunk, total_buckets);
    println!("  块 {last_chunk} (最后): 桶 {start} -> {end}\n");

    // 7. GrowResult 演示
    println!("--- 7. GrowResult 演示 ---");
    let result = GrowResult::success(1024, 2048, 800, 50);
    println!("  成功结果:");
    println!("    成功: {}", result.success);
    println!("    旧大小: {} 桶", result.old_size);
    println!("    新大小: {} 桶", result.new_size);
    println!("    迁移条目: {}", result.entries_migrated);
    println!("    扩容比率: {:.1}x", result.growth_ratio());
    println!("    耗时: {} ms", result.duration_ms);

    // 带数据丢失警告的结果
    let result_with_warning = GrowResult::with_data_loss_tracking(
        1024, 2048, 780, 60, 15, // overflow_buckets_skipped
        5,  // rehash_failures
    );
    println!("\n  带警告的结果:");
    println!("    成功: {}", result_with_warning.success);
    println!(
        "    跳过的溢出桶: {}",
        result_with_warning.overflow_buckets_skipped
    );
    println!("    rehash 失败: {}", result_with_warning.rehash_failures);
    println!(
        "    有数据丢失警告: {}\n",
        result_with_warning.has_data_loss_warning()
    );

    // 8. should_grow 检查
    println!("--- 8. 自动扩容判断 ---");
    let auto_config = GrowConfig::new()
        .with_max_load_factor(0.8)
        .with_auto_grow(true);

    let test_factors = [0.5, 0.75, 0.8, 0.9];
    for factor in &test_factors {
        let should = auto_config.should_grow(*factor);
        println!(
            "  负载因子 {:.0}%: {}",
            factor * 100.0,
            if should {
                "应该扩容"
            } else {
                "不需扩容"
            }
        );
    }

    // 9. MemHashIndex 扩容状态
    println!("\n--- 9. MemHashIndex 扩容状态 ---");
    let mut index2 = MemHashIndex::with_grow_config(
        GrowConfig::new()
            .with_auto_grow(true)
            .with_max_load_factor(0.9),
    );
    let config2 = MemHashIndexConfig::new(1024);
    index2.initialize(&config2);

    println!("  索引大小: {}", index2.size());
    println!("  版本: {}", index2.version());
    println!("  扩容中: {}", index2.is_grow_in_progress());
    println!("  应该扩容: {} (基于当前负载因子)\n", index2.should_grow());

    // 10. 扩容配置的边界情况
    println!("--- 10. 配置边界情况 ---");
    let clamped_config = GrowConfig::new()
        .with_max_load_factor(1.5) // 会被 clamp 到 1.0
        .with_growth_factor(1); // 会被 clamp 到 2

    println!(
        "  设置 max_load_factor=1.5 后: {}",
        clamped_config.max_load_factor
    );
    println!(
        "  设置 growth_factor=1 后: {}",
        clamped_config.growth_factor
    );

    let snapshot = store.stats_snapshot();
    util::assert_observable_activity(&snapshot);
    util::print_prometheus(&snapshot);

    println!("\n=== 示例完成 ===");
}

fn main() {
    run_with_device("NullDisk（纯内存）", NullDisk::new());

    let dir = tempdir().expect("创建临时目录失败");
    let data_path = dir.path().join("oxifaster_index_growth.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("创建数据文件失败");
    run_with_device(
        &format!("FileSystemDisk（文件持久化：{}）", data_path.display()),
        fs_device,
    );
}
