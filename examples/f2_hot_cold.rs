//! F2 Hot-Cold Architecture 示例
//!
//! 演示 oxifaster 的 F2 (Fast & Fair) 两级存储架构。
//!
//! F2 架构将数据分为:
//! - 热存储 (Hot Store): 存储频繁访问的数据，使用内存索引
//! - 冷存储 (Cold Store): 存储不频繁访问的数据，可使用磁盘索引
//!
//! 运行: cargo run --example f2_hot_cold

use std::path::PathBuf;
use std::time::Duration;

use oxifaster::cache::ReadCacheConfig;
use oxifaster::f2::{
    ColdStoreConfig, F2CheckpointPhase, F2CheckpointState, F2CompactionConfig, F2Config,
    HotStoreConfig, ReadOperationStage, RmwOperationStage, StoreCheckpointStatus, StoreType,
};

fn main() {
    println!("=== oxifaster F2 Hot-Cold Architecture 示例 ===\n");

    // 1. HotStoreConfig 配置
    println!("--- 1. 热存储配置 (HotStoreConfig) ---");
    let hot_config = HotStoreConfig::new()
        .with_index_size(1 << 16) // 64K 桶
        .with_log_mem_size(64 * 1024 * 1024) // 64 MB 日志内存
        .with_mutable_fraction(0.6) // 60% 可变区域
        .with_log_path(PathBuf::from("hot_store.log"))
        .with_read_cache(Some(
            ReadCacheConfig::new(16 * 1024 * 1024), // 16 MB 读缓存
        ));

    println!("  索引大小: {} 桶", hot_config.index_size);
    println!(
        "  日志内存: {} MB",
        hot_config.log_mem_size / (1024 * 1024)
    );
    println!("  可变区域: {:.0}%", hot_config.mutable_fraction * 100.0);
    println!("  日志路径: {:?}", hot_config.log_path);
    println!(
        "  读缓存: {}\n",
        if hot_config.read_cache.is_some() {
            "启用"
        } else {
            "禁用"
        }
    );

    // 2. ColdStoreConfig 配置
    println!("--- 2. 冷存储配置 (ColdStoreConfig) ---");
    let cold_config = ColdStoreConfig::new()
        .with_index_size(1 << 18) // 256K 桶 (比热存储大)
        .with_log_mem_size(32 * 1024 * 1024) // 32 MB 日志内存
        .with_log_path(PathBuf::from("cold_store.log"));

    println!("  索引大小: {} 桶", cold_config.index_size);
    println!(
        "  日志内存: {} MB",
        cold_config.log_mem_size / (1024 * 1024)
    );
    println!("  可变区域: {:.0}%", cold_config.mutable_fraction * 100.0);
    println!("  日志路径: {:?}\n", cold_config.log_path);

    // 3. F2CompactionConfig 配置
    println!("--- 3. F2 压缩配置 ---");
    let compaction_config = F2CompactionConfig::new()
        .with_hot_store_enabled(true)
        .with_cold_store_enabled(true)
        .with_hot_log_size_budget(1 << 30) // 1 GB
        .with_cold_log_size_budget(10 << 30) // 10 GB
        .with_trigger_percentage(0.9) // 90% 触发压缩
        .with_check_interval(Duration::from_secs(5));

    println!(
        "  热存储压缩: {}",
        if compaction_config.hot_store_enabled {
            "启用"
        } else {
            "禁用"
        }
    );
    println!(
        "  冷存储压缩: {}",
        if compaction_config.cold_store_enabled {
            "启用"
        } else {
            "禁用"
        }
    );
    println!(
        "  热存储预算: {} GB",
        compaction_config.hot_log_size_budget / (1 << 30)
    );
    println!(
        "  冷存储预算: {} GB",
        compaction_config.cold_log_size_budget / (1 << 30)
    );
    println!(
        "  触发阈值: {:.0}%",
        compaction_config.trigger_percentage * 100.0
    );
    println!("  检查间隔: {:?}\n", compaction_config.check_interval);

    // 4. 完整的 F2Config
    println!("--- 4. 完整 F2 配置 ---");
    let f2_config = F2Config::new()
        .with_hot_store(hot_config.clone())
        .with_cold_store(cold_config.clone())
        .with_compaction(compaction_config.clone());

    match f2_config.validate() {
        Ok(_) => println!("  配置验证: 通过"),
        Err(e) => println!("  配置验证: 失败 - {}", e),
    }
    println!();

    // 5. StoreType 演示
    println!("--- 5. 存储类型 (StoreType) ---");
    let hot_type = StoreType::Hot;
    let cold_type = StoreType::Cold;
    println!("  热存储类型: {:?}", hot_type);
    println!("  冷存储类型: {:?}", cold_type);
    println!("  相等比较: {}\n", hot_type == StoreType::Hot);

    // 6. 操作阶段
    println!("--- 6. 操作阶段 ---");
    println!("  读取阶段:");
    println!("    {:?}", ReadOperationStage::HotLogRead);
    println!("    {:?}", ReadOperationStage::ColdLogRead);
    println!("  RMW 阶段:");
    println!("    {:?}", RmwOperationStage::HotLogRmw);
    println!("    {:?}", RmwOperationStage::ColdLogRead);
    println!("    {:?}\n", RmwOperationStage::HotLogConditionalInsert);

    // 7. F2CheckpointState 演示
    println!("--- 7. F2 检查点状态 ---");
    let mut cp_state = F2CheckpointState::new();
    println!("  初始状态:");
    println!("    正在检查点: {}", cp_state.is_in_progress());
    println!("    版本: {}", cp_state.version());

    // 初始化检查点
    let token = uuid::Uuid::new_v4();
    cp_state.initialize(token, 4);
    println!("  初始化后:");
    println!("    Token: {}", cp_state.token());
    println!("    版本: {}", cp_state.version());

    // 设置持久化序列号
    cp_state.set_persistent_serial_num(0, 100);
    cp_state.set_persistent_serial_num(1, 200);
    println!("    线程 0 序列号: {}", cp_state.get_persistent_serial_num(0));
    println!("    线程 1 序列号: {}\n", cp_state.get_persistent_serial_num(1));

    // 8. F2CheckpointPhase 演示
    println!("--- 8. 检查点阶段 ---");
    let phases = [
        F2CheckpointPhase::Rest,
        F2CheckpointPhase::HotStoreCheckpoint,
        F2CheckpointPhase::ColdStoreCheckpoint,
        F2CheckpointPhase::Recover,
    ];
    for phase in &phases {
        println!("  {:?}", phase);
    }
    println!();

    // 9. StoreCheckpointStatus 演示
    println!("--- 9. 存储检查点状态 ---");
    let statuses = [
        StoreCheckpointStatus::Idle,
        StoreCheckpointStatus::Requested,
        StoreCheckpointStatus::Active,
        StoreCheckpointStatus::Finished,
        StoreCheckpointStatus::Failed,
    ];
    for status in &statuses {
        println!(
            "  {:?} - 已完成: {}",
            status,
            if status.is_done() { "是" } else { "否" }
        );
    }
    println!();

    // 10. 配置验证边界情况
    println!("--- 10. 配置验证边界情况 ---");

    // 太小的预算会验证失败
    let mut invalid_config = F2Config::default();
    invalid_config.compaction.hot_log_size_budget = 1 << 20; // 1 MB - 太小

    match invalid_config.validate() {
        Ok(_) => println!("  1 MB 热存储预算: 通过"),
        Err(e) => println!("  1 MB 热存储预算: 失败 - {}", e),
    }

    // 禁用压缩后可以使用小预算
    let mut config_disabled = F2Config::default();
    config_disabled.compaction.hot_store_enabled = false;
    config_disabled.compaction.hot_log_size_budget = 1 << 20;

    match config_disabled.validate() {
        Ok(_) => println!("  禁用压缩后小预算: 通过"),
        Err(e) => println!("  禁用压缩后小预算: 失败 - {}", e),
    }

    // 11. 可变区域边界
    println!("\n--- 11. 可变区域边界 ---");
    let config_clamp = HotStoreConfig::new()
        .with_mutable_fraction(1.5); // 会被 clamp 到 1.0
    println!("  设置 mutable_fraction=1.5 后: {}", config_clamp.mutable_fraction);

    let config_zero = HotStoreConfig::new()
        .with_mutable_fraction(-0.5); // 会被 clamp 到 0.0
    println!("  设置 mutable_fraction=-0.5 后: {}", config_zero.mutable_fraction);

    println!("\n=== 示例完成 ===");
}
