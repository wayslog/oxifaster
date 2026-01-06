//! Cold Index 示例
//!
//! 演示 oxifaster 的冷索引功能，用于 F2 架构中的磁盘索引。
//!
//! Cold Index 是一个基于磁盘的两级哈希索引：
//! - Level 1: 哈希块 ID (Hash Chunk ID)
//! - Level 2: 块内条目 (Entry within Chunk)
//!
//! 运行: cargo run --example cold_index

use std::path::PathBuf;

use oxifaster::index::{
    ColdIndexConfig, HashBucketEntry, KeyHash, DEFAULT_NUM_BUCKETS_PER_CHUNK, ENTRIES_PER_BUCKET,
};

fn main() {
    println!("=== oxifaster Cold Index 示例 ===\n");

    // 1. ColdIndexConfig 配置
    println!("--- 1. Cold Index 配置 ---");
    let config = ColdIndexConfig::new(
        1 << 14, // 16K 块 (table_size)
        32 * 1024 * 1024, // 32 MB 内存
        0.5, // 50% 可变区域
    )
    .with_root_path(PathBuf::from("cold_index_data"));

    println!("  表大小: {} 块", config.table_size);
    println!("  内存大小: {} MB", config.in_mem_size / (1024 * 1024));
    println!("  可变区域: {:.0}%", config.mutable_fraction * 100.0);
    println!("  存储路径: {:?}\n", config.root_path);

    // 2. 配置验证
    println!("--- 2. 配置验证 ---");
    match config.validate() {
        Ok(_) => println!("  配置有效: table_size 是 2 的幂"),
        Err(_) => println!("  配置无效"),
    }

    // 无效配置示例
    let invalid_config = ColdIndexConfig::new(1000, 32 * 1024 * 1024, 0.5);
    match invalid_config.validate() {
        Ok(_) => println!("  配置有效"),
        Err(_) => println!("  1000 不是 2 的幂: 配置无效\n"),
    }

    // 3. 默认配置
    println!("--- 3. 默认配置 ---");
    let default_config = ColdIndexConfig::default();
    println!("  默认表大小: {} 块", default_config.table_size);
    println!(
        "  默认内存: {} MB",
        default_config.in_mem_size / (1024 * 1024)
    );
    println!("  默认可变区域: {:.0}%", default_config.mutable_fraction * 100.0);
    println!("  默认路径: {:?}\n", default_config.root_path);

    // 4. 块和条目结构
    println!("--- 4. 块和条目结构 ---");
    println!("  每块桶数: {}", DEFAULT_NUM_BUCKETS_PER_CHUNK);
    println!("  每桶条目数: {}", ENTRIES_PER_BUCKET);
    println!(
        "  每块总条目数: {}\n",
        DEFAULT_NUM_BUCKETS_PER_CHUNK * ENTRIES_PER_BUCKET
    );

    // 5. KeyHash 演示
    println!("--- 5. KeyHash 演示 ---");
    let hash_value = 0x123456789ABCDEF0u64;
    let key_hash = KeyHash::new(hash_value);

    println!("  原始哈希值: 0x{:016X}", hash_value);
    println!("  tag (14位): 0x{:04X}", key_hash.tag());
    println!("  hash_table_index (用于定位): {}\n", key_hash.hash_table_index(1024));

    // 6. HashBucketEntry 演示
    println!("--- 6. HashBucketEntry 演示 ---");
    let entry = HashBucketEntry::new(oxifaster::Address::new(5, 1000));
    println!("  地址: {:?}", entry.address());
    println!("  是否未使用: {}", entry.is_unused());
    println!("  是否有效: {}", !entry.is_unused());

    let empty_entry = HashBucketEntry::INVALID;
    println!("  空条目地址: {:?}", empty_entry.address());
    println!("  空条目无效/未使用: {}\n", empty_entry.is_unused());

    // 7. 可变区域边界
    println!("--- 7. 可变区域边界 ---");
    let config_clamp = ColdIndexConfig::new(1 << 14, 32 * 1024 * 1024, 1.5);
    println!(
        "  设置 mutable_fraction=1.5 后: {}",
        config_clamp.mutable_fraction
    );

    let config_neg = ColdIndexConfig::new(1 << 14, 32 * 1024 * 1024, -0.5);
    println!(
        "  设置 mutable_fraction=-0.5 后: {}\n",
        config_neg.mutable_fraction
    );

    // 8. 路径配置
    println!("--- 8. 路径配置 ---");
    let config_custom_path = ColdIndexConfig::default()
        .with_root_path("/data/cold_index");
    println!("  自定义路径: {:?}", config_custom_path.root_path);

    let config_relative = ColdIndexConfig::default()
        .with_root_path("./local_cold_index");
    println!("  相对路径: {:?}\n", config_relative.root_path);

    // 9. 表大小建议
    println!("--- 9. 表大小建议 ---");
    let suggested_sizes = [
        (1 << 16, "64K 块 - 小型数据集"),
        (1 << 18, "256K 块 - 中型数据集"),
        (1 << 20, "1M 块 - 大型数据集"),
        (1 << 22, "4M 块 - 超大数据集"),
    ];
    for (size, description) in &suggested_sizes {
        println!("  {} - {}", size, description);
    }

    println!("\n=== 示例完成 ===");
}
