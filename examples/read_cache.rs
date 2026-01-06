//! Read Cache 示例
//!
//! 演示 oxifaster 的 Read Cache 功能，用于缓存热点数据以提高读取性能。
//!
//! 运行: cargo run --example read_cache

use std::sync::Arc;

use oxifaster::cache::ReadCacheConfig;
use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn main() {
    println!("=== oxifaster Read Cache 示例 ===\n");

    // 1. 配置 Read Cache
    println!("--- 1. 配置 Read Cache ---");
    let cache_config = ReadCacheConfig::new(16 * 1024 * 1024) // 16 MB 缓存
        .with_mutable_fraction(0.9) // 90% 可变区域
        .with_pre_allocate(false) // 按需分配
        .with_copy_to_tail(true); // 启用 Copy-to-Tail 优化

    println!("  缓存大小: {} MB", cache_config.mem_size / (1024 * 1024));
    println!("  可变区域: {:.0}%", cache_config.mutable_fraction * 100.0);
    println!(
        "  只读区域: {} MB",
        cache_config.read_only_size() / (1024 * 1024)
    );
    println!("  Copy-to-Tail: {}\n", cache_config.copy_to_tail);

    // 2. 创建带 Read Cache 的 FasterKV 存储
    println!("--- 2. 创建存储 ---");
    let store_config = FasterKvConfig {
        table_size: 1 << 14, // 16K 哈希桶
        log_memory_size: 1 << 22, // 4 MB 日志内存
        page_size_bits: 18,  // 256 KB 页面
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::with_read_cache(
        store_config,
        device,
        cache_config,
    ));
    println!("  存储创建成功 (带 Read Cache)\n");

    // 3. 插入测试数据
    println!("--- 3. 插入测试数据 ---");
    let num_keys = 1000u64;
    {
        let mut session = store.start_session();
        for i in 1..=num_keys {
            let status = session.upsert(i, i * 100);
            assert_eq!(status, Status::Ok);
        }
    }
    println!("  插入 {} 条记录\n", num_keys);

    // 4. 首次读取 (缓存冷启动)
    println!("--- 4. 首次读取 (缓存冷启动) ---");
    {
        let mut session = store.start_session();

        // 读取一部分数据，这些数据会被缓存
        let hot_keys: Vec<u64> = (1..=100).collect();
        for key in &hot_keys {
            let result = session.read(key);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(key * 100));
        }
        println!("  读取 {} 个热点 key\n", hot_keys.len());
    }

    // 5. 显示缓存统计
    println!("--- 5. 缓存统计 (首次读取后) ---");
    if let Some(stats) = store.read_cache_stats() {
        println!("  读取次数: {}", stats.read_calls());
        println!("  缓存命中: {}", stats.read_hits());
        println!("  缓存未命中: {}", stats.read_misses());
        println!("  命中率: {:.2}%", stats.hit_rate() * 100.0);
        println!("  插入次数: {}", stats.insert_calls());
        println!("  插入成功率: {:.2}%\n", stats.insert_rate() * 100.0);
    }

    // 6. 重复读取热点数据 (应有更高命中率)
    println!("--- 6. 重复读取热点数据 ---");
    {
        let mut session = store.start_session();

        // 多次读取相同的热点数据
        let hot_keys: Vec<u64> = (1..=100).collect();
        for _ in 0..5 {
            for key in &hot_keys {
                let result = session.read(key);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Some(key * 100));
            }
        }
        println!("  重复读取热点 key {} 次\n", 5 * hot_keys.len());
    }

    // 7. 显示最终缓存统计
    println!("--- 7. 最终缓存统计 ---");
    if let Some(stats) = store.read_cache_stats() {
        let summary = stats.summary();
        println!("  总读取次数: {}", summary.read_calls);
        println!("  缓存命中: {}", summary.read_hits);
        println!("  缓存未命中: {}", summary.read_misses);
        println!("  命中率: {:.2}%", summary.hit_rate * 100.0);
        println!("  总插入次数: {}", summary.insert_calls);
        println!("  插入成功率: {:.2}%", summary.insert_rate * 100.0);
        println!("  淘汰记录数: {}", summary.evicted_records);
    }

    // 8. 清空缓存
    println!("\n--- 8. 清空缓存 ---");
    store.clear_read_cache();
    if let Some(stats) = store.read_cache_stats() {
        println!("  清空后读取次数: {}", stats.read_calls());
        println!("  清空后命中数: {}\n", stats.read_hits());
    }

    // 9. 演示缓存失效 (更新数据)
    println!("--- 9. 演示缓存失效 ---");
    {
        let mut session = store.start_session();

        // 先读取一个 key
        let key = 42u64;
        let _ = session.read(&key);

        // 更新该 key
        session.upsert(key, 99999);

        // 再次读取，应该得到新值
        let result = session.read(&key);
        assert_eq!(result.unwrap(), Some(99999));
        println!("  key={} 更新后读取成功: value=99999\n", key);
    }

    // 10. 显示缓存配置
    println!("--- 10. 缓存配置信息 ---");
    if let Some(config) = store.read_cache_config() {
        println!("  总大小: {} bytes", config.mem_size);
        println!("  可变区域大小: {} bytes", config.mutable_size());
        println!("  只读区域大小: {} bytes", config.read_only_size());
        println!("  预分配: {}", config.pre_allocate);
        println!("  Copy-to-Tail: {}", config.copy_to_tail);
    }

    println!("\n=== 示例完成 ===");
}
