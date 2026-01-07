//! 基本的 KV 存储示例
//!
//! 演示 FasterKV 的基本 CRUD 操作
//!
//! 运行: cargo run --example basic_kv

use std::sync::Arc;

use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

fn run_with_device<D: StorageDevice>(device_name: &str, device: D) {
    println!("--- 使用 {device_name} ---\n");
    // 1. 创建配置
    let config = FasterKvConfig {
        table_size: 1 << 16,      // 64K 哈希桶
        log_memory_size: 1 << 24, // 16 MB 日志内存
        page_size_bits: 20,       // 1 MB 页面
        mutable_fraction: 0.9,
    };

    // 3. 创建 FasterKV 存储
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    println!("存储创建成功!");

    // 4. 启动会话
    let mut session = store.start_session();
    println!("会话已启动\n");

    // 5. 插入数据 (Upsert)
    println!("--- 插入数据 ---");
    for i in 1..=10u64 {
        let status = session.upsert(i, i * 100);
        if status == Status::Ok {
            println!("  插入: key={}, value={}", i, i * 100);
        }
    }

    // 6. 读取数据 (Read)
    println!("\n--- 读取数据 ---");
    for i in 1..=10u64 {
        match session.read(&i) {
            Ok(Some(value)) => println!("  读取: key={i}, value={value}"),
            Ok(None) => println!("  读取: key={i}, 未找到"),
            Err(e) => println!("  读取: key={i}, 错误: {e:?}"),
        }
    }

    // 7. 更新数据
    println!("\n--- 更新数据 ---");
    for i in 1..=5u64 {
        let new_value = i * 1000;
        let status = session.upsert(i, new_value);
        if status == Status::Ok {
            println!("  更新: key={i}, new_value={new_value}");
        }
    }

    // 8. 验证更新
    println!("\n--- 验证更新 ---");
    for i in 1..=10u64 {
        if let Ok(Some(value)) = session.read(&i) {
            let expected = if i <= 5 { i * 1000 } else { i * 100 };
            let status = if value == expected { "✓" } else { "✗" };
            println!("  {status} key={i}, value={value} (期望: {expected})");
        }
    }

    // 9. 删除数据
    println!("\n--- 删除数据 ---");
    for i in 1..=3u64 {
        let status = session.delete(&i);
        println!("  删除: key={i}, 状态: {status:?}");
    }

    // 10. 验证删除
    println!("\n--- 验证删除 ---");
    for i in 1..=5u64 {
        match session.read(&i) {
            Ok(Some(value)) => println!("  key={i} 存在, value={value}"),
            Ok(None) => println!("  key={i} 已删除"),
            Err(e) => println!("  key={i} 错误: {e:?}"),
        }
    }

    // 11. 显示统计信息
    println!("\n--- 统计信息 ---");
    let index_stats = store.index_stats();
    println!("  哈希索引:");
    println!("    表大小: {}", index_stats.table_size);
    println!("    已用条目: {}", index_stats.used_entries);
    println!("    负载因子: {:.2}%", index_stats.load_factor * 100.0);

    let log_stats = store.log_stats();
    println!("  混合日志:");
    println!("    尾部地址: {}", log_stats.tail_address);
    println!("    可变区域: {} 字节", log_stats.mutable_bytes);

    println!("\n=== 示例完成 ===");
}

fn main() {
    println!("=== oxifaster 基本 KV 存储示例 ===\n");

    run_with_device("NullDisk（纯内存）", NullDisk::new());

    let dir = tempdir().expect("创建临时目录失败");
    let data_path = dir.path().join("oxifaster_basic_kv.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("创建数据文件失败");
    run_with_device(
        &format!("FileSystemDisk（文件持久化：{}）", data_path.display()),
        fs_device,
    );
}
