//! FASTER Log 示例
//!
//! 演示独立的 FASTER Log 高性能日志功能
//!
//! 运行: cargo run --example faster_log

use oxifaster::device::NullDisk;
use oxifaster::log::{FasterLog, FasterLogConfig};

fn main() {
    println!("=== FASTER Log 示例 ===\n");

    // 1. 创建配置
    let config = FasterLogConfig {
        page_size: 1 << 16,    // 64 KB 页面
        memory_pages: 16,      // 16 个页面 = 1 MB 内存
        segment_size: 1 << 20, // 1 MB 段
        auto_commit_ms: 0,     // 禁用自动提交
    };

    // 2. 创建日志
    let device = NullDisk::new();
    let log = FasterLog::new(config, device);
    println!("日志创建成功!");

    // 3. 追加条目
    println!("\n--- 追加日志条目 ---");
    let mut addresses = Vec::new();

    let entries = [
        "第一条日志消息",
        "这是第二条消息",
        "系统启动成功",
        "用户登录: admin",
        "执行操作: 查询数据库",
        "操作完成",
        "用户登出: admin",
        "系统正常运行中",
    ];

    for (i, entry) in entries.iter().enumerate() {
        match log.append(entry.as_bytes()) {
            Ok(addr) => {
                println!("  [{}] 地址: {} - {}", i + 1, addr, entry);
                addresses.push(addr);
            }
            Err(e) => {
                println!("  [{}] 追加失败: {:?}", i + 1, e);
            }
        }
    }

    // 4. 提交日志
    println!("\n--- 提交日志 ---");
    match log.commit() {
        Ok(committed_addr) => {
            println!("已提交到地址: {committed_addr}");
        }
        Err(e) => {
            println!("提交失败: {e:?}");
        }
    }

    // 5. 读取特定条目
    println!("\n--- 读取特定条目 ---");
    for (i, addr) in addresses.iter().enumerate() {
        if let Some(data) = log.read_entry(*addr) {
            let text = String::from_utf8_lossy(&data);
            println!("  [{}] {} - {}", i + 1, addr, text);
        }
    }

    // 6. 扫描所有条目
    println!("\n--- 扫描所有已提交条目 ---");
    let mut count = 0;
    for (addr, data) in log.scan_all() {
        count += 1;
        let text = String::from_utf8_lossy(&data);
        println!("  {} - {} ({} 字节)", addr, text, data.len());
    }
    println!("共扫描到 {count} 条记录");

    // 7. 追加更多条目
    println!("\n--- 追加更多条目 ---");
    let more_entries = ["新增日志 A", "新增日志 B", "新增日志 C"];

    for entry in &more_entries {
        if let Ok(addr) = log.append(entry.as_bytes()) {
            println!("  追加: {addr} - {entry}");
        }
    }

    // 8. 再次提交
    log.commit().unwrap();
    println!("已提交新条目");

    // 9. 显示统计信息
    println!("\n--- 日志统计 ---");
    let stats = log.get_stats();
    println!("  尾部地址: {}", stats.tail_address);
    println!("  已提交地址: {}", stats.committed_address);
    println!("  起始地址: {}", stats.begin_address);
    println!("  页面大小: {} 字节", stats.page_size);
    println!("  缓冲页数: {}", stats.buffer_pages);

    // 10. 截断日志
    println!("\n--- 截断日志 ---");
    if !addresses.is_empty() {
        let truncate_addr = addresses[3]; // 截断前 4 条
        log.truncate_until(truncate_addr);
        println!("已截断到地址: {truncate_addr}");
        println!("新的起始地址: {}", log.get_begin_address());
    }

    // 11. 关闭日志
    println!("\n--- 关闭日志 ---");
    log.close();
    println!("日志已关闭");
    println!("是否已关闭: {}", log.is_closed());

    println!("\n=== 示例完成 ===");
}
