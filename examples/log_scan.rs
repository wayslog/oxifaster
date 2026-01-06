//! Log Scan 示例
//!
//! 演示 oxifaster 的日志扫描功能，用于遍历 HybridLog 中的记录。
//!
//! 运行: cargo run --example log_scan

use oxifaster::scan::{LogPageStatus, ScanRange};
use oxifaster::Address;

fn main() {
    println!("=== oxifaster Log Scan 示例 ===\n");

    // 1. ScanRange 基本使用
    println!("--- 1. ScanRange 基本使用 ---");
    let begin = Address::new(0, 0);
    let end = Address::new(10, 0);
    let range = ScanRange::new(begin, end);

    println!("  起始地址: {:?}", range.begin);
    println!("  结束地址: {:?}", range.end);
    println!("  是否为空: {}", range.is_empty());
    println!("  页面数量: {}\n", range.page_count());

    // 2. 空范围
    println!("--- 2. 空范围检测 ---");
    let empty_range1 = ScanRange::new(Address::new(5, 0), Address::new(5, 0));
    println!("  相同地址 (5,0) -> (5,0): 空={}", empty_range1.is_empty());

    let empty_range2 = ScanRange::new(Address::new(10, 0), Address::new(5, 0));
    println!("  反向地址 (10,0) -> (5,0): 空={}", empty_range2.is_empty());

    let valid_range = ScanRange::new(Address::new(0, 0), Address::new(1, 0));
    println!("  有效地址 (0,0) -> (1,0): 空={}\n", valid_range.is_empty());

    // 3. LogPageStatus 状态
    println!("--- 3. LogPageStatus 状态 ---");
    let statuses = [
        ("Uninitialized", LogPageStatus::Uninitialized),
        ("Ready", LogPageStatus::Ready),
        ("Pending", LogPageStatus::Pending),
        ("Unavailable", LogPageStatus::Unavailable),
    ];
    for (name, status) in &statuses {
        println!("  {:15}: {:?}", name, status);
    }
    println!();

    // 4. 不同范围的页面计数
    println!("--- 4. 不同范围的页面计数 ---");
    let ranges = [
        (Address::new(0, 0), Address::new(1, 0), "单页"),
        (Address::new(0, 0), Address::new(5, 0), "5页"),
        (Address::new(3, 100), Address::new(10, 500), "跨页"),
        (Address::new(0, 0), Address::new(100, 0), "100页"),
    ];

    for (begin, end, desc) in &ranges {
        let range = ScanRange::new(*begin, *end);
        println!("  {}: {} 页", desc, range.page_count());
    }
    println!();

    // 5. 扫描范围边界
    println!("--- 5. 扫描范围边界 ---");

    // 开始偏移
    let range_with_offset = ScanRange::new(Address::new(0, 100), Address::new(2, 500));
    println!(
        "  起始偏移 100, 结束偏移 500: {} 页",
        range_with_offset.page_count()
    );

    // 最大页面
    let large_range = ScanRange::new(Address::new(0, 0), Address::new(1000, 0));
    println!("  大范围 (0 -> 1000): {} 页", large_range.page_count());
    println!();

    // 6. 扫描模式说明
    println!("--- 6. 扫描模式 ---");
    println!("  LogScanIterator 支持多种模式:");
    println!("  - 单页模式: 每次读取一个页面");
    println!("  - 双缓冲模式: 预取下一页提高性能");
    println!("  - 并发模式: 多线程并行扫描不同页面");
    println!();

    // 7. 典型使用场景
    println!("--- 7. 典型使用场景 ---");
    println!("  1. 日志压缩: 扫描旧记录并压缩到新位置");
    println!("  2. 数据导出: 遍历所有记录进行备份");
    println!("  3. 数据验证: 检查记录完整性");
    println!("  4. 统计分析: 收集记录分布信息");
    println!("  5. 冷热分离: 识别并迁移冷数据");

    println!("\n=== 示例完成 ===");
}
