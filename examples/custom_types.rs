//! 自定义类型示例
//!
//! 演示如何为自定义类型实现 Key 和 Value trait
//!
//! 运行: cargo run --example custom_types

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use oxifaster::device::NullDisk;
use oxifaster::record::{Key, Value};
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

// 自定义键类型: 用户ID
#[derive(Clone, Debug, PartialEq, Eq)]
struct UserId {
    prefix: char,
    number: u32,
}

impl UserId {
    fn new(prefix: char, number: u32) -> Self {
        Self { prefix, number }
    }
}

impl Key for UserId {
    fn size(&self) -> u32 {
        (std::mem::size_of::<char>() + std::mem::size_of::<u32>()) as u32
    }

    fn get_hash(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.prefix.hash(&mut hasher);
        self.number.hash(&mut hasher);
        hasher.finish()
    }
}

// 自定义值类型: 用户信息
#[derive(Clone, Debug, PartialEq)]
struct UserInfo {
    name: String,
    age: u8,
    score: f32,
}

impl UserInfo {
    fn new(name: &str, age: u8, score: f32) -> Self {
        Self {
            name: name.to_string(),
            age,
            score,
        }
    }
}

impl Value for UserInfo {
    fn size(&self) -> u32 {
        (std::mem::size_of::<usize>() + self.name.len() + 1 + 4) as u32
    }
}

fn main() {
    println!("=== oxifaster 自定义类型示例 ===\n");

    // 创建存储
    let config = FasterKvConfig {
        table_size: 1 << 12,
        log_memory_size: 1 << 22,
        page_size_bits: 16,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<UserId, UserInfo, _>::new(config, device));

    let mut session = store.start_session();

    // 创建测试数据
    let users = vec![
        (UserId::new('A', 1001), UserInfo::new("张三", 25, 85.5)),
        (UserId::new('A', 1002), UserInfo::new("李四", 30, 92.0)),
        (UserId::new('B', 2001), UserInfo::new("王五", 28, 78.5)),
        (UserId::new('B', 2002), UserInfo::new("赵六", 35, 88.0)),
        (UserId::new('C', 3001), UserInfo::new("钱七", 22, 95.5)),
    ];

    // 插入用户
    println!("--- 插入用户 ---");
    for (id, info) in &users {
        let status = session.upsert(id.clone(), info.clone());
        if status == Status::Ok {
            println!(
                "  插入: {:?} -> {} (年龄: {}, 分数: {})",
                id, info.name, info.age, info.score
            );
        }
    }

    // 查询用户
    println!("\n--- 查询用户 ---");
    let query_ids = vec![
        UserId::new('A', 1001),
        UserId::new('B', 2001),
        UserId::new('C', 3001),
        UserId::new('D', 4001), // 不存在
    ];

    for id in &query_ids {
        match session.read(id) {
            Ok(Some(info)) => {
                println!(
                    "  找到: {:?} -> {} (年龄: {}, 分数: {})",
                    id, info.name, info.age, info.score
                );
            }
            Ok(None) => {
                println!("  未找到: {id:?}");
            }
            Err(e) => {
                println!("  错误: {id:?} - {e:?}");
            }
        }
    }

    // 更新用户
    println!("\n--- 更新用户 ---");
    let update_id = UserId::new('A', 1001);
    let updated_info = UserInfo::new("张三丰", 26, 90.0);

    if session.upsert(update_id.clone(), updated_info.clone()) == Status::Ok {
        println!("  更新成功: {update_id:?}");

        // 验证更新
        if let Ok(Some(info)) = session.read(&update_id) {
            println!(
                "  验证: {} (年龄: {}, 分数: {})",
                info.name, info.age, info.score
            );
        }
    }

    // 删除用户
    println!("\n--- 删除用户 ---");
    let delete_id = UserId::new('B', 2002);

    if session.delete(&delete_id) == Status::Ok {
        println!("  删除成功: {delete_id:?}");

        // 验证删除
        match session.read(&delete_id) {
            Ok(None) => println!("  验证: 用户已删除"),
            Ok(Some(_)) => println!("  验证: 用户仍存在 (错误)"),
            Err(e) => println!("  验证错误: {e:?}"),
        }
    }

    // 显示统计
    println!("\n--- 统计信息 ---");
    let stats = store.index_stats();
    println!("  已用条目: {}", stats.used_entries);
    println!("  负载因子: {:.2}%", stats.load_factor * 100.0);

    println!("\n=== 示例完成 ===");
}
