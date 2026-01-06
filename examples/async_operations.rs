//! Async Operations 示例
//!
//! 演示 oxifaster 的异步操作功能。
//!
//! 运行: cargo run --example async_operations

use std::sync::Arc;

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{AsyncSessionBuilder, FasterKv, FasterKvConfig};

fn main() {
    println!("=== oxifaster Async Operations 示例 ===\n");

    // 1. 创建存储
    println!("--- 1. 创建存储 ---");
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    println!("  存储已创建\n");

    // 2. 创建 AsyncSession
    println!("--- 2. 创建 AsyncSession ---");
    let mut session = store.start_async_session();
    println!("  Session GUID: {}", session.guid());
    println!("  Thread ID: {}", session.thread_id());
    println!("  是否活动: {}\n", session.is_active());

    // 3. 同步操作 (AsyncSession 也支持同步操作)
    println!("--- 3. 同步操作 ---");
    for i in 1u64..=10 {
        let status = session.upsert(i, i * 100);
        assert_eq!(status, Status::Ok);
    }
    println!("  插入 10 条记录");

    // 读取数据
    for i in 1u64..=5 {
        match session.read(&i) {
            Ok(Some(value)) => println!("  key {i} = {value}"),
            _ => println!("  key {i} 未找到"),
        }
    }
    println!();

    // 4. AsyncSessionBuilder
    println!("--- 4. AsyncSessionBuilder ---");
    let session2 = AsyncSessionBuilder::new(store.clone())
        .thread_id(42)
        .build();
    println!("  使用 Builder 创建, Thread ID: {}", session2.thread_id());
    println!("  Session GUID: {}\n", session2.guid());

    // 5. 从状态恢复
    println!("--- 5. Session 状态管理 ---");
    let state = session.to_session_state();
    println!("  当前状态 GUID: {}", state.guid);
    println!("  当前 Serial Num: {}", state.serial_num);

    // 恢复 session
    let mut restored_session = store.continue_async_session(state.clone());
    println!("  恢复后 GUID: {}", restored_session.guid());
    println!("  恢复后 Serial Num: {}\n", restored_session.serial_num());

    // 6. 删除操作
    println!("--- 6. 删除操作 ---");
    let delete_status = restored_session.delete(&1u64);
    println!("  删除 key 1: {delete_status:?}");

    match restored_session.read(&1u64) {
        Ok(Some(_)) => println!("  key 1 仍存在"),
        Ok(None) => println!("  key 1 已被删除"),
        Err(_) => println!("  读取失败"),
    }
    println!();

    // 7. RMW 操作
    println!("--- 7. RMW (Read-Modify-Write) 操作 ---");
    let before = session.read(&2u64).unwrap();
    println!("  RMW 前 key 2 = {before:?}");

    let rmw_status = session.rmw(2u64, |v| {
        *v += 50;
        true
    });
    println!("  RMW 状态: {rmw_status:?}");

    let after = session.read(&2u64).unwrap();
    println!("  RMW 后 key 2 = {after:?}\n");

    // 8. 刷新 epoch
    println!("--- 8. Epoch 刷新 ---");
    session.refresh();
    println!("  Epoch 已刷新");

    // 9. 结束 session
    println!("\n--- 9. 结束 Session ---");
    session.end();
    println!("  Session 已结束, 是否活动: {}", session.is_active());

    // 10. 异步操作说明
    println!("\n--- 10. 异步操作说明 ---");
    println!("  AsyncSession 提供以下异步方法:");
    println!("  - read_async(&key) -> Future<Result<Option<V>, Status>>");
    println!("  - upsert_async(key, value) -> Future<Status>");
    println!("  - delete_async(&key) -> Future<Status>");
    println!("  - rmw_async(key, modifier) -> Future<Status>");
    println!("  - complete_pending_async() -> Future<bool>");
    println!();
    println!("  这些方法在 tokio 或其他异步运行时中使用:");
    println!("  ```");
    println!("  async fn example() {{");
    println!("      let result = session.read_async(&key).await;");
    println!("      session.upsert_async(key, value).await;");
    println!("  }}");
    println!("  ```");

    println!("\n=== 示例完成 ===");
}
