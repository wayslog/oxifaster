//! Pending I/O 集成测试
//!
//! 验证 FASTER 风格：当记录被标记为"磁盘区"（address < head_address）时：
//! - `read()` 返回 `Err(Status::Pending)`
//! - `complete_pending(true)` 能驱动真实读盘完成
//! - 之后重试 `read()` 能成功读回
//!
//! # 重要：POD 类型限制
//!
//! 磁盘读取功能仅支持 POD（Plain Old Data）类型，如 `u64`, `i64`, `[u8; N]` 等。
//! 对于包含指针的类型（如 `String`, `Vec<T>`），磁盘读取会返回 `Pending` 但永远无法完成。
//! 这是设计上的安全限制，详见 `parse_disk_record` 的文档说明。

use std::sync::Arc;
use std::time::Duration;

use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::Address;
use tempfile::tempdir;

#[test]
fn test_pending_read_and_complete_pending_readback() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MiB
        page_size_bits: 14,       // 16 KiB
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    // 将 page 0 之前的数据落盘并标记为磁盘区：此后 page 0 内的地址都会触发 Pending
    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // 读盘前：返回 Pending
    assert_eq!(session.read(&1u64), Err(Status::Pending));

    // 驱动读盘完成
    assert!(session.complete_pending(true));

    // 读盘后：重试 read 成功
    assert_eq!(session.read(&1u64), Ok(Some(100u64)));
}

#[test]
fn test_complete_pending_with_custom_timeout() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data_timeout.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 999u64);
    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // 先触发一次 read 来提交后台 I/O 请求
    assert_eq!(session.read(&42u64), Err(Status::Pending));

    // 使用自定义超时等待 I/O 完成
    let completed = session.complete_pending_with_timeout(true, Duration::from_secs(5));
    assert!(completed, "POD 类型应该能在超时前完成读盘");

    // 重试应该成功
    assert_eq!(session.read(&42u64), Ok(Some(999u64)));
}

#[test]
fn test_complete_pending_no_wait_returns_immediately() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data_nowait.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // 触发 Pending
    assert_eq!(session.read(&1u64), Err(Status::Pending));

    // wait=false 应该立即返回（可能还没完成）
    let start = std::time::Instant::now();
    let _result = session.complete_pending(false);
    let elapsed = start.elapsed();

    // 应该很快返回（远小于 1 秒）
    assert!(elapsed < Duration::from_secs(1), "wait=false 应该立即返回");
}
