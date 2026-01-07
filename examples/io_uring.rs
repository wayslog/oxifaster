//! io_uring 示例
//!
//! 演示如何使用 `IoUringDevice` 作为 FasterKV 的存储设备。
//!
//! 运行：
//! - Linux + 真实 io_uring：`cargo run --example io_uring --features io_uring`
//! - 其他平台/未开启 feature：会回退到 mock 实现（仅用于演示 API）

use std::sync::Arc;

use oxifaster::device::IoUringDevice;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use tempfile::tempdir;

fn main() {
    println!("=== oxifaster io_uring 示例 ===\n");

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    println!("当前为 Linux + feature=io_uring：将使用真实 io_uring 后端\n");

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    println!(
        "当前非 Linux 或未开启 feature=io_uring：将使用 mock 实现（仅演示 API）。在 Linux 上可通过 `--features io_uring` 启用真实后端\n"
    );

    let dir = tempdir().expect("创建临时目录失败");
    let data_path = dir.path().join("oxifaster_io_uring.dat");

    // 1. 创建配置
    let config = FasterKvConfig::default();

    // 2. 创建 io_uring 设备（在非 Linux/未开启 feature 时会回退为 mock）
    let mut device = IoUringDevice::with_defaults(&data_path);
    device
        .initialize()
        .expect("io_uring 设备初始化失败（mock 下也应返回 Ok）");

    // 3. 创建存储并执行基本读写
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session();

    let upsert_status = session.upsert(1, 100);
    assert_eq!(upsert_status, Status::Ok);

    let read = session.read(&1).expect("读取失败");
    println!("读取 key=1: {read:?}");

    let delete_status = session.delete(&1);
    println!("删除 key=1: {delete_status:?}");

    println!("\n数据文件路径: {}", data_path.display());
    println!("=== 示例完成 ===");
}
