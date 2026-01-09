# oxifaster

[![CI](https://github.com/wayslog/oxifaster/actions/workflows/ci.yml/badge.svg)](https://github.com/wayslog/oxifaster/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/wayslog/oxifaster/graph/badge.svg?token=WUMZDF60BQ)](https://codecov.io/gh/wayslog/oxifaster)

中文 | [English](README.md)

oxifaster 是微软 [FASTER](https://github.com/microsoft/FASTER) 项目的 Rust 移植版本，提供高性能并发键值存储和日志引擎。

## 特性

- **高性能**: 支持超过内存容量的大规模数据高效读写
- **并发安全**: 基于 Epoch 保护机制的无锁并发控制
- **持久化**: 完整的检查点 (Checkpoint) 和恢复 (Recovery) 支持，含 CPR 协议
- **混合日志**: HybridLog 架构，热数据内存 + 冷数据磁盘
- **异步 I/O**: 基于 Tokio 运行时的异步操作支持
- **Read Cache**: 热点数据内存缓存加速读取，透明集成到读取路径
- **Log Compaction**: 日志压缩与空间回收，支持自动后台压缩
- **Index Growth**: 动态哈希表扩容，支持 rehash 回调确保正确性
- **F2 架构**: 热冷数据分离的两级存储，含完整 Checkpoint/Recovery
- **Statistics**: 完整的统计收集，集成到所有 CRUD 操作

## 快速开始

### 安装

在 `Cargo.toml` 中添加依赖:

```toml
[dependencies]
oxifaster = { path = "oxifaster" }
```

启用 Linux `io_uring` 后端（feature gate）：

```toml
[dependencies]
oxifaster = { path = "oxifaster", features = ["io_uring"] }
```

说明：真实 `io_uring` 后端仅在 Linux 可用；在非 Linux（或未开启 feature）时，`IoUringDevice` 会回退到可移植的基于文件的实现（非 io_uring），以保持 API 可用与可编译。

### 基本使用

```rust
use std::sync::Arc;
use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn main() -> std::io::Result<()> {
    // 创建配置
    let config = FasterKvConfig::default();
    
    // 创建存储设备（FileSystemDisk 是默认的持久化设备）
    let device = FileSystemDisk::single_file("oxifaster.db")?;
    
    // 创建存储
    let store = Arc::new(FasterKv::new(config, device));
    
    // 启动会话
    let mut session = store.start_session().expect("failed to start session");
    
    // 插入数据
    session.upsert(42u64, 100u64);
    
    // 读取数据
    if let Ok(Some(value)) = session.read(&42u64) {
        println!("Value: {}", value);
    }
    
    // 删除数据
    session.delete(&42u64);

    Ok(())
}
```

---

## 开发路线图

请参考 [DEV.md](DEV.md) 了解详细的开发路线图、实现状态和技术债分析。

---

## 模块结构

```
oxifaster/
├── src/
│   ├── lib.rs              # 库入口
│   ├── address.rs          # 地址系统 (Address, AtomicAddress)
│   ├── record.rs           # 记录结构 (RecordInfo, Record, Key, Value)
│   ├── status.rs           # 状态码 (Status, OperationStatus)
│   ├── utility.rs          # 工具函数
│   │
│   ├── epoch/              # Epoch 保护机制
│   │   ├── mod.rs
│   │   └── light_epoch.rs
│   │
│   ├── index/              # 哈希索引
│   │   ├── mod.rs
│   │   ├── hash_bucket.rs
│   │   ├── hash_table.rs
│   │   ├── mem_index.rs
│   │   ├── grow.rs         # 索引扩展
│   │   └── cold_index.rs   # 磁盘冷索引
│   │
│   ├── allocator/          # 内存分配器
│   │   ├── mod.rs
│   │   ├── page_allocator.rs
│   │   └── hybrid_log.rs
│   │
│   ├── device/             # 存储设备
│   │   ├── mod.rs
│   │   ├── traits.rs
│   │   ├── file_device.rs
│   │   ├── null_device.rs
│   │   ├── io_uring.rs        # io_uring 入口（Linux + mock）
│   │   ├── io_uring_common.rs # 公共类型
│   │   ├── io_uring_linux.rs  # Linux 后端（feature = "io_uring"）
│   │   └── io_uring_mock.rs   # 非 Linux / 未开 feature 的回退实现
│   │
│   ├── store/              # FasterKV 存储
│   │   ├── mod.rs
│   │   ├── faster_kv.rs
│   │   ├── session.rs
│   │   ├── contexts.rs
│   │   └── state_transitions.rs
│   │
│   ├── checkpoint/         # 检查点和恢复
│   │   ├── mod.rs
│   │   ├── state.rs
│   │   ├── locks.rs        # CPR 检查点锁
│   │   ├── recovery.rs
│   │   └── serialization.rs
│   │
│   ├── log/                # FASTER Log
│   │   ├── mod.rs
│   │   └── faster_log.rs
│   │
│   ├── cache/              # Read Cache
│   │   ├── mod.rs
│   │   ├── config.rs
│   │   ├── read_cache.rs
│   │   ├── record_info.rs
│   │   └── stats.rs
│   │
│   ├── compaction/         # 日志压缩
│   │   ├── mod.rs
│   │   ├── compact.rs
│   │   ├── concurrent.rs    # 并发压缩
│   │   ├── auto_compact.rs  # 自动压缩后台线程
│   │   └── contexts.rs
│   │
│   ├── f2/                 # F2 热冷架构
│   │   ├── mod.rs
│   │   ├── config.rs
│   │   ├── store.rs
│   │   └── state.rs
│   │
│   ├── scan/               # 日志扫描
│   │   ├── mod.rs
│   │   └── log_iterator.rs
│   │
│   └── stats/              # 统计收集
│       ├── mod.rs
│       ├── collector.rs
│       ├── metrics.rs
│       └── reporter.rs
│
├── examples/               # 示例代码
│   ├── async_operations.rs
│   ├── basic_kv.rs
│   ├── cold_index.rs
│   ├── compaction.rs
│   ├── concurrent_access.rs
│   ├── custom_types.rs
│   ├── epoch_protection.rs
│   ├── f2_hot_cold.rs
│   ├── faster_log.rs
│   ├── index_growth.rs
│   ├── log_scan.rs
│   ├── read_cache.rs
│   ├── statistics.rs
│   └── variable_length.rs
│
├── benches/
│   └── ycsb.rs             # YCSB 基准测试
│
└── tests/
    ├── async_session.rs    # 异步会话测试
    ├── basic_ops.rs        # 基本操作测试
    ├── checkpoint_locks.rs # 检查点锁测试
    ├── checkpoint.rs       # 检查点测试
    ├── cold_index.rs       # 冷索引测试
    ├── compaction.rs       # 压缩测试
    ├── f2.rs               # F2 测试
    ├── incremental_checkpoint.rs # 增量检查点测试
    ├── index_growth.rs     # 索引扩展测试
    ├── log_scan.rs         # 日志扫描测试
    ├── read_cache.rs       # 读缓存测试
    ├── recovery.rs         # 恢复测试
    ├── statistics.rs       # 统计测试
    └── varlen.rs           # 变长记录测试
```

---

## 核心概念

### Address (地址)

48 位逻辑地址，用于标识混合日志中的记录位置:
- 25 位页内偏移 (32 MB 每页)
- 23 位页号 (~800 万页)

### Epoch Protection

轻量级的 Epoch 保护机制，用于:
- 安全的内存回收
- 无锁并发控制
- 延迟操作执行

### Hybrid Log

混合日志分配器，管理内存和磁盘存储:
- **可变区域**: 最新的页面，支持就地更新
- **只读区域**: 内存中的旧页面
- **磁盘区域**: 冷数据

### Hash Index

高性能内存哈希索引:
- 缓存行对齐的哈希桶 (64 字节)
- 14 位标签用于快速比较
- 支持溢出桶
- 动态扩容支持

### Read Cache

热点数据缓存:
- 内存中存储频繁读取的记录
- LRU-like 淘汰策略
- 透明集成到读取路径

### Log Compaction

空间回收机制:
- 扫描旧记录区域
- 保留最新版本记录
- 释放已过期空间

---

## API 参考

### FasterKv

主要的 KV 存储类:

```rust
use std::sync::Arc;
use oxifaster::device::FileSystemDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

// 创建存储
let config = FasterKvConfig::default();
let store_device = FileSystemDisk::single_file("oxifaster.db")?;
let store = Arc::new(FasterKv::new(config, store_device));

// 启动会话
let mut session = store.start_session().expect("failed to start session");

// 基本操作
session.upsert(key, value);                    // 插入/更新
let value = session.read(&key)?;               // 读取
session.delete(&key);                          // 删除
session.rmw(key, |v| { *v += 1; true });       // 读-改-写

// Checkpoint 和 Recovery (Phase 2 已完成)
let token = store.checkpoint(checkpoint_dir)?; // 创建检查点
let recovered = FasterKv::recover(            // 从检查点恢复
    checkpoint_dir,
    token,
    config,
    FileSystemDisk::single_file("oxifaster.db")?
)?;

// Session 持久化
let states = store.get_recovered_sessions();   // 获取恢复的 session 状态
let session = store
    .continue_session(state)
    .expect("failed to continue session");     // 从保存的状态恢复 session
```

### FasterKv with Read Cache

启用读缓存:

```rust
use oxifaster::cache::ReadCacheConfig;

let cache_config = ReadCacheConfig::default()
    .with_mem_size(256 * 1024 * 1024)  // 256 MB
    .with_mutable_fraction(0.5);

let store = FasterKv::with_read_cache(config, device, cache_config);
```

### FasterKv with Compaction

启用压缩:

```rust
use oxifaster::compaction::CompactionConfig;

let compaction_config = CompactionConfig::default()
    .with_target_utilization(0.5)
    .with_num_threads(2);

let store = FasterKv::with_compaction_config(config, device, compaction_config);
```

### 自动后台 Compaction

启动后台 compaction worker，并通过返回的 handle 管理其生命周期：

```rust
use oxifaster::compaction::AutoCompactionConfig;

let handle = store.start_auto_compaction(AutoCompactionConfig::new());
drop(handle); // Stops and joins the worker thread
```

### FasterLog

独立的高性能日志:

```rust
use oxifaster::log::faster_log::{FasterLog, FasterLogConfig};
use oxifaster::device::FileSystemDisk;

let config = FasterLogConfig::default();
let device = FileSystemDisk::single_file("oxifaster.log")?;
let log = FasterLog::new(config, device);

// 追加数据
let addr = log.append(b"data")?;

// 提交
log.commit()?;

// 读取
let data = log.read_entry(addr);

// 扫描所有条目
for (addr, data) in log.scan_all() {
    println!("{}: {:?}", addr, data);
}
```

### F2Kv (Hot-Cold Architecture)

两级存储架构:

```rust
use oxifaster::f2::{F2Kv, F2Config, HotStoreConfig, ColdStoreConfig};

let config = F2Config {
    hot: HotStoreConfig::default(),
    cold: ColdStoreConfig::default(),
    ..Default::default()
};

let f2_store = F2Kv::new(config, hot_device, cold_device);
```

### Statistics

收集性能统计:

```rust
use oxifaster::stats::{StatsCollector, StatsConfig};

let stats = store.stats();
println!("Read hits: {}", stats.read_hits);
println!("Read misses: {}", stats.read_misses);
println!("Cache hit rate: {:.2}%", stats.cache_hit_rate() * 100.0);
```

---

## 运行示例

说明：大多数示例会运行两次用于对比设备行为——先使用 `NullDisk`（纯内存），再使用 `FileSystemDisk`（临时文件）。

```bash
# 异步操作
cargo run --example async_operations

# 基本 KV 操作
cargo run --example basic_kv

# io_uring（Linux 上启用 feature 才会使用真实后端）
cargo run --example io_uring --features io_uring

# 冷索引
cargo run --example cold_index

# 压缩
cargo run --example compaction

# 并发访问
cargo run --example concurrent_access

# 自定义类型
cargo run --example custom_types

# Epoch 保护
cargo run --example epoch_protection

# F2 热冷架构
cargo run --example f2_hot_cold

# FasterLog
cargo run --example faster_log

# 索引扩展
cargo run --example index_growth

# 日志扫描
cargo run --example log_scan

# 读缓存
cargo run --example read_cache

# 统计
cargo run --example statistics

# 变长记录
cargo run --example variable_length
```

## 运行测试

```bash
cargo test
```

## 运行基准测试

```bash
cargo bench
```

---

## 贡献指南

欢迎贡献代码! 请查看 [DEV.md](DEV.md) 了解开发路线图和技术债分析。

### 开发流程

1. Fork 本仓库
2. 创建功能分支: `git checkout -b feature/your-feature`
3. 提交更改: `git commit -am 'Add new feature'`
4. 推送分支: `git push origin feature/your-feature`
5. 创建 Pull Request

### 代码规范

- 使用 `cargo fmt` 格式化代码
- 使用 `cargo clippy` 检查代码质量
- 为新功能添加单元测试
- 更新相关文档

---

## 参考资料

- [FASTER 官方文档](https://microsoft.github.io/FASTER/)
- [FASTER C++ 源码](https://github.com/microsoft/FASTER/tree/main/cc)
- [FASTER C# 源码](https://github.com/microsoft/FASTER/tree/main/cs)
- [FASTER 论文](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/)

## 许可证

MIT License

## 致谢

感谢微软 FASTER 团队的开源贡献。
