# oxifaster

oxifaster 是微软 [FASTER](https://github.com/microsoft/FASTER) 项目的 Rust 移植版本，提供高性能并发键值存储和日志引擎。

## 特性

- **高性能**: 支持超过内存容量的大规模数据高效读写
- **并发安全**: 基于 Epoch 保护机制的无锁并发控制
- **持久化**: 支持检查点 (Checkpoint) 和恢复 (Recovery)
- **混合日志**: HybridLog 架构，热数据内存 + 冷数据磁盘
- **异步 I/O**: 基于 Tokio 运行时的异步操作支持

## 快速开始

### 安装

在 `Cargo.toml` 中添加依赖:

```toml
[dependencies]
oxifaster = { path = "path/to/oxifaster" }
```

### 基本使用

```rust
use std::sync::Arc;
use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn main() {
    // 创建配置
    let config = FasterKvConfig::default();
    
    // 创建存储设备 (使用 NullDisk 进行内存测试)
    let device = NullDisk::new();
    
    // 创建存储
    let store = Arc::new(FasterKv::new(config, device));
    
    // 启动会话
    let mut session = store.start_session();
    
    // 插入数据
    session.upsert(42u64, 100u64);
    
    // 读取数据
    if let Ok(Some(value)) = session.read(&42u64) {
        println!("Value: {}", value);
    }
    
    // 删除数据
    session.delete(&42u64);
}
```

---

## 实现状态与开发路线图

本节详细说明 oxifaster 与原始 C++ FASTER 项目的功能对比，以及后续开发计划。

### 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Application                                │
├─────────────────────────────────────────────────────────────────────┤
│                     Session / ThreadContext                          │
├──────────────────────────┬──────────────────────────────────────────┤
│       FasterKV           │           FasterLog                       │
├──────────────────────────┴──────────────────────────────────────────┤
│                         Epoch Protection                             │
├─────────────────────────────────────────────────────────────────────┤
│    Hash Index    │    Hybrid Log (PersistentMemoryMalloc)           │
│  (MemHashIndex)  │  ┌─────────┬─────────┬─────────────────┐         │
│                  │  │ Mutable │ReadOnly │    On-Disk      │         │
│                  │  │ Region  │ Region  │    Region       │         │
│                  │  └─────────┴─────────┴─────────────────┘         │
├─────────────────────────────────────────────────────────────────────┤
│                      Storage Device Layer                            │
│              (NullDisk / FileSystemDisk / Azure)                    │
└─────────────────────────────────────────────────────────────────────┘
```

### 已完成功能 (Phase 1 - Core)

| 模块 | 功能 | 文件 | 状态 |
|------|------|------|:----:|
| **address** | 48位逻辑地址系统 | `src/address.rs` | :white_check_mark: |
| **epoch** | LightEpoch 并发保护框架 | `src/epoch/light_epoch.rs` | :white_check_mark: |
| **index** | MemHashIndex 内存哈希索引 | `src/index/mem_index.rs` | :white_check_mark: |
| **index** | HashBucket 哈希桶 | `src/index/hash_bucket.rs` | :white_check_mark: |
| **index** | HashTable 哈希表 | `src/index/hash_table.rs` | :white_check_mark: |
| **allocator** | PersistentMemoryMalloc 混合日志 | `src/allocator/hybrid_log.rs` | :white_check_mark: |
| **allocator** | PageAllocator 页面分配器 | `src/allocator/page_allocator.rs` | :white_check_mark: |
| **device** | StorageDevice trait | `src/device/traits.rs` | :white_check_mark: |
| **device** | NullDisk 内存设备 | `src/device/null_device.rs` | :white_check_mark: |
| **device** | FileSystemDisk 文件设备 | `src/device/file_device.rs` | :white_check_mark: |
| **store** | FasterKV 核心 (Read/Upsert/RMW/Delete) | `src/store/faster_kv.rs` | :white_check_mark: |
| **store** | Session 会话管理 | `src/store/session.rs` | :white_check_mark: |
| **store** | ThreadContext 线程上下文 | `src/store/contexts.rs` | :white_check_mark: |
| **record** | Record/RecordInfo 记录结构 | `src/record.rs` | :white_check_mark: |
| **record** | Key/Value trait 泛型支持 | `src/record.rs` | :white_check_mark: |
| **log** | FasterLog 基础日志 | `src/log/faster_log.rs` | :white_check_mark: |
| **checkpoint** | Checkpoint 状态结构 | `src/checkpoint/state.rs` | :construction: |
| **checkpoint** | Recovery 恢复结构 | `src/checkpoint/recovery.rs` | :construction: |

**图例**: :white_check_mark: 完成 | :construction: 部分完成 | :x: 未实现

---

### 待实现功能

#### Phase 2: 持久化与恢复 (Durability)

| 功能 | 描述 | 优先级 | C++ 参考文件 |
|------|------|:------:|-------------|
| **Checkpoint 完整实现** | Index + HybridLog 完整检查点流程 | P0 | `checkpoint_state.h` |
| **Recovery 完整实现** | 从检查点恢复完整状态 | P0 | `faster.h` (RecoverHybridLog) |
| **CPR 协议** | Concurrent Prefix Recovery 并发前缀恢复 | P1 | `state_transitions.h` |
| **Snapshot 文件** | 快照文件读写序列化 | P1 | `checkpoint_state.h` |
| **Session 持久化** | 会话状态保存与恢复 | P2 | `faster.h` (ReadCprContexts) |

```rust
// 目标 API
let token = store.checkpoint()?;
store.recover(token)?;
```

#### Phase 3: 性能优化 (Performance)

| 功能 | 描述 | 优先级 | C++ 参考文件 |
|------|------|:------:|-------------|
| **Read Cache** | 热点数据内存缓存 | P1 | `read_cache.h` |
| **Cache Eviction** | 缓存淘汰策略 | P1 | `read_cache.h` |
| **Log Compaction** | 日志压缩与空间回收 | P1 | `compact.h` |
| **Auto Compaction** | 自动后台压缩 | P2 | `faster.h` (AutoCompactHlog) |
| **Concurrent Compaction** | 多线程并发压缩 | P2 | `compact.h` |
| **Index Growth** | 动态哈希表扩容 | P2 | `grow_state.h` |

```rust
// 目标 API
store.enable_read_cache(ReadCacheConfig::default());
store.compact(until_address)?;
store.grow_index()?;
```

#### Phase 4: 高级功能 (Advanced)

| 功能 | 描述 | 优先级 | C++ 参考文件 |
|------|------|:------:|-------------|
| **F2 架构** | 热冷数据分离的两级存储 | P2 | `f2.h` |
| **Cold Index** | 磁盘上的冷索引 | P2 | `cold_index.h` |
| **Log Scan Iterator** | 日志扫描迭代器 | P2 | `log_scan.h` |
| **Checkpoint Locks** | 检查点期间的锁保护 | P2 | `checkpoint_locks.h` |
| **Async Pending Queue** | 异步待处理操作队列 | P3 | `internal_contexts.h` |

```rust
// 目标 API: F2 热冷架构
let f2_store = F2Kv::new(hot_config, cold_config);
```

#### Phase 5: 平台与生态 (Platform)

| 功能 | 描述 | 优先级 | C++ 参考文件 |
|------|------|:------:|-------------|
| **io_uring 支持** | Linux 高性能异步 I/O | P2 | `file_linux.h` |
| **Windows 异步 I/O** | Windows 平台原生异步 | P3 | `file_windows.h` |
| **Azure 存储** | Azure Blob 存储后端 | P3 | `azure.h` |
| **统计信息收集** | 性能指标与分布统计 | P2 | `faster.h` (PrintStats) |
| **TOML 配置** | 配置文件支持 | P3 | `config.h` |

```rust
// 目标 API: io_uring
let device = IoUringDevice::new(path)?;

// 目标 API: Azure
let device = AzureBlobDevice::new(connection_string, container)?;
```

---

### 开发路线图

```
2024 Q1                    2024 Q2                    2024 Q3
   │                          │                          │
   ▼                          ▼                          ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│   Phase 1        │   │   Phase 2        │   │   Phase 3        │
│   Core (Done)    │──▶│   Durability     │──▶│   Performance    │
│                  │   │                  │   │                  │
│ - Address        │   │ - Checkpoint     │   │ - Read Cache     │
│ - Epoch          │   │ - Recovery       │   │ - Compaction     │
│ - Index          │   │ - CPR Protocol   │   │ - Index Growth   │
│ - Hybrid Log     │   │ - Snapshots      │   │                  │
│ - Basic KV Ops   │   │                  │   │                  │
└──────────────────┘   └──────────────────┘   └──────────────────┘
                                                      │
                           ┌──────────────────────────┘
                           ▼
                    ┌──────────────────┐   ┌──────────────────┐
                    │   Phase 4        │   │   Phase 5        │
                    │   Advanced       │──▶│   Platform       │
                    │                  │   │                  │
                    │ - F2 Hot/Cold    │   │ - io_uring       │
                    │ - Cold Index     │   │ - Azure Storage  │
                    │ - Log Scan       │   │ - Statistics     │
                    └──────────────────┘   └──────────────────┘
```

---

### 与 C++ FASTER 的主要差异

| 方面 | C++ FASTER | oxifaster |
|------|-----------|-----------|
| **内存管理** | 手动内存管理 + RAII | Rust 所有权系统 |
| **并发模型** | std::atomic + 手动同步 | std::sync::atomic + 类型安全 |
| **异步运行时** | 回调函数 | Tokio async/await |
| **泛型系统** | C++ 模板 | Rust 泛型 + Trait |
| **错误处理** | 返回码/异常 | Result<T, E> |
| **缓存对齐** | 编译器特定宏 | `#[repr(align(64))]` |

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
│   ├── epoch/              # Epoch 保护机制
│   │   └── light_epoch.rs
│   ├── index/              # 哈希索引
│   │   ├── hash_bucket.rs
│   │   ├── hash_table.rs
│   │   └── mem_index.rs
│   ├── allocator/          # 内存分配器
│   │   ├── page_allocator.rs
│   │   └── hybrid_log.rs
│   ├── device/             # 存储设备
│   │   ├── traits.rs
│   │   ├── file_device.rs
│   │   └── null_device.rs
│   ├── store/              # FasterKV 存储
│   │   ├── faster_kv.rs
│   │   ├── session.rs
│   │   └── contexts.rs
│   ├── checkpoint/         # 检查点和恢复
│   │   ├── state.rs
│   │   └── recovery.rs
│   └── log/                # FASTER Log
│       └── faster_log.rs
├── examples/               # 示例代码
│   ├── basic_kv.rs
│   ├── concurrent_access.rs
│   ├── custom_types.rs
│   ├── epoch_protection.rs
│   └── faster_log.rs
├── benches/
│   └── ycsb.rs             # YCSB 基准测试
└── tests/
    ├── basic_ops.rs        # 基本操作测试
    └── recovery.rs         # 恢复测试
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

---

## API 参考

### FasterKv

主要的 KV 存储类:

```rust
use std::sync::Arc;
use oxifaster::device::NullDisk;
use oxifaster::store::{FasterKv, FasterKvConfig};

// 创建存储
let config = FasterKvConfig::default();
let device = NullDisk::new();
let store = Arc::new(FasterKv::new(config, device));

// 启动会话
let mut session = store.start_session();

// 基本操作
session.upsert(key, value);                    // 插入/更新
let value = session.read(&key)?;               // 读取
session.delete(&key);                          // 删除
session.rmw(key, |v| { *v += 1; true });       // 读-改-写
```

### FasterLog

独立的高性能日志:

```rust
use oxifaster::log::faster_log::{FasterLog, FasterLogConfig};
use oxifaster::device::NullDisk;

let config = FasterLogConfig::default();
let device = NullDisk::new();
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

---

## 运行示例

```bash
cd oxifaster

# 基本 KV 操作
cargo run --example basic_kv

# 并发访问
cargo run --example concurrent_access

# 自定义类型
cargo run --example custom_types

# Epoch 保护
cargo run --example epoch_protection

# FasterLog
cargo run --example faster_log
```

## 运行测试

```bash
cd oxifaster
cargo test
```

## 运行基准测试

```bash
cd oxifaster
cargo bench
```

---

## 贡献指南

欢迎贡献代码! 请查看上方的 **待实现功能** 列表，选择感兴趣的功能进行开发。

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
- [FASTER 论文](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/)

## 许可证

MIT License

## 致谢

感谢微软 FASTER 团队的开源贡献。
