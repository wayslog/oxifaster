# oxifaster 与 Microsoft FASTER 二进制兼容性文档

## 版本信息

- **oxifaster 版本**: 0.1.2
- **FASTER C++ 参考版本**: https://github.com/microsoft/FASTER/tree/main/cc
- **文档日期**: 2026-01-23

## 概述

本文档详细记录了 oxifaster (Rust实现) 与 Microsoft FASTER (C++/C#实现) 之间的二进制兼容性状态。目标是使两个实现能够互操作：

- ✅ 已覆盖元数据/格式层的互操作
- ⚠️ 索引/日志数据的端到端互操作仍需补齐验证

## 兼容性状态总览

| 组件 | 状态 | 说明 |
|------|------|------|
| RecordInfo 结构 | ✅ 完全兼容 | 8字节控制字，位布局完全一致 |
| Address 系统 | ✅ 完全兼容 | 48位逻辑地址，相同的页/偏移划分 |
| Hash Bucket 结构 | ✅ 完全兼容 | 64字节对齐，布局/偏移测试覆盖 |
| 字节序 | ✅ 完全兼容 | 都使用小端字节序 |
| Checkpoint 元数据 | ✅ 完全兼容 | 支持C二进制格式和JSON格式，GUID 使用 Windows 字节序 |
| Hash 函数 | ✅ 完全兼容 | 支持xxHash3/xxHash64/FasterCompat |
| 格式版本标识 | ✅ 已实现 | 通用格式头和自动检测 |
| SpanByte 格式 | ✅ 完全兼容 | 与C# FASTER格式一致 |
| F2 格式 | ✅ 已验证 | 枚举值一致，复用基础格式 |

## 详细兼容性分析

### 1. RecordInfo 结构 (✅ 完全兼容)

**位布局** (8 字节 / 64 位):
```
Bits 0-47:   Previous address (48 位)
Bits 48-60:  Checkpoint version (13 位)
Bit 61:      Invalid flag
Bit 62:      Tombstone flag
Bit 63:      Final bit
```

**验证结果**:
- ✅ 大小: 8 字节
- ✅ 对齐: 8 字节对齐
- ✅ 位布局: 与 C++ FASTER 完全一致
- ✅ 字节序: 小端序

**C++ 参考**:
- 文件: `FASTER/cc/src/core/record.h:39-57`

**oxifaster 实现**:
- 文件: `src/record.rs:28-44`

**测试文件**:
- `tests/compatibility/test_record_format.rs`

### 2. Address 系统 (✅ 完全兼容)

**地址格式** (48 位):
```
Bits 0-24:   Page offset (25 位, 32 MB 每页)
Bits 25-47:  Page number (23 位, ~8 million 页)
```

**验证结果**:
- ✅ 总位数: 48 位
- ✅ 页偏移: 25 位 (MAX_OFFSET = 32 MB)
- ✅ 页号: 23 位 (MAX_PAGE = ~8M)
- ✅ 地址空间: 256 PB (理论最大)

**C++ 参考**:
- 文件: `FASTER/cc/src/core/address.h`

**oxifaster 实现**:
- 文件: `src/address.rs`

### 3. Hash Bucket 结构 (✅ 完全兼容)

**Hot Log Bucket** (64 字节):
```
- 7 个条目 (每个 8 字节)
- 1 个溢出指针 (8 字节)
- 总计 64 字节 (缓存行对齐)
```

**Cold Log Bucket** (64 字节):
```
- 8 个条目 (每个 8 字节)
- 总计 64 字节 (缓存行对齐)
```

**验证结果**:
- ✅ 大小: 64 字节
- ✅ 对齐: 缓存行对齐
- ✅ 条目数量: Hot Log 7+1, Cold Log 8
- ✅ Tag 系统: 14位标签
- ✅ 布局与偏移测试覆盖 (overflow 指针位于 56 字节偏移)
- ✅ C++ `ht.dat` / `ofb.dat` 真实数据抽样验证通过

**C++ 参考**:
- 文件: `FASTER/cc/src/index/hash_bucket.h`

**oxifaster 实现**:
- 文件: `src/index/hash_bucket.rs`

**测试文件**:
- `tests/compatibility/test_hash_index.rs`

### 4. 字节序 (✅ 完全兼容)

**平台**:
- ✅ x86/x64: 小端序 (两个实现都使用)
- ⚠️ ARM: 通常小端序，但需要测试验证
- ⚠️ 大端平台: 需要额外的字节序转换

**oxifaster 保证**:
- 所有二进制序列化都显式使用 `to_le_bytes()` / `from_le_bytes()`
- 与平台无关的小端格式

### 5. Checkpoint 元数据格式 (✅ 已实现兼容)

**C++ FASTER 格式**:
- **IndexMetadata**: C 结构体直接序列化 (固定 56 字节)
  ```c
  struct IndexMetadata {
      uint32_t version;                    // 4 字节
      uint32_t _padding0;                  // 4 字节对齐填充
      uint64_t table_size;                 // 8 字节
      uint64_t num_ht_bytes;              // 8 字节
      uint64_t num_ofb_bytes;             // 8 字节
      uint64_t ofb_count;                 // 8 字节 (FixedPageAddress)
      uint64_t log_begin_address;         // 8 字节
      uint64_t checkpoint_start_address;  // 8 字节
      // 总计: 56 字节
  };
  ```

- **LogMetadata**: C 结构体直接序列化 (固定 2336 字节)
  ```c
  struct LogMetadata {
      bool use_snapshot_file;           // 1 字节
      uint8_t _padding1[3];            // 3 字节对齐
      uint32_t version;                 // 4 字节
      std::atomic<uint32_t> num_threads; // 4 字节
      uint8_t _padding2[4];            // 4 字节对齐
      uint64_t flushed_address;        // 8 字节
      uint64_t final_address;          // 8 字节
      uint64_t monotonic_serial_nums[96]; // 768 字节
      GUID guids[96];                  // 1536 字节 (每个GUID 16字节)
      // 总计: 2336 字节
  };
  ```

**oxifaster 实现**:
- **支持两种格式**:
  - C 二进制格式 (FASTER 兼容): `src/checkpoint/binary_format.rs`
  - JSON 格式 (oxifaster 原生): `src/checkpoint/serialization.rs`
- **自动格式检测**: `src/format/detector.rs` 支持 JSON / C 二进制识别
- **GUID 字节序**: C 二进制格式使用 Windows GUID 布局 (`Uuid::to_bytes_le()` / `from_bytes_le()`)

**验证结果**:
- ✅ C 二进制格式实现完成
- ✅ 序列化/反序列化测试通过
- ✅ 字段布局与字节序验证正确
- ✅ GUID 字节序与 Windows GUID 一致
- ✅ 支持往返转换
- ✅ 使用 C++ demo 生成 info.dat/ht.dat/ofb.dat 实测通过

**实现文件**:
- `src/checkpoint/binary_format.rs`: C 风格序列化实现
- `src/checkpoint/serialization.rs`: JSON 序列化实现
- `src/format/detector.rs`: 格式自动检测

**测试文件**:
- `tests/compatibility/test_checkpoint_format.rs`
- `tests/compatibility/test_roundtrip.rs`
- `tests/guid_byteorder_test.rs`
- `tools/faster_cpp_demo.cpp`

### 6. Hash 函数 (✅ 已实现兼容)

**oxifaster**:
- **支持多种哈希算法**:
  - xxHash3 (默认，通过 `hash-xxh3` 特性)
  - xxHash64 (可选，通过 `hash-xxh64` 特性)
  - FasterCompat (FASTER兼容哈希，总是可用)
- 实现文件: `src/codec/multi_hash.rs`

**C++ FASTER**:
- **FasterHash 实现**: 在 `utility.h` 中定义
- **算法**: 乘法哈希，魔数 40343，右旋转 6 位
- 参考文件: `FASTER/cc/src/core/utility.h`

**验证结果**:
- ✅ FasterCompat 哈希与 C++ 实现完全一致
- ✅ 所有已知测试向量验证通过
- ✅ 哈希值确定性验证通过
- ✅ 支持多种哈希算法切换

**实现文件**:
- `src/codec/multi_hash.rs`: 多哈希算法支持
- `tests/compatibility/test_hash.rs`: 哈希兼容性测试

### 7. 格式版本标识 (✅ 已实现)

**oxifaster**:
- 通用格式头: 24 字节 (magic/version/flags/checksum)
- 格式魔数: `OXFLOG1\0` / `FASTER01` / `OXFCKPT1` / `OXFIDX1`
- 自动检测: `FormatDetector` 支持格式头、魔数、JSON 与 C 二进制识别
- 文件: `src/format/header.rs`
- 文件: `src/format/detector.rs`
- Log 元数据仍使用 `OXFLOG1\0` 魔数: `src/log/format.rs:54-55`

**C++ FASTER**:
- Checkpoint 文件本身无统一魔数
- 自动检测基于字段合理性 (version/table_size)

### 8. SpanByte 格式 (✅ 完全兼容)

**oxifaster SpanByte**:
- 4 字节头部 (长度)
- 可选 8 字节元数据
- 可变长度数据
- 文件: `src/varlen/span_byte.rs`

**验证结果**:
- ✅ 头部位布局与 C# FASTER 一致
- ✅ 元数据标志位与 8 字节 metadata 对齐一致
- ✅ 小端序与最大长度约束一致

**测试文件**:
- `tests/compatibility/test_span_byte.rs`

## 测试基础设施

### 格式检查工具

**工具**: `tools/format_inspector.rs`

**功能**:
- 解析和显示二进制文件的详细结构
- 支持 checkpoint、log、index 格式
- 十六进制转储
- 自动格式检测

**用法**:
```bash
# 编译工具
cargo build --bin format_inspector --features clap

# 检查 checkpoint 文件
cargo run --bin format_inspector --features clap -- checkpoint.dat --format checkpoint

# 检查 log 文件
cargo run --bin format_inspector --features clap -- log.dat --format log

# 自动检测格式
cargo run --bin format_inspector --features clap -- file.dat --format auto

# 显示十六进制转储
cargo run --bin format_inspector --features clap -- file.dat --hex-dump
```

### 兼容性测试套件

**测试文件**:
- `tests/compatibility/test_record_format.rs`: RecordInfo 格式测试
- `tests/compatibility/test_checkpoint_format.rs`: Checkpoint C 二进制格式测试
- `tests/compatibility/test_hash.rs`: 哈希兼容性测试
- `tests/compatibility/test_format_detection.rs`: 格式头与自动检测测试
- `tests/compatibility/test_span_byte.rs`: SpanByte 兼容性测试
- `tests/compatibility/test_f2.rs`: F2 枚举兼容性测试
- `tests/compatibility/test_roundtrip.rs`: 往返测试
- `tests/compatibility/test_hash_index.rs`: Hash Index 布局测试

**运行测试**:
```bash
# 运行所有兼容性测试
cargo test --test compatibility

# 运行特定测试
cargo test --test compatibility test_record_info
```

**测试覆盖**:
- ✅ RecordInfo 大小和对齐
- ✅ RecordInfo 位布局
- ✅ RecordInfo 字节序
- ✅ RecordInfo C++ 兼容性
- ✅ Checkpoint 元数据格式 (C 二进制)
- ✅ Hash 函数兼容
- ✅ 格式头与自动检测
- ✅ SpanByte 格式
- ✅ F2 枚举值一致性
- ✅ 往返测试
- ✅ Hash Index 布局

## 兼容性使用方式

目前没有统一的兼容性配置结构体，兼容性能力通过 API 与特性开关使用：

- C 二进制 checkpoint 元数据: `CIndexMetadata` / `CLogMetadata`
- 哈希算法: `HashAlgorithm` (FasterCompat / XXHash3 / XXHash64)
- 格式检测: `FormatDetector` / `UniversalFormatHeader`
- 哈希特性: `hash-xxh3` / `hash-xxh64`

## 互操作与验证

### 兼容性验证工具

**工具**: `tools/verify_compatibility.rs`

**用法**:
```bash
cargo build --bin verify_compatibility --features clap

cargo run --bin verify_compatibility --features clap -- \
  --index-metadata index.meta \
  --log-metadata log.meta \
  --index-file index.dat \
  --log-file log.dat \
  --verbose

# C++ checkpoint 目录验证
cargo run --bin verify_compatibility --features clap -- \
  --cpp-index-dir /path/to/index-checkpoints/<token> \
  --log-metadata /path/to/cpr-checkpoints/<token>/info.dat \
  --verbose
```

**说明**:
- C++ 索引 checkpoint 文件为 `ht.dat` + `ofb.dat`，oxifaster 使用 `index.dat`
- 互操作流程可直接使用下方 demo 工具与脚本

### 索引互操作流程 (✅ 已实测)

**工具**:
- C++: `tools/faster_cpp_demo.cpp`
- Rust: `tools/interop_index_tool.rs`
- 脚本: `tools/run_interop_flows.sh`

**一键脚本**:
```bash
# 默认尝试 ../../cpp/FASTER 或 ../cpp/FASTER，可通过 FASTER_CPP_ROOT 覆盖
tools/run_interop_flows.sh
```

**流程 A: C++ 生成 -> Rust 操作 -> C++ 二次打开**:
```bash
clang++ -std=c++17 -I/path/to/FASTER/cc/src \
  tools/faster_cpp_demo.cpp -o data/faster_cpp_demo

data/faster_cpp_demo generate --out data/cpp_gen
cargo run --bin interop_index_tool --features clap -- \
  verify --dir data/cpp_gen
cargo run --bin interop_index_tool --features clap -- \
  update --dir data/cpp_gen --out data/cpp_updated --entries 100=4096,200=8192
data/faster_cpp_demo verify --dir data/cpp_updated
```

**流程 B: Rust 生成 -> C++ 操作 -> Rust 二次打开**:
```bash
cargo run --bin interop_index_tool --features clap -- \
  generate --out data/rust_gen
data/faster_cpp_demo verify --dir data/rust_gen
data/faster_cpp_demo update --dir data/rust_gen --out data/rust_updated --entries 100=4096,200=8192
cargo run --bin interop_index_tool --features clap -- \
  verify --dir data/rust_updated
```

**说明**:
- `data/` 目录用于互操作验证，已加入 `.gitignore`
- demo 会写入 `manifest.txt` 与 `token.txt` 作为查找验证依据
- `100=4096,200=8192` 为示例更新项，确保与默认 manifest 不冲突
```

## 性能影响

### 兼容模式开销

| 操作 | 原生模式 | 兼容模式 | 开销 |
|------|---------|---------|------|
| Checkpoint 序列化 | JSON | C 二进制 | ~5% |
| Checkpoint 反序列化 | JSON | C 二进制 | ~5% |
| 哈希计算 | xxHash3 | 兼容哈希 | 待测 |
| 读操作 | 无影响 | 无影响 | 0% |
| 写操作 | 无影响 | 无影响 | 0% |

**注意**: 上述数字是估算值，实际性能影响需要通过基准测试验证。

## 实施路线图

### 已完成
- ✅ 兼容性测试基础设施与格式检查工具
- ✅ Checkpoint C 二进制兼容与自动检测
- ✅ 多哈希算法与 FasterCompat 哈希
- ✅ 通用格式头与格式检测
- ✅ SpanByte 兼容性验证
- ✅ F2 枚举兼容性验证
- ✅ 往返测试与兼容性验证工具
- ✅ C++/Rust 索引 checkpoint 双向互操作流程

### 待完成
- 暂无

## 成功标准

### 功能性
- ✅ Checkpoint 元数据互操作
- ✅ 索引互操作 (ht.dat/ofb.dat 双向生成、更新、查找验证)
- ⚠️ Log 互操作 (SpanByte 已验证，完整 log 文件待与 C++ 数据对照)
- ⚠️ Log 互操作 (SpanByte 已验证，完整 log 文件待与 C++ 数据对照)

### 正确性
- ✅ RecordInfo/Checkpoint/SpanByte/格式检测测试通过
- ✅ 往返测试覆盖
- ✅ Hash Index 布局验证

### 性能
- ⏳ 兼容模式开销 < 5%
- ⏳ 基准测试验证

### 文档
- ✅ 二进制兼容性文档更新
- ✅ 格式检查/验证工具说明
- ⏳ 自动迁移工具文档 (如后续提供)

### 可维护性
- ✅ 清晰的测试结构
- ✅ 格式检查工具
- ✅ 兼容性验证工具
- ⏳ Hash Index 兼容性测试补齐

## 已知限制

1. **平台限制**:
   - 主要测试平台: x86_64 Linux/macOS
   - ARM 平台: 需要额外测试
   - 大端平台: 需要字节序转换

2. **版本兼容性**:
   - 当前基于 FASTER C++ main 分支
   - 旧版本的 FASTER 可能不兼容

3. **特性限制**:
   - 某些高级特性可能需要额外兼容性工作
   - F2 模式仅验证枚举值，底层格式复用基础实现

## 参考资料

### FASTER 官方资源
- [FASTER 官方文档](https://microsoft.github.io/FASTER/)
- [FASTER C++ 源代码](https://github.com/microsoft/FASTER/tree/main/cc)
- [FASTER 论文](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/)

### oxifaster 资源
- [oxifaster 仓库](https://github.com/wayslog/oxifaster)
- [开发文档](DEV.md)
- [项目说明](CLAUDE.md)

## 贡献指南

如果你发现兼容性问题或想要改进兼容性，请：

1. 在 GitHub 上创建 Issue
2. 提供详细的复现步骤
3. 包含测试数据和期望结果
4. 提交 Pull Request 修复

## 更新日志

### 2026-01-23
- ✅ 完成 C 二进制 checkpoint 元数据兼容与 GUID 字节序处理
- ✅ 多哈希算法与 FasterCompat 哈希实现
- ✅ 通用格式头与自动检测
- ✅ SpanByte 兼容性验证
- ✅ F2 枚举兼容性验证
- ✅ 新增往返测试与兼容性验证工具
- ✅ Hash Bucket 布局与偏移测试覆盖
- ✅ C++ demo 数据实测与对齐填充修正

---

**维护者**: oxifaster 团队
**最后更新**: 2026-01-23
