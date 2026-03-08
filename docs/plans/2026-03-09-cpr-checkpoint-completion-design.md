# CPR/Checkpoint 全面完善设计

**日期**: 2026-03-09
**状态**: 已批准
**范围**: WaitPending I/O 排水、序列号验证、并发检查点稳健性、测试策略
**兼容性**: 允许破坏性变更 (0.x 版本)

---

## 背景

oxifaster 的 CPR (Concurrent Prefix Recovery) 基础设施已实现约 85%,核心状态机和 barrier 同步逻辑完备。但深入分析发现 9 个关键差距,影响生产级正确性保证:

1. WaitPending 阶段缺少重试和超时机制
2. 序列号验证几乎是 no-op
3. 驱动线程单点故障
4. FoldOver 后端耐久性竞争
5. 检查点与 compaction 无互斥
6. 检查点产物无校验和
7. 恢复时不验证边界一致性
8. 会话恢复后序列号不连续
9. 测试覆盖不足

---

## 第一部分: WaitPending I/O 排水

### 问题

当前 `cpr_refresh()` 中 WaitPending 阶段的 ack 逻辑:
- 线程只尝试 ack 一次,如果 `prev_ctx_is_drained() == false` 就跳过
- 后续 `refresh()` 调用不会重试 ack
- 没有超时机制: 如果某线程 pending IO 未完成,检查点无限挂起
- 没有日志记录阻塞进度的线程

### 设计

#### 1.1 WaitPending 重试机制

修改 `src/store/faster_kv.rs` 中 `cpr_refresh()` 的 WaitPending 分支:

```rust
Phase::WaitPending => {
    // 关键变化: 移除 last_cpr_state 检查,允许多次尝试 ack
    // 通过 ActiveCheckpoint 内部的 ack 位图保证幂等性
    if ctx.prev_ctx_is_drained() {
        self.cpr.with_active_mut(|active| {
            active.set_phase(state.phase, state.version);
            active.ack(ctx.thread_id);  // ack 内部是幂等的(位图操作)
        });
        ctx.last_cpr_state = state_sig;
    }
    // 如果 prev_ctx 未 drained,不更新 last_cpr_state,下次 refresh 继续尝试
}
```

要点:
- `ack()` 基于位图,天然幂等,多次调用安全
- 只在成功 ack 后更新 `last_cpr_state`,否则下次 `refresh()` 继续尝试
- 无需额外状态,利用现有 `last_cpr_state` 签名机制

#### 1.2 超时机制

在驱动线程的 barrier 等待循环中增加超时:

```rust
// checkpoint.rs - 驱动线程等待循环
struct CheckpointConfig {
    /// WaitPending 阶段超时时间,默认 30 秒
    pub wait_pending_timeout: Duration,
}

// 在 drive_wait_pending() 中:
let deadline = Instant::now() + self.config.wait_pending_timeout;

loop {
    if barrier_complete {
        break;
    }
    if Instant::now() > deadline {
        let stalled = active.pending_threads();
        return Err(CheckpointError::WaitPendingTimeout {
            stalled_threads: stalled,
            elapsed: self.config.wait_pending_timeout,
        });
    }
    std::thread::yield_now();
}
```

超时后行为:
- 返回错误,不强制取消检查点
- 调用者可选择重试或放弃
- 系统状态回滚到 `Rest` 阶段

#### 1.3 进度监控

在 `ActiveCheckpoint` 上增加查询方法:

```rust
impl ActiveCheckpoint {
    /// 返回尚未 ack 的线程 ID 列表
    pub fn pending_threads(&self) -> Vec<u32> {
        // 遍历 participants 位图,找出已参与但未 ack 的线程
    }
}
```

在驱动线程等待循环中,每 5 秒通过 tracing 打印等待中的线程。

### 涉及文件

- `src/store/faster_kv.rs` - `cpr_refresh()` WaitPending 分支
- `src/store/faster_kv/checkpoint.rs` - 驱动线程等待循环
- `src/store/faster_kv/cpr.rs` - `ActiveCheckpoint::pending_threads()`
- `src/config.rs` - `wait_pending_timeout` 配置项

---

## 第二部分: 序列号验证与检查点边界一致性

### 问题

- `validate_session_serials()` 只检查 `u64::MAX`,几乎是 no-op
- 恢复时不验证序列号单调性
- 会话恢复后不保证序列号连续

### 设计

#### 2.1 检查点时序列号捕获增强

在 `WaitPending` 完成后,精确捕获每个参与会话的序列号:

```rust
// LogMetadata.session_states 中每个 SessionState 增加字段
pub struct SessionState {
    pub guid: Uuid,
    pub serial_num: u64,
    pub checkpoint_version: u32,  // 新增: 该会话参与的检查点版本
}
```

语义定义: `serial_num` 表示 "此会话中,序号 <= serial_num 的所有操作都已包含在检查点前缀中"。

#### 2.2 恢复时序列号验证

增强 `validate_session_serials()`:

```rust
fn validate_session_serials(log_meta: &LogMetadata) -> Result<(), RecoveryError> {
    for session in &log_meta.session_states {
        // 1. 基本合法性
        if session.serial_num == u64::MAX {
            return Err(RecoveryError::InvalidSerialNumber {
                guid: session.guid,
                serial: session.serial_num,
            });
        }
    }

    // 2. 同一 guid 不应重复(同一检查点内)
    let mut seen_guids = HashSet::new();
    for session in &log_meta.session_states {
        if !seen_guids.insert(session.guid) {
            return Err(RecoveryError::DuplicateSessionGuid {
                guid: session.guid,
            });
        }
    }

    Ok(())
}
```

增量链验证:

```rust
fn validate_incremental_chain_serials(
    chain: &[LogMetadata],
) -> Result<(), RecoveryError> {
    // 对每个 guid,验证其 serial_num 在链中单调递增
    let mut serial_map: HashMap<Uuid, u64> = HashMap::new();
    for (i, meta) in chain.iter().enumerate() {
        for session in &meta.session_states {
            if let Some(&prev_serial) = serial_map.get(&session.guid) {
                if session.serial_num < prev_serial {
                    return Err(RecoveryError::SerialNumberRegression {
                        guid: session.guid,
                        checkpoint_index: i,
                        current: session.serial_num,
                        previous: prev_serial,
                    });
                }
            }
            serial_map.insert(session.guid, session.serial_num);
        }
    }
    Ok(())
}
```

#### 2.3 会话恢复保护

```rust
impl Session {
    pub fn from_state(state: &SessionState) -> Self {
        let mut session = Session::new();
        session.guid = state.guid;
        session.serial_num = state.serial_num;
        // 后续操作自动从 serial_num + 1 开始
        session
    }

    /// 返回恢复的序列号,供调用者查询
    pub fn recovered_serial_num(&self) -> Option<u64> {
        if self.is_recovered { Some(self.serial_num) } else { None }
    }
}
```

### 新增错误类型

```rust
pub enum RecoveryError {
    InvalidSerialNumber { guid: Uuid, serial: u64 },
    DuplicateSessionGuid { guid: Uuid },
    SerialNumberRegression {
        guid: Uuid,
        checkpoint_index: usize,
        current: u64,
        previous: u64,
    },
    CorruptedMetadata { path: PathBuf, expected_crc: u32, actual_crc: u32 },
}
```

### 涉及文件

- `src/checkpoint/state.rs` - `SessionState` 增加 `checkpoint_version`
- `src/store/faster_kv/checkpoint.rs` - `validate_session_serials()` 增强
- `src/checkpoint/recovery.rs` - 增量链序列号验证
- `src/store/session.rs` - `from_state()` 和 `recovered_serial_num()`

---

## 第三部分: 并发检查点稳健性

### 3.1 驱动线程故障检测与接管

**问题**: 驱动线程是单点故障,崩溃后系统卡死。

**设计**:

在 `ActiveCheckpoint` 中增加心跳:

```rust
pub struct ActiveCheckpoint {
    // 现有字段...
    driver_thread_id: u32,
    driver_heartbeat: AtomicU64,  // 新增: 驱动线程心跳 (epoch tick)
    heartbeat_stale_threshold: u64,  // 新增: 心跳过期阈值 (默认 1000 ticks)
}

impl ActiveCheckpoint {
    pub fn update_heartbeat(&self) {
        self.driver_heartbeat.fetch_add(1, Ordering::Release);
    }

    pub fn try_takeover(&self, new_driver_id: u32) -> bool {
        let last_beat = self.driver_heartbeat.load(Ordering::Acquire);
        // 简化: 使用 CAS 尝试更换 driver_thread_id
        // 只有心跳过期才允许接管
        // 实际实现需要更精细的竞争控制
    }
}
```

参与线程在 `cpr_refresh()` 中检查:

```rust
// 在 cpr_refresh 中,如果当前阶段停滞过久
if !is_driver && active.is_driver_stale() {
    if active.try_takeover(ctx.thread_id) {
        // 成功接管,从当前阶段继续驱动
        tracing::warn!("Thread {} took over checkpoint from stalled driver {}",
                       ctx.thread_id, old_driver);
    }
}
```

### 3.2 FoldOver 耐久性修复

**问题**: `flush_until()` 异步执行,metadata 可能先于数据持久化。

**修复**:

```rust
// checkpoint.rs - drive_wait_flush() FoldOver 分支
LogCheckpointBackend::FoldOver => {
    // 步骤 1: 刷新数据页到设备
    unsafe {
        (*self.hlog.get()).flush_until(final_address)?;
    }

    // 步骤 2: 确保设备级耐久性 (新增)
    unsafe {
        (*self.hlog.get()).device().flush()?;
    }

    // 步骤 3: 写 metadata (数据已耐久后才写)
    let meta = write_log_meta(false)?;

    // 步骤 4: fsync checkpoint artifacts
    fsync_checkpoint_artifacts(false)?;
}
```

这确保了 "数据先于元数据" 的耐久性顺序。

### 3.3 检查点与 Compaction 互斥

**设计**:

```rust
pub struct FasterKv<K, V, D> {
    // 现有字段...
    /// 检查点与 compaction 互斥锁
    /// 检查点持写锁, compaction 持读锁
    checkpoint_compaction_lock: parking_lot::RwLock<()>,
}

// checkpoint() 入口
pub fn checkpoint(&self) -> Result<Uuid> {
    let _guard = self.checkpoint_compaction_lock.write();
    self.drive_checkpoint()
}

// compact() 入口
pub fn compact(&self) -> Result<()> {
    let _guard = self.checkpoint_compaction_lock.read();
    self.drive_compaction()
}
```

### 3.4 检查点产物校验和

**格式**:

```
[magic: 4 bytes "OXCK"] [crc32: 4 bytes] [json_content: variable]
```

```rust
pub fn write_metadata_with_checksum(meta: &impl Serialize, path: &Path) -> io::Result<()> {
    let json = serde_json::to_vec(meta)?;
    let crc = crc32fast::hash(&json);

    let mut file = File::create(path)?;
    file.write_all(b"OXCK")?;
    file.write_all(&crc.to_le_bytes())?;
    file.write_all(&json)?;
    file.flush()?;
    Ok(())
}

pub fn read_metadata_with_checksum<T: DeserializeOwned>(path: &Path) -> Result<T, RecoveryError> {
    let data = fs::read(path)?;

    // 向后兼容: 检查 magic
    if data.len() >= 8 && &data[0..4] == b"OXCK" {
        let expected_crc = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let json = &data[8..];
        let actual_crc = crc32fast::hash(json);
        if expected_crc != actual_crc {
            return Err(RecoveryError::CorruptedMetadata {
                path: path.to_path_buf(),
                expected_crc,
                actual_crc,
            });
        }
        Ok(serde_json::from_slice(json)?)
    } else {
        // 旧格式: 无校验和,直接解析
        Ok(serde_json::from_slice(&data)?)
    }
}
```

### 涉及文件

- `src/store/faster_kv/cpr.rs` - 心跳、接管、pending_threads
- `src/store/faster_kv/checkpoint.rs` - FoldOver 修复、互斥锁集成
- `src/store/faster_kv.rs` - 互斥锁字段、compact/checkpoint 入口
- `src/checkpoint/serialization.rs` - 校验和读写
- `Cargo.toml` - 增加 `crc32fast` 依赖

---

## 第四部分: 测试策略

### 4.1 WaitPending 排水测试

```
test_wait_pending_drains_all_threads
  - 4 线程并发写入,部分线程有 pending IO (通过大量磁盘读触发)
  - 触发检查点
  - 验证所有线程的 pending IO 在检查点完成前被清空
  - 验证恢复后数据完整

test_wait_pending_timeout
  - 配置极短超时 (100ms)
  - 模拟一个线程持有长时间 pending IO (不调用 refresh)
  - 验证超时错误返回,包含卡住的线程 ID
  - 验证系统状态回滚到 Rest

test_wait_pending_retry_ack
  - 线程在首次 refresh 时 pending IO 未完成
  - 后续 refresh 中 IO 完成,验证最终成功 ack
  - 检查点正常完成
```

### 4.2 序列号验证测试

```
test_serial_number_monotonic_after_recovery
  - 写入 N 条记录,检查点
  - 恢复,验证 session.serial_num == N
  - 继续写入,验证从 N+1 开始

test_serial_number_consistency_in_incremental_chain
  - 创建 3 个增量检查点
  - 恢复,验证每个检查点的序列号递增
  - 篡改中间检查点的序列号,验证报错

test_corrupted_serial_number_detected
  - 手动修改 log metadata 中的序列号为异常值
  - 验证恢复时检测到并报错
```

### 4.3 并发正确性测试

```
test_checkpoint_under_concurrent_load
  - 8 线程高并发写入 (每线程 10000 条)
  - 同时触发检查点
  - 恢复后验证: 所有恢复的 key 对应正确的 value
  - 验证检查点前缀语义: 恢复的数据集是某个一致前缀

test_driver_takeover_on_stall
  - 启动检查点,驱动线程在 WaitFlush 阶段停止更新心跳
  - 验证另一个参与线程接管驱动权
  - 检查点最终完成

test_checkpoint_compaction_mutual_exclusion
  - 并发触发检查点和 compaction
  - 验证不会同时执行
  - 验证两者最终都完成
```

### 4.4 耐久性测试

```
test_foldover_durability_ordering
  - 使用 FoldOver 后端创建检查点
  - 验证 device.flush() 在 metadata 写入之前被调用
  - (通过自定义 StorageDevice mock 记录调用顺序)

test_metadata_checksum_validation
  - 写入带校验和的 metadata
  - 读取,验证正常
  - 翻转一个字节,验证 CorruptedMetadata 错误

test_backward_compatible_metadata_loading
  - 写入旧格式 metadata (纯 JSON,无校验和)
  - 使用新代码加载,验证正常解析
```

### 测试文件

- `tests/checkpoint_wait_pending.rs` - WaitPending 排水测试
- `tests/checkpoint_serial_numbers.rs` - 序列号验证测试
- `tests/checkpoint_concurrent_robust.rs` - 并发正确性测试
- `tests/checkpoint_durability.rs` - 耐久性测试

---

## 新增依赖

```toml
[dependencies]
crc32fast = "1"  # 检查点产物校验和
```

---

## 实现优先级

| 优先级 | 任务 | 预估工作量 |
|--------|------|-----------|
| P0 | WaitPending 重试机制 | 0.5 天 |
| P0 | FoldOver 耐久性修复 | 0.5 天 |
| P0 | 序列号验证增强 | 1 天 |
| P1 | 超时机制 + 进度监控 | 0.5 天 |
| P1 | 检查点与 Compaction 互斥 | 0.5 天 |
| P1 | 检查点产物校验和 | 1 天 |
| P1 | 驱动线程故障检测与接管 | 1 天 |
| P2 | 全部测试编写 | 2 天 |

**总预估**: 7 天

---

## 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| 驱动线程接管可能引入新竞争 | 使用 CAS 保证原子接管,充分测试 |
| 校验和格式变更影响旧数据 | 向后兼容: 旧格式按原方式解析 |
| 超时值选择不当 | 提供可配置参数,默认 30 秒 |
| `SessionState` 增加字段影响序列化 | serde 默认值处理,旧数据 `checkpoint_version` 默认 0 |
