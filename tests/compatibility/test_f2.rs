// F2 格式兼容性测试
//
// 目标: 验证 oxifaster 的 F2 架构与 C++ FASTER 的 F2 格式兼容性
//
// F2 (Fast & Fair) 是 FASTER 的两层架构，包括：
// - Hot Store: 热数据存储（频繁访问）
// - Cold Store: 冷数据存储（不频繁访问）

#[cfg(feature = "f2")]
use oxifaster::f2::{F2CheckpointPhase, StoreCheckpointStatus};

// C++ FASTER 的枚举值（来自 checkpoint_state_f2.h）
// enum CheckpointPhase : uint8_t {
//   REST = 0,
//   HOT_STORE_CHECKPOINT,
//   COLD_STORE_CHECKPOINT,
//   RECOVER,
// };

#[cfg(feature = "f2")]
#[test]
fn test_f2_checkpoint_phase_values() {
    // 验证枚举值与 C++ FASTER 一致
    assert_eq!(F2CheckpointPhase::Rest as u8, 0);
    assert_eq!(F2CheckpointPhase::HotStoreCheckpoint as u8, 1);
    assert_eq!(F2CheckpointPhase::ColdStoreCheckpoint as u8, 2);
    assert_eq!(F2CheckpointPhase::Recover as u8, 3);
}

#[cfg(feature = "f2")]
#[test]
fn test_f2_checkpoint_phase_conversion() {
    // 测试从 u8 转换
    assert_eq!(F2CheckpointPhase::from_u8(0), Some(F2CheckpointPhase::Rest));
    assert_eq!(
        F2CheckpointPhase::from_u8(1),
        Some(F2CheckpointPhase::HotStoreCheckpoint)
    );
    assert_eq!(
        F2CheckpointPhase::from_u8(2),
        Some(F2CheckpointPhase::ColdStoreCheckpoint)
    );
    assert_eq!(
        F2CheckpointPhase::from_u8(3),
        Some(F2CheckpointPhase::Recover)
    );
    assert_eq!(F2CheckpointPhase::from_u8(4), None);
    assert_eq!(F2CheckpointPhase::from_u8(255), None);
}

// C++ FASTER 的枚举值（来自 checkpoint_state_f2.h）
// enum StoreCheckpointStatus : uint8_t {
//   IDLE = 0,
//   REQUESTED,
//   ACTIVE,
//   FINISHED,
//   FAILED,
// };

#[cfg(feature = "f2")]
#[test]
fn test_store_checkpoint_status_values() {
    // 验证枚举值与 C++ FASTER 一致
    assert_eq!(StoreCheckpointStatus::Idle as u8, 0);
    assert_eq!(StoreCheckpointStatus::Requested as u8, 1);
    assert_eq!(StoreCheckpointStatus::Active as u8, 2);
    assert_eq!(StoreCheckpointStatus::Finished as u8, 3);
    assert_eq!(StoreCheckpointStatus::Failed as u8, 4);
}

#[cfg(feature = "f2")]
#[test]
fn test_store_checkpoint_status_conversion() {
    // 测试从 u8 转换
    assert_eq!(
        StoreCheckpointStatus::from_u8(0),
        Some(StoreCheckpointStatus::Idle)
    );
    assert_eq!(
        StoreCheckpointStatus::from_u8(1),
        Some(StoreCheckpointStatus::Requested)
    );
    assert_eq!(
        StoreCheckpointStatus::from_u8(2),
        Some(StoreCheckpointStatus::Active)
    );
    assert_eq!(
        StoreCheckpointStatus::from_u8(3),
        Some(StoreCheckpointStatus::Finished)
    );
    assert_eq!(
        StoreCheckpointStatus::from_u8(4),
        Some(StoreCheckpointStatus::Failed)
    );
    assert_eq!(StoreCheckpointStatus::from_u8(5), None);
    assert_eq!(StoreCheckpointStatus::from_u8(255), None);
}

#[cfg(feature = "f2")]
#[test]
fn test_store_checkpoint_status_is_done() {
    // 测试 is_done 方法
    assert!(!StoreCheckpointStatus::Idle.is_done());
    assert!(!StoreCheckpointStatus::Requested.is_done());
    assert!(!StoreCheckpointStatus::Active.is_done());
    assert!(StoreCheckpointStatus::Finished.is_done());
    assert!(StoreCheckpointStatus::Failed.is_done());
}

#[cfg(feature = "f2")]
#[test]
fn test_f2_checkpoint_phase_default() {
    // 验证默认值
    let phase = F2CheckpointPhase::default();
    assert_eq!(phase, F2CheckpointPhase::Rest);
    assert_eq!(phase as u8, 0);
}

#[cfg(feature = "f2")]
#[test]
fn test_store_checkpoint_status_default() {
    // 验证默认值
    let status = StoreCheckpointStatus::default();
    assert_eq!(status, StoreCheckpointStatus::Idle);
    assert_eq!(status as u8, 0);
}

#[cfg(feature = "f2")]
#[test]
fn test_f2_checkpoint_phase_repr() {
    // 验证 repr(u8) 保证了内存布局
    use std::mem;
    assert_eq!(mem::size_of::<F2CheckpointPhase>(), 1);
    assert_eq!(mem::align_of::<F2CheckpointPhase>(), 1);
}

#[cfg(feature = "f2")]
#[test]
fn test_store_checkpoint_status_repr() {
    // 验证 repr(u8) 保证了内存布局
    use std::mem;
    assert_eq!(mem::size_of::<StoreCheckpointStatus>(), 1);
    assert_eq!(mem::align_of::<StoreCheckpointStatus>(), 1);
}

// F2 架构验证：
// F2 依赖于已经在其他 phase 验证的格式：
// - Phase 1: RecordInfo 格式 ✓
// - Phase 2: Checkpoint 元数据格式 ✓
// - Phase 3: Hash 函数 ✓
// - Phase 4: 格式识别 ✓
// - Phase 5: SpanByte 格式 ✓
//
// F2 本身不引入新的二进制格式，它只是逻辑上的组织：
// - 两个独立的 FasterKv 实例（hot 和 cold）
// - 每个实例使用相同的底层格式
// - Checkpoint 状态使用上述枚举值

#[test]
fn test_f2_format_compatibility_note() {
    // 此测试作为文档，说明 F2 的兼容性验证策略
    //
    // F2 格式兼容性验证通过以下方式完成：
    // 1. 枚举值验证（本文件）
    // 2. 依赖组件验证（Phase 1-5）
    // 3. 逻辑组织（两个 FasterKv 实例）
    //
    // 因为 F2 不引入新的二进制格式，所以不需要额外的二进制格式测试。
    // Hot 和 Cold store 分别使用标准的 FasterKv 格式，
    // 这些格式已经在 Phase 1-5 中得到验证。

    // 此测试仅作为文档说明
}

// 如果 f2 feature 未启用，提供占位测试
#[cfg(not(feature = "f2"))]
#[test]
fn test_f2_feature_disabled() {
    // F2 feature 未启用，跳过 F2 测试
    // 这是预期行为，因为 F2 是可选特性
}
