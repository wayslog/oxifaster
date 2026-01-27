// Checkpoint 格式兼容性测试
//
// 目标: 验证 oxifaster 的 checkpoint 元数据格式与 C++ FASTER 的兼容性

use oxifaster::address::Address;
use oxifaster::checkpoint::binary_format::{CIndexMetadata, CLogMetadata};
use oxifaster::checkpoint::{IndexMetadata, LogMetadata};
use std::mem;
use uuid::Uuid;

// ============ IndexMetadata 测试 ============

#[test]
fn test_c_index_metadata_size() {
    // IndexMetadata 必须是 56 字节
    assert_eq!(mem::size_of::<CIndexMetadata>(), 56);
    assert_eq!(CIndexMetadata::SIZE, 56);
}

#[test]
fn test_c_index_metadata_layout() {
    // 验证字段布局和偏移
    let meta = CIndexMetadata {
        version: 0x01020304,
        table_size: 0x1122334455667788,
        num_ht_bytes: 0xAABBCCDDEEFF0011,
        num_ofb_bytes: 0x2233445566778899,
        ofb_count: 0x00112233_44556677,
        log_begin_address: 0xFFEEDDCC_BBAA9988,
        checkpoint_start_address: 0x11223344_55667788,
    };

    let bytes = meta.serialize();

    // 验证字段偏移
    // version (offset 0, 4 bytes)
    assert_eq!(&bytes[0..4], &0x01020304u32.to_le_bytes());

    // table_size (offset 8, 8 bytes)
    assert_eq!(&bytes[8..16], &0x1122334455667788u64.to_le_bytes());

    // num_ht_bytes (offset 16, 8 bytes)
    assert_eq!(&bytes[16..24], &0xAABBCCDDEEFF0011u64.to_le_bytes());

    // num_ofb_bytes (offset 24, 8 bytes)
    assert_eq!(&bytes[24..32], &0x2233445566778899u64.to_le_bytes());

    // ofb_count (offset 32, 8 bytes)
    assert_eq!(&bytes[32..40], &0x0011223344556677u64.to_le_bytes());

    // log_begin_address (offset 40, 8 bytes)
    assert_eq!(&bytes[40..48], &0xFFEEDDCCBBAA9988u64.to_le_bytes());

    // checkpoint_start_address (offset 48, 8 bytes)
    assert_eq!(&bytes[48..56], &0x1122334455667788u64.to_le_bytes());
}

#[test]
fn test_c_index_metadata_serialize_deserialize() {
    let original = CIndexMetadata {
        version: 1,
        table_size: 2048,
        num_ht_bytes: 131072,
        num_ofb_bytes: 8192,
        ofb_count: 20,
        log_begin_address: 0x1000,
        checkpoint_start_address: 0x2000,
    };

    let bytes = original.serialize();
    let restored = CIndexMetadata::deserialize(&bytes).unwrap();

    assert_eq!(restored.version, original.version);
    assert_eq!(restored.table_size, original.table_size);
    assert_eq!(restored.num_ht_bytes, original.num_ht_bytes);
    assert_eq!(restored.num_ofb_bytes, original.num_ofb_bytes);
    assert_eq!(restored.ofb_count, original.ofb_count);
    assert_eq!(restored.log_begin_address, original.log_begin_address);
    assert_eq!(
        restored.checkpoint_start_address,
        original.checkpoint_start_address
    );
}

#[test]
fn test_index_metadata_conversion_roundtrip() {
    // 创建 oxifaster IndexMetadata
    let token = Uuid::new_v4();
    let mut index_meta = IndexMetadata::with_token(token);
    index_meta.version = 3;
    index_meta.table_size = 4096;
    index_meta.num_ht_bytes = 262144;
    index_meta.num_ofb_bytes = 16384;
    index_meta.num_buckets = 50;
    index_meta.log_begin_address = Address::new(0, 5000);
    index_meta.checkpoint_start_address = Address::new(10, 10000);

    // 转换为 C 格式
    let c_meta = CIndexMetadata::from_metadata(&index_meta);

    // 序列化
    let bytes = c_meta.serialize();
    assert_eq!(bytes.len(), 56);

    // 反序列化
    let restored_c_meta = CIndexMetadata::deserialize(&bytes).unwrap();

    // 转换回 Rust 格式
    let restored_meta = restored_c_meta.to_metadata(token);

    // 验证所有字段
    assert_eq!(restored_meta.version, index_meta.version);
    assert_eq!(restored_meta.table_size, index_meta.table_size);
    assert_eq!(restored_meta.num_ht_bytes, index_meta.num_ht_bytes);
    assert_eq!(restored_meta.num_ofb_bytes, index_meta.num_ofb_bytes);
    assert_eq!(restored_meta.num_buckets, index_meta.num_buckets);
    assert_eq!(
        restored_meta.log_begin_address,
        index_meta.log_begin_address
    );
    assert_eq!(
        restored_meta.checkpoint_start_address,
        index_meta.checkpoint_start_address
    );
}

// ============ LogMetadata 测试 ============

#[test]
fn test_c_log_metadata_size() {
    // LogMetadata 必须是 2336 字节
    assert_eq!(mem::size_of::<CLogMetadata>(), 2336);
    assert_eq!(CLogMetadata::SIZE, 2336);
}

#[test]
fn test_c_log_metadata_layout() {
    let mut meta = CLogMetadata::new();
    meta.use_snapshot_file = true;
    meta.version = 0x01020304;
    meta.num_threads = 0x05060708;
    meta.flushed_address = 0x1122334455667788;
    meta.final_address = 0xAABBCCDDEEFF0011;

    let bytes = meta.serialize();

    // 验证字段偏移
    // use_snapshot_file (offset 0, 1 byte)
    assert_eq!(bytes[0], 1);

    // padding1 (offset 1, 3 bytes)
    assert_eq!(bytes[1], 0);
    assert_eq!(bytes[2], 0);
    assert_eq!(bytes[3], 0);

    // version (offset 4, 4 bytes)
    assert_eq!(&bytes[4..8], &0x01020304u32.to_le_bytes());

    // num_threads (offset 8, 4 bytes)
    assert_eq!(&bytes[8..12], &0x05060708u32.to_le_bytes());

    // padding2 (offset 12, 4 bytes)
    assert_eq!(bytes[12], 0);
    assert_eq!(bytes[13], 0);
    assert_eq!(bytes[14], 0);
    assert_eq!(bytes[15], 0);

    // flushed_address (offset 16, 8 bytes)
    assert_eq!(&bytes[16..24], &0x1122334455667788u64.to_le_bytes());

    // final_address (offset 24, 8 bytes)
    assert_eq!(&bytes[24..32], &0xAABBCCDDEEFF0011u64.to_le_bytes());

    // monotonic_serial_nums starts at offset 32 (768 bytes = 96 * 8)
    // guids starts at offset 800 (1536 bytes = 96 * 16)
}

#[test]
fn test_c_log_metadata_serialize_deserialize() {
    let mut original = CLogMetadata::new();
    original.use_snapshot_file = true;
    original.version = 5;
    original.num_threads = 8;
    original.flushed_address = 0x10000;
    original.final_address = 0x20000;

    // 设置会话数据
    original.monotonic_serial_nums[0] = 100;
    original.guids[0] = Uuid::new_v4().as_u128();
    original.monotonic_serial_nums[1] = 200;
    original.guids[1] = Uuid::new_v4().as_u128();
    original.monotonic_serial_nums[2] = 300;
    original.guids[2] = Uuid::new_v4().as_u128();

    let bytes = original.serialize();
    let restored = CLogMetadata::deserialize(&bytes).unwrap();

    assert_eq!(restored.use_snapshot_file, original.use_snapshot_file);
    assert_eq!(restored.version, original.version);
    assert_eq!(restored.num_threads, original.num_threads);
    assert_eq!(restored.flushed_address, original.flushed_address);
    assert_eq!(restored.final_address, original.final_address);

    // 验证会话数据
    for i in 0..3 {
        assert_eq!(
            restored.monotonic_serial_nums[i],
            original.monotonic_serial_nums[i]
        );
        assert_eq!(restored.guids[i], original.guids[i]);
    }

    // 验证未设置的会话为 0
    for i in 3..96 {
        assert_eq!(restored.monotonic_serial_nums[i], 0);
        assert_eq!(restored.guids[i], 0);
    }
}

#[test]
fn test_log_metadata_conversion_roundtrip() {
    // 创建 oxifaster LogMetadata
    let token = Uuid::new_v4();
    let mut log_meta = LogMetadata::with_token(token);
    log_meta.use_snapshot_file = true;
    log_meta.version = 7;
    log_meta.num_threads = 4;
    log_meta.flushed_until_address = Address::new(20, 15000);
    log_meta.final_address = Address::new(30, 25000);

    // 添加会话状态
    let session1 = Uuid::new_v4();
    let session2 = Uuid::new_v4();
    log_meta.add_session(session1, 1000);
    log_meta.add_session(session2, 2000);

    // 转换为 C 格式
    let c_meta = CLogMetadata::from_metadata(&log_meta);

    // 序列化
    let bytes = c_meta.serialize();
    assert_eq!(bytes.len(), 2336);

    // 反序列化
    let restored_c_meta = CLogMetadata::deserialize(&bytes).unwrap();

    // 转换回 Rust 格式
    let restored_meta = restored_c_meta.to_metadata(token);

    // 验证所有字段
    assert_eq!(restored_meta.use_snapshot_file, log_meta.use_snapshot_file);
    assert_eq!(restored_meta.version, log_meta.version);
    assert_eq!(restored_meta.num_threads, log_meta.num_threads);
    assert_eq!(
        restored_meta.flushed_until_address,
        log_meta.flushed_until_address
    );
    assert_eq!(restored_meta.final_address, log_meta.final_address);

    // 验证会话状态
    assert_eq!(restored_meta.session_states.len(), 2);
    assert_eq!(restored_meta.session_states[0].guid, session1);
    assert_eq!(restored_meta.session_states[0].serial_num, 1000);
    assert_eq!(restored_meta.session_states[1].guid, session2);
    assert_eq!(restored_meta.session_states[1].serial_num, 2000);
}

#[test]
fn test_max_sessions_limit() {
    // 测试会话数量限制
    let token = Uuid::new_v4();
    let mut log_meta = LogMetadata::with_token(token);

    // 添加超过 96 个会话
    for i in 0..100 {
        log_meta.add_session(Uuid::new_v4(), i);
    }

    // 转换为 C 格式（应该只保留前 96 个）
    let c_meta = CLogMetadata::from_metadata(&log_meta);

    // 验证只有 96 个会话
    let mut non_zero_count = 0;
    for i in 0..96 {
        if c_meta.guids[i] != 0 {
            non_zero_count += 1;
        }
    }
    assert_eq!(non_zero_count, 96);

    // 序列化和反序列化
    let bytes = c_meta.serialize();
    let restored_c_meta = CLogMetadata::deserialize(&bytes).unwrap();
    let restored_meta = restored_c_meta.to_metadata(token);

    // 应该只恢复 96 个会话
    assert_eq!(restored_meta.session_states.len(), 96);
}

#[test]
fn test_binary_compatibility_with_cpp() {
    // 模拟 C++ FASTER 创建的二进制数据
    // 这个测试确保我们能够正确解析 C++ 创建的数据

    // 创建一个模拟的 C++ IndexMetadata 二进制数据
    let mut cpp_index_bytes = vec![0u8; 56];
    let mut offset = 0;

    // version = 1
    cpp_index_bytes[offset..offset + 4].copy_from_slice(&1u32.to_le_bytes());
    offset += 4;

    // padding (4 bytes)
    offset += 4;

    // table_size = 1024
    cpp_index_bytes[offset..offset + 8].copy_from_slice(&1024u64.to_le_bytes());
    offset += 8;

    // num_ht_bytes = 65536
    cpp_index_bytes[offset..offset + 8].copy_from_slice(&65536u64.to_le_bytes());
    offset += 8;

    // num_ofb_bytes = 4096
    cpp_index_bytes[offset..offset + 8].copy_from_slice(&4096u64.to_le_bytes());
    offset += 8;

    // ofb_count = 10
    cpp_index_bytes[offset..offset + 8].copy_from_slice(&10u64.to_le_bytes());
    offset += 8;

    // log_begin_address = 0x1000
    cpp_index_bytes[offset..offset + 8].copy_from_slice(&0x1000u64.to_le_bytes());
    offset += 8;

    // checkpoint_start_address = 0x2000
    cpp_index_bytes[offset..offset + 8].copy_from_slice(&0x2000u64.to_le_bytes());

    // oxifaster 应该能够解析这个数据
    let c_meta = CIndexMetadata::deserialize(&cpp_index_bytes).unwrap();
    assert_eq!(c_meta.version, 1);
    assert_eq!(c_meta.table_size, 1024);
    assert_eq!(c_meta.num_ht_bytes, 65536);
    assert_eq!(c_meta.num_ofb_bytes, 4096);
    assert_eq!(c_meta.ofb_count, 10);
    assert_eq!(c_meta.log_begin_address, 0x1000);
    assert_eq!(c_meta.checkpoint_start_address, 0x2000);

    // 转换为 Rust 格式
    let index_meta = c_meta.to_metadata(Uuid::new_v4());
    assert_eq!(index_meta.version, 1);
    assert_eq!(index_meta.table_size, 1024);
    assert_eq!(index_meta.num_buckets, 10);
}
