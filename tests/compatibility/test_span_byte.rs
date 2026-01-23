// SpanByte 格式兼容性测试
//
// 目标: 验证 oxifaster 的 SpanByte 与 C# FASTER 的 SpanByte 格式兼容性

use oxifaster::varlen::{SpanByte, SpanByteView};

// SpanByte 常量
const SPAN_BYTE_HEADER_SIZE: usize = 4;
const SPAN_BYTE_METADATA_SIZE: usize = 8;
const MAX_SPAN_BYTE_LENGTH: u32 = (1 << 30) - 1;

// C# FASTER 的常量（来自 SpanByte.cs）
const K_UNSERIALIZED_BIT_MASK: u32 = 1 << 31;
const K_EXTRA_METADATA_BIT_MASK: u32 = 1 << 30;
const K_HEADER_MASK: u32 = 0x3 << 30;

#[test]
fn test_span_byte_constants() {
    // 验证常量与 C# FASTER 一致
    assert_eq!(SPAN_BYTE_HEADER_SIZE, 4, "Header size must be 4 bytes");
    assert_eq!(SPAN_BYTE_METADATA_SIZE, 8, "Metadata size must be 8 bytes");

    // 验证位掩码
    assert_eq!(K_UNSERIALIZED_BIT_MASK, 0x80000000);
    assert_eq!(K_EXTRA_METADATA_BIT_MASK, 0x40000000);
    assert_eq!(K_HEADER_MASK, 0xC0000000);
}

#[test]
fn test_span_byte_header_format() {
    // 测试基本的 SpanByte（无元数据）
    let data = b"hello world";
    let span = SpanByte::from_slice(data);

    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    // 验证头部格式
    let header = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    // 验证长度字段（低30位）
    let payload_len = header & !K_HEADER_MASK;
    assert_eq!(payload_len, data.len() as u32);

    // 验证 serialized 标志（位31应该为0）
    assert_eq!(header & K_UNSERIALIZED_BIT_MASK, 0);

    // 验证元数据标志（位30应该为0）
    assert_eq!(header & K_EXTRA_METADATA_BIT_MASK, 0);

    // 验证负载数据
    assert_eq!(&buffer[4..4 + data.len()], data);
}

#[test]
fn test_span_byte_with_metadata_format() {
    // 测试带元数据的 SpanByte
    let data = b"test data";
    let metadata: u64 = 0x123456789ABCDEF0;
    let mut span = SpanByte::from_slice(data);
    span.set_metadata(metadata);

    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    // 验证头部
    let header = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    // 验证长度字段
    let payload_len = header & !K_HEADER_MASK;
    assert_eq!(payload_len, data.len() as u32);

    // 验证 serialized 标志（位31应该为0）
    assert_eq!(header & K_UNSERIALIZED_BIT_MASK, 0);

    // 验证元数据标志（位30应该为1）
    assert_eq!(
        header & K_EXTRA_METADATA_BIT_MASK,
        K_EXTRA_METADATA_BIT_MASK
    );

    // 验证元数据字段（小端序）
    let meta_bytes = &buffer[4..12];
    let stored_metadata = u64::from_le_bytes(meta_bytes.try_into().unwrap());
    assert_eq!(stored_metadata, metadata);

    // 验证负载数据
    assert_eq!(&buffer[12..12 + data.len()], data);
}

#[test]
fn test_span_byte_metadata_size_calculation() {
    // C# FASTER 的元数据大小计算：(length & kExtraMetadataBitMask) >> (30 - 3)
    // 当位30为1时，结果为 0x40000000 >> 27 = 8
    // 当位30为0时，结果为 0

    let mut span_no_meta = SpanByte::from_slice(b"data");
    assert_eq!(span_no_meta.metadata_size(), 0);

    span_no_meta.set_metadata(123);
    assert_eq!(span_no_meta.metadata_size(), 8);

    // 验证计算与 C# 一致
    let header_with_meta = span_no_meta.len() as u32 | K_EXTRA_METADATA_BIT_MASK;
    let cs_metadata_size = (header_with_meta & K_EXTRA_METADATA_BIT_MASK) >> (30 - 3);
    assert_eq!(cs_metadata_size, 8);

    span_no_meta.clear_metadata();
    let header_no_meta = span_no_meta.len() as u32;
    let cs_metadata_size_zero = (header_no_meta & K_EXTRA_METADATA_BIT_MASK) >> (30 - 3);
    assert_eq!(cs_metadata_size_zero, 0);
}

#[test]
fn test_span_byte_total_size() {
    // C# FASTER TotalSize = sizeof(int) + Length
    // 其中 Length 包含元数据（如果有）

    // 无元数据情况
    let span1 = SpanByte::from_slice(b"test");
    assert_eq!(span1.total_size(), 4 + 4); // header + payload

    // 有元数据情况
    let mut span2 = SpanByte::from_slice(b"test");
    span2.set_metadata(999);
    assert_eq!(span2.total_size(), 4 + 8 + 4); // header + metadata + payload
}

#[test]
fn test_span_byte_serialization_roundtrip() {
    // 测试序列化往返，验证二进制兼容性

    let test_cases = vec![
        (b"".as_slice(), None),
        (b"a", None),
        (b"hello", None),
        (b"hello world test data", None),
        (b"test", Some(123u64)),
        (b"data with metadata", Some(0xFFFFFFFFFFFFFFFF)),
        (b"short", Some(0)),
        (b"another test", Some(1)),
    ];

    for (data, metadata) in test_cases {
        let mut original = SpanByte::from_slice(data);
        if let Some(meta) = metadata {
            original.set_metadata(meta);
        }

        // 序列化
        let mut buffer = vec![0u8; original.total_size()];
        let written = original.serialize_to(&mut buffer);
        assert_eq!(written, original.total_size());

        // 反序列化
        let deserialized = SpanByte::deserialize_from(&buffer).unwrap();

        // 验证数据一致
        assert_eq!(deserialized.as_slice(), data);
        assert_eq!(deserialized.metadata(), metadata);
        assert_eq!(deserialized.len(), data.len());
    }
}

#[test]
fn test_span_byte_view_parsing() {
    // 测试 SpanByteView 能够正确解析序列化的数据

    let data = b"view test data";
    let metadata: u64 = 0xDEADBEEF;
    let mut span = SpanByte::from_slice(data);
    span.set_metadata(metadata);

    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    // 使用 SpanByteView 解析
    let view = SpanByteView::parse(&buffer).unwrap();

    // 验证解析结果
    assert_eq!(view.payload(), data);
    assert_eq!(view.metadata(), Some(metadata));

    // 验证头部
    let header = view.header();
    assert_eq!(header & !K_HEADER_MASK, data.len() as u32);
    assert_eq!(header & K_UNSERIALIZED_BIT_MASK, 0);
    assert_eq!(
        header & K_EXTRA_METADATA_BIT_MASK,
        K_EXTRA_METADATA_BIT_MASK
    );
}

#[test]
fn test_span_byte_endianness() {
    // 验证字节序为小端（与 C# FASTER 一致）

    let data = b"endian test";
    let metadata: u64 = 0x0102030405060708;
    let mut span = SpanByte::from_slice(data);
    span.set_metadata(metadata);

    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    // 验证头部是小端序
    let header_bytes = &buffer[0..4];
    assert_eq!(
        header_bytes[0],
        (data.len() as u32 | K_EXTRA_METADATA_BIT_MASK) as u8
    );

    // 验证元数据是小端序
    let metadata_bytes = &buffer[4..12];
    assert_eq!(metadata_bytes[0], 0x08); // 最低字节
    assert_eq!(metadata_bytes[7], 0x01); // 最高字节
}

#[test]
fn test_span_byte_max_length() {
    // C# FASTER 的最大长度是 2^30 - 1 (约1GB)
    const MAX_LENGTH: u32 = (1 << 30) - 1;

    // 验证最大长度常量
    assert_eq!(MAX_SPAN_BYTE_LENGTH, MAX_LENGTH);

    // 验证头部掩码只使用低30位
    assert_eq!(MAX_LENGTH & K_HEADER_MASK, 0);
    assert_eq!(!K_HEADER_MASK, 0x3FFFFFFF);
}

#[test]
fn test_span_byte_empty() {
    // 测试空 SpanByte
    let span = SpanByte::new();

    assert_eq!(span.len(), 0);
    assert!(span.is_empty());
    assert_eq!(span.total_size(), 4); // 只有头部

    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    // 验证头部
    let header = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
    assert_eq!(header, 0); // 长度为0，无标志位
}

#[test]
fn test_span_byte_flags_preservation() {
    // 测试在设置/清除元数据时标志位是否正确保持

    let mut span = SpanByte::from_slice(b"data");

    // 初始状态：无元数据
    assert!(!span.has_metadata());

    // 设置元数据
    span.set_metadata(123);
    assert!(span.has_metadata());
    assert_eq!(span.len(), 4); // 长度不变

    // 清除元数据
    span.clear_metadata();
    assert!(!span.has_metadata());
    assert_eq!(span.len(), 4); // 长度不变
}

#[test]
fn test_span_byte_binary_layout() {
    // 详细验证二进制布局与 C# FASTER 一致

    let data = b"layout test";
    let metadata: u64 = 0x42;
    let mut span = SpanByte::from_slice(data);
    span.set_metadata(metadata);

    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    // 预期布局：
    // 字节 0-3: 头部 (4字节，小端)
    // 字节 4-11: 元数据 (8字节，小端)
    // 字节 12-22: 负载 (11字节)

    let expected_total = 4 + 8 + 11;
    assert_eq!(buffer.len(), expected_total);

    // 验证头部位置
    let header_start = 0;
    let header_end = 4;
    let header = u32::from_le_bytes(buffer[header_start..header_end].try_into().unwrap());
    assert_eq!(header & !K_HEADER_MASK, data.len() as u32);

    // 验证元数据位置
    let meta_start = 4;
    let meta_end = 12;
    let stored_meta = u64::from_le_bytes(buffer[meta_start..meta_end].try_into().unwrap());
    assert_eq!(stored_meta, metadata);

    // 验证负载位置
    let payload_start = 12;
    let payload_end = 12 + data.len();
    assert_eq!(&buffer[payload_start..payload_end], data);
}

#[test]
fn test_span_byte_unserialized_flag() {
    // C# FASTER 使用位31表示 unserialized/serialized
    // 序列化的 SpanByte 应该清除位31

    let span = SpanByte::from_slice(b"test");
    let mut buffer = vec![0u8; span.total_size()];
    span.serialize_to(&mut buffer);

    let header = u32::from_le_bytes(buffer[0..4].try_into().unwrap());

    // 验证 unserialized 位为0（表示已序列化）
    assert_eq!(header & K_UNSERIALIZED_BIT_MASK, 0);

    // 验证 SpanByteView 拒绝 unserialized 数据
    let mut unserialized_buffer = buffer.clone();
    let unserialized_header = header | K_UNSERIALIZED_BIT_MASK;
    unserialized_buffer[0..4].copy_from_slice(&unserialized_header.to_le_bytes());

    let view = SpanByteView::parse(&unserialized_buffer);
    assert!(
        view.is_none(),
        "SpanByteView should reject unserialized data"
    );
}

#[test]
fn test_span_byte_large_payloads() {
    // 测试大负载（接近但不超过最大值）
    let sizes = vec![1024, 4096, 1024 * 1024]; // 1KB, 4KB, 1MB

    for size in sizes {
        let data = vec![0xAB; size];
        let span = SpanByte::from_vec(data.clone());

        assert_eq!(span.len(), size);
        assert_eq!(span.as_slice(), &data);

        // 测试序列化
        let mut buffer = vec![0u8; span.total_size()];
        span.serialize_to(&mut buffer);

        // 测试反序列化
        let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
        assert_eq!(deserialized.len(), size);
        assert_eq!(deserialized.as_slice(), &data);
    }
}

#[test]
fn test_span_byte_metadata_edge_cases() {
    // 测试元数据的边界情况

    let test_values = vec![
        0u64,
        1u64,
        0xFF,
        0xFFFF,
        0xFFFFFFFF,
        0xFFFFFFFFFFFFFFFF,
        0x123456789ABCDEF0,
    ];

    for &value in &test_values {
        let mut span = SpanByte::from_slice(b"test");
        span.set_metadata(value);

        let mut buffer = vec![0u8; span.total_size()];
        span.serialize_to(&mut buffer);

        let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
        assert_eq!(deserialized.metadata(), Some(value));
    }
}

#[test]
fn test_span_byte_view_write_header() {
    // 测试 SpanByteView::write_header 功能

    let payload = b"test payload";
    let metadata = Some(0xDEADBEEFu64);

    let total_size = SpanByteView::total_size(payload.len(), metadata.is_some());
    let mut buffer = vec![0u8; total_size];

    // 写入头部
    let (payload_slice, written_size) =
        SpanByteView::write_header(&mut buffer, payload.len(), metadata).unwrap();

    assert_eq!(written_size, total_size);
    assert_eq!(payload_slice.len(), payload.len());

    // 写入负载
    payload_slice.copy_from_slice(payload);

    // 验证可以解析
    let view = SpanByteView::parse(&buffer).unwrap();
    assert_eq!(view.payload(), payload);
    assert_eq!(view.metadata(), metadata);
}
