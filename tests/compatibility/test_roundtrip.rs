// 往返测试 - 完整的序列化/反序列化流程测试
//
// 目标: 验证数据可以在 oxifaster 和 C++ FASTER 之间互相转换而不丢失信息

use oxifaster::address::Address;
use oxifaster::checkpoint::binary_format::{CIndexMetadata, CLogMetadata};
use oxifaster::codec::multi_hash::HashAlgorithm;
use oxifaster::format::{FormatDetector, FormatFlags, FormatType, UniversalFormatHeader};
use oxifaster::record::RecordInfo;
use oxifaster::varlen::SpanByte;
use std::io::Cursor;

#[test]
fn test_record_info_roundtrip() {
    // 测试 RecordInfo 的往返转换
    let test_cases = vec![
        (0u64, 0u16, false, false, false),         // 零值
        (1, 1, true, false, false),                // 最小非零值
        (0xFFFFFFFFFFFF, 8191, true, true, true),  // 最大值
        (0x123456789ABC, 100, false, true, false), // 随机值
        (0xDEADBEEF0000, 4095, true, false, true), // 混合值
    ];

    for (prev_addr, version, invalid, tombstone, final_bit) in test_cases {
        // 创建 RecordInfo
        let info = RecordInfo::new(
            Address::from_control(prev_addr),
            version,
            invalid,
            tombstone,
            final_bit,
        );

        // 序列化为 u64
        let encoded = info.control();

        // 反序列化
        let decoded = RecordInfo::from_control(encoded);

        // 验证所有字段
        assert_eq!(decoded.previous_address().control(), prev_addr);
        assert_eq!(decoded.checkpoint_version(), version);
        assert_eq!(decoded.is_invalid(), invalid);
        assert_eq!(decoded.is_tombstone(), tombstone);
        assert_eq!(decoded.is_final(), final_bit);
    }
}

#[test]
fn test_index_metadata_roundtrip() {
    // 测试 IndexMetadata 的往返转换
    let original = CIndexMetadata {
        version: 1,
        table_size: 1024,
        num_ht_bytes: 65536,
        num_ofb_bytes: 8192,
        ofb_count: 128,
        log_begin_address: 0,
        checkpoint_start_address: 1024 * 1024,
    };

    // 序列化
    let buf = original.serialize();

    // 反序列化
    let deserialized = CIndexMetadata::deserialize(&buf).unwrap();

    // 验证所有字段
    assert_eq!(deserialized.version, original.version);
    assert_eq!(deserialized.table_size, original.table_size);
    assert_eq!(deserialized.num_ht_bytes, original.num_ht_bytes);
    assert_eq!(deserialized.num_ofb_bytes, original.num_ofb_bytes);
    assert_eq!(deserialized.ofb_count, original.ofb_count);
    assert_eq!(deserialized.log_begin_address, original.log_begin_address);
    assert_eq!(
        deserialized.checkpoint_start_address,
        original.checkpoint_start_address
    );
}

#[test]
fn test_log_metadata_roundtrip() {
    // 测试 LogMetadata 的往返转换
    let mut original = CLogMetadata {
        use_snapshot_file: true,
        _padding1: [0; 3],
        version: 1,
        num_threads: 8,
        _padding2: [0; 4],
        flushed_address: 1024 * 1024,
        final_address: 2 * 1024 * 1024,
        monotonic_serial_nums: [0u64; 96],
        guids: [0u128; 96],
    };

    // 填充测试数据 - 使用实际的 UUID
    for i in 0..10 {
        original.monotonic_serial_nums[i] = i as u64 * 1000;
        // 创建一个 UUID 并存储为 u128
        let uuid = uuid::Uuid::new_v4();
        original.guids[i] = uuid.as_u128();
    }

    // 序列化
    let buf = original.serialize();

    // 反序列化
    let deserialized = CLogMetadata::deserialize(&buf).unwrap();

    // 验证所有字段
    assert_eq!(deserialized.use_snapshot_file, original.use_snapshot_file);
    assert_eq!(deserialized.version, original.version);
    assert_eq!(deserialized.num_threads, original.num_threads);
    assert_eq!(deserialized.flushed_address, original.flushed_address);
    assert_eq!(deserialized.final_address, original.final_address);

    // 验证数组
    for i in 0..10 {
        assert_eq!(
            deserialized.monotonic_serial_nums[i],
            original.monotonic_serial_nums[i]
        );
        // 验证 GUID 往返
        assert_eq!(
            deserialized.guids[i], original.guids[i],
            "GUID mismatch at index {}",
            i
        );

        // 额外验证：转换回 UUID 字符串应该相同
        let original_uuid = uuid::Uuid::from_u128(original.guids[i]);
        let deserialized_uuid = uuid::Uuid::from_u128(deserialized.guids[i]);
        assert_eq!(
            original_uuid, deserialized_uuid,
            "UUID mismatch at index {}: {} != {}",
            i, original_uuid, deserialized_uuid
        );
    }
}

#[test]
fn test_hash_algorithm_consistency() {
    // 测试不同哈希算法的一致性
    let test_keys: Vec<&[u8]> = vec![
        b"hello",
        b"world",
        b"test_key_12345",
        b"another_test_key_with_longer_length",
        b"",
        b"x",
    ];

    // 只测试启用的算法
    let algorithms = vec![
        HashAlgorithm::XXHash3,      // 默认启用
        HashAlgorithm::FasterCompat, // 总是可用
    ];

    for algorithm in algorithms {
        for key in &test_keys {
            // 计算哈希两次
            let hash1 = algorithm.hash64(key);
            let hash2 = algorithm.hash64(key);

            // 验证一致性
            assert_eq!(
                hash1, hash2,
                "Hash algorithm {:?} inconsistent for key: {:?}",
                algorithm, key
            );
        }
    }
}

#[test]
fn test_format_header_roundtrip() {
    // 测试格式头的往返转换
    let test_cases = vec![
        (
            FormatType::OxifasterLog,
            1u32,
            FormatFlags::empty().with_hash_xxhash3(),
        ),
        (
            FormatType::FasterCompat,
            1,
            FormatFlags::empty()
                .with_c_binary_compat()
                .with_hash_faster_compat(),
        ),
        (
            FormatType::OxifasterCheckpoint,
            2,
            FormatFlags::empty().with_json_format().with_hash_xxhash64(),
        ),
        (
            FormatType::OxifasterIndex,
            1,
            FormatFlags::empty().with_compressed().with_hash_xxhash3(),
        ),
    ];

    for (format_type, version, flags) in test_cases {
        // 创建格式头
        let header = UniversalFormatHeader::new(format_type, version, flags).unwrap();

        // 编码
        let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
        header.encode(&mut buf).unwrap();

        // 解码
        let decoded = UniversalFormatHeader::decode(&buf).unwrap();

        // 验证
        assert_eq!(decoded.format_type(), format_type);
        assert_eq!(decoded.version, version);
        assert_eq!(decoded.flags().value(), flags.value());
        assert!(decoded.verify().is_ok());
    }
}

#[test]
fn test_format_detection_roundtrip() {
    // 测试格式检测的往返
    let format_types = vec![
        FormatType::OxifasterLog,
        FormatType::FasterCompat,
        FormatType::OxifasterCheckpoint,
        FormatType::OxifasterIndex,
    ];

    for format_type in format_types {
        // 创建完整的格式头
        let flags = FormatFlags::empty().with_hash_xxhash3();
        let header = UniversalFormatHeader::new(format_type, 1, flags).unwrap();

        // 编码
        let mut buf = vec![0u8; UniversalFormatHeader::SIZE + 100];
        header.encode(&mut buf).unwrap();

        // 检测格式
        let mut cursor = Cursor::new(&buf);
        let result = FormatDetector::detect(&mut cursor).unwrap();

        // 验证检测结果
        assert_eq!(result.format_type, format_type);
        assert_eq!(result.version, Some(1));
        assert!(result.has_header);

        // 验证 cursor 位置恢复
        assert_eq!(cursor.position(), 0);
    }
}

#[test]
fn test_span_byte_roundtrip() {
    // 测试 SpanByte 的往返转换
    let test_cases = vec![
        (b"".as_slice(), None),
        (b"hello", None),
        (b"world with data", Some(42u64)),
        (b"test with large metadata", Some(0xDEADBEEFCAFEBABE)),
        (
            b"longer test data with more bytes to test serialization",
            Some(12345),
        ),
    ];

    for (data, metadata) in test_cases {
        // 创建 SpanByte
        let mut span = SpanByte::from_slice(data);
        if let Some(meta) = metadata {
            span.set_metadata(meta);
        }

        // 序列化
        let mut buf = vec![0u8; span.total_size()];
        let written = span.serialize_to(&mut buf);
        assert_eq!(written, span.total_size());

        // 反序列化
        let deserialized = SpanByte::deserialize_from(&buf).unwrap();

        // 验证数据
        assert_eq!(deserialized.as_slice(), data);
        assert_eq!(deserialized.metadata(), metadata);
        assert_eq!(deserialized.len(), data.len());

        // 如果有元数据，验证元数据
        if metadata.is_some() {
            assert!(deserialized.has_metadata());
        } else {
            assert!(!deserialized.has_metadata());
        }
    }
}

#[test]
fn test_complete_checkpoint_roundtrip() {
    // 测试完整的 checkpoint 流程往返

    // 创建索引元数据
    let index_meta = CIndexMetadata {
        version: 1,
        table_size: 2048,
        num_ht_bytes: 131072,
        num_ofb_bytes: 16384,
        ofb_count: 256,
        log_begin_address: 0,
        checkpoint_start_address: 2 * 1024 * 1024,
    };

    // 创建日志元数据
    let mut log_meta = CLogMetadata {
        use_snapshot_file: true,
        _padding1: [0; 3],
        version: 1,
        num_threads: 16,
        _padding2: [0; 4],
        flushed_address: 4 * 1024 * 1024,
        final_address: 8 * 1024 * 1024,
        monotonic_serial_nums: [0u64; 96],
        guids: [0u128; 96],
    };

    // 填充会话数据 - 使用实际的 UUID
    for i in 0..16 {
        log_meta.monotonic_serial_nums[i] = i as u64 * 10000;
        let uuid = uuid::Uuid::new_v4();
        log_meta.guids[i] = uuid.as_u128();
    }

    // 序列化索引元数据
    let index_buf = index_meta.serialize();

    // 序列化日志元数据
    let log_buf = log_meta.serialize();

    // 反序列化索引元数据
    let index_recovered = CIndexMetadata::deserialize(&index_buf).unwrap();
    assert_eq!(index_recovered.version, index_meta.version);
    assert_eq!(index_recovered.table_size, index_meta.table_size);
    assert_eq!(
        index_recovered.checkpoint_start_address,
        index_meta.checkpoint_start_address
    );

    // 反序列化日志元数据
    let log_recovered = CLogMetadata::deserialize(&log_buf).unwrap();
    assert_eq!(log_recovered.version, log_meta.version);
    assert_eq!(log_recovered.num_threads, log_meta.num_threads);
    assert_eq!(log_recovered.flushed_address, log_meta.flushed_address);
    assert_eq!(log_recovered.final_address, log_meta.final_address);

    // 验证会话数据
    for i in 0..16 {
        assert_eq!(
            log_recovered.monotonic_serial_nums[i],
            log_meta.monotonic_serial_nums[i]
        );
        assert_eq!(log_recovered.guids[i], log_meta.guids[i]);
    }
}

#[test]
fn test_multi_format_roundtrip() {
    // 测试多种格式的往返

    // 测试1: RecordInfo
    let record = RecordInfo::new(
        Address::from_control(0x123456789ABC),
        100,
        false,
        false,
        false,
    );
    let record_encoded = record.control();
    let record_decoded = RecordInfo::from_control(record_encoded);
    assert_eq!(
        record.previous_address().control(),
        record_decoded.previous_address().control()
    );

    // 测试2: FasterCompat 哈希
    let key = b"test_key";
    let hash1 = HashAlgorithm::FasterCompat.hash64(key);
    let hash2 = HashAlgorithm::FasterCompat.hash64(key);
    assert_eq!(hash1, hash2);

    // 测试3: 格式头
    let header = UniversalFormatHeader::new(
        FormatType::FasterCompat,
        1,
        FormatFlags::empty()
            .with_c_binary_compat()
            .with_hash_faster_compat(),
    )
    .unwrap();
    let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
    header.encode(&mut buf).unwrap();
    let decoded = UniversalFormatHeader::decode(&buf).unwrap();
    assert_eq!(decoded.format_type(), FormatType::FasterCompat);
    assert!(decoded.flags().is_c_binary_compat());
    assert!(decoded.flags().is_hash_faster_compat());

    // 测试4: SpanByte
    let span = SpanByte::from_slice(b"roundtrip test");
    let mut span_buf = vec![0u8; span.total_size()];
    span.serialize_to(&mut span_buf);
    let span_decoded = SpanByte::deserialize_from(&span_buf).unwrap();
    assert_eq!(span.as_slice(), span_decoded.as_slice());
}

#[test]
fn test_cross_platform_roundtrip() {
    // 测试跨平台的往返（字节序）

    // 所有数值都使用小端序
    let test_values: Vec<u64> = vec![
        0,
        1,
        0xFF,
        0xFFFF,
        0xFFFFFFFF,
        0xFFFFFFFFFFFFFFFF,
        0x0123456789ABCDEF,
    ];

    for value in test_values {
        // 小端序编码
        let bytes = value.to_le_bytes();

        // 小端序解码
        let decoded = u64::from_le_bytes(bytes);

        // 验证
        assert_eq!(value, decoded);

        // 验证字节顺序
        assert_eq!(bytes[0], (value & 0xFF) as u8); // 最低字节
        assert_eq!(bytes[7], (value >> 56) as u8); // 最高字节
    }
}

#[test]
fn test_large_data_roundtrip() {
    // 测试大数据的往返

    // 创建大的 SpanByte (1MB)
    let large_data = vec![0xAB; 1024 * 1024];
    let span = SpanByte::from_vec(large_data.clone());

    assert_eq!(span.len(), 1024 * 1024);
    assert_eq!(span.as_slice(), &large_data);

    // 序列化
    let mut buf = vec![0u8; span.total_size()];
    span.serialize_to(&mut buf);

    // 反序列化
    let deserialized = SpanByte::deserialize_from(&buf).unwrap();
    assert_eq!(deserialized.len(), 1024 * 1024);
    assert_eq!(deserialized.as_slice(), &large_data);
}

#[test]
fn test_empty_data_roundtrip() {
    // 测试空数据的往返

    // 空 SpanByte
    let span = SpanByte::new();
    assert_eq!(span.len(), 0);
    assert!(span.is_empty());

    let mut buf = vec![0u8; span.total_size()];
    span.serialize_to(&mut buf);

    let deserialized = SpanByte::deserialize_from(&buf).unwrap();
    assert_eq!(deserialized.len(), 0);
    assert!(deserialized.is_empty());

    // 空键的哈希
    let empty_slice: &[u8] = b"";
    let empty_hash = HashAlgorithm::FasterCompat.hash64(empty_slice);
    assert_eq!(empty_hash, HashAlgorithm::FasterCompat.hash64(empty_slice));
}
