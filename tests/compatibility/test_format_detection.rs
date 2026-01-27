// 格式识别兼容性测试
//
// 目标: 验证格式自动检测和格式头功能

use oxifaster::format::{FormatDetector, FormatFlags, FormatType, UniversalFormatHeader};
use std::io::Cursor;

#[test]
fn test_detect_oxifaster_log_format() {
    // 创建一个带有 oxifaster log 魔数的缓冲区
    let mut buf = vec![0u8; 64];
    buf[..8].copy_from_slice(b"OXFLOG1\0");

    let result = FormatDetector::detect_from_bytes(&buf).unwrap();

    assert_eq!(result.format_type, FormatType::OxifasterLog);
    assert!(!result.has_header); // 只有魔数，没有完整格式头
}

#[test]
fn test_detect_faster_compat_format() {
    // 创建一个带有 FASTER 魔数的缓冲区
    let mut buf = vec![0u8; 64];
    buf[..8].copy_from_slice(b"FASTER01");

    let result = FormatDetector::detect_from_bytes(&buf).unwrap();

    assert_eq!(result.format_type, FormatType::FasterCompat);
    assert!(!result.has_header);
}

#[test]
fn test_detect_with_complete_header() {
    // 创建完整的格式头
    let flags = FormatFlags::empty()
        .with_c_binary_compat()
        .with_hash_faster_compat();

    let header = UniversalFormatHeader::new(FormatType::FasterCompat, 1, flags).unwrap();

    let mut buf = vec![0u8; UniversalFormatHeader::SIZE + 20];
    header.encode(&mut buf).unwrap();

    let result = FormatDetector::detect_from_bytes(&buf).unwrap();

    assert_eq!(result.format_type, FormatType::FasterCompat);
    assert_eq!(result.version, Some(1));
    assert!(result.has_header);
    assert!(result.info.contains("Valid format header"));
}

#[test]
fn test_detect_json_checkpoint() {
    let json_data = br#"{
        "index_metadata": {
            "version": 1,
            "table_size": 1024
        }
    }"#;

    let result = FormatDetector::detect_from_bytes(json_data).unwrap();

    assert_eq!(result.format_type, FormatType::Json);
    assert!(result.info.contains("JSON"));
}

#[test]
fn test_detect_c_binary_checkpoint() {
    // 模拟 C 风格 IndexMetadata
    let mut buf = vec![0u8; 64];
    // version = 1
    buf[0..4].copy_from_slice(&1u32.to_le_bytes());
    // padding 4 bytes
    // table_size = 1024 (2^10)
    buf[8..16].copy_from_slice(&1024u64.to_le_bytes());
    // num_ht_bytes
    buf[16..24].copy_from_slice(&(1024u64 * 64).to_le_bytes());
    // num_ofb_bytes
    buf[24..32].copy_from_slice(&0u64.to_le_bytes());

    let result = FormatDetector::detect_from_bytes(&buf).unwrap();

    assert_eq!(result.format_type, FormatType::FasterCompat);
    assert!(result.info.contains("C-style binary checkpoint"));
}

#[test]
fn test_detect_from_reader() {
    // 创建一个带有格式头的缓冲区
    let flags = FormatFlags::empty().with_hash_xxhash3();
    let header = UniversalFormatHeader::new(FormatType::OxifasterLog, 1, flags).unwrap();

    let mut buf = vec![0u8; UniversalFormatHeader::SIZE + 10];
    header.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let result = FormatDetector::detect(&mut cursor).unwrap();

    assert_eq!(result.format_type, FormatType::OxifasterLog);
    assert_eq!(result.version, Some(1));
    assert!(result.has_header);

    // 验证 cursor 位置被恢复到原始位置
    assert_eq!(cursor.position(), 0);
}

#[test]
fn test_format_header_with_various_flags() {
    let test_cases = vec![
        ("XXHash3", FormatFlags::empty().with_hash_xxhash3()),
        ("XXHash64", FormatFlags::empty().with_hash_xxhash64()),
        (
            "FASTER Compat Hash",
            FormatFlags::empty().with_hash_faster_compat(),
        ),
        (
            "C Binary + FASTER Hash",
            FormatFlags::empty()
                .with_c_binary_compat()
                .with_hash_faster_compat(),
        ),
        (
            "JSON + XXHash3",
            FormatFlags::empty().with_json_format().with_hash_xxhash3(),
        ),
        (
            "Compressed",
            FormatFlags::empty().with_compressed().with_hash_xxhash3(),
        ),
        (
            "Encrypted",
            FormatFlags::empty().with_encrypted().with_hash_xxhash3(),
        ),
    ];

    for (name, flags) in test_cases {
        let header = UniversalFormatHeader::new(FormatType::OxifasterLog, 1, flags).unwrap();

        let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
        header.encode(&mut buf).unwrap();

        let decoded = UniversalFormatHeader::decode(&buf).unwrap();

        assert_eq!(decoded.version, 1, "Failed for: {}", name);
        assert_eq!(decoded.format_flags, flags.value(), "Failed for: {}", name);
        assert!(decoded.verify().is_ok(), "Failed for: {}", name);

        // 验证标志位正确解析
        let decoded_flags = decoded.flags();
        assert_eq!(decoded_flags.value(), flags.value(), "Failed for: {}", name);
    }
}

#[test]
fn test_format_header_all_format_types() {
    let format_types = vec![
        FormatType::OxifasterLog,
        FormatType::FasterCompat,
        FormatType::OxifasterCheckpoint,
        FormatType::OxifasterIndex,
    ];

    let flags = FormatFlags::empty().with_hash_xxhash3();

    for format_type in format_types {
        let header = UniversalFormatHeader::new(format_type, 1, flags).unwrap();

        let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
        header.encode(&mut buf).unwrap();

        let decoded = UniversalFormatHeader::decode(&buf).unwrap();

        assert_eq!(decoded.format_type(), format_type);
        assert_eq!(decoded.version, 1);
        assert!(decoded.verify().is_ok());
    }
}

#[test]
fn test_format_header_version_numbers() {
    let flags = FormatFlags::empty();

    for version in [1u32, 2, 10, 100, 255, 1000, u32::MAX] {
        let header = UniversalFormatHeader::new(FormatType::OxifasterLog, version, flags).unwrap();

        let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
        header.encode(&mut buf).unwrap();

        let decoded = UniversalFormatHeader::decode(&buf).unwrap();

        assert_eq!(decoded.version, version);
        assert!(decoded.verify().is_ok());
    }
}

#[test]
fn test_format_header_corruption_detection() {
    let flags = FormatFlags::empty().with_hash_xxhash3();
    let header = UniversalFormatHeader::new(FormatType::OxifasterLog, 1, flags).unwrap();

    let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
    header.encode(&mut buf).unwrap();

    // 测试各种损坏情况
    let corruption_tests = vec![
        ("Corrupt magic byte", 0, 0xFF),
        ("Corrupt version byte", 8, 0xFF),
        ("Corrupt flags byte", 12, 0xFF),
    ];

    for (name, offset, corrupt_value) in corruption_tests {
        let mut corrupted = buf.clone();
        corrupted[offset] = corrupt_value;

        let result = UniversalFormatHeader::decode(&corrupted);

        // 魔数损坏可能导致解码失败或校验失败
        if let Ok(header) = result {
            assert!(
                header.verify().is_err(),
                "Should detect corruption: {}",
                name
            );
        }
    }
}

#[test]
fn test_detect_empty_file() {
    let empty_buf: &[u8] = &[];

    let result = FormatDetector::detect_from_bytes(empty_buf).unwrap();

    assert_eq!(result.format_type, FormatType::Unknown);
    assert!(result.info.contains("Empty"));
}

#[test]
fn test_detect_too_short_buffer() {
    // 只有 4 字节，不足以构成任何完整格式
    let short_buf = b"OXFL";

    let result = FormatDetector::detect_from_bytes(short_buf).unwrap();

    assert_eq!(result.format_type, FormatType::Unknown);
}

#[test]
fn test_detect_unknown_magic() {
    let unknown_buf = b"BADMAGIC12345678";

    let result = FormatDetector::detect_from_bytes(unknown_buf).unwrap();

    assert_eq!(result.format_type, FormatType::Unknown);
}

#[test]
fn test_format_flags_combinations() {
    // 测试多个标志位的组合
    let flags = FormatFlags::empty()
        .with_c_binary_compat()
        .with_json_format()
        .with_hash_xxhash3()
        .with_hash_xxhash64()
        .with_hash_faster_compat()
        .with_compressed()
        .with_encrypted();

    assert!(flags.is_c_binary_compat());
    assert!(flags.is_json_format());
    assert!(flags.is_hash_xxhash3());
    assert!(flags.is_hash_xxhash64());
    assert!(flags.is_hash_faster_compat());
    assert!(flags.is_compressed());
    assert!(flags.is_encrypted());

    // 验证原始值
    let expected_value = FormatFlags::C_BINARY_COMPAT
        | FormatFlags::JSON_FORMAT
        | FormatFlags::HASH_XXHASH3
        | FormatFlags::HASH_XXHASH64
        | FormatFlags::HASH_FASTER_COMPAT
        | FormatFlags::COMPRESSED
        | FormatFlags::ENCRYPTED;

    assert_eq!(flags.value(), expected_value);
}

#[test]
fn test_format_type_name_consistency() {
    let types = vec![
        (FormatType::OxifasterLog, "OxifasterLog"),
        (FormatType::FasterCompat, "FasterCompat"),
        (FormatType::OxifasterCheckpoint, "OxifasterCheckpoint"),
        (FormatType::OxifasterIndex, "OxifasterIndex"),
        (FormatType::Json, "JSON"),
        (FormatType::Unknown, "Unknown"),
    ];

    for (format_type, expected_name) in types {
        assert_eq!(format_type.name(), expected_name);
        assert_eq!(format!("{}", format_type), expected_name);
    }
}

#[test]
fn test_format_type_magic_consistency() {
    // 验证每个格式类型的魔数是一致的
    let types_with_magic = vec![
        FormatType::OxifasterLog,
        FormatType::FasterCompat,
        FormatType::OxifasterCheckpoint,
        FormatType::OxifasterIndex,
    ];

    for format_type in types_with_magic {
        if let Some(magic) = format_type.magic() {
            // 从魔数识别应该返回相同的格式类型
            assert_eq!(FormatType::from_magic(magic), format_type);
        }
    }
}
