//! 格式自动检测器
//!
//! 提供文件格式的自动识别功能

use std::io::{self, Read, Seek, SeekFrom};

use super::{FormatType, UniversalFormatHeader};

/// 格式检测结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectionResult {
    /// 检测到的格式类型
    pub format_type: FormatType,
    /// 格式版本（如果有）
    pub version: Option<u32>,
    /// 是否有完整的格式头
    pub has_header: bool,
    /// 额外信息
    pub info: String,
}

impl DetectionResult {
    /// 创建新的检测结果
    pub fn new(format_type: FormatType) -> Self {
        Self {
            format_type,
            version: None,
            has_header: false,
            info: String::new(),
        }
    }

    /// 设置版本号
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// 标记有完整格式头
    pub fn with_header(mut self) -> Self {
        self.has_header = true;
        self
    }

    /// 添加额外信息
    pub fn with_info(mut self, info: impl Into<String>) -> Self {
        self.info = info.into();
        self
    }
}

/// 格式检测器
pub struct FormatDetector;

impl FormatDetector {
    /// 最小读取字节数（用于魔数识别）
    const MIN_READ_SIZE: usize = 64;

    /// 从 Reader 检测格式
    ///
    /// 读取文件头部信息并尝试识别格式类型。
    /// 此方法会尝试将 reader 位置重置到起始位置。
    pub fn detect<R: Read + Seek>(reader: &mut R) -> io::Result<DetectionResult> {
        // 保存当前位置
        let original_pos = reader.stream_position()?;

        // 读取前面的字节
        reader.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; Self::MIN_READ_SIZE];
        let bytes_read = reader.read(&mut buf)?;

        // 恢复位置
        reader.seek(SeekFrom::Start(original_pos))?;

        if bytes_read == 0 {
            return Ok(DetectionResult::new(FormatType::Unknown).with_info("Empty file"));
        }

        // 截取实际读取的字节
        buf.truncate(bytes_read);

        Self::detect_from_bytes(&buf)
    }

    /// 从字节切片检测格式
    pub fn detect_from_bytes(buf: &[u8]) -> io::Result<DetectionResult> {
        if buf.is_empty() {
            return Ok(DetectionResult::new(FormatType::Unknown).with_info("Empty buffer"));
        }

        // 尝试解析通用格式头
        if buf.len() >= UniversalFormatHeader::SIZE {
            if let Ok(header) = UniversalFormatHeader::decode(buf) {
                if header.verify().is_ok() {
                    return Ok(DetectionResult::new(header.format_type())
                        .with_version(header.version)
                        .with_header()
                        .with_info(format!(
                            "Valid format header found: {}",
                            header.format_type()
                        )));
                }
            }
        }

        // 尝试识别魔数（不依赖完整格式头）
        if buf.len() >= 8 {
            let format_type = FormatType::from_magic(buf);
            if format_type != FormatType::Unknown && format_type != FormatType::Json {
                return Ok(DetectionResult::new(format_type)
                    .with_info(format!("Magic number detected: {}", format_type)));
            }
        }

        // 检测 JSON 格式
        if Self::is_json_format(buf) {
            return Ok(DetectionResult::new(FormatType::Json).with_info("JSON format detected"));
        }

        // 检测 C 风格二进制 checkpoint
        if Self::is_c_binary_checkpoint(buf) {
            return Ok(DetectionResult::new(FormatType::FasterCompat)
                .with_info("C-style binary checkpoint detected"));
        }

        Ok(DetectionResult::new(FormatType::Unknown).with_info("Unable to determine format"))
    }

    /// 检测是否为 JSON 格式
    fn is_json_format(buf: &[u8]) -> bool {
        if buf.is_empty() {
            return false;
        }

        // 跳过前导空白
        let trimmed = buf.iter().skip_while(|&&b| b.is_ascii_whitespace());

        // 检查第一个非空白字符
        if let Some(&first) = trimmed.clone().next() {
            if first == b'{' || first == b'[' {
                // 尝试查找配对的结束符
                let content: Vec<u8> = trimmed.copied().collect();
                if content.len() < 2 {
                    return false;
                }

                // 简单检查：至少有开始和某些 JSON 结构特征
                let s = String::from_utf8_lossy(&content);
                return s.contains(':') || s.contains(',');
            }
        }

        false
    }

    /// 检测是否为 C 风格二进制 checkpoint
    fn is_c_binary_checkpoint(buf: &[u8]) -> bool {
        // IndexMetadata 是 56 字节
        if buf.len() < 56 {
            return false;
        }

        // 读取 version 字段（前 4 字节）
        let version = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);

        // 版本号应该是一个合理的值（例如 1-100）
        if version == 0 || version > 100 {
            return false;
        }

        // 读取 table_size 字段（跳过 4 字节对齐填充）
        let table_size = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);

        // table_size 应该是 2 的幂
        if table_size == 0 || (table_size & (table_size - 1)) != 0 {
            return false;
        }

        // 如果版本号和 table_size 都合理，可能是 C 风格 checkpoint
        true
    }

    /// 从文件路径检测格式
    pub fn detect_from_file(path: impl AsRef<std::path::Path>) -> io::Result<DetectionResult> {
        let mut file = std::fs::File::open(path)?;
        Self::detect(&mut file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{FormatFlags, UniversalFormatHeader};

    #[test]
    fn test_detect_universal_format_header() {
        let flags = FormatFlags::empty().with_hash_xxhash3();
        let header = UniversalFormatHeader::new(FormatType::OxifasterLog, 1, flags).unwrap();

        let mut buf = vec![0u8; UniversalFormatHeader::SIZE + 10];
        header.encode(&mut buf).unwrap();

        let result = FormatDetector::detect_from_bytes(&buf).unwrap();

        assert_eq!(result.format_type, FormatType::OxifasterLog);
        assert_eq!(result.version, Some(1));
        assert!(result.has_header);
    }

    #[test]
    fn test_detect_magic_without_full_header() {
        // 只有魔数，没有完整格式头
        let buf = b"OXFLOG1\0some random data";

        let result = FormatDetector::detect_from_bytes(buf).unwrap();

        assert_eq!(result.format_type, FormatType::OxifasterLog);
        assert!(!result.has_header);
    }

    #[test]
    fn test_detect_json_format() {
        let json_buf = br#"{
            "version": 1,
            "table_size": 1024,
            "data": []
        }"#;

        let result = FormatDetector::detect_from_bytes(json_buf).unwrap();

        assert_eq!(result.format_type, FormatType::Json);
    }

    #[test]
    fn test_detect_json_with_whitespace() {
        let json_buf = b"   \n\t  { \"foo\": 123 }";

        let result = FormatDetector::detect_from_bytes(json_buf).unwrap();

        assert_eq!(result.format_type, FormatType::Json);
    }

    #[test]
    fn test_detect_c_binary_checkpoint() {
        // 构造一个简单的 C IndexMetadata
        let mut buf = vec![0u8; 64];
        // version = 1
        buf[0..4].copy_from_slice(&1u32.to_le_bytes());
        // padding 4 bytes
        // table_size = 1024 (2^10, 2的幂)
        buf[8..16].copy_from_slice(&1024u64.to_le_bytes());

        let result = FormatDetector::detect_from_bytes(&buf).unwrap();

        assert_eq!(result.format_type, FormatType::FasterCompat);
    }

    #[test]
    fn test_detect_empty_buffer() {
        let buf: &[u8] = &[];

        let result = FormatDetector::detect_from_bytes(buf).unwrap();

        assert_eq!(result.format_type, FormatType::Unknown);
    }

    #[test]
    fn test_detect_unknown_format() {
        let buf = b"RANDOM DATA THAT DOES NOT MATCH ANY FORMAT";

        let result = FormatDetector::detect_from_bytes(buf).unwrap();

        assert_eq!(result.format_type, FormatType::Unknown);
    }

    #[test]
    fn test_is_json_format_negative_cases() {
        assert!(!FormatDetector::is_json_format(b""));
        assert!(!FormatDetector::is_json_format(b"plain text"));
        assert!(!FormatDetector::is_json_format(b"{")); // 只有左括号，没有其他 JSON 结构
        assert!(!FormatDetector::is_json_format(b"["));
    }

    #[test]
    fn test_is_c_binary_checkpoint_negative_cases() {
        // 版本号为 0
        let mut buf = vec![0u8; 64];
        buf[0..4].copy_from_slice(&0u32.to_le_bytes());
        buf[8..16].copy_from_slice(&1024u64.to_le_bytes());
        assert!(!FormatDetector::is_c_binary_checkpoint(&buf));

        // table_size 不是 2 的幂
        let mut buf = vec![0u8; 64];
        buf[0..4].copy_from_slice(&1u32.to_le_bytes());
        buf[8..16].copy_from_slice(&1000u64.to_le_bytes());
        assert!(!FormatDetector::is_c_binary_checkpoint(&buf));

        // 缓冲区太小
        assert!(!FormatDetector::is_c_binary_checkpoint(&[0u8; 10]));
    }
}
