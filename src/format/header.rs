//! 通用格式头 - 用于识别和验证 FASTER 二进制文件格式
//!
//! 提供统一的格式识别机制，支持多种文件类型和兼容模式

use std::fmt;

/// 格式魔数定义
pub mod magic {
    /// oxifaster Log 格式 (版本 1)
    pub const OXFLOG1: [u8; 8] = *b"OXFLOG1\0";

    /// FASTER C++ 兼容格式 (版本 1)
    pub const FASTER01: [u8; 8] = *b"FASTER01";

    /// oxifaster Checkpoint 格式 (版本 1)
    pub const OXFCKPT1: [u8; 8] = *b"OXFCKPT1";

    /// oxifaster Index 格式 (版本 1)
    pub const OXFIDX1: [u8; 8] = *b"OXFIDX1\0";
}

/// 文件格式类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatType {
    /// oxifaster Log 格式
    OxifasterLog,
    /// FASTER C++ 兼容格式
    FasterCompat,
    /// oxifaster Checkpoint 元数据
    OxifasterCheckpoint,
    /// oxifaster Index 文件
    OxifasterIndex,
    /// JSON 格式（用于 checkpoint 元数据）
    Json,
    /// 未知格式
    Unknown,
}

impl FormatType {
    /// 从魔数识别格式类型
    pub fn from_magic(magic: &[u8]) -> Self {
        if magic.is_empty() {
            return Self::Unknown;
        }

        // JSON 格式只需要检查第一个字节
        if magic[0] == b'{' || magic[0] == b'[' {
            return Self::Json;
        }

        // 其他格式需要至少 8 字节
        if magic.len() < 8 {
            return Self::Unknown;
        }

        let magic_slice = &magic[..8];
        if magic_slice == magic::OXFLOG1 {
            Self::OxifasterLog
        } else if magic_slice == magic::FASTER01 {
            Self::FasterCompat
        } else if magic_slice == magic::OXFCKPT1 {
            Self::OxifasterCheckpoint
        } else if magic_slice == magic::OXFIDX1 {
            Self::OxifasterIndex
        } else {
            Self::Unknown
        }
    }

    /// 获取格式名称
    pub fn name(&self) -> &'static str {
        match self {
            Self::OxifasterLog => "OxifasterLog",
            Self::FasterCompat => "FasterCompat",
            Self::OxifasterCheckpoint => "OxifasterCheckpoint",
            Self::OxifasterIndex => "OxifasterIndex",
            Self::Json => "JSON",
            Self::Unknown => "Unknown",
        }
    }

    /// 获取对应的魔数
    pub fn magic(&self) -> Option<&'static [u8; 8]> {
        match self {
            Self::OxifasterLog => Some(&magic::OXFLOG1),
            Self::FasterCompat => Some(&magic::FASTER01),
            Self::OxifasterCheckpoint => Some(&magic::OXFCKPT1),
            Self::OxifasterIndex => Some(&magic::OXFIDX1),
            Self::Json | Self::Unknown => None,
        }
    }
}

impl fmt::Display for FormatType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// 格式标志位
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatFlags(u32);

impl FormatFlags {
    /// C 风格二进制兼容模式
    pub const C_BINARY_COMPAT: u32 = 1 << 0;
    /// JSON 格式
    pub const JSON_FORMAT: u32 = 1 << 1;
    /// 使用 XXHash3 哈希
    pub const HASH_XXHASH3: u32 = 1 << 2;
    /// 使用 XXHash64 哈希
    pub const HASH_XXHASH64: u32 = 1 << 3;
    /// 使用 FASTER 兼容哈希
    pub const HASH_FASTER_COMPAT: u32 = 1 << 4;
    /// 启用压缩
    pub const COMPRESSED: u32 = 1 << 5;
    /// 启用加密
    pub const ENCRYPTED: u32 = 1 << 6;

    /// 创建新的格式标志
    pub fn new(flags: u32) -> Self {
        Self(flags)
    }

    /// 创建空的格式标志
    pub fn empty() -> Self {
        Self(0)
    }

    /// 添加 C 风格二进制兼容标志
    pub fn with_c_binary_compat(mut self) -> Self {
        self.0 |= Self::C_BINARY_COMPAT;
        self
    }

    /// 添加 JSON 格式标志
    pub fn with_json_format(mut self) -> Self {
        self.0 |= Self::JSON_FORMAT;
        self
    }

    /// 添加 XXHash3 哈希标志
    pub fn with_hash_xxhash3(mut self) -> Self {
        self.0 |= Self::HASH_XXHASH3;
        self
    }

    /// 添加 XXHash64 哈希标志
    pub fn with_hash_xxhash64(mut self) -> Self {
        self.0 |= Self::HASH_XXHASH64;
        self
    }

    /// 添加 FASTER 兼容哈希标志
    pub fn with_hash_faster_compat(mut self) -> Self {
        self.0 |= Self::HASH_FASTER_COMPAT;
        self
    }

    /// 添加压缩标志
    pub fn with_compressed(mut self) -> Self {
        self.0 |= Self::COMPRESSED;
        self
    }

    /// 添加加密标志
    pub fn with_encrypted(mut self) -> Self {
        self.0 |= Self::ENCRYPTED;
        self
    }

    /// 检查是否为 C 风格二进制兼容
    pub fn is_c_binary_compat(&self) -> bool {
        self.0 & Self::C_BINARY_COMPAT != 0
    }

    /// 检查是否为 JSON 格式
    pub fn is_json_format(&self) -> bool {
        self.0 & Self::JSON_FORMAT != 0
    }

    /// 检查是否使用 XXHash3
    pub fn is_hash_xxhash3(&self) -> bool {
        self.0 & Self::HASH_XXHASH3 != 0
    }

    /// 检查是否使用 XXHash64
    pub fn is_hash_xxhash64(&self) -> bool {
        self.0 & Self::HASH_XXHASH64 != 0
    }

    /// 检查是否使用 FASTER 兼容哈希
    pub fn is_hash_faster_compat(&self) -> bool {
        self.0 & Self::HASH_FASTER_COMPAT != 0
    }

    /// 检查是否启用压缩
    pub fn is_compressed(&self) -> bool {
        self.0 & Self::COMPRESSED != 0
    }

    /// 检查是否启用加密
    pub fn is_encrypted(&self) -> bool {
        self.0 & Self::ENCRYPTED != 0
    }

    /// 获取原始标志值
    pub fn value(&self) -> u32 {
        self.0
    }
}

/// 通用格式头错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatHeaderError {
    /// 缓冲区太小
    BufferTooSmall,
    /// 魔数不匹配
    MagicMismatch,
    /// 校验和不匹配
    ChecksumMismatch,
    /// 不支持的版本
    UnsupportedVersion(u32),
    /// 无效格式
    InvalidFormat,
}

impl fmt::Display for FormatHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "buffer too small for format header"),
            Self::MagicMismatch => write!(f, "format magic mismatch"),
            Self::ChecksumMismatch => write!(f, "format header checksum mismatch"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported format version: {}", v),
            Self::InvalidFormat => write!(f, "invalid format"),
        }
    }
}

impl std::error::Error for FormatHeaderError {}

/// 通用格式头
///
/// 布局 (24 字节):
/// - magic: [u8; 8]      魔数（格式标识）
/// - version: u32        版本号
/// - format_flags: u32   格式标志位
/// - checksum: u64       校验和（XOR 前 16 字节）
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct UniversalFormatHeader {
    /// 魔数（格式标识）
    pub magic: [u8; 8],
    /// 版本号
    pub version: u32,
    /// 格式标志位
    pub format_flags: u32,
    /// 校验和
    pub checksum: u64,
}

impl UniversalFormatHeader {
    /// 格式头大小（字节）
    pub const SIZE: usize = 24;

    /// 校验和偏移
    const CHECKSUM_OFFSET: usize = 16;

    /// 创建新的格式头
    pub fn new(format_type: FormatType, version: u32, flags: FormatFlags) -> Option<Self> {
        let magic = *format_type.magic()?;
        let mut header = Self {
            magic,
            version,
            format_flags: flags.value(),
            checksum: 0,
        };
        header.checksum = header.compute_checksum();
        Some(header)
    }

    /// 编码格式头到缓冲区
    pub fn encode(&self, buf: &mut [u8]) -> Result<(), FormatHeaderError> {
        if buf.len() < Self::SIZE {
            return Err(FormatHeaderError::BufferTooSmall);
        }

        let mut offset = 0;

        // 魔数
        buf[offset..offset + 8].copy_from_slice(&self.magic);
        offset += 8;

        // 版本
        buf[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;

        // 格式标志
        buf[offset..offset + 4].copy_from_slice(&self.format_flags.to_le_bytes());
        offset += 4;

        // 校验和
        buf[offset..offset + 8].copy_from_slice(&self.checksum.to_le_bytes());

        Ok(())
    }

    /// 从缓冲区解码格式头
    pub fn decode(buf: &[u8]) -> Result<Self, FormatHeaderError> {
        if buf.len() < Self::SIZE {
            return Err(FormatHeaderError::BufferTooSmall);
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let format_flags = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let checksum = u64::from_le_bytes(buf[16..24].try_into().unwrap());

        let header = Self {
            magic,
            version,
            format_flags,
            checksum,
        };

        // 验证校验和
        let computed_checksum = header.compute_checksum();
        if checksum != computed_checksum {
            return Err(FormatHeaderError::ChecksumMismatch);
        }

        Ok(header)
    }

    /// 计算校验和（XOR 前 16 字节）
    fn compute_checksum(&self) -> u64 {
        let mut buf = [0u8; Self::CHECKSUM_OFFSET];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.format_flags.to_le_bytes());

        compute_xor_checksum(&buf)
    }

    /// 获取格式类型
    pub fn format_type(&self) -> FormatType {
        FormatType::from_magic(&self.magic)
    }

    /// 获取格式标志
    pub fn flags(&self) -> FormatFlags {
        FormatFlags::new(self.format_flags)
    }

    /// 验证格式头
    pub fn verify(&self) -> Result<(), FormatHeaderError> {
        let computed = self.compute_checksum();
        if self.checksum != computed {
            return Err(FormatHeaderError::ChecksumMismatch);
        }

        let format_type = self.format_type();
        if format_type == FormatType::Unknown {
            return Err(FormatHeaderError::InvalidFormat);
        }

        Ok(())
    }
}

/// 计算 XOR 校验和
pub fn compute_xor_checksum(data: &[u8]) -> u64 {
    let mut checksum: u64 = 0;

    let mut chunks = data.chunks_exact(8);
    for chunk in chunks.by_ref() {
        checksum ^= u64::from_le_bytes(chunk.try_into().unwrap());
    }

    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut last = [0u8; 8];
        last[..remainder.len()].copy_from_slice(remainder);
        checksum ^= u64::from_le_bytes(last);
    }

    checksum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_type_from_magic() {
        assert_eq!(
            FormatType::from_magic(&magic::OXFLOG1),
            FormatType::OxifasterLog
        );
        assert_eq!(
            FormatType::from_magic(&magic::FASTER01),
            FormatType::FasterCompat
        );
        assert_eq!(
            FormatType::from_magic(&magic::OXFCKPT1),
            FormatType::OxifasterCheckpoint
        );
        assert_eq!(
            FormatType::from_magic(&magic::OXFIDX1),
            FormatType::OxifasterIndex
        );
        assert_eq!(FormatType::from_magic(b"{\"foo\":"), FormatType::Json);
        assert_eq!(FormatType::from_magic(b"UNKNOWN\0"), FormatType::Unknown);
    }

    #[test]
    fn test_format_flags() {
        let flags = FormatFlags::empty()
            .with_c_binary_compat()
            .with_hash_xxhash3();

        assert!(flags.is_c_binary_compat());
        assert!(flags.is_hash_xxhash3());
        assert!(!flags.is_json_format());
        assert!(!flags.is_compressed());
    }

    #[test]
    fn test_universal_format_header_roundtrip() {
        let flags = FormatFlags::empty()
            .with_c_binary_compat()
            .with_hash_faster_compat();

        let header = UniversalFormatHeader::new(FormatType::FasterCompat, 1, flags).unwrap();

        let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
        header.encode(&mut buf).unwrap();

        let decoded = UniversalFormatHeader::decode(&buf).unwrap();

        assert_eq!(decoded.magic, magic::FASTER01);
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.format_flags, flags.value());
        assert_eq!(decoded.checksum, header.checksum);
        assert_eq!(decoded.format_type(), FormatType::FasterCompat);
    }

    #[test]
    fn test_format_header_verify() {
        let flags = FormatFlags::empty().with_hash_xxhash3();
        let header = UniversalFormatHeader::new(FormatType::OxifasterLog, 1, flags).unwrap();

        assert!(header.verify().is_ok());
    }

    #[test]
    fn test_format_header_checksum_mismatch() {
        let flags = FormatFlags::empty();
        let mut header = UniversalFormatHeader::new(FormatType::OxifasterLog, 1, flags).unwrap();

        // 篡改版本号
        header.version = 999;

        // 验证应该失败
        assert_eq!(
            header.verify().unwrap_err(),
            FormatHeaderError::ChecksumMismatch
        );
    }

    #[test]
    fn test_format_header_encode_decode() {
        let test_cases = vec![
            (
                FormatType::OxifasterLog,
                FormatFlags::empty().with_hash_xxhash3(),
            ),
            (
                FormatType::FasterCompat,
                FormatFlags::empty()
                    .with_c_binary_compat()
                    .with_hash_faster_compat(),
            ),
            (
                FormatType::OxifasterCheckpoint,
                FormatFlags::empty().with_json_format(),
            ),
        ];

        for (format_type, flags) in test_cases {
            let header = UniversalFormatHeader::new(format_type, 1, flags).unwrap();
            let mut buf = vec![0u8; UniversalFormatHeader::SIZE];
            header.encode(&mut buf).unwrap();
            let decoded = UniversalFormatHeader::decode(&buf).unwrap();

            assert_eq!(decoded.format_type(), format_type);
            assert_eq!(decoded.flags().value(), flags.value());
            assert!(decoded.verify().is_ok());
        }
    }
}
