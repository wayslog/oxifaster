// C 风格二进制格式序列化
//
// 目标: 与 Microsoft FASTER C++ 实现的 checkpoint 元数据格式完全兼容
//
// 参考: /Users/xuesong.zhao/repo/cpp/FASTER/cc/src/core/checkpoint_state.h:29-108

use std::io::{self, Read, Write};
use std::mem;

use uuid::Uuid;

use crate::address::Address;
use crate::checkpoint::{IndexMetadata, LogMetadata, SessionState};

/// C 风格 IndexMetadata (固定 56 字节)
///
/// C++ 结构:
/// ```cpp
/// struct IndexMetadata {
///     uint32_t version;                    // 4 字节
///     uint64_t table_size;                 // 8 字节
///     uint64_t num_ht_bytes;              // 8 字节
///     uint64_t num_ofb_bytes;             // 8 字节
///     uint64_t ofb_count;                 // 8 字节 (FixedPageAddress)
///     uint64_t log_begin_address;         // 8 字节
///     uint64_t checkpoint_start_address;  // 8 字节
///     // 总计: 56 字节
/// };
/// ```
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CIndexMetadata {
    /// Index version
    pub version: u32,
    /// Hash table size (number of buckets)
    pub table_size: u64,
    /// Number of bytes in the hash table
    pub num_ht_bytes: u64,
    /// Number of bytes in overflow buckets
    pub num_ofb_bytes: u64,
    /// Overflow bucket count (corresponds to num_buckets)
    pub ofb_count: u64,
    /// Log begin address
    pub log_begin_address: u64,
    /// Checkpoint start address
    pub checkpoint_start_address: u64,
}

impl CIndexMetadata {
    /// 结构大小 (56 字节)
    pub const SIZE: usize = 56;

    /// 创建新的 C 风格元数据
    pub fn new() -> Self {
        Self {
            version: 0,
            table_size: 0,
            num_ht_bytes: 0,
            num_ofb_bytes: 0,
            ofb_count: 0,
            log_begin_address: 0,
            checkpoint_start_address: 0,
        }
    }

    /// 从 IndexMetadata 转换
    pub fn from_metadata(meta: &IndexMetadata) -> Self {
        Self {
            version: meta.version,
            table_size: meta.table_size,
            num_ht_bytes: meta.num_ht_bytes,
            num_ofb_bytes: meta.num_ofb_bytes,
            ofb_count: meta.num_buckets, // C++ 中是 overflow bucket count
            log_begin_address: meta.log_begin_address.control(),
            checkpoint_start_address: meta.checkpoint_start_address.control(),
        }
    }

    /// 转换为 IndexMetadata
    pub fn to_metadata(&self, token: Uuid) -> IndexMetadata {
        IndexMetadata {
            token,
            version: self.version,
            table_size: self.table_size,
            num_ht_bytes: self.num_ht_bytes,
            num_ofb_bytes: self.num_ofb_bytes,
            num_buckets: self.ofb_count,
            num_entries: 0, // C++ 格式不包含此字段，需要从其他地方获取
            log_begin_address: Address::from_control(self.log_begin_address),
            checkpoint_start_address: Address::from_control(self.checkpoint_start_address),
        }
    }

    /// 序列化为字节数组 (小端)
    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let mut offset = 0;

        // version (4 字节)
        bytes[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;

        // padding (4 字节)
        offset += 4;

        // table_size (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.table_size.to_le_bytes());
        offset += 8;

        // num_ht_bytes (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.num_ht_bytes.to_le_bytes());
        offset += 8;

        // num_ofb_bytes (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.num_ofb_bytes.to_le_bytes());
        offset += 8;

        // ofb_count (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.ofb_count.to_le_bytes());
        offset += 8;

        // log_begin_address (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.log_begin_address.to_le_bytes());
        offset += 8;

        // checkpoint_start_address (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.checkpoint_start_address.to_le_bytes());

        bytes
    }

    /// 从字节数组反序列化 (小端)
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid CIndexMetadata size: expected {}, got {}",
                    Self::SIZE,
                    bytes.len()
                ),
            ));
        }

        let mut offset = 0;

        // version
        let version = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // padding
        offset += 4;

        // table_size
        let table_size = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // num_ht_bytes
        let num_ht_bytes = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // num_ofb_bytes
        let num_ofb_bytes = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // ofb_count
        let ofb_count = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // log_begin_address
        let log_begin_address = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // checkpoint_start_address
        let checkpoint_start_address = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);

        Ok(Self {
            version,
            table_size,
            num_ht_bytes,
            num_ofb_bytes,
            ofb_count,
            log_begin_address,
            checkpoint_start_address,
        })
    }

    /// 写入到 writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.serialize())
    }

    /// 从 reader 读取
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut bytes = [0u8; Self::SIZE];
        reader.read_exact(&mut bytes)?;
        Self::deserialize(&bytes)
    }
}

impl Default for CIndexMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// C 风格 LogMetadata (固定 2336 字节)
///
/// C++ 结构:
/// ```cpp
/// struct LogMetadata {
///     bool use_snapshot_file;           // 1 字节
///     uint8_t _padding1[3];            // 3 字节对齐
///     uint32_t version;                 // 4 字节
///     std::atomic<uint32_t> num_threads; // 4 字节
///     uint8_t _padding2[4];            // 4 字节对齐
///     uint64_t flushed_address;        // 8 字节
///     uint64_t final_address;          // 8 字节
///     uint64_t monotonic_serial_nums[96]; // 768 字节
///     GUID guids[96];                  // 1536 字节 (每个GUID 16字节)
///     // 总计: 2336 字节
/// };
/// ```
#[repr(C)]
#[derive(Debug, Clone)]
pub struct CLogMetadata {
    /// Whether to use snapshot file
    pub use_snapshot_file: bool,
    /// Padding for alignment
    pub _padding1: [u8; 3],
    /// Version at checkpoint
    pub version: u32,
    /// Number of active threads
    pub num_threads: u32,
    /// Padding for alignment
    pub _padding2: [u8; 4],
    /// Flushed until address
    pub flushed_address: u64,
    /// Final address at checkpoint
    pub final_address: u64,
    /// Monotonic serial numbers (96 sessions)
    pub monotonic_serial_nums: [u64; 96],
    /// Session GUIDs (96 sessions, each 128 bits)
    pub guids: [u128; 96],
}

impl CLogMetadata {
    /// 结构大小 (2336 字节)
    pub const SIZE: usize = 2336;

    /// 最大会话数 (96)
    pub const MAX_SESSIONS: usize = 96;

    /// 创建新的 C 风格元数据
    pub fn new() -> Self {
        Self {
            use_snapshot_file: false,
            _padding1: [0; 3],
            version: 0,
            num_threads: 0,
            _padding2: [0; 4],
            flushed_address: 0,
            final_address: 0,
            monotonic_serial_nums: [0; 96],
            guids: [0; 96],
        }
    }

    /// 从 LogMetadata 转换
    pub fn from_metadata(meta: &LogMetadata) -> Self {
        let mut result = Self::new();
        result.use_snapshot_file = meta.use_snapshot_file;
        result.version = meta.version;
        result.num_threads = meta.num_threads;
        result.flushed_address = meta.flushed_until_address.control();
        result.final_address = meta.final_address.control();

        // 复制会话状态 (最多 96 个)
        for (i, session) in meta.session_states.iter().enumerate() {
            if i >= Self::MAX_SESSIONS {
                break;
            }
            result.monotonic_serial_nums[i] = session.serial_num;
            // 注意：这里仍然使用 as_u128() 存储到内部字段
            // 序列化时会正确转换为 Windows GUID 格式
            result.guids[i] = session.guid.as_u128();
        }

        result
    }

    /// 转换为 LogMetadata
    pub fn to_metadata(&self, token: Uuid) -> LogMetadata {
        let mut meta = LogMetadata {
            token,
            use_snapshot_file: self.use_snapshot_file,
            version: self.version,
            num_threads: self.num_threads,
            begin_address: Address::default(),
            final_address: Address::from_control(self.final_address),
            flushed_until_address: Address::from_control(self.flushed_address),
            use_object_log: false,
            session_states: Vec::new(),
            delta_tail_address: -1,
            prev_snapshot_token: None,
            is_incremental: false,
            snapshot_final_address: Address::default(),
            start_logical_address: Address::default(),
            head_address: Address::default(),
            next_version: 0,
            snapshot_start_flushed_address: Address::default(),
        };

        // 恢复会话状态 (只恢复非零的)
        for i in 0..Self::MAX_SESSIONS {
            if self.guids[i] != 0 {
                let guid = Uuid::from_u128(self.guids[i]);
                let serial_num = self.monotonic_serial_nums[i];
                meta.session_states
                    .push(SessionState::new(guid, serial_num));
            }
        }

        meta
    }

    /// 序列化为字节数组 (小端)
    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let mut offset = 0;

        // use_snapshot_file (1 字节)
        bytes[offset] = if self.use_snapshot_file { 1 } else { 0 };
        offset += 1;

        // padding1 (3 字节)
        offset += 3;

        // version (4 字节)
        bytes[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;

        // num_threads (4 字节)
        bytes[offset..offset + 4].copy_from_slice(&self.num_threads.to_le_bytes());
        offset += 4;

        // padding2 (4 字节)
        offset += 4;

        // flushed_address (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.flushed_address.to_le_bytes());
        offset += 8;

        // final_address (8 字节)
        bytes[offset..offset + 8].copy_from_slice(&self.final_address.to_le_bytes());
        offset += 8;

        // monotonic_serial_nums (768 字节 = 96 * 8)
        for i in 0..96 {
            bytes[offset..offset + 8].copy_from_slice(&self.monotonic_serial_nums[i].to_le_bytes());
            offset += 8;
        }

        // guids (1536 字节 = 96 * 16)
        // 使用 Uuid::to_bytes_le() 以匹配 Windows GUID 布局（混合字节序）
        for i in 0..96 {
            let uuid = Uuid::from_u128(self.guids[i]);
            bytes[offset..offset + 16].copy_from_slice(&uuid.to_bytes_le());
            offset += 16;
        }

        bytes
    }

    /// 从字节数组反序列化 (小端)
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid CLogMetadata size: expected {}, got {}",
                    Self::SIZE,
                    bytes.len()
                ),
            ));
        }

        let mut result = Self::new();
        let mut offset = 0;

        // use_snapshot_file
        result.use_snapshot_file = bytes[offset] != 0;
        offset += 1;

        // padding1
        offset += 3;

        // version
        result.version = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // num_threads
        result.num_threads = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // padding2
        offset += 4;

        // flushed_address
        result.flushed_address = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // final_address
        result.final_address = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // monotonic_serial_nums
        for i in 0..96 {
            result.monotonic_serial_nums[i] = u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            offset += 8;
        }

        // guids
        // 使用 Uuid::from_bytes_le() 以匹配 Windows GUID 布局（混合字节序）
        for i in 0..96 {
            let guid_bytes: [u8; 16] = [
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
                bytes[offset + 8],
                bytes[offset + 9],
                bytes[offset + 10],
                bytes[offset + 11],
                bytes[offset + 12],
                bytes[offset + 13],
                bytes[offset + 14],
                bytes[offset + 15],
            ];
            let uuid = Uuid::from_bytes_le(guid_bytes);
            result.guids[i] = uuid.as_u128();
            offset += 16;
        }

        Ok(result)
    }

    /// 写入到 writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.serialize())
    }

    /// 从 reader 读取
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut bytes = [0u8; Self::SIZE];
        reader.read_exact(&mut bytes)?;
        Self::deserialize(&bytes)
    }
}

impl Default for CLogMetadata {
    fn default() -> Self {
        Self::new()
    }
}

// 验证结构大小
const _: () = assert!(mem::size_of::<CIndexMetadata>() == 56);
const _: () = assert!(mem::size_of::<CLogMetadata>() == 2336);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_c_index_metadata_size() {
        assert_eq!(mem::size_of::<CIndexMetadata>(), 56);
        assert_eq!(CIndexMetadata::SIZE, 56);
    }

    #[test]
    fn test_c_log_metadata_size() {
        assert_eq!(mem::size_of::<CLogMetadata>(), 2336);
        assert_eq!(CLogMetadata::SIZE, 2336);
    }

    #[test]
    fn test_c_index_metadata_serialize_deserialize() {
        let mut meta = CIndexMetadata::new();
        meta.version = 1;
        meta.table_size = 1024;
        meta.num_ht_bytes = 65536;
        meta.num_ofb_bytes = 4096;
        meta.ofb_count = 10;
        meta.log_begin_address = 0x1000;
        meta.checkpoint_start_address = 0x2000;

        let bytes = meta.serialize();
        assert_eq!(bytes.len(), 56);

        let restored = CIndexMetadata::deserialize(&bytes).unwrap();

        assert_eq!(restored.version, meta.version);
        assert_eq!(restored.table_size, meta.table_size);
        assert_eq!(restored.num_ht_bytes, meta.num_ht_bytes);
        assert_eq!(restored.num_ofb_bytes, meta.num_ofb_bytes);
        assert_eq!(restored.ofb_count, meta.ofb_count);
        assert_eq!(restored.log_begin_address, meta.log_begin_address);
        assert_eq!(
            restored.checkpoint_start_address,
            meta.checkpoint_start_address
        );
    }

    #[test]
    fn test_c_log_metadata_serialize_deserialize() {
        let mut meta = CLogMetadata::new();
        meta.use_snapshot_file = true;
        meta.version = 5;
        meta.num_threads = 4;
        meta.flushed_address = 0x10000;
        meta.final_address = 0x20000;

        // 设置一些会话数据
        meta.monotonic_serial_nums[0] = 100;
        meta.guids[0] = Uuid::new_v4().as_u128();
        meta.monotonic_serial_nums[1] = 200;
        meta.guids[1] = Uuid::new_v4().as_u128();

        let bytes = meta.serialize();
        assert_eq!(bytes.len(), 2336);

        let restored = CLogMetadata::deserialize(&bytes).unwrap();

        assert_eq!(restored.use_snapshot_file, meta.use_snapshot_file);
        assert_eq!(restored.version, meta.version);
        assert_eq!(restored.num_threads, meta.num_threads);
        assert_eq!(restored.flushed_address, meta.flushed_address);
        assert_eq!(restored.final_address, meta.final_address);
        assert_eq!(
            restored.monotonic_serial_nums[0],
            meta.monotonic_serial_nums[0]
        );
        assert_eq!(restored.guids[0], meta.guids[0]);
        assert_eq!(
            restored.monotonic_serial_nums[1],
            meta.monotonic_serial_nums[1]
        );
        assert_eq!(restored.guids[1], meta.guids[1]);
    }

    #[test]
    fn test_c_index_metadata_endianness() {
        let meta = CIndexMetadata {
            version: 0x01020304,
            table_size: 0x0102030405060708,
            num_ht_bytes: 0,
            num_ofb_bytes: 0,
            ofb_count: 0,
            log_begin_address: 0,
            checkpoint_start_address: 0,
        };

        let bytes = meta.serialize();

        // 验证小端字节序
        // version (4 字节)
        assert_eq!(bytes[0], 0x04);
        assert_eq!(bytes[1], 0x03);
        assert_eq!(bytes[2], 0x02);
        assert_eq!(bytes[3], 0x01);

        // table_size (8 字节)
        assert_eq!(bytes[8], 0x08);
        assert_eq!(bytes[9], 0x07);
        assert_eq!(bytes[10], 0x06);
        assert_eq!(bytes[11], 0x05);
        assert_eq!(bytes[12], 0x04);
        assert_eq!(bytes[13], 0x03);
        assert_eq!(bytes[14], 0x02);
        assert_eq!(bytes[15], 0x01);
    }

    #[test]
    fn test_c_log_metadata_endianness() {
        let meta = CLogMetadata {
            use_snapshot_file: true,
            _padding1: [0; 3],
            version: 0x01020304,
            num_threads: 0x05060708,
            _padding2: [0; 4],
            flushed_address: 0,
            final_address: 0,
            monotonic_serial_nums: [0; 96],
            guids: [0; 96],
        };

        let mut meta = meta;
        let uuid = Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();
        meta.guids[0] = uuid.as_u128();

        let bytes = meta.serialize();

        // 验证小端字节序
        // use_snapshot_file (1 字节)
        assert_eq!(bytes[0], 1);

        // padding1 (3 字节)
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 0);
        assert_eq!(bytes[3], 0);

        // version (4 字节)
        assert_eq!(bytes[4], 0x04);
        assert_eq!(bytes[5], 0x03);
        assert_eq!(bytes[6], 0x02);
        assert_eq!(bytes[7], 0x01);

        // num_threads (4 字节)
        assert_eq!(bytes[8], 0x08);
        assert_eq!(bytes[9], 0x07);
        assert_eq!(bytes[10], 0x06);
        assert_eq!(bytes[11], 0x05);

        // GUIDs start after the header and monotonic serial numbers.
        let guid_offset = 1 + 3 + 4 + 4 + 4 + 8 + 8 + (CLogMetadata::MAX_SESSIONS * 8);
        let expected = [
            0xa4, 0xa3, 0xa2, 0xa1, 0xb2, 0xb1, 0xc2, 0xc1, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6,
            0xd7, 0xd8,
        ];
        assert_eq!(&bytes[guid_offset..guid_offset + 16], expected.as_slice());
    }

    #[test]
    fn test_index_metadata_conversion() {
        let token = Uuid::new_v4();
        let mut index_meta = IndexMetadata::with_token(token);
        index_meta.version = 1;
        index_meta.table_size = 2048;
        index_meta.num_ht_bytes = 131072;
        index_meta.num_ofb_bytes = 8192;
        index_meta.num_buckets = 20;
        index_meta.log_begin_address = Address::new(0, 1000);
        index_meta.checkpoint_start_address = Address::new(5, 2000);

        // 转换为 C 格式
        let c_meta = CIndexMetadata::from_metadata(&index_meta);

        assert_eq!(c_meta.version, index_meta.version);
        assert_eq!(c_meta.table_size, index_meta.table_size);
        assert_eq!(c_meta.num_ht_bytes, index_meta.num_ht_bytes);
        assert_eq!(c_meta.num_ofb_bytes, index_meta.num_ofb_bytes);
        assert_eq!(c_meta.ofb_count, index_meta.num_buckets);
        assert_eq!(
            c_meta.log_begin_address,
            index_meta.log_begin_address.control()
        );
        assert_eq!(
            c_meta.checkpoint_start_address,
            index_meta.checkpoint_start_address.control()
        );

        // 转换回 Rust 格式
        let restored = c_meta.to_metadata(token);

        assert_eq!(restored.version, index_meta.version);
        assert_eq!(restored.table_size, index_meta.table_size);
        assert_eq!(restored.num_ht_bytes, index_meta.num_ht_bytes);
        assert_eq!(restored.num_ofb_bytes, index_meta.num_ofb_bytes);
        assert_eq!(restored.num_buckets, index_meta.num_buckets);
        assert_eq!(restored.log_begin_address, index_meta.log_begin_address);
        assert_eq!(
            restored.checkpoint_start_address,
            index_meta.checkpoint_start_address
        );
    }

    #[test]
    fn test_log_metadata_conversion() {
        let token = Uuid::new_v4();
        let mut log_meta = LogMetadata::with_token(token);
        log_meta.use_snapshot_file = true;
        log_meta.version = 3;
        log_meta.num_threads = 8;
        log_meta.flushed_until_address = Address::new(10, 5000);
        log_meta.final_address = Address::new(15, 10000);

        // 添加会话状态
        log_meta.add_session(Uuid::new_v4(), 100);
        log_meta.add_session(Uuid::new_v4(), 200);

        // 转换为 C 格式
        let c_meta = CLogMetadata::from_metadata(&log_meta);

        assert_eq!(c_meta.use_snapshot_file, log_meta.use_snapshot_file);
        assert_eq!(c_meta.version, log_meta.version);
        assert_eq!(c_meta.num_threads, log_meta.num_threads);
        assert_eq!(
            c_meta.flushed_address,
            log_meta.flushed_until_address.control()
        );
        assert_eq!(c_meta.final_address, log_meta.final_address.control());

        // 转换回 Rust 格式
        let restored = c_meta.to_metadata(token);

        assert_eq!(restored.use_snapshot_file, log_meta.use_snapshot_file);
        assert_eq!(restored.version, log_meta.version);
        assert_eq!(restored.num_threads, log_meta.num_threads);
        assert_eq!(
            restored.flushed_until_address,
            log_meta.flushed_until_address
        );
        assert_eq!(restored.final_address, log_meta.final_address);
        assert_eq!(restored.session_states.len(), 2);
    }
}
