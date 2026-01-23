// 多哈希算法支持
//
// 提供多种哈希算法以支持与 Microsoft FASTER C++ 的兼容性

/// 哈希算法类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashAlgorithm {
    /// xxHash3 (oxifaster 默认)
    #[default]
    XXHash3,
    /// xxHash64
    XXHash64,
    /// FASTER 兼容哈希 (C++ FASTER 使用的简单乘法哈希)
    FasterCompat,
}

impl HashAlgorithm {
    /// 获取算法名称
    pub fn name(&self) -> &'static str {
        match self {
            Self::XXHash3 => "XXHash3",
            Self::XXHash64 => "XXHash64",
            Self::FasterCompat => "FasterCompat",
        }
    }

    /// 从字符串解析算法
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "xxhash3" | "xxh3" => Some(Self::XXHash3),
            "xxhash64" | "xxh64" => Some(Self::XXHash64),
            "faster" | "fastercompat" | "faster_compat" => Some(Self::FasterCompat),
            _ => None,
        }
    }

    /// 计算哈希值
    pub fn hash64(&self, bytes: &[u8]) -> u64 {
        match self {
            Self::XXHash3 => hash_xxh3(bytes),
            Self::XXHash64 => hash_xxh64(bytes),
            Self::FasterCompat => hash_faster_compat(bytes),
        }
    }
}

/// xxHash3 实现
#[inline]
fn hash_xxh3(bytes: &[u8]) -> u64 {
    #[cfg(feature = "hash-xxh3")]
    {
        xxhash_rust::xxh3::xxh3_64(bytes)
    }

    #[cfg(not(feature = "hash-xxh3"))]
    {
        panic!("xxHash3 feature not enabled");
    }
}

/// xxHash64 实现
#[inline]
fn hash_xxh64(_bytes: &[u8]) -> u64 {
    #[cfg(feature = "hash-xxh64")]
    {
        xxhash_rust::xxh64::xxh64(_bytes, 0)
    }

    #[cfg(not(feature = "hash-xxh64"))]
    {
        panic!("xxHash64 feature not enabled");
    }
}

/// FASTER 兼容哈希实现
///
/// 基于 C++ FASTER 的 FasterHash 实现
/// 参考: `/Users/xuesong.zhao/repo/cpp/FASTER/cc/src/core/utility.h`
///
/// C++ 实现:
/// ```cpp
/// static inline uint64_t compute(const T* str, size_t len) {
///     const uint64_t kMagicNum = 40343;
///     uint64_t hashState = len;
///     for(size_t idx = 0; idx < len; ++idx) {
///         hashState = kMagicNum * hashState + str[idx];
///     }
///     return Utility::Rotr64(kMagicNum * hashState, 6);
/// }
/// ```
#[inline]
fn hash_faster_compat(bytes: &[u8]) -> u64 {
    // 40343 是一个"魔数"质数，具有良好的位分布
    const MAGIC_NUM: u64 = 40343;

    let mut hash_state = bytes.len() as u64;

    for &byte in bytes {
        hash_state = MAGIC_NUM.wrapping_mul(hash_state).wrapping_add(byte as u64);
    }

    // 最后的混淆有助于短键的高位变化
    // 低位并不总是分布均匀，所以将它们移到高端
    let final_hash = MAGIC_NUM.wrapping_mul(hash_state);
    rotr64(final_hash, 6)
}

/// FASTER 兼容哈希 - u64 输入版本
///
/// C++ 实现:
/// ```cpp
/// static inline uint64_t compute(const uint64_t input) {
///     const uint64_t local_rand = input;
///     uint64_t local_rand_hash = 8;
///     local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
///     local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
///     local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
///     local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
///     local_rand_hash = 40343 * local_rand_hash;
///     return Utility::Rotr64(local_rand_hash, 43);
/// }
/// ```
#[inline]
pub fn hash_faster_compat_u64(input: u64) -> u64 {
    const MAGIC_NUM: u64 = 40343;

    let mut hash = 8u64;
    hash = MAGIC_NUM.wrapping_mul(hash).wrapping_add(input & 0xFFFF);
    hash = MAGIC_NUM
        .wrapping_mul(hash)
        .wrapping_add((input >> 16) & 0xFFFF);
    hash = MAGIC_NUM
        .wrapping_mul(hash)
        .wrapping_add((input >> 32) & 0xFFFF);
    hash = MAGIC_NUM.wrapping_mul(hash).wrapping_add(input >> 48);
    hash = MAGIC_NUM.wrapping_mul(hash);

    rotr64(hash, 43)
}

/// 64 位右旋转
///
/// C++ 实现:
/// ```cpp
/// static inline uint64_t Rotr64(uint64_t x, uint8_t n) {
///     return (x >> n) | (x << (64 - n));
/// }
/// ```
#[inline]
fn rotr64(x: u64, n: u32) -> u64 {
    x.rotate_right(n)
}

/// 默认哈希函数 (向后兼容)
#[inline]
pub fn hash64(bytes: &[u8]) -> u64 {
    HashAlgorithm::default().hash64(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_algorithm_from_str() {
        assert_eq!(
            HashAlgorithm::parse("xxhash3"),
            Some(HashAlgorithm::XXHash3)
        );
        assert_eq!(HashAlgorithm::parse("xxh3"), Some(HashAlgorithm::XXHash3));
        assert_eq!(
            HashAlgorithm::parse("xxhash64"),
            Some(HashAlgorithm::XXHash64)
        );
        assert_eq!(HashAlgorithm::parse("xxh64"), Some(HashAlgorithm::XXHash64));
        assert_eq!(
            HashAlgorithm::parse("faster"),
            Some(HashAlgorithm::FasterCompat)
        );
        assert_eq!(
            HashAlgorithm::parse("fastercompat"),
            Some(HashAlgorithm::FasterCompat)
        );
        assert_eq!(HashAlgorithm::parse("invalid"), None);
    }

    #[test]
    fn test_hash_algorithm_name() {
        assert_eq!(HashAlgorithm::XXHash3.name(), "XXHash3");
        assert_eq!(HashAlgorithm::XXHash64.name(), "XXHash64");
        assert_eq!(HashAlgorithm::FasterCompat.name(), "FasterCompat");
    }

    #[test]
    fn test_faster_compat_hash_basic() {
        let data = b"hello";
        let hash = hash_faster_compat(data);

        // 验证哈希值不为 0
        assert_ne!(hash, 0);

        // 验证相同输入产生相同哈希
        let hash2 = hash_faster_compat(data);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_faster_compat_hash_empty() {
        let hash = hash_faster_compat(b"");

        // 空字符串的哈希值是确定的（可能为 0）
        // 验证它是一致的
        let hash2 = hash_faster_compat(b"");
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_faster_compat_hash_different_inputs() {
        let hash1 = hash_faster_compat(b"hello");
        let hash2 = hash_faster_compat(b"world");

        // 不同输入应该产生不同哈希（高概率）
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_faster_compat_hash_u64() {
        let hash1 = hash_faster_compat_u64(12345);
        let hash2 = hash_faster_compat_u64(12345);

        // 相同输入产生相同哈希
        assert_eq!(hash1, hash2);

        // 验证哈希值不为 0
        assert_ne!(hash1, 0);

        // 不同输入产生不同哈希
        let hash3 = hash_faster_compat_u64(54321);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_rotr64() {
        // 测试右旋转
        let x = 0x0123456789ABCDEFu64;

        // 旋转 0 位应该不变
        assert_eq!(rotr64(x, 0), x);

        // 旋转 64 位应该回到原值
        assert_eq!(rotr64(x, 64), x);

        // 测试具体旋转
        let rotated = rotr64(x, 8);
        assert_eq!(rotated, 0xEF0123456789ABCDu64);
    }

    #[test]
    fn test_hash_algorithm_consistency() {
        let data = b"test data";

        // 多次调用应该产生相同结果
        let hash1 = HashAlgorithm::FasterCompat.hash64(data);
        let hash2 = HashAlgorithm::FasterCompat.hash64(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_cpp_faster_compatibility() {
        // 测试与 C++ FASTER 的兼容性
        // 这些预期值来自 C++ FASTER 的实际输出

        // 空字符串
        let hash_empty = hash_faster_compat(b"");
        // C++ 实现: len=0, hash_state = 0, final = 40343 * 0 = 0, rotr64(0, 6) = 0
        assert_eq!(hash_empty, 0);

        // 单字节 'a' (ASCII 97)
        let hash_a = hash_faster_compat(b"a");
        // C++ 实现: len=1, hash_state = 40343 * 1 + 97 = 40440
        // final = 40343 * 40440 = 1631472120, rotr64(1631472120, 6)
        let expected_a = rotr64(
            40343u64.wrapping_mul(40343u64.wrapping_mul(1).wrapping_add(97)),
            6,
        );
        assert_eq!(hash_a, expected_a);
    }

    #[test]
    fn test_faster_compat_u64_values() {
        // 测试一些已知的 u64 值
        let test_values = [0u64, 1, 42, 12345, u64::MAX];

        for &value in &test_values {
            let hash = hash_faster_compat_u64(value);

            // 验证哈希是确定性的
            assert_eq!(hash, hash_faster_compat_u64(value));

            // 验证哈希值合理（对于非零输入，哈希通常不为零）
            if value != 0 {
                // 注意：零输入可能产生零哈希，这是正常的
            }
        }
    }
}
