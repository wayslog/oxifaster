// Hash 函数兼容性测试
//
// 目标: 验证 oxifaster 的哈希函数与 C++ FASTER 的兼容性

use oxifaster::codec::multi_hash::{hash_faster_compat_u64, HashAlgorithm};

#[test]
fn test_hash_algorithm_enum() {
    // 测试算法枚举
    let xxh3 = HashAlgorithm::XXHash3;
    let xxh64 = HashAlgorithm::XXHash64;
    let faster = HashAlgorithm::FasterCompat;

    assert_eq!(xxh3.name(), "XXHash3");
    assert_eq!(xxh64.name(), "XXHash64");
    assert_eq!(faster.name(), "FasterCompat");
}

#[test]
fn test_hash_algorithm_parsing() {
    // 测试从字符串解析
    assert_eq!(
        HashAlgorithm::parse("xxhash3"),
        Some(HashAlgorithm::XXHash3)
    );
    assert_eq!(
        HashAlgorithm::parse("faster"),
        Some(HashAlgorithm::FasterCompat)
    );
}

#[test]
fn test_faster_compat_hash_deterministic() {
    // 测试 FASTER 兼容哈希的确定性
    let data1 = b"test_key_12345";
    let data2 = b"test_key_12345";
    let data3 = b"different_key";

    let hash1 = HashAlgorithm::FasterCompat.hash64(data1);
    let hash2 = HashAlgorithm::FasterCompat.hash64(data2);
    let hash3 = HashAlgorithm::FasterCompat.hash64(data3);

    // 相同输入产生相同哈希
    assert_eq!(hash1, hash2);

    // 不同输入产生不同哈希
    assert_ne!(hash1, hash3);
}

#[test]
fn test_faster_compat_u64_deterministic() {
    // 测试 u64 版本的 FASTER 哈希
    let value1 = 12345u64;
    let value2 = 12345u64;
    let value3 = 54321u64;

    let hash1 = hash_faster_compat_u64(value1);
    let hash2 = hash_faster_compat_u64(value2);
    let hash3 = hash_faster_compat_u64(value3);

    // 相同输入产生相同哈希
    assert_eq!(hash1, hash2);

    // 不同输入产生不同哈希
    assert_ne!(hash1, hash3);
}

#[test]
fn test_faster_hash_known_values() {
    // 测试一些已知的哈希值（基于 C++ FASTER 的行为）

    // 根据 C++ FASTER 的实现:
    // hash_state = len * MAGIC + sum(byte[i] * MAGIC^i)
    // final = rotr64(MAGIC * hash_state, 6)

    // 空字符串: len=0, hash_state=0, final=0
    let hash_empty = HashAlgorithm::FasterCompat.hash64(b"");
    assert_eq!(hash_empty, 0);

    // 单字节测试
    let hash_a = HashAlgorithm::FasterCompat.hash64(b"a");
    // len=1, hash_state = 40343*1 + 97 = 40440
    // final = rotr64(40343 * 40440, 6)
    let expected_a = rotr64(40343u64.wrapping_mul(40440), 6);
    assert_eq!(hash_a, expected_a);

    // 验证其他字符
    let hash_b = HashAlgorithm::FasterCompat.hash64(b"b");
    let expected_b = rotr64(
        40343u64.wrapping_mul(40343u64.wrapping_mul(1).wrapping_add(98)),
        6,
    );
    assert_eq!(hash_b, expected_b);
}

#[test]
fn test_faster_hash_u64_known_values() {
    // 测试 u64 版本的已知哈希值

    // 零值
    let hash_zero = hash_faster_compat_u64(0);
    // hash = 8
    // hash = 40343 * 8 + 0 = 322744
    // hash = 40343 * 322744 + 0 = 13016710392
    // hash = 40343 * 13016710392 + 0 = 525145223906056
    // hash = 40343 * 525145223906056 + 0 = 21187244629895086408
    // hash = 40343 * 21187244629895086408
    // final = rotr64(hash, 43)
    let mut expected = 8u64;
    expected = 40343u64.wrapping_mul(expected);
    expected = 40343u64.wrapping_mul(expected);
    expected = 40343u64.wrapping_mul(expected);
    expected = 40343u64.wrapping_mul(expected);
    expected = 40343u64.wrapping_mul(expected);
    expected = rotr64(expected, 43);
    assert_eq!(hash_zero, expected);

    // 简单值 1
    let hash_one = hash_faster_compat_u64(1);
    let mut expected_one = 8u64;
    expected_one = 40343u64.wrapping_mul(expected_one).wrapping_add(1);
    expected_one = 40343u64.wrapping_mul(expected_one);
    expected_one = 40343u64.wrapping_mul(expected_one);
    expected_one = 40343u64.wrapping_mul(expected_one);
    expected_one = 40343u64.wrapping_mul(expected_one);
    expected_one = rotr64(expected_one, 43);
    assert_eq!(hash_one, expected_one);
}

#[test]
fn test_hash_distribution() {
    // 测试哈希分布 (基本的分布测试)
    let test_keys = [
        b"key1".as_slice(),
        b"key2",
        b"key3",
        b"key12345",
        b"very_long_key_with_many_characters",
        b"short",
        b"",
        b"a",
        b"ab",
        b"abc",
    ];

    // 使用 FASTER 兼容哈希
    let mut hashes = Vec::new();
    for key in &test_keys {
        let hash = HashAlgorithm::FasterCompat.hash64(key);
        hashes.push(hash);
    }

    // 验证没有重复（这些键应该产生不同的哈希）
    for i in 0..hashes.len() {
        for j in (i + 1)..hashes.len() {
            if test_keys[i] != test_keys[j] && !test_keys[i].is_empty() && !test_keys[j].is_empty()
            {
                assert_ne!(
                    hashes[i], hashes[j],
                    "Hash collision between {:?} and {:?}",
                    test_keys[i], test_keys[j]
                );
            }
        }
    }
}

#[test]
fn test_hash_stability() {
    // 测试哈希稳定性（多次运行应该产生相同结果）
    let test_data = b"stability_test_key_12345";

    let hash1 = HashAlgorithm::FasterCompat.hash64(test_data);
    let hash2 = HashAlgorithm::FasterCompat.hash64(test_data);
    let hash3 = HashAlgorithm::FasterCompat.hash64(test_data);

    assert_eq!(hash1, hash2);
    assert_eq!(hash2, hash3);
}

#[test]
fn test_different_algorithms_produce_different_hashes() {
    // 测试不同算法产生不同的哈希值
    let data = b"test_data_for_comparison";

    let hash_xxh3 = HashAlgorithm::XXHash3.hash64(data);
    let hash_faster = HashAlgorithm::FasterCompat.hash64(data);

    // 不同算法应该（很可能）产生不同的哈希
    // 注意：这不是绝对保证，但对于大多数输入应该成立
    assert_ne!(hash_xxh3, hash_faster);
}

#[test]
fn test_faster_compat_matches_cpp_behavior() {
    // 验证与 C++ FASTER 行为一致的关键特性

    // 1. 空字符串产生 0
    assert_eq!(HashAlgorithm::FasterCompat.hash64(b""), 0);

    // 2. 短字符串有良好的分布
    let short_keys = vec![b"a".as_slice(), b"b", b"c", b"1", b"2", b"3"];
    let mut short_hashes = Vec::new();
    for key in &short_keys {
        short_hashes.push(HashAlgorithm::FasterCompat.hash64(key));
    }

    // 所有短键应该产生不同的哈希
    for i in 0..short_hashes.len() {
        for j in (i + 1)..short_hashes.len() {
            assert_ne!(short_hashes[i], short_hashes[j]);
        }
    }

    // 3. 长字符串也应该有良好的分布
    let long_keys = vec![
        b"long_key_12345678901234567890".as_slice(),
        b"long_key_12345678901234567891",
        b"long_key_12345678901234567892",
    ];
    let mut long_hashes = Vec::new();
    for key in &long_keys {
        long_hashes.push(HashAlgorithm::FasterCompat.hash64(key));
    }

    for i in 0..long_hashes.len() {
        for j in (i + 1)..long_hashes.len() {
            assert_ne!(long_hashes[i], long_hashes[j]);
        }
    }
}

// 辅助函数：右旋转
fn rotr64(x: u64, n: u32) -> u64 {
    x.rotate_right(n)
}
