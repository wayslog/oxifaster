// 测试 GUID 字节序
//
// 验证 Rust uuid crate 与 C++ FASTER GUID 的字节序兼容性

use uuid::Uuid;

#[test]
fn test_uuid_byteorder() {
    // 创建一个已知的 UUID
    // 标准格式: "67e55044-10b1-426f-9247-bb680e5fe0c8"
    let uuid_str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
    let uuid = Uuid::parse_str(uuid_str).unwrap();

    println!("UUID: {}", uuid);

    // 方法1: as_u128() - 返回网络字节序（大端）
    let as_u128 = uuid.as_u128();
    let u128_le_bytes = as_u128.to_le_bytes();
    println!("as_u128(): 0x{:032x}", as_u128);
    println!("as_u128().to_le_bytes(): {:02x?}", &u128_le_bytes);

    // 方法2: as_bytes() - 返回 RFC 4122 字节序 (big-endian)
    let as_bytes = uuid.as_bytes();
    println!("as_bytes(): {:02x?}", as_bytes);

    // 方法3: to_bytes_le() - 返回小端字节序
    let to_bytes_le = uuid.to_bytes_le();
    println!("to_bytes_le(): {:02x?}", &to_bytes_le);

    // 解析 UUID 的各个字段
    let fields = uuid.as_fields();
    println!("\nUUID fields:");
    println!(
        "  d1 (u32): 0x{:08x} = {:?}",
        fields.0,
        fields.0.to_le_bytes()
    );
    println!(
        "  d2 (u16): 0x{:04x} = {:?}",
        fields.1,
        fields.1.to_le_bytes()
    );
    println!(
        "  d3 (u16): 0x{:04x} = {:?}",
        fields.2,
        fields.2.to_le_bytes()
    );
    println!("  d4 (u8[8]): {:02x?}", fields.3);

    // 模拟 Windows GUID 布局（混合字节序）
    let mut windows_guid = [0u8; 16];
    windows_guid[0..4].copy_from_slice(&fields.0.to_le_bytes()); // d1 小端
    windows_guid[4..6].copy_from_slice(&fields.1.to_le_bytes()); // d2 小端
    windows_guid[6..8].copy_from_slice(&fields.2.to_le_bytes()); // d3 小端
    windows_guid[8..16].copy_from_slice(fields.3); // d4 原样（大端）
    println!("\nWindows GUID layout: {:02x?}", &windows_guid);

    // 比较不同方法
    println!("\nComparison:");
    println!(
        "  as_bytes() == Windows GUID: {}",
        as_bytes == &windows_guid
    );
    println!(
        "  to_bytes_le() == Windows GUID: {}",
        to_bytes_le == windows_guid
    );
    println!(
        "  as_u128().to_le_bytes() == Windows GUID: {}",
        u128_le_bytes == windows_guid
    );
}

#[test]
fn test_uuid_roundtrip_with_c_compat() {
    // 测试与 C++ FASTER 兼容的往返转换
    let original_uuid = Uuid::new_v4();
    println!("Original UUID: {}", original_uuid);

    // 错误的方法（当前实现）：as_u128() + to_le_bytes()
    let wrong_bytes = original_uuid.as_u128().to_le_bytes();

    // 正确的方法1：to_bytes_le()
    let correct_bytes_le = original_uuid.to_bytes_le();

    // 正确的方法2：as_bytes() (RFC 4122，与 Linux uuid_t 兼容)
    let correct_bytes_rfc = original_uuid.as_bytes();

    println!("\nSerialization:");
    println!("  Wrong (as_u128 + to_le): {:02x?}", &wrong_bytes);
    println!("  Correct (to_bytes_le): {:02x?}", &correct_bytes_le);
    println!("  Correct (as_bytes, RFC): {:02x?}", correct_bytes_rfc);

    // 反序列化测试
    println!("\nDeserialization:");

    // 从 as_u128().to_le_bytes() 反序列化
    let from_wrong = u128::from_le_bytes(wrong_bytes);
    let uuid_from_wrong = Uuid::from_u128(from_wrong);
    println!("  From wrong bytes: {}", uuid_from_wrong);

    // 从 to_bytes_le() 反序列化
    let uuid_from_le = Uuid::from_bytes_le(correct_bytes_le);
    println!("  From le bytes: {}", uuid_from_le);

    // 从 as_bytes() 反序列化
    let uuid_from_rfc = Uuid::from_bytes(*correct_bytes_rfc);
    println!("  From RFC bytes: {}", uuid_from_rfc);

    // 验证往返
    assert_eq!(uuid_from_le, original_uuid, "to_bytes_le roundtrip failed");
    assert_eq!(uuid_from_rfc, original_uuid, "as_bytes roundtrip failed");

    // 错误方法的往返会失败
    if uuid_from_wrong != original_uuid {
        println!("\nWARNING: as_u128() roundtrip produces different UUID!");
        println!("  Original:  {}", original_uuid);
        println!("  Roundtrip: {}", uuid_from_wrong);
    }
}
