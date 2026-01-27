// RecordInfo 格式兼容性测试
//
// 目标: 验证 oxifaster 的 RecordInfo 与 C++ FASTER 的二进制兼容性
//
// RecordInfo 结构 (8 字节):
// - Bits 0-47: Previous address (48 位)
// - Bits 48-60: Version (13 位)
// - Bit 61: Invalid flag
// - Bit 62: Tombstone flag
// - Bit 63: Final bit

use oxifaster::address::Address;
use oxifaster::record::RecordInfo;
use std::mem;

#[test]
fn test_record_info_size() {
    // RecordInfo 必须是 8 字节 (64 位)
    assert_eq!(mem::size_of::<RecordInfo>(), 8);
}

#[test]
fn test_record_info_alignment() {
    // RecordInfo 应该是 8 字节对齐
    assert_eq!(mem::align_of::<RecordInfo>(), 8);
}

#[test]
fn test_record_info_default() {
    let info = RecordInfo::default();

    // 默认值应该是全 0
    assert_eq!(info.control(), 0);
}

#[test]
fn test_record_info_previous_address() {
    // 测试设置前置地址 (48 位)
    let test_address = Address::from_control(0x0000_FFFF_FFFF_FFFF); // 48 位全 1
    let info = RecordInfo::new(test_address, 0, false, false, false);

    let control = info.control();

    // 前 48 位应该被设置
    assert_eq!(control & 0x0000_FFFF_FFFF_FFFF, 0x0000_FFFF_FFFF_FFFF);

    // 高 16 位应该仍然是 0
    assert_eq!(control >> 48, 0);

    // 验证读取
    assert_eq!(info.previous_address().control(), test_address.control());
}

#[test]
fn test_record_info_version() {
    // 测试设置版本 (13 位)
    let test_version = 0x1FFF; // 13 位全 1
    let info = RecordInfo::new(Address::default(), test_version, false, false, false);

    let control = info.control();

    // 版本应该在位 48-60
    let version_mask = 0x1FFF_0000_0000_0000u64;
    assert_eq!((control & version_mask) >> 48, test_version as u64);

    // 前 48 位 (地址) 应该是 0
    assert_eq!(control & 0x0000_FFFF_FFFF_FFFF, 0);

    // 标志位 (位 61-63) 应该是 0
    assert_eq!(control >> 61, 0);

    // 验证读取
    assert_eq!(info.checkpoint_version(), test_version);
}

#[test]
fn test_record_info_invalid_flag() {
    // 设置 invalid 标志
    let info = RecordInfo::new(Address::default(), 0, true, false, false);

    let control = info.control();

    // Bit 61 应该被设置
    assert_eq!((control >> 61) & 1, 1);

    // 验证读取
    assert!(info.is_invalid());

    // 其他位应该是 0
    assert_eq!(control & 0x1FFF_FFFF_FFFF_FFFF, 0);
}

#[test]
fn test_record_info_tombstone_flag() {
    // 设置 tombstone 标志
    let info = RecordInfo::new(Address::default(), 0, false, true, false);

    let control = info.control();

    // Bit 62 应该被设置
    assert_eq!((control >> 62) & 1, 1);

    // 验证读取
    assert!(info.is_tombstone());

    // 其他位应该是 0
    assert_eq!(control & 0x3FFF_FFFF_FFFF_FFFF, 0);
}

#[test]
fn test_record_info_final_bit() {
    // 设置 final bit
    let info = RecordInfo::new(Address::default(), 0, false, false, true);

    let control = info.control();

    // Bit 63 应该被设置
    assert_eq!((control >> 63) & 1, 1);

    // 验证读取
    assert!(info.is_final());

    // 其他位应该是 0
    assert_eq!(control & 0x7FFF_FFFF_FFFF_FFFF, 0);
}

#[test]
fn test_record_info_combined() {
    // 设置所有字段
    let address = Address::from_control(0x0000_1234_5678_9ABC);
    let version = 0x0FFF; // 12 位

    let info = RecordInfo::new(address, version, true, false, true);

    let control = info.control();

    // 验证地址
    assert_eq!(control & 0x0000_FFFF_FFFF_FFFF, address.control());

    // 验证版本
    assert_eq!((control >> 48) & 0x1FFF, version as u64);

    // 验证标志
    assert_eq!((control >> 61) & 1, 1); // invalid
    assert_eq!((control >> 62) & 1, 0); // tombstone
    assert_eq!((control >> 63) & 1, 1); // final_bit

    // 验证读取方法
    assert_eq!(info.previous_address().control(), address.control());
    assert_eq!(info.checkpoint_version(), version);
    assert!(info.is_invalid());
    assert!(!info.is_tombstone());
    assert!(info.is_final());
}

#[test]
fn test_record_info_endianness() {
    // 测试小端字节序
    // 直接使用控制字而不是地址，因为地址的高位可能被截断
    let test_value: u64 = 0x0102_0304_0506_0708;
    let info = RecordInfo::from_control(test_value);

    let control = info.control();
    assert_eq!(control, test_value);

    let bytes = control.to_le_bytes();

    // 小端字节序: 低字节在前
    assert_eq!(bytes[0], 0x08);
    assert_eq!(bytes[1], 0x07);
    assert_eq!(bytes[2], 0x06);
    assert_eq!(bytes[3], 0x05);
    assert_eq!(bytes[4], 0x04);
    assert_eq!(bytes[5], 0x03);
    assert_eq!(bytes[6], 0x02);
    assert_eq!(bytes[7], 0x01);
}

#[test]
fn test_record_info_c_layout_compatibility() {
    // C++ FASTER RecordInfo 布局:
    // struct RecordInfo {
    //     uint64_t word;  // 8 字节
    // }
    //
    // 位布局:
    // - Bits 0-47: previous_address
    // - Bits 48-60: version
    // - Bit 61: invalid
    // - Bit 62: tombstone
    // - Bit 63: final_bit

    // 模拟 C++ 设置的值
    let cpp_word: u64 = 0x8234_5678_9ABC_DEF0;
    //                   ^^^^ ^^^^
    //                   |    版本和标志位
    //                   地址部分

    // 从 C++ 风格的控制字重建
    let info = RecordInfo::from_control(cpp_word);

    // 验证解析结果
    let address = info.previous_address();
    let version = info.checkpoint_version();
    let final_bit = info.is_final();

    println!("C++ word: 0x{:016x}", cpp_word);
    println!("Parsed address: 0x{:012x}", address.control());
    println!("Parsed version: 0x{:04x}", version);
    println!("Final bit: {}", final_bit);

    // 验证地址 (前 48 位)
    assert_eq!(address.control(), cpp_word & 0x0000_FFFF_FFFF_FFFF);

    // 验证版本 (位 48-60)
    assert_eq!(version as u64, (cpp_word >> 48) & 0x1FFF);

    // 验证 final bit (位 63)
    assert_eq!(final_bit, (cpp_word >> 63) == 1);
}

#[test]
fn test_record_info_set_previous_address() {
    // 测试运行时修改前置地址
    let initial_address = Address::from_control(0x1234);
    let info = RecordInfo::new(initial_address, 0x100, false, false, false);

    // 修改地址
    let new_address = Address::from_control(0x5678_9ABC);
    info.set_previous_address(new_address);

    // 验证地址已更新
    assert_eq!(info.previous_address().control(), new_address.control());

    // 验证其他字段未改变
    assert_eq!(info.checkpoint_version(), 0x100);
    assert!(!info.is_invalid());
    assert!(!info.is_tombstone());
    assert!(!info.is_final());
}

#[test]
fn test_record_info_set_flags() {
    // 测试运行时修改标志位
    let info = RecordInfo::new(Address::default(), 0, false, false, false);

    // 设置 invalid 标志
    info.set_invalid(true);
    assert!(info.is_invalid());

    // 设置 tombstone 标志
    info.set_tombstone(true);
    assert!(info.is_tombstone());

    // 清除 invalid 标志
    info.set_invalid(false);
    assert!(!info.is_invalid());
    assert!(info.is_tombstone()); // tombstone 应该仍然设置
}

#[test]
fn test_record_info_bit_positions() {
    // 验证位位置常量
    // 这些位位置必须与 C++ FASTER 完全一致

    // Invalid bit 在位 61
    let info = RecordInfo::new(Address::default(), 0, true, false, false);
    let invalid_mask = 1u64 << 61;
    assert_eq!(info.control() & invalid_mask, invalid_mask);

    // Tombstone bit 在位 62
    let info = RecordInfo::new(Address::default(), 0, false, true, false);
    let tombstone_mask = 1u64 << 62;
    assert_eq!(info.control() & tombstone_mask, tombstone_mask);

    // Final bit 在位 63
    let info = RecordInfo::new(Address::default(), 0, false, false, true);
    let final_mask = 1u64 << 63;
    assert_eq!(info.control() & final_mask, final_mask);
}
