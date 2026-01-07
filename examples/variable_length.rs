//! Variable Length Records 示例
//!
//! 演示 oxifaster 的变长记录功能 (SpanByte)。
//!
//! 运行: cargo run --example variable_length

use std::sync::Arc;

use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::varlen::{SpanByte, SpanByteBuilder};
use tempfile::tempdir;

fn run_fasterkv_with_spanbyte<D: StorageDevice>(device_name: &str, device: D) {
    // 8. 使用 SpanByte 作为 Key 和 Value
    println!("--- 8. 在 FasterKv 中使用 SpanByte（{device_name}） ---");
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<SpanByte, SpanByte, _>::new(config, device));

    {
        let mut session = store.start_session();

        // 插入字符串键值
        let key = SpanByte::from_string("user:1");
        let value = SpanByte::from_string("Alice");
        let status = session.upsert(key.clone(), value);
        println!("  插入 user:1 -> Alice: {status:?}");

        // 读取
        match session.read(&key) {
            Ok(Some(v)) => println!("  读取 user:1: {}", v.to_string_lossy()),
            Ok(None) => println!("  user:1 未找到"),
            Err(_) => println!("  读取失败"),
        }

        // 更新
        let new_value = SpanByte::from_string("Alice Smith");
        session.upsert(key.clone(), new_value);
        if let Ok(Some(v)) = session.read(&key) {
            println!("  更新后 user:1: {}", v.to_string_lossy());
        }

        // 删除
        let delete_status = session.delete(&key);
        println!("  删除 user:1: {delete_status:?}");
    }
    println!();
}

fn main() {
    println!("=== oxifaster Variable Length Records 示例 ===\n");

    // 1. SpanByte 基本使用
    println!("--- 1. SpanByte 基本使用 ---");
    let span1 = SpanByte::from_slice(b"hello world");
    println!("  从字节切片创建: {span1:?}");
    println!("  长度: {} 字节", span1.len());
    println!("  总大小 (含头部): {} 字节", span1.total_size());
    println!("  内容: {}\n", span1.to_string_lossy());

    // 2. 从不同类型创建
    println!("--- 2. 从不同类型创建 ---");
    let from_str = SpanByte::from_string("from string");
    let from_string = SpanByte::from(String::from("from String"));
    let from_vec = SpanByte::from_vec(vec![1, 2, 3, 4, 5]);

    println!("  从 &str: {from_str:?}");
    println!("  从 String: {from_string:?}");
    println!("  从 Vec<u8>: {from_vec:?}\n");

    // 3. SpanByteBuilder
    println!("--- 3. SpanByteBuilder 构建器 ---");
    let built = SpanByteBuilder::new()
        .append(b"hello")
        .push(b' ')
        .append(b"world")
        .metadata(42)
        .build();

    println!("  构建的 SpanByte: {built:?}");
    println!("  内容: {}", built.to_string_lossy());
    println!("  元数据: {:?}\n", built.metadata());

    // 4. 元数据操作
    println!("--- 4. 元数据操作 ---");
    let mut span_with_meta = SpanByte::from_slice(b"data with metadata");
    println!("  初始: has_metadata = {}", span_with_meta.has_metadata());

    span_with_meta.set_metadata(12345678);
    println!("  设置后: metadata = {:?}", span_with_meta.metadata());
    println!("  has_metadata = {}", span_with_meta.has_metadata());

    span_with_meta.clear_metadata();
    println!(
        "  清除后: has_metadata = {}\n",
        span_with_meta.has_metadata()
    );

    // 5. 序列化和反序列化
    println!("--- 5. 序列化和反序列化 ---");
    let original = SpanByte::from_slice(b"serialize me");
    let mut buffer = vec![0u8; original.total_size()];

    let bytes_written = original.serialize_to(&mut buffer);
    println!("  原始: {original:?}");
    println!("  序列化字节数: {bytes_written}");

    let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
    println!("  反序列化: {deserialized:?}");
    println!("  相等: {}\n", original == deserialized);

    // 6. 带元数据的序列化
    println!("--- 6. 带元数据的序列化 ---");
    let mut with_meta = SpanByte::from_slice(b"has metadata");
    with_meta.set_metadata(999);

    let total_size = with_meta.total_size();
    println!("  总大小 (含元数据): {total_size} 字节");
    println!("  = 4 (头部) + 8 (元数据) + {} (数据)", with_meta.len());

    let mut buffer2 = vec![0u8; total_size];
    with_meta.serialize_to(&mut buffer2);

    let restored = SpanByte::deserialize_from(&buffer2).unwrap();
    println!("  恢复的元数据: {:?}\n", restored.metadata());

    // 7. 哈希计算
    println!("--- 7. 哈希计算 ---");
    let key1 = SpanByte::from_slice(b"key1");
    let key2 = SpanByte::from_slice(b"key1");
    let key3 = SpanByte::from_slice(b"key2");

    println!("  key1 哈希: 0x{:016X}", key1.get_hash());
    println!("  key2 哈希: 0x{:016X}", key2.get_hash());
    println!("  key3 哈希: 0x{:016X}", key3.get_hash());
    println!("  key1 == key2: {}", key1.get_hash() == key2.get_hash());
    println!("  key1 == key3: {}\n", key1.get_hash() == key3.get_hash());

    run_fasterkv_with_spanbyte("NullDisk（纯内存）", NullDisk::new());

    let dir = tempdir().expect("创建临时目录失败");
    let data_path = dir.path().join("oxifaster_variable_length.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("创建数据文件失败");
    run_fasterkv_with_spanbyte(
        &format!("FileSystemDisk（文件持久化：{}）", data_path.display()),
        fs_device,
    );

    // 9. 二进制数据
    println!("--- 9. 二进制数据支持 ---");
    let binary_data: Vec<u8> = (0u8..=255).collect();
    let binary_span = SpanByte::from_vec(binary_data);
    println!("  二进制数据长度: {} 字节", binary_span.len());
    println!("  前 10 字节: {:?}", &binary_span.as_slice()[..10]);
    println!("  后 10 字节: {:?}", &binary_span.as_slice()[246..]);
    println!();

    // 10. 空 SpanByte
    println!("--- 10. 空 SpanByte ---");
    let empty = SpanByte::new();
    println!("  空 SpanByte 长度: {}", empty.len());
    println!("  is_empty: {}", empty.is_empty());

    // 11. 最大长度说明
    println!("\n--- 11. 限制说明 ---");
    println!("  最大有效负载长度: 1GB (2^30 - 1 字节)");
    println!("  头部大小: 4 字节");
    println!("  可选元数据: 8 字节");
    println!("  格式: [length:4][metadata?:8][payload:N]");

    println!("\n=== 示例完成 ===");
}
