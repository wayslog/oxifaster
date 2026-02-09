//! Variable-length records example.
//!
//! Demonstrates:
//! - `SpanByte` as a FASTER-style serialized envelope format (standalone utility)
//! - varlen `FasterKv` usage via opt-in wrapper types (`Utf8`)
//!
//! Run: `cargo run --example variable_length`

use std::sync::Arc;

use oxifaster::codec::Utf8;
use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
#[cfg(feature = "prometheus")]
use oxifaster::stats::prometheus::PrometheusRenderer;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::varlen::{SpanByte, SpanByteBuilder};
use tempfile::tempdir;

fn run_fasterkv_with_varlen_utf8<D: StorageDevice>(device_name: &str, device: D) {
    println!("--- FasterKv varlen (Utf8) ({device_name}) ---");
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<Utf8, Utf8, _>::new(config, device));

    {
        let mut session = store.start_session().unwrap();

        let key = Utf8::from("user:1");
        let value = Utf8::from("Alice");
        let status = session.upsert(key.clone(), value.clone());
        println!("  Upsert user:1 -> Alice: {status:?}");

        let read_back = session.read(&key).expect("read failed");
        let read_back = match read_back {
            Some(v) => v,
            None => panic!("expected user:1 to exist after upsert"),
        };
        assert_eq!(read_back, value);
        println!("  Read user:1: {}", read_back.0);

        let new_value = Utf8::from("Alice Smith");
        session.upsert(key.clone(), new_value);
        let updated = session.read(&key).expect("read after update failed");
        let updated = match updated {
            Some(v) => v,
            None => panic!("expected user:1 to exist after update"),
        };
        assert_eq!(updated.0, "Alice Smith");
        println!("  Updated user:1: {}", updated.0);

        let delete_status = session.delete(&key);
        println!("  Delete user:1: {delete_status:?}");
        let after_delete = session.read(&key).expect("read after delete failed");
        assert_eq!(after_delete, None);
    }

    #[cfg(feature = "prometheus")]
    {
        let snapshot = store.stats_snapshot();
        let text = PrometheusRenderer::new().render_snapshot(&snapshot);
        println!("\n--- Prometheus metrics (store stats) ---\n{text}");
        assert!(text.contains("oxifaster_operations_total"));
    }
    println!();
}

fn main() {
    println!("=== oxifaster variable-length records example ===\n");

    println!("--- 1. SpanByte basics ---");
    let span1 = SpanByte::from_slice(b"hello world");
    println!("  From bytes: {span1:?}");
    println!("  Length: {} bytes", span1.len());
    println!("  Total size (incl header): {} bytes", span1.total_size());
    println!("  Display: {}\n", span1.to_string_lossy());

    println!("--- 2. Build from different sources ---");
    let from_str = SpanByte::from_string("from string");
    let from_string = SpanByte::from(String::from("from String"));
    let from_vec = SpanByte::from_vec(vec![1, 2, 3, 4, 5]);

    println!("  From &str: {from_str:?}");
    println!("  From String: {from_string:?}");
    println!("  From Vec<u8>: {from_vec:?}\n");

    println!("--- 3. SpanByteBuilder ---");
    let built = SpanByteBuilder::new()
        .append(b"hello")
        .push(b' ')
        .append(b"world")
        .metadata(42)
        .build();

    println!("  Built: {built:?}");
    println!("  Display: {}", built.to_string_lossy());
    println!("  Metadata: {:?}\n", built.metadata());

    println!("--- 4. Metadata operations ---");
    let mut span_with_meta = SpanByte::from_slice(b"data with metadata");
    println!(
        "  Initial: has_metadata = {}",
        span_with_meta.has_metadata()
    );

    span_with_meta.set_metadata(12345678);
    println!("  Set: metadata = {:?}", span_with_meta.metadata());
    println!("  has_metadata = {}", span_with_meta.has_metadata());

    span_with_meta.clear_metadata();
    println!(
        "  Cleared: has_metadata = {}\n",
        span_with_meta.has_metadata()
    );

    println!("--- 5. Serialize/deserialize ---");
    let original = SpanByte::from_slice(b"serialize me");
    let mut buffer = vec![0u8; original.total_size()];

    let bytes_written = original.serialize_to(&mut buffer);
    println!("  Original: {original:?}");
    println!("  Bytes written: {bytes_written}");

    let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
    println!("  Deserialized: {deserialized:?}");
    println!("  Equal: {}\n", original == deserialized);

    println!("--- 6. Serialize with metadata ---");
    let mut with_meta = SpanByte::from_slice(b"has metadata");
    with_meta.set_metadata(999);

    let total_size = with_meta.total_size();
    println!("  Total size (with metadata): {total_size} bytes");
    println!(
        "  = 4 (header) + 8 (metadata) + {} (payload)",
        with_meta.len()
    );

    let mut buffer2 = vec![0u8; total_size];
    with_meta.serialize_to(&mut buffer2);

    let restored = SpanByte::deserialize_from(&buffer2).unwrap();
    println!("  Restored metadata: {:?}\n", restored.metadata());

    // 7. Hashing
    println!("--- 7. Hashing ---");
    let key1 = SpanByte::from_slice(b"key1");
    let key2 = SpanByte::from_slice(b"key1");
    let key3 = SpanByte::from_slice(b"key2");

    println!("  key1 hash: 0x{:016X}", key1.get_hash());
    println!("  key2 hash: 0x{:016X}", key2.get_hash());
    println!("  key3 hash: 0x{:016X}", key3.get_hash());
    println!("  key1 == key2: {}", key1.get_hash() == key2.get_hash());
    println!("  key1 == key3: {}\n", key1.get_hash() == key3.get_hash());

    run_fasterkv_with_varlen_utf8("NullDisk (in-memory)", NullDisk::new());

    let dir = tempdir().expect("failed to create tempdir");
    let data_path = dir.path().join("oxifaster_variable_length.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("failed to create data file");
    run_fasterkv_with_varlen_utf8(
        &format!("FileSystemDisk (persistent file: {})", data_path.display()),
        fs_device,
    );

    // 9. Binary payload
    println!("--- 9. Binary payload ---");
    let binary_data: Vec<u8> = (0u8..=255).collect();
    let binary_span = SpanByte::from_vec(binary_data);
    println!("  Length: {} bytes", binary_span.len());
    println!("  First 10 bytes: {:?}", &binary_span.as_slice()[..10]);
    println!("  Last 10 bytes: {:?}", &binary_span.as_slice()[246..]);
    println!();

    // 10. Empty SpanByte
    println!("--- 10. Empty SpanByte ---");
    let empty = SpanByte::new();
    println!("  Length: {}", empty.len());
    println!("  is_empty: {}", empty.is_empty());

    // 11. Limits
    println!("\n--- 11. Limits ---");
    println!("  Max payload length: 1GiB (2^30 - 1 bytes)");
    println!("  Header size: 4 bytes");
    println!("  Optional metadata: 8 bytes");
    println!("  Format: [length:4][metadata?:8][payload:N]");

    println!("\n=== Done ===");
}
