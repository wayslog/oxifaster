//! Variable Length Records 集成测试
//!
//! 测试 SpanByte 和变长记录功能。

use std::sync::Arc;

use oxifaster::codec::{RawBytes, Utf8};
use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::varlen::{SpanByte, SpanByteBuilder, VarLenKey, VarLenStruct, VarLenValue};

// ============ SpanByte Basic Tests ============

#[test]
fn test_span_byte_new() {
    let span = SpanByte::new();
    assert!(span.is_empty());
    assert_eq!(span.len(), 0);
}

#[test]
fn test_span_byte_from_slice() {
    let data = b"hello world";
    let span = SpanByte::from_slice(data);

    assert_eq!(span.len(), 11);
    assert_eq!(span.as_slice(), data);
}

#[test]
fn test_span_byte_from_vec() {
    let data = vec![1, 2, 3, 4, 5];
    let span = SpanByte::from_vec(data.clone());

    assert_eq!(span.len(), 5);
    assert_eq!(span.as_slice(), &data[..]);
}

#[test]
fn test_span_byte_from_str() {
    let span = SpanByte::from_string("test");

    assert_eq!(span.len(), 4);
    assert_eq!(span.to_string_lossy(), "test");
}

#[test]
fn test_span_byte_from_string() {
    let span = SpanByte::from(String::from("test"));

    assert_eq!(span.len(), 4);
}

// ============ SpanByte Metadata Tests ============

#[test]
fn test_span_byte_metadata_initial() {
    let span = SpanByte::from_slice(b"data");

    assert!(!span.has_metadata());
    assert_eq!(span.metadata(), None);
}

#[test]
fn test_span_byte_set_metadata() {
    let mut span = SpanByte::from_slice(b"data");
    span.set_metadata(12345);

    assert!(span.has_metadata());
    assert_eq!(span.metadata(), Some(12345));
}

#[test]
fn test_span_byte_clear_metadata() {
    let mut span = SpanByte::from_slice(b"data");
    span.set_metadata(12345);
    span.clear_metadata();

    assert!(!span.has_metadata());
    assert_eq!(span.metadata(), None);
}

#[test]
fn test_span_byte_metadata_size() {
    let span_no_meta = SpanByte::from_slice(b"data");
    assert_eq!(span_no_meta.metadata_size(), 0);

    let mut span_with_meta = SpanByte::from_slice(b"data");
    span_with_meta.set_metadata(1);
    assert_eq!(span_with_meta.metadata_size(), 8);
}

// ============ SpanByte Serialization Tests ============

#[test]
fn test_span_byte_serialize_deserialize() {
    let original = SpanByte::from_slice(b"test data");
    let mut buffer = vec![0u8; original.total_size()];

    let written = original.serialize_to(&mut buffer);
    assert_eq!(written, original.total_size());

    let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_span_byte_serialize_with_metadata() {
    let mut original = SpanByte::from_slice(b"test");
    original.set_metadata(9999);

    let mut buffer = vec![0u8; original.total_size()];
    original.serialize_to(&mut buffer);

    let deserialized = SpanByte::deserialize_from(&buffer).unwrap();
    assert_eq!(deserialized.as_slice(), original.as_slice());
    assert_eq!(deserialized.metadata(), Some(9999));
}

#[test]
fn test_span_byte_deserialize_incomplete() {
    let result = SpanByte::deserialize_from(&[0, 0, 0]); // Too short for header
    assert!(result.is_none());
}

#[test]
fn test_span_byte_total_size() {
    let span = SpanByte::from_slice(b"data");
    assert_eq!(span.total_size(), 4 + 4); // header + data

    let mut span_with_meta = SpanByte::from_slice(b"data");
    span_with_meta.set_metadata(1);
    assert_eq!(span_with_meta.total_size(), 4 + 8 + 4); // header + metadata + data
}

// ============ SpanByte Hash Tests ============

#[test]
fn test_span_byte_hash_equal() {
    let span1 = SpanByte::from_slice(b"same");
    let span2 = SpanByte::from_slice(b"same");

    assert_eq!(span1.get_hash(), span2.get_hash());
}

#[test]
fn test_span_byte_hash_different() {
    let span1 = SpanByte::from_slice(b"one");
    let span2 = SpanByte::from_slice(b"two");

    assert_ne!(span1.get_hash(), span2.get_hash());
}

// ============ SpanByte Equality Tests ============

#[test]
fn test_span_byte_equality() {
    let span1 = SpanByte::from_slice(b"same");
    let span2 = SpanByte::from_slice(b"same");
    let span3 = SpanByte::from_slice(b"different");

    assert_eq!(span1, span2);
    assert_ne!(span1, span3);
}

// ============ SpanByteBuilder Tests ============

#[test]
fn test_builder_basic() {
    let span = SpanByteBuilder::new().append(b"hello").build();

    assert_eq!(span.as_slice(), b"hello");
}

#[test]
fn test_builder_push() {
    let span = SpanByteBuilder::new()
        .push(b'a')
        .push(b'b')
        .push(b'c')
        .build();

    assert_eq!(span.as_slice(), b"abc");
}

#[test]
fn test_builder_with_metadata() {
    let span = SpanByteBuilder::new().append(b"data").metadata(42).build();

    assert_eq!(span.metadata(), Some(42));
}

#[test]
fn test_builder_with_capacity() {
    let span = SpanByteBuilder::with_capacity(100).append(b"test").build();

    assert_eq!(span.len(), 4);
}

// ============ VarLen Traits Tests ============

#[test]
fn test_varlen_struct_trait() {
    let span = SpanByte::from_slice(b"test");

    let size = VarLenStruct::serialized_size(&span);
    let mut buffer = vec![0u8; size];

    let written = VarLenStruct::serialize(&span, &mut buffer);
    assert_eq!(written, size);

    let deserialized = SpanByte::deserialize(&buffer).unwrap();
    assert_eq!(span, deserialized);
}

#[test]
fn test_varlen_key_trait() {
    let span = SpanByte::from_slice(b"key");

    let initial_len = VarLenKey::initial_length(&span);
    assert_eq!(initial_len, span.serialized_size());
}

#[test]
fn test_varlen_value_trait() {
    let span = SpanByte::from_slice(b"value");

    let initial_len = VarLenValue::initial_length(&span);
    assert_eq!(initial_len, span.serialized_size());
}

#[test]
fn test_varlen_value_in_place_update() {
    let mut span1 = SpanByte::from_slice(b"long value here");
    let span2 = SpanByte::from_slice(b"short");
    let span3 = SpanByte::from_slice(b"this is a much longer value");

    // Smaller can update in place
    assert!(VarLenValue::try_in_place_update(&mut span1, &span2));

    // Larger cannot
    assert!(!VarLenValue::try_in_place_update(&mut span1, &span3));
}

// ============ FasterKv Integration Tests ============

fn create_test_store() -> Arc<FasterKv<Utf8, Utf8, NullDisk>> {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    Arc::new(FasterKv::new(config, device).unwrap())
}

#[test]
fn test_fasterkv_varlen_utf8_upsert_read() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let key = Utf8::from("key1");
    let value = Utf8::from("value1");

    let status = session.upsert(key.clone(), value.clone());
    assert_eq!(status, Status::Ok);

    let result = session.read(&key);
    assert_eq!(result, Ok(Some(value)));
}

#[test]
fn test_fasterkv_varlen_utf8_delete() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let key = Utf8::from("key1");
    let value = Utf8::from("value1");

    session.upsert(key.clone(), value);
    let delete_status = session.delete(&key);
    assert_eq!(delete_status, Status::Ok);

    let result = session.read(&key);
    assert_eq!(result, Ok(None));
}

#[test]
fn test_fasterkv_varlen_utf8_update() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    let key = Utf8::from("key1");
    let value1 = Utf8::from("value1");
    let value2 = Utf8::from("value2");

    session.upsert(key.clone(), value1);
    session.upsert(key.clone(), value2.clone());

    let result = session.read(&key);
    assert_eq!(result, Ok(Some(value2)));
}

#[test]
fn test_fasterkv_varlen_utf8_multiple_keys() {
    let store = create_test_store();
    let mut session = store.start_session().unwrap();

    for i in 0..100 {
        let key = Utf8::from(format!("key{i}"));
        let value = Utf8::from(format!("value{i}"));
        session.upsert(key, value);
    }

    for i in 0..100 {
        let key = Utf8::from(format!("key{i}"));
        let expected = Utf8::from(format!("value{i}"));
        let result = session.read(&key);
        assert_eq!(result, Ok(Some(expected)));
    }
}

#[test]
fn test_fasterkv_varlen_raw_bytes() {
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<RawBytes, RawBytes, _>::new(config, NullDisk::new()).unwrap());
    let mut session = store.start_session().unwrap();

    let key = RawBytes::from(vec![0, 1, 2, 3, 4]);
    let value = RawBytes::from((0u8..=255).collect::<Vec<u8>>());

    session.upsert(key.clone(), value.clone());
    let result = session.read(&key);
    assert_eq!(result, Ok(Some(value)));
}

// ============ Edge Cases ============

#[test]
fn test_span_byte_empty() {
    let empty = SpanByte::new();

    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.total_size(), 4); // Just header
}

#[test]
fn test_span_byte_deref() {
    let span = SpanByte::from_slice(b"hello");

    // Test Deref
    let slice: &[u8] = &span;
    assert_eq!(slice, b"hello");
}

#[test]
fn test_span_byte_as_ref() {
    let span = SpanByte::from_slice(b"hello");

    // Test AsRef
    let slice: &[u8] = span.as_ref();
    assert_eq!(slice, b"hello");
}

#[test]
fn test_span_byte_debug() {
    let short = SpanByte::from_slice(b"short");
    let debug_str = format!("{short:?}");
    assert!(debug_str.contains("SpanByte"));

    let long = SpanByte::from_vec(vec![0u8; 100]);
    let long_debug = format!("{long:?}");
    assert!(long_debug.contains("bytes"));
}

#[test]
fn test_span_byte_display() {
    let span = SpanByte::from_string("hello");
    let display = format!("{span}");
    assert_eq!(display, "hello");
}

#[test]
fn test_span_byte_clone() {
    let original = SpanByte::from_slice(b"clone me");
    let cloned = original.clone();

    assert_eq!(original, cloned);
}

#[test]
fn test_span_byte_mut_slice() {
    let mut span = SpanByte::from_slice(b"mutable");
    let slice = span.as_mut_slice();
    slice[0] = b'M';

    assert_eq!(span.as_slice()[0], b'M');
}
