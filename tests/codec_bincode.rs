//! 测试 bincode 编解码器功能

use oxifaster::codec::{Bincode, KeyCodec, SerdeBincodeCodec, ValueCodec};
use oxifaster::status::Status;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestStruct {
    id: u64,
    name: String,
    values: Vec<i32>,
}

#[test]
fn test_bincode_basic_i32() {
    let value = Bincode(42i32);
    let len = <SerdeBincodeCodec<i32> as ValueCodec<Bincode<i32>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<i32> as ValueCodec<Bincode<i32>>>::encode_into(&value, &mut buf).unwrap();

    let decoded =
        <SerdeBincodeCodec<i32> as ValueCodec<Bincode<i32>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, 42i32);
}

#[test]
fn test_bincode_basic_i64() {
    let value = Bincode(9876543210i64);
    let len = <SerdeBincodeCodec<i64> as ValueCodec<Bincode<i64>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<i64> as ValueCodec<Bincode<i64>>>::encode_into(&value, &mut buf).unwrap();

    let decoded =
        <SerdeBincodeCodec<i64> as ValueCodec<Bincode<i64>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, 9876543210i64);
}

#[test]
fn test_bincode_basic_u64() {
    let value = Bincode(u64::MAX);
    let len = <SerdeBincodeCodec<u64> as ValueCodec<Bincode<u64>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<u64> as ValueCodec<Bincode<u64>>>::encode_into(&value, &mut buf).unwrap();

    let decoded =
        <SerdeBincodeCodec<u64> as ValueCodec<Bincode<u64>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, u64::MAX);
}

#[test]
fn test_bincode_string() {
    let value = Bincode(String::from("Hello, FASTER!"));
    let len =
        <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::encode_into(&value, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, "Hello, FASTER!");
}

#[test]
fn test_bincode_empty_string() {
    let value = Bincode(String::new());
    let len =
        <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::encode_into(&value, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, "");
}

#[test]
fn test_bincode_vec() {
    let value = Bincode(vec![1, 2, 3, 4, 5]);
    let len = <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encoded_len(&value)
        .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encode_into(&value, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_bincode_empty_vec() {
    let value: Bincode<Vec<i32>> = Bincode(vec![]);
    let len = <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encoded_len(&value)
        .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encode_into(&value, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, Vec::<i32>::new());
}

#[test]
fn test_bincode_custom_struct() {
    let value = Bincode(TestStruct {
        id: 12345,
        name: String::from("test"),
        values: vec![10, 20, 30],
    });

    let len =
        <SerdeBincodeCodec<TestStruct> as ValueCodec<Bincode<TestStruct>>>::encoded_len(&value)
            .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<TestStruct> as ValueCodec<Bincode<TestStruct>>>::encode_into(
        &value, &mut buf,
    )
    .unwrap();

    let decoded =
        <SerdeBincodeCodec<TestStruct> as ValueCodec<Bincode<TestStruct>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, value.0);
}

#[test]
fn test_bincode_struct_large_vec() {
    let large_vec: Vec<i32> = (0..10000).collect();
    let value = Bincode(TestStruct {
        id: 99999,
        name: String::from("large"),
        values: large_vec.clone(),
    });

    let len =
        <SerdeBincodeCodec<TestStruct> as ValueCodec<Bincode<TestStruct>>>::encoded_len(&value)
            .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<TestStruct> as ValueCodec<Bincode<TestStruct>>>::encode_into(
        &value, &mut buf,
    )
    .unwrap();

    let decoded =
        <SerdeBincodeCodec<TestStruct> as ValueCodec<Bincode<TestStruct>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0.values.len(), 10000);
    assert_eq!(decoded.0.values, large_vec);
}

#[test]
fn test_bincode_symmetry_roundtrip() {
    let original = Bincode(vec![100, 200, 300]);

    let len =
        <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encoded_len(&original)
            .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encode_into(
        &original, &mut buf,
    )
    .unwrap();

    let decoded =
        <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::decode(&buf).unwrap();

    let len2 =
        <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encoded_len(&decoded)
            .unwrap();
    assert_eq!(len, len2);

    let mut buf2 = vec![0u8; len2];
    <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encode_into(
        &decoded, &mut buf2,
    )
    .unwrap();

    assert_eq!(buf, buf2);
}

#[test]
fn test_bincode_key_codec_encode_decode() {
    let key = Bincode(String::from("test_key"));
    let len =
        <SerdeBincodeCodec<String> as KeyCodec<Bincode<String>>>::encoded_len(&key).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<String> as KeyCodec<Bincode<String>>>::encode_into(&key, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<String> as KeyCodec<Bincode<String>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, "test_key");
}

#[test]
fn test_bincode_key_codec_equals() {
    let key = Bincode(42u64);
    let len = <SerdeBincodeCodec<u64> as KeyCodec<Bincode<u64>>>::encoded_len(&key).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<u64> as KeyCodec<Bincode<u64>>>::encode_into(&key, &mut buf).unwrap();

    let equals =
        <SerdeBincodeCodec<u64> as KeyCodec<Bincode<u64>>>::equals_encoded(&buf, &key).unwrap();
    assert!(equals);

    let different_key = Bincode(99u64);
    let not_equals = <SerdeBincodeCodec<u64> as KeyCodec<Bincode<u64>>>::equals_encoded(
        &buf,
        &different_key,
    )
    .unwrap();
    assert!(!not_equals);
}

#[test]
fn test_bincode_corrupted_data() {
    let corrupted = vec![0xff, 0xff, 0xff, 0xff];
    let result = <SerdeBincodeCodec<i32> as ValueCodec<Bincode<i32>>>::decode(&corrupted);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Status::Corruption);
}

#[test]
fn test_bincode_empty_buffer() {
    let empty: Vec<u8> = vec![];
    let result = <SerdeBincodeCodec<i32> as ValueCodec<Bincode<i32>>>::decode(&empty);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), Status::Corruption);
}

#[test]
fn test_bincode_truncated_data() {
    let value = Bincode(vec![1, 2, 3, 4, 5]);
    let len = <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encoded_len(&value)
        .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::encode_into(&value, &mut buf)
        .unwrap();

    let truncated = &buf[0..buf.len() / 2];
    let result = <SerdeBincodeCodec<Vec<i32>> as ValueCodec<Bincode<Vec<i32>>>>::decode(truncated);
    assert!(result.is_err());
}

#[test]
fn test_bincode_wrapper_type() {
    let val1 = Bincode(42);
    let val2 = Bincode(42);
    let val3 = Bincode(43);

    assert_eq!(val1, val2);
    assert_ne!(val1, val3);
    assert_eq!(val1.clone(), val2);
}

#[test]
fn test_bincode_large_string() {
    let large_str = "x".repeat(100_000);
    let value = Bincode(large_str.clone());

    let len =
        <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::encode_into(&value, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<String> as ValueCodec<Bincode<String>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0.len(), 100_000);
    assert_eq!(decoded.0, large_str);
}

#[test]
fn test_bincode_nested_struct() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Inner {
        x: i32,
        y: i32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Outer {
        inner: Inner,
        name: String,
    }

    let value = Bincode(Outer {
        inner: Inner { x: 10, y: 20 },
        name: String::from("outer"),
    });

    let len =
        <SerdeBincodeCodec<Outer> as ValueCodec<Bincode<Outer>>>::encoded_len(&value).unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<Outer> as ValueCodec<Bincode<Outer>>>::encode_into(&value, &mut buf)
        .unwrap();

    let decoded =
        <SerdeBincodeCodec<Outer> as ValueCodec<Bincode<Outer>>>::decode(&buf).unwrap();
    assert_eq!(decoded.0, value.0);
}

#[test]
fn test_bincode_option_type() {
    let some_value = Bincode(Some(42i32));
    let len =
        <SerdeBincodeCodec<Option<i32>> as ValueCodec<Bincode<Option<i32>>>>::encoded_len(
            &some_value,
        )
        .unwrap();
    let mut buf = vec![0u8; len];
    <SerdeBincodeCodec<Option<i32>> as ValueCodec<Bincode<Option<i32>>>>::encode_into(
        &some_value,
        &mut buf,
    )
    .unwrap();

    let decoded = <SerdeBincodeCodec<Option<i32>> as ValueCodec<Bincode<Option<i32>>>>::decode(
        &buf,
    )
    .unwrap();
    assert_eq!(decoded.0, Some(42i32));

    let none_value: Bincode<Option<i32>> = Bincode(None);
    let len2 =
        <SerdeBincodeCodec<Option<i32>> as ValueCodec<Bincode<Option<i32>>>>::encoded_len(
            &none_value,
        )
        .unwrap();
    let mut buf2 = vec![0u8; len2];
    <SerdeBincodeCodec<Option<i32>> as ValueCodec<Bincode<Option<i32>>>>::encode_into(
        &none_value,
        &mut buf2,
    )
    .unwrap();

    let decoded2 = <SerdeBincodeCodec<Option<i32>> as ValueCodec<Bincode<Option<i32>>>>::decode(
        &buf2,
    )
    .unwrap();
    assert_eq!(decoded2.0, None);
}
