//! Encoding/decoding model for persistence.
//!
//! This module defines the persistence boundary for `oxifaster`.
//! A store instance uses a `KeyCodec` and `ValueCodec` (selected by the key/value types)
//! to turn keys/values into stable bytes for hashing, equality, in-log storage, and disk I/O.

mod bincode;
mod bytes;
mod hash;

/// Multi-algorithm hash support for FASTER C++ compatibility
pub mod multi_hash;

pub use bincode::Bincode;
pub use hash::hash64;
pub use multi_hash::{HashAlgorithm, hash_faster_compat_u64};

use crate::status::Status;

/// Trait binding a key type to its default persistence codec.
pub trait PersistKey: Clone + Eq + Send + Sync + 'static {
    /// Default codec for this key type.
    type Codec: KeyCodec<Self>;
}

/// Trait binding a value type to its default persistence codec.
pub trait PersistValue: Clone + Send + Sync + 'static {
    /// Default codec for this value type.
    type Codec: ValueCodec<Self>;
}

/// Encode/decode and hashing contract for keys.
pub trait KeyCodec<K>: Send + Sync + 'static {
    /// Whether this codec produces fixed-size bytes for any `K`.
    const IS_FIXED: bool;
    /// Fixed encoded length in bytes (only meaningful when `IS_FIXED` is true).
    const FIXED_LEN: usize;

    /// Encoded length for the given key.
    fn encoded_len(key: &K) -> Result<usize, Status>;
    /// Encode the key into the provided buffer.
    ///
    /// Implementations must write exactly `encoded_len(key)` bytes.
    fn encode_into(key: &K, dst: &mut [u8]) -> Result<(), Status>;
    /// Compare an encoded key (from the log) to a typed key.
    ///
    /// This should avoid allocations when possible.
    fn equals_encoded(encoded: &[u8], key: &K) -> Result<bool, Status>;
    /// Decode an encoded key into an owned value.
    fn decode(encoded: &[u8]) -> Result<K, Status>;
    /// Hash encoded key bytes (stable, deterministic).
    #[inline]
    fn hash_encoded(encoded: &[u8]) -> u64 {
        hash::hash64(encoded)
    }

    /// Hash a typed key (stable, deterministic).
    ///
    /// Implementations may override this to avoid allocations.
    #[inline]
    fn hash(key: &K) -> Result<u64, Status> {
        let len = Self::encoded_len(key)?;
        let mut buf = vec![0u8; len];
        Self::encode_into(key, &mut buf)?;
        Ok(Self::hash_encoded(&buf))
    }
}

/// Encode/decode contract for values.
pub trait ValueCodec<V>: Send + Sync + 'static {
    /// Whether this codec produces fixed-size bytes for any `V`.
    const IS_FIXED: bool;
    /// Fixed encoded length in bytes (only meaningful when `IS_FIXED` is true).
    const FIXED_LEN: usize;

    /// Encoded length for the given value.
    fn encoded_len(value: &V) -> Result<usize, Status>;
    /// Encode the value into the provided buffer.
    ///
    /// Implementations must write exactly `encoded_len(value)` bytes.
    fn encode_into(value: &V, dst: &mut [u8]) -> Result<(), Status>;
    /// Decode an encoded value into an owned value.
    fn decode(encoded: &[u8]) -> Result<V, Status>;
}

pub use bincode::SerdeBincodeCodec;
pub use bytes::{BlittableCodec, RawBytes, RawBytesCodec, Utf8, Utf8Codec};

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time sanity checks for codec associated constants.
    // These are more robust than runtime assertions and satisfy clippy's
    // `assertions_on_constants` lint.
    const _: () = {
        assert!(<BlittableCodec<u64> as KeyCodec<u64>>::IS_FIXED);
        assert!(<BlittableCodec<u64> as KeyCodec<u64>>::FIXED_LEN == 8);
        assert!(!<RawBytesCodec as KeyCodec<RawBytes>>::IS_FIXED);
        assert!(<RawBytesCodec as KeyCodec<RawBytes>>::FIXED_LEN == 0);
    };

    // Test hash64 function
    #[test]
    fn test_hash64_deterministic() {
        let data = b"test data for hashing";
        let hash1 = hash64(data);
        let hash2 = hash64(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash64_matches_selected_algorithm() {
        let inputs: [&[u8]; 4] = [b"", b"hello", b"world", b"test data for hashing"];

        for input in inputs {
            let expected = {
                #[cfg(feature = "hash-xxh3")]
                {
                    xxhash_rust::xxh3::xxh3_64(input)
                }

                #[cfg(all(not(feature = "hash-xxh3"), feature = "hash-xxh64"))]
                {
                    xxhash_rust::xxh64::xxh64(input, 0)
                }
            };
            assert_eq!(hash64(input), expected);
        }
    }

    #[test]
    fn test_hash64_empty() {
        let empty: &[u8] = &[];
        let _hash = hash64(empty);
    }

    // Test BlittableCodec for fixed-size types
    #[test]
    fn test_blittable_codec_u64_roundtrip() {
        let key: u64 = 12345678901234567890u64;
        let len = <BlittableCodec<u64> as KeyCodec<u64>>::encoded_len(&key).unwrap();
        assert_eq!(len, 8);

        let mut buf = vec![0u8; len];
        <BlittableCodec<u64> as KeyCodec<u64>>::encode_into(&key, &mut buf).unwrap();

        let decoded = <BlittableCodec<u64> as KeyCodec<u64>>::decode(&buf).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_blittable_codec_equals_encoded() {
        let key: u64 = 42;
        let mut buf = vec![0u8; 8];
        <BlittableCodec<u64> as KeyCodec<u64>>::encode_into(&key, &mut buf).unwrap();

        assert!(<BlittableCodec<u64> as KeyCodec<u64>>::equals_encoded(&buf, &42).unwrap());
        assert!(!<BlittableCodec<u64> as KeyCodec<u64>>::equals_encoded(&buf, &43).unwrap());
    }

    #[test]
    fn test_blittable_codec_hash() {
        let key: u64 = 42;
        let hash1 = <BlittableCodec<u64> as KeyCodec<u64>>::hash(&key).unwrap();
        let hash2 = <BlittableCodec<u64> as KeyCodec<u64>>::hash(&key).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_blittable_codec_decode_wrong_size() {
        let buf = [0u8; 4];
        let result = <BlittableCodec<u64> as KeyCodec<u64>>::decode(&buf);
        assert!(result.is_err());
    }

    // Test RawBytesCodec
    #[test]
    fn test_rawbytes_codec_roundtrip() {
        let key = RawBytes::from(b"hello world".to_vec());
        let len = <RawBytesCodec as KeyCodec<RawBytes>>::encoded_len(&key).unwrap();
        assert_eq!(len, 11);

        let mut buf = vec![0u8; len];
        <RawBytesCodec as KeyCodec<RawBytes>>::encode_into(&key, &mut buf).unwrap();

        let decoded = <RawBytesCodec as KeyCodec<RawBytes>>::decode(&buf).unwrap();
        assert_eq!(decoded, key);
    }

    // Test Utf8Codec
    #[test]
    fn test_utf8_codec_roundtrip() {
        let key = Utf8::from("hello world");
        let len = <Utf8Codec as KeyCodec<Utf8>>::encoded_len(&key).unwrap();
        assert_eq!(len, 11);

        let mut buf = vec![0u8; len];
        <Utf8Codec as KeyCodec<Utf8>>::encode_into(&key, &mut buf).unwrap();

        let decoded = <Utf8Codec as KeyCodec<Utf8>>::decode(&buf).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_utf8_codec_decode_invalid_utf8() {
        let invalid = [0xFF, 0xFE, 0x00, 0x01];
        let result = <Utf8Codec as KeyCodec<Utf8>>::decode(&invalid);
        assert!(result.is_err());
    }

    // Test Bincode
    #[test]
    fn test_bincode_roundtrip() {
        let key = Bincode(42u32);
        let len = <SerdeBincodeCodec<u32> as KeyCodec<Bincode<u32>>>::encoded_len(&key).unwrap();

        let mut buf = vec![0u8; len];
        <SerdeBincodeCodec<u32> as KeyCodec<Bincode<u32>>>::encode_into(&key, &mut buf).unwrap();

        let decoded = <SerdeBincodeCodec<u32> as KeyCodec<Bincode<u32>>>::decode(&buf).unwrap();
        assert_eq!(decoded.0, 42);
    }

    // Test PersistKey implementations
    #[test]
    fn test_persist_key_u64() {
        let key: u64 = 42;
        let hash1 = <u64 as PersistKey>::Codec::hash(&key).unwrap();
        let hash2 = <u64 as PersistKey>::Codec::hash(&key).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_persist_key_rawbytes() {
        let key = RawBytes::from(b"key".to_vec());
        let hash1 = <RawBytes as PersistKey>::Codec::hash(&key).unwrap();
        let hash2 = <RawBytes as PersistKey>::Codec::hash(&key).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_persist_key_utf8() {
        let key = Utf8::from("key");
        let hash1 = <Utf8 as PersistKey>::Codec::hash(&key).unwrap();
        let hash2 = <Utf8 as PersistKey>::Codec::hash(&key).unwrap();
        assert_eq!(hash1, hash2);
    }

    // Test KeyCodec::hash default implementation
    #[test]
    fn test_keycodec_default_hash() {
        let key: u64 = 12345;
        let hash1 = <BlittableCodec<u64> as KeyCodec<u64>>::hash(&key).unwrap();

        let mut buf = vec![0u8; 8];
        <BlittableCodec<u64> as KeyCodec<u64>>::encode_into(&key, &mut buf).unwrap();
        let hash2 = <BlittableCodec<u64> as KeyCodec<u64>>::hash_encoded(&buf);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_keycodec_hash_encoded() {
        let encoded = b"some encoded bytes";
        let hash1 = <BlittableCodec<u64> as KeyCodec<u64>>::hash_encoded(encoded);
        let hash2 = hash64(encoded);
        assert_eq!(hash1, hash2);
    }

    // Edge cases
    #[test]
    fn test_empty_rawbytes() {
        let key = RawBytes::from(vec![]);
        let len = <RawBytesCodec as KeyCodec<RawBytes>>::encoded_len(&key).unwrap();
        assert_eq!(len, 0);

        let buf: Vec<u8> = vec![];
        let decoded = <RawBytesCodec as KeyCodec<RawBytes>>::decode(&buf).unwrap();
        assert_eq!(decoded.0.len(), 0);
    }

    #[test]
    fn test_empty_utf8() {
        let key = Utf8::from("");
        let len = <Utf8Codec as KeyCodec<Utf8>>::encoded_len(&key).unwrap();
        assert_eq!(len, 0);

        let buf: Vec<u8> = vec![];
        let decoded = <Utf8Codec as KeyCodec<Utf8>>::decode(&buf).unwrap();
        assert_eq!(decoded.0, "");
    }

    #[test]
    fn test_rawbytes_debug() {
        let rb = RawBytes::from(vec![1, 2, 3]);
        let debug = format!("{:?}", rb);
        assert!(debug.contains("RawBytes"));
    }

    #[test]
    fn test_utf8_debug() {
        let u = Utf8::from("test");
        let debug = format!("{:?}", u);
        assert!(debug.contains("Utf8"));
    }

    #[test]
    fn test_bincode_debug() {
        let b = Bincode(42);
        let debug = format!("{:?}", b);
        assert!(debug.contains("Bincode"));
    }

    // Test ValueCodec implementations
    #[test]
    fn test_blittable_value_codec() {
        let value: u64 = 999;
        let len = <BlittableCodec<u64> as ValueCodec<u64>>::encoded_len(&value).unwrap();
        assert_eq!(len, 8);

        let mut buf = vec![0u8; len];
        <BlittableCodec<u64> as ValueCodec<u64>>::encode_into(&value, &mut buf).unwrap();

        let decoded = <BlittableCodec<u64> as ValueCodec<u64>>::decode(&buf).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_rawbytes_value_codec() {
        let value = RawBytes::from(b"value data".to_vec());
        let len = <RawBytesCodec as ValueCodec<RawBytes>>::encoded_len(&value).unwrap();

        let mut buf = vec![0u8; len];
        <RawBytesCodec as ValueCodec<RawBytes>>::encode_into(&value, &mut buf).unwrap();

        let decoded = <RawBytesCodec as ValueCodec<RawBytes>>::decode(&buf).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_utf8_value_codec() {
        let value = Utf8::from("value data");
        let len = <Utf8Codec as ValueCodec<Utf8>>::encoded_len(&value).unwrap();

        let mut buf = vec![0u8; len];
        <Utf8Codec as ValueCodec<Utf8>>::encode_into(&value, &mut buf).unwrap();

        let decoded = <Utf8Codec as ValueCodec<Utf8>>::decode(&buf).unwrap();
        assert_eq!(decoded, value);
    }
}
