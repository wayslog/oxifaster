//! Encoding/decoding model for persistence.
//!
//! This module defines the persistence boundary for `oxifaster`.
//! A store instance uses a `KeyCodec` and `ValueCodec` (selected by the key/value types)
//! to turn keys/values into stable bytes for hashing, equality, in-log storage, and disk I/O.

mod bincode;
mod bytes;
mod hash;

pub use bincode::Bincode;
pub use hash::hash64;

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
