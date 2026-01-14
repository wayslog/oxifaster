//! Built-in codecs for common Rust types.

use std::mem;

use bytemuck::Pod;

use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::status::Status;

/// Blittable codec for `bytemuck::Pod` types.
pub struct BlittableCodec<T>(std::marker::PhantomData<T>);

impl<T: Pod> BlittableCodec<T> {
    #[inline]
    fn len() -> usize {
        mem::size_of::<T>()
    }
}

impl<T: Pod + Send + Sync> KeyCodec<T> for BlittableCodec<T> {
    const IS_FIXED: bool = true;
    const FIXED_LEN: usize = mem::size_of::<T>();

    #[inline]
    fn encoded_len(_key: &T) -> Result<usize, Status> {
        Ok(Self::len())
    }

    #[inline]
    fn encode_into(key: &T, dst: &mut [u8]) -> Result<(), Status> {
        dst.copy_from_slice(bytemuck::bytes_of(key));
        Ok(())
    }

    #[inline]
    fn equals_encoded(encoded: &[u8], key: &T) -> Result<bool, Status> {
        Ok(encoded == bytemuck::bytes_of(key))
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<T, Status> {
        if encoded.len() != Self::len() {
            return Err(Status::Corruption);
        }
        Ok(bytemuck::pod_read_unaligned(encoded))
    }

    #[inline]
    fn hash(key: &T) -> Result<u64, Status> {
        Ok(crate::codec::hash64(bytemuck::bytes_of(key)))
    }
}

impl<T: Pod + Send + Sync> ValueCodec<T> for BlittableCodec<T> {
    const IS_FIXED: bool = true;
    const FIXED_LEN: usize = mem::size_of::<T>();

    #[inline]
    fn encoded_len(_value: &T) -> Result<usize, Status> {
        Ok(Self::len())
    }

    #[inline]
    fn encode_into(value: &T, dst: &mut [u8]) -> Result<(), Status> {
        dst.copy_from_slice(bytemuck::bytes_of(value));
        Ok(())
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<T, Status> {
        if encoded.len() != Self::len() {
            return Err(Status::Corruption);
        }
        Ok(bytemuck::pod_read_unaligned(encoded))
    }
}

impl<T: Pod + Clone + Eq + Send + Sync + 'static> PersistKey for T {
    type Codec = BlittableCodec<T>;
}

impl<T: Pod + Clone + Send + Sync + 'static> PersistValue for T {
    type Codec = BlittableCodec<T>;
}

/// Wrapper type to opt into raw-bytes persistence for keys/values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RawBytes(pub bytes::Bytes);

impl From<bytes::Bytes> for RawBytes {
    fn from(value: bytes::Bytes) -> Self {
        Self(value)
    }
}

impl From<Vec<u8>> for RawBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(bytes::Bytes::from(value))
    }
}

/// Codec for `RawBytes` as raw bytes (no envelope).
pub struct RawBytesCodec;

impl KeyCodec<RawBytes> for RawBytesCodec {
    const IS_FIXED: bool = false;
    const FIXED_LEN: usize = 0;

    #[inline]
    fn encoded_len(key: &RawBytes) -> Result<usize, Status> {
        Ok(key.0.len())
    }

    #[inline]
    fn encode_into(key: &RawBytes, dst: &mut [u8]) -> Result<(), Status> {
        dst.copy_from_slice(key.0.as_ref());
        Ok(())
    }

    #[inline]
    fn equals_encoded(encoded: &[u8], key: &RawBytes) -> Result<bool, Status> {
        Ok(encoded == key.0.as_ref())
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<RawBytes, Status> {
        Ok(RawBytes(bytes::Bytes::copy_from_slice(encoded)))
    }

    #[inline]
    fn hash(key: &RawBytes) -> Result<u64, Status> {
        Ok(crate::codec::hash64(key.0.as_ref()))
    }
}

impl ValueCodec<RawBytes> for RawBytesCodec {
    const IS_FIXED: bool = false;
    const FIXED_LEN: usize = 0;

    #[inline]
    fn encoded_len(value: &RawBytes) -> Result<usize, Status> {
        Ok(value.0.len())
    }

    #[inline]
    fn encode_into(value: &RawBytes, dst: &mut [u8]) -> Result<(), Status> {
        dst.copy_from_slice(value.0.as_ref());
        Ok(())
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<RawBytes, Status> {
        Ok(RawBytes(bytes::Bytes::copy_from_slice(encoded)))
    }
}

impl PersistKey for RawBytes {
    type Codec = RawBytesCodec;
}

impl PersistValue for RawBytes {
    type Codec = RawBytesCodec;
}

/// Wrapper type to opt into UTF-8 persistence for keys/values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Utf8(pub String);

impl From<String> for Utf8 {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Utf8 {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Codec for `Utf8` as raw UTF-8 bytes (no envelope).
pub struct Utf8Codec;

impl KeyCodec<Utf8> for Utf8Codec {
    const IS_FIXED: bool = false;
    const FIXED_LEN: usize = 0;

    #[inline]
    fn encoded_len(key: &Utf8) -> Result<usize, Status> {
        Ok(key.0.len())
    }

    #[inline]
    fn encode_into(key: &Utf8, dst: &mut [u8]) -> Result<(), Status> {
        dst.copy_from_slice(key.0.as_bytes());
        Ok(())
    }

    #[inline]
    fn equals_encoded(encoded: &[u8], key: &Utf8) -> Result<bool, Status> {
        Ok(encoded == key.0.as_bytes())
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<Utf8, Status> {
        let s = String::from_utf8(encoded.to_vec()).map_err(|_| Status::Corruption)?;
        Ok(Utf8(s))
    }

    #[inline]
    fn hash(key: &Utf8) -> Result<u64, Status> {
        Ok(crate::codec::hash64(key.0.as_bytes()))
    }
}

impl ValueCodec<Utf8> for Utf8Codec {
    const IS_FIXED: bool = false;
    const FIXED_LEN: usize = 0;

    #[inline]
    fn encoded_len(value: &Utf8) -> Result<usize, Status> {
        Ok(value.0.len())
    }

    #[inline]
    fn encode_into(value: &Utf8, dst: &mut [u8]) -> Result<(), Status> {
        dst.copy_from_slice(value.0.as_bytes());
        Ok(())
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<Utf8, Status> {
        let s = String::from_utf8(encoded.to_vec()).map_err(|_| Status::Corruption)?;
        Ok(Utf8(s))
    }
}

impl PersistKey for Utf8 {
    type Codec = Utf8Codec;
}

impl PersistValue for Utf8 {
    type Codec = Utf8Codec;
}
