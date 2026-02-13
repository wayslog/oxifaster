//! Serde+bincode opt-in persistence (via wrapper types).
//!
//! This uses `SpanByte` envelope by default (FASTER-style) for serialized payloads.

use std::io::Cursor;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::status::Status;
use crate::varlen::SpanByteView;

/// Wrapper type to opt into serde+bincode persistence.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Bincode<T>(pub T);

/// Serde+bincode codec that stores values in a `SpanByte` envelope.
pub struct SerdeBincodeCodec<T>(PhantomData<T>);

impl<T> SerdeBincodeCodec<T> {
    #[inline]
    fn payload_len(value: &T) -> Result<usize, Status>
    where
        T: Serialize,
    {
        let n = bincode::serialized_size(value).map_err(|_| Status::InvalidArgument)?;
        usize::try_from(n).map_err(|_| Status::ResourceExhausted)
    }

    #[inline]
    fn encode_enveloped(value: &T, dst: &mut [u8]) -> Result<(), Status>
    where
        T: Serialize,
    {
        let payload_len = Self::payload_len(value)?;
        let total = SpanByteView::total_size(payload_len, false);
        if dst.len() != total {
            return Err(Status::Corruption);
        }

        let (payload_dst, _) = SpanByteView::write_header(dst, payload_len, None)?;
        let mut cur = Cursor::new(payload_dst);
        bincode::serialize_into(&mut cur, value).map_err(|_| Status::InvalidArgument)?;
        Ok(())
    }

    #[inline]
    fn decode_enveloped(bytes: &[u8]) -> Result<T, Status>
    where
        T: DeserializeOwned,
    {
        let view = SpanByteView::parse(bytes).ok_or(Status::Corruption)?;
        bincode::deserialize(view.payload()).map_err(|_| Status::Corruption)
    }
}

impl<T> KeyCodec<Bincode<T>> for SerdeBincodeCodec<T>
where
    T: Serialize + DeserializeOwned + Eq + Clone + Send + Sync + 'static,
{
    const IS_FIXED: bool = false;
    const FIXED_LEN: usize = 0;

    #[inline]
    fn encoded_len(key: &Bincode<T>) -> Result<usize, Status> {
        let payload_len = Self::payload_len(&key.0)?;
        Ok(SpanByteView::total_size(payload_len, false))
    }

    #[inline]
    fn encode_into(key: &Bincode<T>, dst: &mut [u8]) -> Result<(), Status> {
        Self::encode_enveloped(&key.0, dst)
    }

    #[inline]
    fn equals_encoded(encoded: &[u8], key: &Bincode<T>) -> Result<bool, Status> {
        Ok(Self::decode_enveloped(encoded)? == key.0)
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<Bincode<T>, Status> {
        Self::decode_enveloped(encoded).map(Bincode)
    }
}

impl<T> ValueCodec<Bincode<T>> for SerdeBincodeCodec<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    const IS_FIXED: bool = false;
    const FIXED_LEN: usize = 0;

    #[inline]
    fn encoded_len(value: &Bincode<T>) -> Result<usize, Status> {
        let payload_len = Self::payload_len(&value.0)?;
        Ok(SpanByteView::total_size(payload_len, false))
    }

    #[inline]
    fn encode_into(value: &Bincode<T>, dst: &mut [u8]) -> Result<(), Status> {
        Self::encode_enveloped(&value.0, dst)
    }

    #[inline]
    fn decode(encoded: &[u8]) -> Result<Bincode<T>, Status> {
        Self::decode_enveloped(encoded).map(Bincode)
    }
}

impl<T> PersistKey for Bincode<T>
where
    T: Serialize + DeserializeOwned + Eq + Clone + Send + Sync + 'static,
{
    type Codec = SerdeBincodeCodec<T>;
}

impl<T> PersistValue for Bincode<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Codec = SerdeBincodeCodec<T>;
}
