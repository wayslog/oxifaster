use std::slice;

use crate::address::Address;
use crate::codec::{KeyCodec, PersistKey, PersistValue, ValueCodec};
use crate::record::RecordInfo;
use crate::status::Status;
use crate::utility::pad_alignment;
use crate::varlen::SpanByteView;

const RECORD_INFO_SIZE: usize = std::mem::size_of::<RecordInfo>();
const VARLEN_LENGTHS_SIZE: usize = 2 * std::mem::size_of::<u32>(); // key_len(u32) + value_len(u32)

#[inline]
pub(crate) const fn is_fixed_record<K, V>() -> bool
where
    K: PersistKey,
    V: PersistValue,
{
    <K as PersistKey>::Codec::IS_FIXED && <V as PersistValue>::Codec::IS_FIXED
}

#[inline]
pub(crate) fn fixed_disk_len<K, V>() -> usize
where
    K: PersistKey,
    V: PersistValue,
{
    debug_assert!(is_fixed_record::<K, V>());
    RECORD_INFO_SIZE + <K as PersistKey>::Codec::FIXED_LEN + <V as PersistValue>::Codec::FIXED_LEN
}

#[inline]
pub(crate) fn fixed_alloc_len<K, V>() -> usize
where
    K: PersistKey,
    V: PersistValue,
{
    pad_alignment(fixed_disk_len::<K, V>(), RECORD_INFO_SIZE)
}

#[inline]
pub(crate) fn varlen_disk_len(key_len: usize, value_len: usize) -> usize {
    RECORD_INFO_SIZE + VARLEN_LENGTHS_SIZE + key_len + value_len
}

#[inline]
pub(crate) fn varlen_alloc_len(key_len: usize, value_len: usize) -> usize {
    pad_alignment(varlen_disk_len(key_len, value_len), RECORD_INFO_SIZE)
}

/// Compute the record layout for write operations (upsert/delete).
///
/// Returns `(disk_len, alloc_len, key_len, value_len)` where `value_len` is 0 for varlen tombstones.
pub(crate) fn layout_for_ops<K, V>(
    key: &K,
    value: Option<&V>,
) -> Result<(usize, usize, usize, usize), Status>
where
    K: PersistKey,
    V: PersistValue,
{
    if is_fixed_record::<K, V>() {
        Ok((
            fixed_disk_len::<K, V>(),
            fixed_alloc_len::<K, V>(),
            <K as PersistKey>::Codec::FIXED_LEN,
            <V as PersistValue>::Codec::FIXED_LEN,
        ))
    } else {
        let key_len = <K as PersistKey>::Codec::encoded_len(key)?;
        let value_len = match value {
            Some(value) => <V as PersistValue>::Codec::encoded_len(value)?,
            None => 0,
        };
        Ok((
            varlen_disk_len(key_len, value_len),
            varlen_alloc_len(key_len, value_len),
            key_len,
            value_len,
        ))
    }
}

/// Borrowed view of a record's encoded key/value bytes.
///
/// The underlying memory is owned by the hybrid log. The returned slices are valid only while
/// the caller keeps the record memory pinned (e.g. by holding an `EpochGuard` for the duration of
/// the borrow).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordView<'a> {
    address: Address,
    tombstone: bool,
    key: &'a [u8],
    value: Option<&'a [u8]>,
    value_len: usize,
}

impl<'a> RecordView<'a> {
    #[inline]
    /// Address of the record in the hybrid log.
    pub fn address(&self) -> Address {
        self.address
    }

    #[inline]
    /// Whether the record is a tombstone (delete marker).
    pub fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    #[inline]
    /// Encoded key bytes.
    pub fn key_bytes(&self) -> &'a [u8] {
        self.key
    }

    #[inline]
    /// Encoded value bytes, if the record is not a tombstone.
    pub fn value_bytes(&self) -> Option<&'a [u8]> {
        self.value
    }

    #[inline]
    /// Encoded value length (0 for tombstones).
    pub fn value_len(&self) -> usize {
        self.value_len
    }

    /// Best-effort SpanByte view for the key bytes.
    #[inline]
    pub fn key_as_spanbyte(&self) -> Option<SpanByteView<'a>> {
        SpanByteView::parse(self.key)
    }

    /// Best-effort SpanByte view for the value bytes.
    #[inline]
    pub fn value_as_spanbyte(&self) -> Option<SpanByteView<'a>> {
        self.value.and_then(SpanByteView::parse)
    }
}

pub(crate) unsafe fn record_info_at<'a>(ptr: *const u8) -> &'a RecordInfo {
    unsafe { &*(ptr as *const RecordInfo) }
}

pub(crate) unsafe fn record_view_from_memory<'a, K, V>(
    address: Address,
    ptr: *const u8,
    limit: usize,
) -> Result<RecordView<'a>, Status>
where
    K: PersistKey,
    V: PersistValue,
{
    if limit < RECORD_INFO_SIZE {
        return Err(Status::Corruption);
    }

    let info = unsafe { record_info_at(ptr) };
    if is_fixed_record::<K, V>() {
        let total = fixed_disk_len::<K, V>();
        if limit < total {
            return Err(Status::Corruption);
        }

        let key_len = <K as PersistKey>::Codec::FIXED_LEN;
        let encoded_value_len = <V as PersistValue>::Codec::FIXED_LEN;

        let key_ptr = unsafe { ptr.add(RECORD_INFO_SIZE) };
        let val_ptr = unsafe { key_ptr.add(key_len) };

        let key = unsafe { slice::from_raw_parts(key_ptr, key_len) };
        let tombstone = info.is_tombstone();
        let value = if tombstone {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(val_ptr, encoded_value_len) })
        };

        Ok(RecordView {
            address,
            tombstone,
            key,
            value,
            value_len: if tombstone { 0 } else { encoded_value_len },
        })
    } else {
        if limit < RECORD_INFO_SIZE + VARLEN_LENGTHS_SIZE {
            return Err(Status::Corruption);
        }

        let len_ptr = unsafe { ptr.add(RECORD_INFO_SIZE) };
        let len_bytes = unsafe { slice::from_raw_parts(len_ptr, VARLEN_LENGTHS_SIZE) };
        let key_len =
            u32::from_le_bytes(len_bytes[0..4].try_into().map_err(|_| Status::Corruption)?)
                as usize;
        let value_len =
            u32::from_le_bytes(len_bytes[4..8].try_into().map_err(|_| Status::Corruption)?)
                as usize;

        let total = RECORD_INFO_SIZE
            .checked_add(VARLEN_LENGTHS_SIZE)
            .and_then(|n| n.checked_add(key_len))
            .and_then(|n| n.checked_add(value_len))
            .ok_or(Status::Corruption)?;
        if limit < total {
            return Err(Status::Corruption);
        }

        let key_ptr = unsafe { len_ptr.add(VARLEN_LENGTHS_SIZE) };
        let val_ptr = unsafe { key_ptr.add(key_len) };

        let key = unsafe { slice::from_raw_parts(key_ptr, key_len) };
        let tombstone = info.is_tombstone();
        if tombstone && value_len != 0 {
            return Err(Status::Corruption);
        }
        let value = if tombstone {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(val_ptr, value_len) })
        };

        Ok(RecordView {
            address,
            tombstone,
            key,
            value,
            value_len: if tombstone { 0 } else { value_len },
        })
    }
}

pub(crate) fn encode_key_to_stack_or_vec<K>(key: &K) -> Result<Vec<u8>, Status>
where
    K: PersistKey,
{
    let len = <K as PersistKey>::Codec::encoded_len(key)?;
    let mut buf = vec![0u8; len];
    <K as PersistKey>::Codec::encode_into(key, &mut buf)?;
    Ok(buf)
}

pub(crate) fn encode_value_to_stack_or_vec<V>(value: &V) -> Result<Vec<u8>, Status>
where
    V: PersistValue,
{
    let len = <V as PersistValue>::Codec::encoded_len(value)?;
    let mut buf = vec![0u8; len];
    <V as PersistValue>::Codec::encode_into(value, &mut buf)?;
    Ok(buf)
}
