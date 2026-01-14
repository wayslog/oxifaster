//! Stable hashing for encoded key bytes.
//!
//! The hash must be deterministic across runs. We do not use `DefaultHasher`.

#[cfg(not(any(feature = "hash-xxh3", feature = "hash-xxh64")))]
compile_error!("Enable a hash feature: `hash-xxh3` (default) or `hash-xxh64`.");

/// Hash key bytes into a 64-bit value (deterministic).
#[inline]
pub fn hash64(bytes: &[u8]) -> u64 {
    #[cfg(feature = "hash-xxh3")]
    {
        xxhash_rust::xxh3::xxh3_64(bytes)
    }

    #[cfg(all(not(feature = "hash-xxh3"), feature = "hash-xxh64"))]
    {
        xxhash_rust::xxh64::xxh64(bytes, 0)
    }
}
