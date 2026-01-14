//! Pending I/O integration tests.
//!
//! Validates the FASTER-style behavior: when a record is in the "on-disk region"
//! (`address < head_address`):
//! - `read()` returns `Err(Status::Pending)`
//! - `complete_pending(true)` drives real device I/O to completion
//! - retrying `read()` succeeds after completion
//!
//! Also covers disk hash-chain traversal when multiple keys collide into the same hash bucket.

use std::sync::Arc;
use std::time::Duration;

use oxifaster::codec::{KeyCodec, PersistKey};
use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use oxifaster::Address;
use tempfile::tempdir;

#[test]
fn test_pending_read_and_complete_pending_readback() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MiB
        page_size_bits: 14,       // 16 KiB
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);

    // Flush page 0 and shift the head to page 1: any address in page 0 is now "on disk".
    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // Before I/O completes, reads return Pending.
    assert_eq!(session.read(&1u64), Err(Status::Pending));

    // Drive the background I/O to completion.
    assert!(session.complete_pending(true));

    // Retrying the read succeeds after completion.
    assert_eq!(session.read(&1u64), Ok(Some(100u64)));
}

#[test]
fn test_complete_pending_with_custom_timeout() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data_timeout.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    session.upsert(42u64, 999u64);
    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // Trigger a read once to submit a background I/O request.
    assert_eq!(session.read(&42u64), Err(Status::Pending));

    // Wait for I/O completion with a custom timeout.
    let completed = session.complete_pending_with_timeout(true, Duration::from_secs(5));
    assert!(
        completed,
        "POD types should complete the disk read within the timeout"
    );

    // Retrying should succeed.
    assert_eq!(session.read(&42u64), Ok(Some(999u64)));
}

#[test]
fn test_complete_pending_no_wait_returns_immediately() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data_nowait.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    session.upsert(1u64, 100u64);
    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // Trigger Pending.
    assert_eq!(session.read(&1u64), Err(Status::Pending));

    // wait=false returns immediately (completion is not guaranteed).
    let start = std::time::Instant::now();
    let _result = session.complete_pending(false);
    let elapsed = start.elapsed();

    // Should return quickly (well under 1 second).
    assert!(
        elapsed < Duration::from_secs(1),
        "wait=false should return immediately"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CollidingKey(u64);

struct CollidingKeyCodec;

impl KeyCodec<CollidingKey> for CollidingKeyCodec {
    const IS_FIXED: bool = true;
    const FIXED_LEN: usize = std::mem::size_of::<u64>();

    fn encoded_len(_key: &CollidingKey) -> Result<usize, Status> {
        Ok(Self::FIXED_LEN)
    }

    fn encode_into(key: &CollidingKey, dst: &mut [u8]) -> Result<(), Status> {
        if dst.len() != Self::FIXED_LEN {
            return Err(Status::Corruption);
        }
        dst.copy_from_slice(&key.0.to_le_bytes());
        Ok(())
    }

    fn equals_encoded(encoded: &[u8], key: &CollidingKey) -> Result<bool, Status> {
        Ok(Self::decode(encoded)? == *key)
    }

    fn decode(encoded: &[u8]) -> Result<CollidingKey, Status> {
        let bytes: [u8; 8] = encoded.try_into().map_err(|_| Status::Corruption)?;
        Ok(CollidingKey(u64::from_le_bytes(bytes)))
    }

    fn hash_encoded(_encoded: &[u8]) -> u64 {
        0x9E37_79B9_7F4A_7C15
    }
}

impl PersistKey for CollidingKey {
    type Codec = CollidingKeyCodec;
}

#[test]
fn test_pending_read_traverses_disk_hash_chain_on_collision() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("data_collision.db");

    let device = FileSystemDisk::single_file(&data_path).unwrap();
    let config = FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20, // 1 MiB
        page_size_bits: 14,       // 16 KiB
        mutable_fraction: 0.9,
    };

    let store = Arc::new(FasterKv::<CollidingKey, u64, _>::new(config, device));
    let mut session = store.start_session().unwrap();

    let key1 = CollidingKey(1);
    let key2 = CollidingKey(2);
    session.upsert(key1, 100u64);
    session.upsert(key2, 200u64);

    assert_eq!(store.flush_and_shift_head(Address::new(1, 0)), Status::Ok);

    // The bucket head is `key2` (newer record). Reading `key1` must traverse the disk chain.
    for _ in 0..16 {
        match session.read(&key1) {
            Ok(Some(v)) => {
                assert_eq!(v, 100u64);
                return;
            }
            Ok(None) => panic!("expected key1 to be present"),
            Err(Status::Pending) => {
                assert!(session.complete_pending(true));
            }
            Err(s) => panic!("unexpected status: {s:?}"),
        }
    }

    panic!("read did not complete; possible Pending loop on disk collision traversal");
}
