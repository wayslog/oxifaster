use std::sync::Arc;

use oxifaster::codec::Utf8;
use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1024,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    }
}

#[test]
fn test_read_view_fixed_record() {
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config(), NullDisk::new()).unwrap());
    let mut session = store.start_session().unwrap();

    let key = 42u64;
    let value = 100u64;
    assert_eq!(session.upsert(key, value), Status::Ok);

    let guard = session.pin();
    let view = session.read_view(&guard, &key).unwrap().unwrap();

    assert!(!view.is_tombstone());
    assert_eq!(view.key_bytes(), bytemuck::bytes_of(&key));
    assert_eq!(view.value_bytes().unwrap(), bytemuck::bytes_of(&value));
    assert_eq!(view.value_len(), bytemuck::bytes_of(&value).len());

    assert_eq!(session.delete(&key), Status::Ok);
    let tombstone = session.read_view(&guard, &key).unwrap().unwrap();
    assert!(tombstone.is_tombstone());
    assert_eq!(tombstone.key_bytes(), bytemuck::bytes_of(&key));
    assert!(tombstone.value_bytes().is_none());
    assert_eq!(tombstone.value_len(), 0);
}

#[test]
fn test_read_view_varlen_utf8() {
    let store = Arc::new(FasterKv::<Utf8, Utf8, _>::new(config(), NullDisk::new()).unwrap());
    let mut session = store.start_session().unwrap();

    let key = Utf8::from("k1");
    let value = Utf8::from("v1");
    assert_eq!(session.upsert(key.clone(), value), Status::Ok);

    let guard = session.pin();
    let view = session.read_view(&guard, &key).unwrap().unwrap();
    assert!(!view.is_tombstone());
    assert_eq!(view.key_bytes(), b"k1");
    assert_eq!(view.value_bytes().unwrap(), b"v1");

    assert_eq!(session.delete(&key), Status::Ok);
    let tombstone = session.read_view(&guard, &key).unwrap().unwrap();
    assert!(tombstone.is_tombstone());
    assert_eq!(tombstone.key_bytes(), b"k1");
    assert!(tombstone.value_bytes().is_none());
}
