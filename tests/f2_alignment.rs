#![cfg(feature = "f2")]

use oxifaster::device::NullDisk;
use oxifaster::f2::{F2Config, F2Kv};

#[test]
fn test_f2_u128_key_value_alignment_roundtrip() {
    let config = F2Config::default();
    let store: F2Kv<u128, u128, _> = F2Kv::new(config, NullDisk::new(), NullDisk::new()).unwrap();

    let key = 42u128;
    let value = 100u128;
    store.upsert(key, value).unwrap();
    assert_eq!(store.read(&key).unwrap(), Some(value));

    store.rmw(key, |v| *v += 1).unwrap();
    assert_eq!(store.read(&key).unwrap(), Some(value + 1));
}
