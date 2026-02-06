use oxifaster::device::FileSystemDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn test_store_config() -> FasterKvConfig {
    FasterKvConfig {
        table_size: 1 << 10,
        log_memory_size: 1 << 20,
        page_size_bits: 14,
        mutable_fraction: 0.9,
    }
}

#[test]
fn conditional_insert_after_delete_should_succeed() {
    let dir = tempdir().expect("tempdir");
    let data_path = dir.path().join("oxifaster_conditional_insert.dat");

    let device = FileSystemDisk::single_file(&data_path).expect("open device");
    let store = Arc::new(FasterKv::<u64, u64, _>::new(test_store_config(), device));

    let mut s = store.start_session().expect("start_session");
    assert_eq!(s.upsert(1, 10), Status::Ok);
    assert_eq!(s.delete(&1), Status::Ok);

    // A tombstone must make the key logically absent. Older versions of the key
    // must not cause conditional_insert to abort.
    assert_eq!(s.conditional_insert(1, 20), Status::Ok);
    assert_eq!(s.read(&1).expect("read"), Some(20));
}
