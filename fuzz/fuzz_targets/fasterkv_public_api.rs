#![no_main]

use std::collections::HashMap;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

const OP_BYTES: usize = 17;
const MAX_OPS: usize = 2048;

fn build_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
    let config = FasterKvConfig::new(1 << 12, 1 << 24);
    Arc::new(FasterKv::new(config, NullDisk::new()).expect("fuzz: failed to create store"))
}

fuzz_target!(|data: &[u8]| {
    let store = build_store();
    let mut session = store
        .start_session()
        .expect("fuzz: failed to start session");
    let mut model = HashMap::<u64, u64>::new();

    for chunk in data.chunks_exact(OP_BYTES).take(MAX_OPS) {
        let op = chunk[0] % 5;
        let key = u64::from_le_bytes(chunk[1..9].try_into().expect("fixed key bytes"));
        let value = u64::from_le_bytes(chunk[9..17].try_into().expect("fixed value bytes"));

        match op {
            0 => {
                let status = session.upsert(key, value);
                assert!(status.is_ok(), "upsert status: {}", status);
                model.insert(key, value);
            }
            1 => {
                let status = session.delete(&key);
                assert!(
                    matches!(status, Status::Ok | Status::NotFound),
                    "delete status: {}",
                    status
                );
                model.remove(&key);
            }
            2 => {
                let actual = session.read(&key).expect("read should not fail");
                let expected = model.get(&key).copied();
                assert_eq!(actual, expected, "read mismatch for key={key}");
            }
            3 => {
                let status = session.conditional_insert(key, value);
                if model.contains_key(&key) {
                    assert_eq!(status, Status::Aborted, "expected abort for existing key");
                } else {
                    assert_eq!(status, Status::Ok, "expected insert for missing key");
                    model.insert(key, value);
                }
            }
            _ => {
                session.refresh();
            }
        }
    }
});
