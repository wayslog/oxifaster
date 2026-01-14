//! Custom types example
//!
//! Demonstrates how to persist custom types using serde+bincode opt-in wrappers.
//!
//! Run: `cargo run --example custom_types`

use std::sync::Arc;

use oxifaster::codec::Bincode;
use oxifaster::device::{FileSystemDisk, NullDisk, StorageDevice};
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

// Custom key type: user id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UserId {
    prefix: char,
    number: u32,
}

impl UserId {
    fn new(prefix: char, number: u32) -> Self {
        Self { prefix, number }
    }
}

// Custom value type: user profile.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct UserInfo {
    name: String,
    age: u8,
    score: f32,
}

impl UserInfo {
    fn new(name: &str, age: u8, score: f32) -> Self {
        Self {
            name: name.to_string(),
            age,
            score,
        }
    }
}

fn run_with_device<D: StorageDevice>(device_name: &str, device: D) {
    println!("=== oxifaster custom types example ({device_name}) ===\n");

    let config = FasterKvConfig {
        table_size: 1 << 12,
        log_memory_size: 1 << 22,
        page_size_bits: 16,
        mutable_fraction: 0.9,
    };
    let store = Arc::new(FasterKv::<Bincode<UserId>, Bincode<UserInfo>, _>::new(
        config, device,
    ));

    let mut session = store.start_session().unwrap();

    let users = vec![
        (UserId::new('A', 1001), UserInfo::new("Alice", 25, 85.5)),
        (UserId::new('A', 1002), UserInfo::new("Bob", 30, 92.0)),
        (UserId::new('B', 2001), UserInfo::new("Carol", 28, 78.5)),
        (UserId::new('B', 2002), UserInfo::new("Dave", 35, 88.0)),
        (UserId::new('C', 3001), UserInfo::new("Eve", 22, 95.5)),
    ];

    println!("--- Upsert users ---");
    for (id, info) in &users {
        let status = session.upsert(Bincode(id.clone()), Bincode(info.clone()));
        if status == Status::Ok {
            println!(
                "  Upsert: {:?} -> {} (age: {}, score: {})",
                id, info.name, info.age, info.score
            );
        }
    }

    println!("\n--- Read users ---");
    let query_ids = vec![
        UserId::new('A', 1001),
        UserId::new('B', 2001),
        UserId::new('C', 3001),
        UserId::new('D', 4001), // not found
    ];

    for id in &query_ids {
        match session.read(&Bincode(id.clone())) {
            Ok(Some(Bincode(info))) => {
                println!(
                    "  Found: {:?} -> {} (age: {}, score: {})",
                    id, info.name, info.age, info.score
                );
            }
            Ok(None) => {
                println!("  Not found: {id:?}");
            }
            Err(e) => {
                println!("  Error: {id:?} - {e:?}");
            }
        }
    }

    println!("\n--- Update user ---");
    let update_id = UserId::new('A', 1001);
    let updated_info = UserInfo::new("Alice (updated)", 26, 90.0);

    if session.upsert(Bincode(update_id.clone()), Bincode(updated_info.clone())) == Status::Ok {
        println!("  Update ok: {update_id:?}");

        if let Ok(Some(Bincode(info))) = session.read(&Bincode(update_id.clone())) {
            println!(
                "  Verify: {} (age: {}, score: {})",
                info.name, info.age, info.score
            );
        }
    }

    println!("\n--- Delete user ---");
    let delete_id = UserId::new('B', 2002);

    if session.delete(&Bincode(delete_id.clone())) == Status::Ok {
        println!("  Delete ok: {delete_id:?}");

        match session.read(&Bincode(delete_id.clone())) {
            Ok(None) => println!("  Verify: user deleted"),
            Ok(Some(_)) => println!("  Verify: user still exists (unexpected)"),
            Err(e) => println!("  Verify error: {e:?}"),
        }
    }

    println!("\n--- Index stats ---");
    let stats = store.index_stats();
    println!("  Used entries: {}", stats.used_entries);
    println!("  Load factor: {:.2}%", stats.load_factor * 100.0);

    println!("\n=== Done ===");
}

fn main() {
    run_with_device("NullDisk (in-memory)", NullDisk::new());

    let dir = tempdir().expect("failed to create temp dir");
    let data_path = dir.path().join("oxifaster_custom_types.dat");
    let fs_device = FileSystemDisk::single_file(&data_path).expect("failed to create data file");
    run_with_device(
        &format!("FileSystemDisk (file-backed: {})", data_path.display()),
        fs_device,
    );
}
