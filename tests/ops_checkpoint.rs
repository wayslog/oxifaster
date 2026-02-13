use std::fs;

use oxifaster::ops::{CheckpointSelfCheckOptions, checkpoint_self_check};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn test_checkpoint_self_check_dry_run() {
    let dir = tempdir().unwrap();
    let token = Uuid::new_v4();
    let cp_dir = dir.path().join(token.to_string());
    fs::create_dir_all(&cp_dir).unwrap();

    let report = checkpoint_self_check(
        dir.path(),
        CheckpointSelfCheckOptions {
            repair: true,
            dry_run: true,
            remove_invalid: true,
        },
    )
    .unwrap();

    assert_eq!(report.total, 1);
    assert_eq!(report.valid, 0);
    assert_eq!(report.invalid, 1);
    assert_eq!(report.removed.len(), 0);
    assert_eq!(report.planned_removals.len(), 1);
    assert!(cp_dir.exists());
}

#[test]
fn test_checkpoint_self_check_repair_removes_invalid() {
    let dir = tempdir().unwrap();
    let token = Uuid::new_v4();
    let cp_dir = dir.path().join(token.to_string());
    fs::create_dir_all(&cp_dir).unwrap();

    let report = checkpoint_self_check(
        dir.path(),
        CheckpointSelfCheckOptions {
            repair: true,
            dry_run: false,
            remove_invalid: true,
        },
    )
    .unwrap();

    assert_eq!(report.total, 1);
    assert_eq!(report.valid, 0);
    assert_eq!(report.invalid, 1);
    assert_eq!(report.removed.len(), 1);
    assert!(report.repaired);
    assert!(!cp_dir.exists());
}
