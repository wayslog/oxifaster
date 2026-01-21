//! 测试 ops 模块功能

use oxifaster::ops::{
    CheckpointIssue, CheckpointSelfCheckOptions, CheckpointSelfCheckReport,
};
use uuid::Uuid;

#[test]
fn test_checkpoint_self_check_options_default() {
    let opts = CheckpointSelfCheckOptions::default();

    assert!(!opts.repair);
    assert!(!opts.dry_run);
    assert!(opts.remove_invalid);
}

#[test]
fn test_checkpoint_self_check_options_custom() {
    let opts = CheckpointSelfCheckOptions {
        repair: true,
        dry_run: true,
        remove_invalid: false,
    };

    assert!(opts.repair);
    assert!(opts.dry_run);
    assert!(!opts.remove_invalid);
}

#[test]
fn test_checkpoint_issue() {
    let token = Uuid::new_v4();
    let issue = CheckpointIssue {
        token,
        reason: "Test error".to_string(),
    };

    assert_eq!(issue.token, token);
    assert_eq!(issue.reason, "Test error");
}

#[test]
fn test_checkpoint_self_check_report_new() {
    let report = oxifaster::ops::CheckpointSelfCheckReport {
        total: 0,
        valid: 0,
        invalid: 0,
        removed: Vec::new(),
        planned_removals: Vec::new(),
        issues: Vec::new(),
        dry_run: false,
        repaired: false,
    };

    assert_eq!(report.total, 0);
    assert_eq!(report.valid, 0);
    assert_eq!(report.invalid, 0);
    assert!(report.removed.is_empty());
    assert!(report.planned_removals.is_empty());
    assert!(report.issues.is_empty());
    assert!(!report.dry_run);
    assert!(!report.repaired);
}

#[test]
fn test_checkpoint_self_check_report_with_data() {
    let token1 = Uuid::new_v4();
    let token2 = Uuid::new_v4();

    let report = CheckpointSelfCheckReport {
        total: 10,
        valid: 8,
        invalid: 2,
        removed: vec![token1],
        planned_removals: vec![token2],
        issues: vec![CheckpointIssue {
            token: token1,
            reason: "Invalid metadata".to_string(),
        }],
        dry_run: false,
        repaired: true,
    };

    assert_eq!(report.total, 10);
    assert_eq!(report.valid, 8);
    assert_eq!(report.invalid, 2);
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0], token1);
    assert_eq!(report.planned_removals.len(), 1);
    assert_eq!(report.planned_removals[0], token2);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(report.issues[0].token, token1);
    assert!(!report.dry_run);
    assert!(report.repaired);
}

#[test]
fn test_checkpoint_self_check_options_clone() {
    let opts1 = CheckpointSelfCheckOptions {
        repair: true,
        dry_run: false,
        remove_invalid: true,
    };

    let opts2 = opts1.clone();

    assert_eq!(opts1.repair, opts2.repair);
    assert_eq!(opts1.dry_run, opts2.dry_run);
    assert_eq!(opts1.remove_invalid, opts2.remove_invalid);
}

#[test]
fn test_checkpoint_issue_clone() {
    let token = Uuid::new_v4();
    let issue1 = CheckpointIssue {
        token,
        reason: "Test error".to_string(),
    };

    let issue2 = issue1.clone();

    assert_eq!(issue1.token, issue2.token);
    assert_eq!(issue1.reason, issue2.reason);
}

#[test]
fn test_checkpoint_self_check_report_clone() {
    let token = Uuid::new_v4();
    let report1 = CheckpointSelfCheckReport {
        total: 5,
        valid: 4,
        invalid: 1,
        removed: vec![token],
        planned_removals: Vec::new(),
        issues: Vec::new(),
        dry_run: false,
        repaired: true,
    };

    let report2 = report1.clone();

    assert_eq!(report1.total, report2.total);
    assert_eq!(report1.valid, report2.valid);
    assert_eq!(report1.invalid, report2.invalid);
    assert_eq!(report1.removed, report2.removed);
    assert_eq!(report1.dry_run, report2.dry_run);
    assert_eq!(report1.repaired, report2.repaired);
}

#[test]
fn test_checkpoint_self_check_report_empty() {
    let report = CheckpointSelfCheckReport {
        total: 0,
        valid: 0,
        invalid: 0,
        removed: Vec::new(),
        planned_removals: Vec::new(),
        issues: Vec::new(),
        dry_run: true,
        repaired: false,
    };

    assert!(report.removed.is_empty());
    assert!(report.planned_removals.is_empty());
    assert!(report.issues.is_empty());
    assert!(report.dry_run);
    assert!(!report.repaired);
}

#[test]
fn test_checkpoint_self_check_options_repair_disabled() {
    let opts = CheckpointSelfCheckOptions {
        repair: false,
        dry_run: false,
        remove_invalid: true,
    };

    assert!(!opts.repair);
    assert!(opts.remove_invalid);
}

#[test]
fn test_checkpoint_self_check_options_dry_run() {
    let opts = CheckpointSelfCheckOptions {
        repair: true,
        dry_run: true,
        remove_invalid: true,
    };

    assert!(opts.repair);
    assert!(opts.dry_run);
}

#[test]
fn test_checkpoint_issue_multiple() {
    let token1 = Uuid::new_v4();
    let token2 = Uuid::new_v4();

    let issue1 = CheckpointIssue {
        token: token1,
        reason: "Error 1".to_string(),
    };

    let issue2 = CheckpointIssue {
        token: token2,
        reason: "Error 2".to_string(),
    };

    let issues = vec![issue1, issue2];

    assert_eq!(issues.len(), 2);
    assert_eq!(issues[0].token, token1);
    assert_eq!(issues[1].token, token2);
}

#[test]
fn test_checkpoint_self_check_report_statistics() {
    let report = CheckpointSelfCheckReport {
        total: 100,
        valid: 95,
        invalid: 5,
        removed: vec![Uuid::new_v4(); 3],
        planned_removals: vec![Uuid::new_v4(); 2],
        issues: vec![
            CheckpointIssue {
                token: Uuid::new_v4(),
                reason: "Issue 1".to_string(),
            },
            CheckpointIssue {
                token: Uuid::new_v4(),
                reason: "Issue 2".to_string(),
            },
        ],
        dry_run: false,
        repaired: true,
    };

    let success_rate = report.valid as f64 / report.total as f64;
    assert!(success_rate >= 0.9);

    assert_eq!(report.removed.len(), 3);
    assert_eq!(report.planned_removals.len(), 2);
    assert_eq!(report.issues.len(), 2);
}
