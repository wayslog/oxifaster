//! Operational helpers for self-check and recovery.

use std::fs;
use std::io;
use std::path::Path;

use uuid::Uuid;

use crate::cache::ReadCacheConfig;
use crate::checkpoint::{
    CheckpointToken, LogMetadata, build_incremental_chain, find_latest_checkpoint,
    validate_checkpoint, validate_incremental_checkpoint,
};
use crate::codec::{PersistKey, PersistValue};
use crate::compaction::CompactionConfig;
use crate::device::StorageDevice;
use crate::store::{FasterKv, FasterKvConfig};

/// Options for checkpoint self-check operations.
#[derive(Debug, Clone)]
pub struct CheckpointSelfCheckOptions {
    /// Whether to repair issues by removing invalid checkpoint directories.
    pub repair: bool,
    /// Whether to run in dry-run mode (report only, no writes).
    pub dry_run: bool,
    /// Whether to remove invalid checkpoints during repair.
    pub remove_invalid: bool,
}

impl Default for CheckpointSelfCheckOptions {
    fn default() -> Self {
        Self {
            repair: false,
            dry_run: false,
            remove_invalid: true,
        }
    }
}

/// Self-check issue for a checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointIssue {
    /// Checkpoint token.
    pub token: CheckpointToken,
    /// Issue description.
    pub reason: String,
}

/// Report returned by checkpoint self-check.
#[derive(Debug, Clone)]
pub struct CheckpointSelfCheckReport {
    /// Total checkpoint directories scanned.
    pub total: usize,
    /// Valid checkpoints detected.
    pub valid: usize,
    /// Invalid checkpoints detected.
    pub invalid: usize,
    /// Tokens removed during repair.
    pub removed: Vec<CheckpointToken>,
    /// Tokens that would be removed in dry-run mode.
    pub planned_removals: Vec<CheckpointToken>,
    /// Issues detected during validation.
    pub issues: Vec<CheckpointIssue>,
    /// Whether the run was dry-run.
    pub dry_run: bool,
    /// Whether any repairs were applied.
    pub repaired: bool,
}

impl CheckpointSelfCheckReport {
    fn new(dry_run: bool) -> Self {
        Self {
            total: 0,
            valid: 0,
            invalid: 0,
            removed: Vec::new(),
            planned_removals: Vec::new(),
            issues: Vec::new(),
            dry_run,
            repaired: false,
        }
    }
}

/// Run a self-check on checkpoints under `base_dir`.
pub fn checkpoint_self_check(
    base_dir: &Path,
    options: CheckpointSelfCheckOptions,
) -> io::Result<CheckpointSelfCheckReport> {
    let mut report = CheckpointSelfCheckReport::new(options.dry_run);

    let entries = match fs::read_dir(base_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(report),
        Err(err) => return Err(err),
    };

    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let token = match Uuid::parse_str(&name) {
            Ok(token) => token,
            Err(_) => continue,
        };
        report.total += 1;

        let cp_dir = entry.path();
        if let Err(err) = validate_checkpoint_dir(base_dir, &cp_dir, token) {
            report.invalid += 1;
            report.issues.push(CheckpointIssue {
                token,
                reason: err.to_string(),
            });
            if tracing::enabled!(tracing::Level::WARN) {
                tracing::warn!(token = %token, error = %err, "checkpoint self-check failed");
            }

            if options.repair && options.remove_invalid {
                if options.dry_run {
                    report.planned_removals.push(token);
                } else {
                    fs::remove_dir_all(&cp_dir)?;
                    report.removed.push(token);
                    report.repaired = true;
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(token = %token, "checkpoint self-check removed invalid checkpoint");
                    }
                }
            }
            continue;
        }

        report.valid += 1;
    }

    Ok(report)
}

/// Recover the latest checkpoint under `checkpoint_dir`.
pub fn recover_latest<K, V, D>(
    checkpoint_dir: &Path,
    config: FasterKvConfig,
    device: D,
) -> io::Result<(CheckpointToken, FasterKv<K, V, D>)>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    recover_latest_with_full_config(
        checkpoint_dir,
        config,
        device,
        CompactionConfig::default(),
        None,
    )
}

/// Recover the latest checkpoint using full configuration options.
pub fn recover_latest_with_full_config<K, V, D>(
    checkpoint_dir: &Path,
    config: FasterKvConfig,
    device: D,
    compaction_config: CompactionConfig,
    cache_config: Option<ReadCacheConfig>,
) -> io::Result<(CheckpointToken, FasterKv<K, V, D>)>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    let info = find_latest_checkpoint(checkpoint_dir).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no valid checkpoint found for recovery",
        )
    })?;
    let token = info.token;
    let store = FasterKv::recover_with_full_config(
        checkpoint_dir,
        token,
        config,
        device,
        compaction_config,
        cache_config,
    )?;
    Ok((token, store))
}

/// Run checkpoint self-check then recover the latest checkpoint.
pub fn recover_latest_with_self_check<K, V, D>(
    checkpoint_dir: &Path,
    config: FasterKvConfig,
    device: D,
    compaction_config: CompactionConfig,
    cache_config: Option<ReadCacheConfig>,
    self_check: CheckpointSelfCheckOptions,
) -> io::Result<(
    CheckpointToken,
    FasterKv<K, V, D>,
    CheckpointSelfCheckReport,
)>
where
    K: PersistKey,
    V: PersistValue,
    D: StorageDevice,
{
    let report = checkpoint_self_check(checkpoint_dir, self_check)?;
    let (token, store) = recover_latest_with_full_config(
        checkpoint_dir,
        config,
        device,
        compaction_config,
        cache_config,
    )?;
    Ok((token, store, report))
}

fn validate_checkpoint_dir(
    base_dir: &Path,
    checkpoint_dir: &Path,
    token: CheckpointToken,
) -> io::Result<()> {
    let log_meta_path = checkpoint_dir.join("log.meta");
    if !log_meta_path.exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "missing log.meta"));
    }

    let log_meta = LogMetadata::read_from_file(&log_meta_path)?;
    if log_meta.is_incremental {
        validate_incremental_checkpoint(checkpoint_dir)?;
        build_incremental_chain(base_dir, token)?;
    } else {
        validate_checkpoint(checkpoint_dir)?;
    }

    Ok(())
}
