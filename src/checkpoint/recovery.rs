//! Recovery functionality for FASTER
//!
//! This module provides recovery from checkpoints.
//!
//! Recovery process:
//! 1. Find valid checkpoint(s) in the checkpoint directory
//! 2. Load index and log metadata from the checkpoint
//! 3. Recover the hash index from the checkpoint data
//! 4. Recover the hybrid log from the checkpoint snapshot
//! 5. For incremental checkpoints, apply delta logs
//! 6. Restore session states for continuation

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::address::Address;
use crate::checkpoint::{
    delta_log_path, delta_metadata_path, CheckpointToken, DeltaLogMetadata, IndexMetadata,
    LogMetadata, SessionState,
};
use crate::status::Status;

/// Status of a recovery operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryStatus {
    /// Recovery not started
    #[default]
    NotStarted,
    /// Recovery in progress - loading metadata
    LoadingMetadata,
    /// Recovery in progress - recovering index
    RecoveringIndex,
    /// Recovery in progress - recovering log
    RecoveringLog,
    /// Recovery in progress - restoring sessions
    RestoringSessions,
    /// Recovery completed successfully
    Completed,
    /// Recovery failed
    Failed,
}

impl RecoveryStatus {
    /// Check if recovery is in progress
    pub fn is_in_progress(&self) -> bool {
        matches!(
            self,
            RecoveryStatus::LoadingMetadata
                | RecoveryStatus::RecoveringIndex
                | RecoveryStatus::RecoveringLog
                | RecoveryStatus::RestoringSessions
        )
    }
}

/// Information about a checkpoint for recovery
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// Checkpoint token
    pub token: CheckpointToken,
    /// Index metadata
    pub index_metadata: Option<IndexMetadata>,
    /// Log metadata
    pub log_metadata: Option<LogMetadata>,
    /// Checkpoint directory
    pub directory: PathBuf,
    /// Creation time (from metadata file mtime)
    pub created_at: Option<std::time::SystemTime>,
    /// Delta log metadata (for incremental checkpoints)
    pub delta_metadata: Option<DeltaLogMetadata>,
    /// Base snapshot checkpoint directory (for incremental checkpoints)
    pub base_checkpoint_dir: Option<PathBuf>,
}

impl CheckpointInfo {
    /// Create a new checkpoint info
    pub fn new(token: CheckpointToken, directory: PathBuf) -> Self {
        Self {
            token,
            index_metadata: None,
            log_metadata: None,
            directory,
            created_at: None,
            delta_metadata: None,
            base_checkpoint_dir: None,
        }
    }

    /// Load checkpoint metadata from disk
    pub fn load(&mut self) -> io::Result<()> {
        // Load index metadata
        let index_path = self.directory.join("index.meta");
        if index_path.exists() {
            self.index_metadata = Some(IndexMetadata::read_from_file(&index_path)?);

            // Get file modification time
            if let Ok(metadata) = fs::metadata(&index_path) {
                self.created_at = metadata.modified().ok();
            }
        }

        // Load log metadata
        let log_path = self.directory.join("log.meta");
        if log_path.exists() {
            self.log_metadata = Some(LogMetadata::read_from_file(&log_path)?);
        }

        // Load delta metadata if this is an incremental checkpoint
        let delta_path = delta_metadata_path(&self.directory);
        if delta_path.exists() {
            self.delta_metadata = Some(DeltaLogMetadata::read_from_file(&delta_path)?);
        }

        Ok(())
    }

    /// Check if the checkpoint is valid
    pub fn is_valid(&self) -> bool {
        self.index_metadata.is_some() && self.log_metadata.is_some()
    }

    /// Check if this is an incremental checkpoint
    pub fn is_incremental(&self) -> bool {
        self.log_metadata
            .as_ref()
            .is_some_and(|log_meta| log_meta.is_incremental)
    }

    /// Get the base snapshot token for incremental checkpoints
    pub fn base_snapshot_token(&self) -> Option<CheckpointToken> {
        self.log_metadata
            .as_ref()
            .and_then(|m| m.prev_snapshot_token)
    }

    /// Check if all checkpoint files exist
    pub fn files_exist(&self) -> bool {
        let has_basic = self.directory.join("index.meta").exists()
            && self.directory.join("index.dat").exists()
            && self.directory.join("log.meta").exists();

        if self.is_incremental() {
            // Incremental checkpoints need delta log instead of full snapshot
            has_basic && delta_log_path(&self.directory, 0).exists()
        } else {
            // Full checkpoints need log snapshot
            has_basic && self.directory.join("log.snapshot").exists()
        }
    }

    /// Get the checkpoint version
    pub fn version(&self) -> Option<u32> {
        self.log_metadata.as_ref().map(|m| m.version)
    }

    /// Get session states from the checkpoint
    pub fn session_states(&self) -> &[SessionState] {
        match self.log_metadata.as_ref() {
            Some(log_meta) => log_meta.session_states.as_slice(),
            None => &[],
        }
    }

    /// Resolve the base checkpoint directory for incremental checkpoints
    ///
    /// # Arguments
    /// * `checkpoint_base_dir` - Base directory containing all checkpoints
    pub fn resolve_base_checkpoint(&mut self, checkpoint_base_dir: &Path) -> io::Result<()> {
        if !self.is_incremental() {
            return Ok(());
        }

        let base_token = self.base_snapshot_token().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Incremental checkpoint missing base snapshot token",
            )
        })?;

        let base_dir = checkpoint_base_dir.join(base_token.to_string());
        if !base_dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Base checkpoint directory not found: {}",
                    base_dir.display()
                ),
            ));
        }

        self.base_checkpoint_dir = Some(base_dir);
        Ok(())
    }
}

/// Recovery state and operations
#[derive(Debug)]
pub struct RecoveryState {
    /// Current recovery status
    status: RecoveryStatus,
    /// Checkpoint being recovered
    checkpoint_info: Option<CheckpointInfo>,
    /// Recovered begin address
    begin_address: Address,
    /// Recovered head address  
    head_address: Address,
    /// Recovered tail address
    tail_address: Address,
    /// Recovered version
    version: u32,
    /// Error message if recovery failed
    error_message: Option<String>,
}

impl RecoveryState {
    /// Create a new recovery state
    pub fn new() -> Self {
        Self {
            status: RecoveryStatus::NotStarted,
            checkpoint_info: None,
            begin_address: Address::INVALID,
            head_address: Address::INVALID,
            tail_address: Address::INVALID,
            version: 0,
            error_message: None,
        }
    }

    /// Get the current recovery status
    pub fn status(&self) -> RecoveryStatus {
        self.status
    }

    /// Get the recovered begin address
    pub fn begin_address(&self) -> Address {
        self.begin_address
    }

    /// Get the recovered head address
    pub fn head_address(&self) -> Address {
        self.head_address
    }

    /// Get the recovered tail address
    pub fn tail_address(&self) -> Address {
        self.tail_address
    }

    /// Get the recovered version
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Get the checkpoint info
    pub fn checkpoint_info(&self) -> Option<&CheckpointInfo> {
        self.checkpoint_info.as_ref()
    }

    /// Get the error message if recovery failed
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Start recovery from a checkpoint
    pub fn start_recovery(&mut self, checkpoint_dir: &Path, token: CheckpointToken) -> Status {
        if self.status.is_in_progress() {
            return Status::Aborted;
        }

        self.status = RecoveryStatus::LoadingMetadata;
        self.error_message = None;

        // Load checkpoint info
        let cp_dir = checkpoint_dir.join(token.to_string());
        let mut info = CheckpointInfo::new(token, cp_dir);

        if let Err(e) = info.load() {
            self.status = RecoveryStatus::Failed;
            self.error_message = Some(format!("Failed to load checkpoint metadata: {e}"));
            return Status::IoError;
        }

        if !info.is_valid() {
            self.status = RecoveryStatus::Failed;
            self.error_message = Some("Checkpoint metadata is incomplete".to_string());
            return Status::Corruption;
        }

        if !info.files_exist() {
            self.status = RecoveryStatus::Failed;
            self.error_message = Some("Checkpoint files are missing".to_string());
            return Status::Corruption;
        }

        // Extract recovery information from log metadata
        if let Some(ref log_meta) = info.log_metadata {
            self.begin_address = log_meta.begin_address;
            self.head_address = log_meta.begin_address; // All data will be in memory after recovery
            self.tail_address = log_meta.final_address;
            self.version = log_meta.version;
        }

        self.checkpoint_info = Some(info);
        self.status = RecoveryStatus::RecoveringIndex;

        Status::Ok
    }

    /// Mark index recovery as complete and move to log recovery
    pub fn index_recovered(&mut self) -> Status {
        if self.status != RecoveryStatus::RecoveringIndex {
            return Status::Aborted;
        }
        self.status = RecoveryStatus::RecoveringLog;
        Status::Ok
    }

    /// Mark log recovery as complete and move to session restoration
    pub fn log_recovered(&mut self) -> Status {
        if self.status != RecoveryStatus::RecoveringLog {
            return Status::Aborted;
        }
        self.status = RecoveryStatus::RestoringSessions;
        Status::Ok
    }

    /// Complete recovery
    pub fn complete_recovery(&mut self) -> Status {
        if !self.status.is_in_progress() && self.status != RecoveryStatus::RestoringSessions {
            return Status::Aborted;
        }

        self.status = RecoveryStatus::Completed;
        Status::Ok
    }

    /// Mark recovery as failed
    pub fn fail_recovery(&mut self, message: &str) {
        self.status = RecoveryStatus::Failed;
        self.error_message = Some(message.to_string());
    }

    /// Reset recovery state
    pub fn reset(&mut self) {
        self.status = RecoveryStatus::NotStarted;
        self.checkpoint_info = None;
        self.begin_address = Address::INVALID;
        self.head_address = Address::INVALID;
        self.tail_address = Address::INVALID;
        self.version = 0;
        self.error_message = None;
    }
}

impl Default for RecoveryState {
    fn default() -> Self {
        Self::new()
    }
}

/// Find the latest checkpoint in a directory
///
/// Scans the base directory for valid checkpoint subdirectories
/// and returns the one with the highest version number.
pub fn find_latest_checkpoint(base_dir: &Path) -> Option<CheckpointInfo> {
    list_checkpoints(base_dir).into_iter().next()
}

/// List all checkpoints in a directory
///
/// Scans the base directory for valid checkpoint subdirectories
/// (those with UUID names that contain valid checkpoint files).
pub fn list_checkpoints(base_dir: &Path) -> Vec<CheckpointInfo> {
    let mut checkpoints = Vec::new();
    if !base_dir.is_dir() {
        return checkpoints;
    }

    let entries = match fs::read_dir(base_dir) {
        Ok(entries) => entries,
        Err(_) => return checkpoints,
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        // Try to parse directory name as UUID
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if let Ok(token) = Uuid::parse_str(name) {
                let mut info = CheckpointInfo::new(token, path);

                // Try to load metadata (ignore errors - invalid checkpoints are filtered)
                if info.load().is_ok() && info.is_valid() {
                    checkpoints.push(info);
                }
            }
        }
    }

    // Sort by version (newest first)
    checkpoints.sort_by_key(|b| std::cmp::Reverse(b.version().unwrap_or(0)));

    checkpoints
}

fn validate_required_files(checkpoint_dir: &Path, required_files: &[&str]) -> io::Result<()> {
    for file in required_files {
        let path = checkpoint_dir.join(file);
        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Missing checkpoint file: {file}"),
            ));
        }
    }

    Ok(())
}

/// Validate that a checkpoint directory contains all required files
pub fn validate_checkpoint(checkpoint_dir: &Path) -> io::Result<()> {
    validate_required_files(
        checkpoint_dir,
        &["index.meta", "index.dat", "log.meta", "log.snapshot"],
    )
}

/// Validate that a checkpoint directory contains all required files for an incremental checkpoint
pub fn validate_incremental_checkpoint(checkpoint_dir: &Path) -> io::Result<()> {
    validate_required_files(checkpoint_dir, &["index.meta", "index.dat", "log.meta"])?;

    // Check for delta log
    let delta_path = delta_log_path(checkpoint_dir, 0);
    if !delta_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Missing delta log file",
        ));
    }

    // Check for delta metadata
    let delta_meta_path = delta_metadata_path(checkpoint_dir);
    if !delta_meta_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Missing delta metadata file",
        ));
    }

    Ok(())
}

/// Build the complete incremental checkpoint chain for recovery
///
/// Given an incremental checkpoint, this function finds all checkpoints in the chain
/// back to the base snapshot.
///
/// # Arguments
/// * `base_dir` - Base directory containing all checkpoints
/// * `target_token` - The target checkpoint token to recover to
///
/// # Returns
/// Vector of CheckpointInfo in order from base snapshot to target (base first)
pub fn build_incremental_chain(
    base_dir: &Path,
    target_token: CheckpointToken,
) -> io::Result<Vec<CheckpointInfo>> {
    let mut chain = Vec::new();
    let mut current_token = target_token;

    loop {
        let cp_dir = base_dir.join(current_token.to_string());
        let mut info = CheckpointInfo::new(current_token, cp_dir);
        info.load()?;

        if !info.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid checkpoint in chain: {current_token}"),
            ));
        }

        if info.is_incremental() {
            // Add to chain and continue to base
            let base_token = info.base_snapshot_token().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Incremental checkpoint missing base token",
                )
            })?;
            chain.push(info);
            current_token = base_token;
        } else {
            // Found the base snapshot
            chain.push(info);
            break;
        }
    }

    // Reverse to get base-first order
    chain.reverse();
    Ok(chain)
}

/// Get information about the incremental checkpoint chain
#[derive(Debug, Clone)]
pub struct IncrementalRecoveryInfo {
    /// The base snapshot checkpoint
    pub base_checkpoint: CheckpointInfo,
    /// Incremental checkpoints in order (after base)
    pub incrementals: Vec<CheckpointInfo>,
    /// Total number of delta log entries to apply
    pub total_delta_entries: u64,
}

impl IncrementalRecoveryInfo {
    /// Build recovery info from a checkpoint chain
    pub fn from_chain(mut chain: Vec<CheckpointInfo>) -> io::Result<Self> {
        if chain.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Empty checkpoint chain",
            ));
        }

        // Take the first element as base checkpoint
        let base_checkpoint = chain.remove(0);
        let incrementals = chain;

        // Calculate total delta entries
        let total_delta_entries = incrementals
            .iter()
            .filter_map(|c| c.delta_metadata.as_ref())
            .map(|m| m.num_entries)
            .sum();

        Ok(Self {
            base_checkpoint,
            incrementals,
            total_delta_entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_status() {
        assert_eq!(RecoveryStatus::default(), RecoveryStatus::NotStarted);
        assert!(!RecoveryStatus::NotStarted.is_in_progress());
        assert!(RecoveryStatus::LoadingMetadata.is_in_progress());
        assert!(RecoveryStatus::RecoveringIndex.is_in_progress());
        assert!(RecoveryStatus::RecoveringLog.is_in_progress());
        assert!(RecoveryStatus::RestoringSessions.is_in_progress());
        assert!(!RecoveryStatus::Completed.is_in_progress());
        assert!(!RecoveryStatus::Failed.is_in_progress());
    }

    #[test]
    fn test_recovery_state() {
        let state = RecoveryState::new();
        assert_eq!(state.status(), RecoveryStatus::NotStarted);
        assert!(state.begin_address().is_invalid());
        assert!(state.head_address().is_invalid());
        assert!(state.tail_address().is_invalid());
        assert_eq!(state.version(), 0);
        assert!(state.error_message().is_none());
    }

    #[test]
    fn test_checkpoint_info() {
        let token = Uuid::new_v4();
        let info = CheckpointInfo::new(token, PathBuf::from("/tmp/checkpoint"));

        assert_eq!(info.token, token);
        assert!(!info.is_valid()); // No metadata loaded
        assert!(info.session_states().is_empty());
        assert!(info.version().is_none());
    }

    #[test]
    fn test_recovery_state_lifecycle() {
        let mut state = RecoveryState::new();

        // Cannot progress from NotStarted without starting
        assert_eq!(state.index_recovered(), Status::Aborted);

        // Reset works from any state
        state.reset();
        assert_eq!(state.status(), RecoveryStatus::NotStarted);
    }

    #[test]
    fn test_recovery_state_failure() {
        let mut state = RecoveryState::new();
        state.fail_recovery("Test error");

        assert_eq!(state.status(), RecoveryStatus::Failed);
        assert_eq!(state.error_message(), Some("Test error"));
    }

    #[test]
    fn test_list_checkpoints_empty_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let checkpoints = list_checkpoints(temp_dir.path());
        assert!(checkpoints.is_empty());
    }

    #[test]
    fn test_list_checkpoints_nonexistent_dir() {
        let checkpoints = list_checkpoints(Path::new("/nonexistent/path"));
        assert!(checkpoints.is_empty());
    }

    #[test]
    fn test_find_latest_checkpoint_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let latest = find_latest_checkpoint(temp_dir.path());
        assert!(latest.is_none());
    }

    #[test]
    fn test_validate_checkpoint_missing_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = validate_checkpoint(temp_dir.path());
        assert!(result.is_err());
    }
}
