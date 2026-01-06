//! Serialization support for checkpoint metadata
//!
//! This module provides serialization and deserialization for checkpoint
//! metadata structures, enabling persistence to disk.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::address::Address;
use crate::checkpoint::CheckpointToken;

/// Serializable version of IndexMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableIndexMetadata {
    /// Checkpoint token (UUID as string)
    pub token: String,
    /// Index version
    #[serde(default)]
    pub version: u32,
    /// Hash table size
    pub table_size: u64,
    /// Number of bytes in the hash table
    #[serde(default)]
    pub num_ht_bytes: u64,
    /// Number of bytes in overflow buckets
    #[serde(default)]
    pub num_ofb_bytes: u64,
    /// Number of overflow buckets
    pub num_buckets: u64,
    /// Number of entries
    pub num_entries: u64,
    /// Log begin address (as u64)
    #[serde(default)]
    pub log_begin_address: u64,
    /// Checkpoint start address (as u64)
    #[serde(default)]
    pub checkpoint_start_address: u64,
}

impl SerializableIndexMetadata {
    /// Create from IndexMetadata
    pub fn from_metadata(meta: &super::IndexMetadata) -> Self {
        Self {
            token: meta.token.to_string(),
            version: meta.version,
            table_size: meta.table_size,
            num_ht_bytes: meta.num_ht_bytes,
            num_ofb_bytes: meta.num_ofb_bytes,
            num_buckets: meta.num_buckets,
            num_entries: meta.num_entries,
            log_begin_address: meta.log_begin_address.control(),
            checkpoint_start_address: meta.checkpoint_start_address.control(),
        }
    }

    /// Convert to IndexMetadata
    pub fn to_metadata(&self) -> io::Result<super::IndexMetadata> {
        let token = self.token.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UUID: {e}"))
        })?;

        Ok(super::IndexMetadata {
            token,
            version: self.version,
            table_size: self.table_size,
            num_ht_bytes: self.num_ht_bytes,
            num_ofb_bytes: self.num_ofb_bytes,
            num_buckets: self.num_buckets,
            num_entries: self.num_entries,
            log_begin_address: Address::from_control(self.log_begin_address),
            checkpoint_start_address: Address::from_control(self.checkpoint_start_address),
        })
    }
}

/// Serializable version of SessionState
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableSessionState {
    /// Session GUID (as string)
    pub guid: String,
    /// Monotonic serial number
    pub serial_num: u64,
}

impl SerializableSessionState {
    /// Create from SessionState
    pub fn from_state(state: &super::SessionState) -> Self {
        Self {
            guid: state.guid.to_string(),
            serial_num: state.serial_num,
        }
    }

    /// Convert to SessionState
    pub fn to_state(&self) -> io::Result<super::SessionState> {
        let guid = self.guid.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UUID: {e}"))
        })?;
        Ok(super::SessionState::new(guid, self.serial_num))
    }
}

/// Serializable version of LogMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableLogMetadata {
    /// Checkpoint token (UUID as string)
    pub token: String,
    /// Whether to use snapshot file
    #[serde(default)]
    pub use_snapshot_file: bool,
    /// Version at checkpoint
    pub version: u32,
    /// Number of active threads
    #[serde(default)]
    pub num_threads: u32,
    /// Begin address (as u64)
    pub begin_address: u64,
    /// Final address at checkpoint (as u64)
    pub final_address: u64,
    /// Flushed until address (as u64)
    pub flushed_until_address: u64,
    /// Object log exists
    pub use_object_log: bool,
    /// Session states
    #[serde(default)]
    pub session_states: Vec<SerializableSessionState>,

    // === Incremental checkpoint fields ===
    /// Delta log tail address (-1 if not incremental)
    #[serde(default = "default_delta_tail_address")]
    pub delta_tail_address: i64,
    /// Base snapshot token for incremental checkpoints
    #[serde(default)]
    pub prev_snapshot_token: Option<String>,
    /// Whether this is an incremental checkpoint
    #[serde(default)]
    pub is_incremental: bool,
    /// Snapshot final logical address
    #[serde(default)]
    pub snapshot_final_address: u64,
    /// Start logical address when checkpoint started
    #[serde(default)]
    pub start_logical_address: u64,
    /// Head address at checkpoint
    #[serde(default)]
    pub head_address: u64,
    /// Next version number
    #[serde(default)]
    pub next_version: u32,
    /// Snapshot start flushed logical address
    #[serde(default)]
    pub snapshot_start_flushed_address: u64,
}

/// Default value for delta_tail_address
fn default_delta_tail_address() -> i64 {
    -1
}

impl SerializableLogMetadata {
    /// Create from LogMetadata
    pub fn from_metadata(meta: &super::LogMetadata) -> Self {
        Self {
            token: meta.token.to_string(),
            use_snapshot_file: meta.use_snapshot_file,
            version: meta.version,
            num_threads: meta.num_threads,
            begin_address: meta.begin_address.control(),
            final_address: meta.final_address.control(),
            flushed_until_address: meta.flushed_until_address.control(),
            use_object_log: meta.use_object_log,
            session_states: meta
                .session_states
                .iter()
                .map(SerializableSessionState::from_state)
                .collect(),
            // Incremental checkpoint fields
            delta_tail_address: meta.delta_tail_address,
            prev_snapshot_token: meta.prev_snapshot_token.map(|t| t.to_string()),
            is_incremental: meta.is_incremental,
            snapshot_final_address: meta.snapshot_final_address.control(),
            start_logical_address: meta.start_logical_address.control(),
            head_address: meta.head_address.control(),
            next_version: meta.next_version,
            snapshot_start_flushed_address: meta.snapshot_start_flushed_address.control(),
        }
    }

    /// Convert to LogMetadata
    pub fn to_metadata(&self) -> io::Result<super::LogMetadata> {
        let token = self.token.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UUID: {e}"))
        })?;

        let session_states: Result<Vec<_>, _> =
            self.session_states.iter().map(|s| s.to_state()).collect();

        let prev_snapshot_token = if let Some(ref t) = self.prev_snapshot_token {
            Some(t.parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid prev snapshot UUID: {e}"),
                )
            })?)
        } else {
            None
        };

        Ok(super::LogMetadata {
            token,
            use_snapshot_file: self.use_snapshot_file,
            version: self.version,
            num_threads: self.num_threads,
            begin_address: Address::from_control(self.begin_address),
            final_address: Address::from_control(self.final_address),
            flushed_until_address: Address::from_control(self.flushed_until_address),
            use_object_log: self.use_object_log,
            session_states: session_states?,
            // Incremental checkpoint fields
            delta_tail_address: self.delta_tail_address,
            prev_snapshot_token,
            is_incremental: self.is_incremental,
            snapshot_final_address: Address::from_control(self.snapshot_final_address),
            start_logical_address: Address::from_control(self.start_logical_address),
            head_address: Address::from_control(self.head_address),
            next_version: self.next_version,
            snapshot_start_flushed_address: Address::from_control(
                self.snapshot_start_flushed_address,
            ),
        })
    }
}

/// Serializable checkpoint info combining index and log metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableCheckpointInfo {
    /// Checkpoint type
    pub checkpoint_type: u8,
    /// Index metadata
    pub index: SerializableIndexMetadata,
    /// Log metadata
    pub log: SerializableLogMetadata,
}

// ============ IndexMetadata serialization methods ============

impl super::IndexMetadata {
    /// Serialize to JSON bytes
    pub fn serialize_json(&self) -> io::Result<Vec<u8>> {
        let serializable = SerializableIndexMetadata::from_metadata(self);
        serde_json::to_vec_pretty(&serializable)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Deserialize from JSON bytes
    pub fn deserialize_json(data: &[u8]) -> io::Result<Self> {
        let serializable: SerializableIndexMetadata = serde_json::from_slice(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        serializable.to_metadata()
    }

    /// Serialize to binary format (bincode)
    pub fn serialize_binary(&self) -> io::Result<Vec<u8>> {
        let serializable = SerializableIndexMetadata::from_metadata(self);
        bincode::serialize(&serializable).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Deserialize from binary format
    pub fn deserialize_binary(data: &[u8]) -> io::Result<Self> {
        let serializable: SerializableIndexMetadata = bincode::deserialize(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        serializable.to_metadata()
    }

    /// Write to file (JSON format)
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let data = self.serialize_json()?;
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(&data)?;
        file.flush()
    }

    /// Read from file (JSON format)
    pub fn read_from_file(path: &Path) -> io::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::deserialize_json(&data)
    }
}

// ============ LogMetadata serialization methods ============

impl super::LogMetadata {
    /// Serialize to JSON bytes
    pub fn serialize_json(&self) -> io::Result<Vec<u8>> {
        let serializable = SerializableLogMetadata::from_metadata(self);
        serde_json::to_vec_pretty(&serializable)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Deserialize from JSON bytes
    pub fn deserialize_json(data: &[u8]) -> io::Result<Self> {
        let serializable: SerializableLogMetadata = serde_json::from_slice(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        serializable.to_metadata()
    }

    /// Serialize to binary format (bincode)
    pub fn serialize_binary(&self) -> io::Result<Vec<u8>> {
        let serializable = SerializableLogMetadata::from_metadata(self);
        bincode::serialize(&serializable).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Deserialize from binary format
    pub fn deserialize_binary(data: &[u8]) -> io::Result<Self> {
        let serializable: SerializableLogMetadata = bincode::deserialize(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        serializable.to_metadata()
    }

    /// Write to file (JSON format)
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let data = self.serialize_json()?;
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(&data)?;
        file.flush()
    }

    /// Read from file (JSON format)
    pub fn read_from_file(path: &Path) -> io::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::deserialize_json(&data)
    }
}

/// Create checkpoint directory structure
pub fn create_checkpoint_directory(
    base_dir: &Path,
    token: CheckpointToken,
) -> io::Result<std::path::PathBuf> {
    let checkpoint_dir = base_dir.join(token.to_string());
    fs::create_dir_all(&checkpoint_dir)?;
    Ok(checkpoint_dir)
}

/// Get the index metadata file path
pub fn index_metadata_path(checkpoint_dir: &Path) -> std::path::PathBuf {
    checkpoint_dir.join("index.meta")
}

/// Get the index data file path
pub fn index_data_path(checkpoint_dir: &Path) -> std::path::PathBuf {
    checkpoint_dir.join("index.dat")
}

/// Get the log metadata file path
pub fn log_metadata_path(checkpoint_dir: &Path) -> std::path::PathBuf {
    checkpoint_dir.join("log.meta")
}

/// Get the log snapshot file path
pub fn log_snapshot_path(checkpoint_dir: &Path) -> std::path::PathBuf {
    checkpoint_dir.join("log.snapshot")
}

/// Get the delta log file path for a specific delta index
pub fn delta_log_path(checkpoint_dir: &Path, delta_index: u32) -> std::path::PathBuf {
    checkpoint_dir.join(format!("delta.{delta_index}.log"))
}

/// Get the delta log metadata file path
pub fn delta_metadata_path(checkpoint_dir: &Path) -> std::path::PathBuf {
    checkpoint_dir.join("delta.meta")
}

/// Get the incremental checkpoint info file path
pub fn incremental_info_path(checkpoint_dir: &Path) -> std::path::PathBuf {
    checkpoint_dir.join("incremental.info")
}

/// Delta log metadata for incremental checkpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLogMetadata {
    /// Checkpoint token
    pub token: String,
    /// Base snapshot token
    pub base_snapshot_token: String,
    /// Version at this incremental checkpoint
    pub version: u32,
    /// Delta log tail address
    pub delta_tail_address: i64,
    /// Previous snapshot final address (where the base snapshot ended)
    pub prev_snapshot_final_address: u64,
    /// Current snapshot final address (where this incremental ends)
    pub current_final_address: u64,
    /// Timestamp of this checkpoint
    pub timestamp: u64,
    /// Number of delta entries in this log
    pub num_entries: u64,
}

impl DeltaLogMetadata {
    /// Create new delta log metadata
    pub fn new(token: CheckpointToken, base_snapshot_token: CheckpointToken, version: u32) -> Self {
        Self {
            token: token.to_string(),
            base_snapshot_token: base_snapshot_token.to_string(),
            version,
            delta_tail_address: 0,
            prev_snapshot_final_address: 0,
            current_final_address: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            num_entries: 0,
        }
    }

    /// Serialize to JSON bytes
    pub fn serialize_json(&self) -> io::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Deserialize from JSON bytes
    pub fn deserialize_json(data: &[u8]) -> io::Result<Self> {
        serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Write to file (JSON format)
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let data = self.serialize_json()?;
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(&data)?;
        file.flush()
    }

    /// Read from file (JSON format)
    pub fn read_from_file(path: &Path) -> io::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::deserialize_json(&data)
    }

    /// Get the token as UUID
    pub fn get_token(&self) -> io::Result<CheckpointToken> {
        self.token.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid token UUID: {e}"),
            )
        })
    }

    /// Get the base snapshot token as UUID
    pub fn get_base_token(&self) -> io::Result<CheckpointToken> {
        self.base_snapshot_token.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid base token UUID: {e}"),
            )
        })
    }
}

/// Incremental checkpoint chain information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalCheckpointChain {
    /// Base snapshot token (the full snapshot this chain starts from)
    pub base_snapshot_token: String,
    /// List of incremental checkpoint tokens in order
    pub incremental_tokens: Vec<String>,
    /// Total number of delta entries across all incremental checkpoints
    pub total_delta_entries: u64,
    /// Whether the chain is complete (valid from base to latest)
    pub is_complete: bool,
}

impl IncrementalCheckpointChain {
    /// Create a new chain starting from a base snapshot
    pub fn new(base_snapshot_token: CheckpointToken) -> Self {
        Self {
            base_snapshot_token: base_snapshot_token.to_string(),
            incremental_tokens: Vec::new(),
            total_delta_entries: 0,
            is_complete: true,
        }
    }

    /// Add an incremental checkpoint to the chain
    pub fn add_incremental(&mut self, token: CheckpointToken, num_entries: u64) {
        self.incremental_tokens.push(token.to_string());
        self.total_delta_entries += num_entries;
    }

    /// Get the latest token in the chain (or base if no incrementals)
    pub fn latest_token(&self) -> io::Result<CheckpointToken> {
        if let Some(last) = self.incremental_tokens.last() {
            last.parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid token: {e}"))
            })
        } else {
            self.base_snapshot_token.parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid base token: {e}"),
                )
            })
        }
    }

    /// Get number of incremental checkpoints in the chain
    pub fn num_incrementals(&self) -> usize {
        self.incremental_tokens.len()
    }

    /// Serialize to JSON bytes
    pub fn serialize_json(&self) -> io::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Deserialize from JSON bytes
    pub fn deserialize_json(data: &[u8]) -> io::Result<Self> {
        serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Write to file (JSON format)
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let data = self.serialize_json()?;
        let mut file = BufWriter::new(File::create(path)?);
        file.write_all(&data)?;
        file.flush()
    }

    /// Read from file (JSON format)
    pub fn read_from_file(path: &Path) -> io::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::deserialize_json(&data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{IndexMetadata, LogMetadata};
    use uuid::Uuid;

    #[test]
    fn test_index_metadata_json_serialization() {
        let mut meta = IndexMetadata::with_token(Uuid::new_v4());
        meta.table_size = 1024;
        meta.num_buckets = 100;
        meta.num_entries = 500;
        meta.version = 1;
        meta.log_begin_address = Address::new(0, 0);
        meta.checkpoint_start_address = Address::new(5, 100);

        let json = meta.serialize_json().unwrap();
        let restored = IndexMetadata::deserialize_json(&json).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.table_size, restored.table_size);
        assert_eq!(meta.num_buckets, restored.num_buckets);
        assert_eq!(meta.num_entries, restored.num_entries);
        assert_eq!(meta.version, restored.version);
        assert_eq!(meta.log_begin_address, restored.log_begin_address);
        assert_eq!(
            meta.checkpoint_start_address,
            restored.checkpoint_start_address
        );
    }

    #[test]
    fn test_index_metadata_binary_serialization() {
        let mut meta = IndexMetadata::with_token(Uuid::new_v4());
        meta.table_size = 2048;
        meta.num_buckets = 200;
        meta.num_entries = 1000;

        let binary = meta.serialize_binary().unwrap();
        let restored = IndexMetadata::deserialize_binary(&binary).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.table_size, restored.table_size);
        assert_eq!(meta.num_buckets, restored.num_buckets);
        assert_eq!(meta.num_entries, restored.num_entries);
    }

    #[test]
    fn test_log_metadata_json_serialization() {
        let mut meta = LogMetadata::with_token(Uuid::new_v4());
        meta.version = 5;
        meta.begin_address = Address::new(0, 0);
        meta.final_address = Address::new(10, 1024);
        meta.flushed_until_address = Address::new(8, 512);
        meta.use_object_log = false;
        meta.use_snapshot_file = true;
        meta.add_session(Uuid::new_v4(), 100);
        meta.add_session(Uuid::new_v4(), 200);

        let json = meta.serialize_json().unwrap();
        let restored = LogMetadata::deserialize_json(&json).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.version, restored.version);
        assert_eq!(meta.begin_address, restored.begin_address);
        assert_eq!(meta.final_address, restored.final_address);
        assert_eq!(meta.flushed_until_address, restored.flushed_until_address);
        assert_eq!(meta.use_object_log, restored.use_object_log);
        assert_eq!(meta.use_snapshot_file, restored.use_snapshot_file);
        assert_eq!(meta.num_threads, restored.num_threads);
        assert_eq!(meta.session_states.len(), restored.session_states.len());
    }

    #[test]
    fn test_log_metadata_binary_serialization() {
        let mut meta = LogMetadata::with_token(Uuid::new_v4());
        meta.version = 10;
        meta.begin_address = Address::new(1, 100);
        meta.final_address = Address::new(20, 2048);
        meta.flushed_until_address = Address::new(15, 1024);
        meta.use_object_log = true;

        let binary = meta.serialize_binary().unwrap();
        let restored = LogMetadata::deserialize_binary(&binary).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.version, restored.version);
        assert_eq!(meta.begin_address, restored.begin_address);
        assert_eq!(meta.final_address, restored.final_address);
        assert_eq!(meta.flushed_until_address, restored.flushed_until_address);
        assert_eq!(meta.use_object_log, restored.use_object_log);
    }

    #[test]
    fn test_index_metadata_file_io() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("index.meta");

        let mut meta = IndexMetadata::with_token(Uuid::new_v4());
        meta.table_size = 4096;
        meta.num_buckets = 50;
        meta.num_entries = 250;

        meta.write_to_file(&file_path).unwrap();
        let restored = IndexMetadata::read_from_file(&file_path).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.table_size, restored.table_size);
    }

    #[test]
    fn test_log_metadata_file_io() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("log.meta");

        let mut meta = LogMetadata::with_token(Uuid::new_v4());
        meta.version = 3;
        meta.begin_address = Address::new(0, 0);
        meta.final_address = Address::new(5, 512);
        meta.flushed_until_address = Address::new(4, 256);
        meta.use_object_log = false;

        meta.write_to_file(&file_path).unwrap();
        let restored = LogMetadata::read_from_file(&file_path).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.version, restored.version);
        assert_eq!(meta.final_address, restored.final_address);
    }

    #[test]
    fn test_create_checkpoint_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let token = Uuid::new_v4();

        let checkpoint_dir = create_checkpoint_directory(temp_dir.path(), token).unwrap();

        assert!(checkpoint_dir.exists());
        assert!(checkpoint_dir.ends_with(token.to_string()));
    }

    #[test]
    fn test_path_helpers() {
        let checkpoint_dir = Path::new("/tmp/checkpoint/abc123");

        assert_eq!(
            index_metadata_path(checkpoint_dir),
            Path::new("/tmp/checkpoint/abc123/index.meta")
        );
        assert_eq!(
            index_data_path(checkpoint_dir),
            Path::new("/tmp/checkpoint/abc123/index.dat")
        );
        assert_eq!(
            log_metadata_path(checkpoint_dir),
            Path::new("/tmp/checkpoint/abc123/log.meta")
        );
        assert_eq!(
            log_snapshot_path(checkpoint_dir),
            Path::new("/tmp/checkpoint/abc123/log.snapshot")
        );
        assert_eq!(
            delta_log_path(checkpoint_dir, 0),
            Path::new("/tmp/checkpoint/abc123/delta.0.log")
        );
        assert_eq!(
            delta_log_path(checkpoint_dir, 3),
            Path::new("/tmp/checkpoint/abc123/delta.3.log")
        );
        assert_eq!(
            delta_metadata_path(checkpoint_dir),
            Path::new("/tmp/checkpoint/abc123/delta.meta")
        );
        assert_eq!(
            incremental_info_path(checkpoint_dir),
            Path::new("/tmp/checkpoint/abc123/incremental.info")
        );
    }

    #[test]
    fn test_delta_log_metadata_serialization() {
        let token = Uuid::new_v4();
        let base_token = Uuid::new_v4();

        let mut meta = DeltaLogMetadata::new(token, base_token, 5);
        meta.delta_tail_address = 4096;
        meta.prev_snapshot_final_address = 1000;
        meta.current_final_address = 2000;
        meta.num_entries = 50;

        let json = meta.serialize_json().unwrap();
        let restored = DeltaLogMetadata::deserialize_json(&json).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.base_snapshot_token, restored.base_snapshot_token);
        assert_eq!(meta.version, restored.version);
        assert_eq!(meta.delta_tail_address, restored.delta_tail_address);
        assert_eq!(meta.num_entries, restored.num_entries);
    }

    #[test]
    fn test_delta_log_metadata_file_io() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("delta.meta");

        let token = Uuid::new_v4();
        let base_token = Uuid::new_v4();

        let mut meta = DeltaLogMetadata::new(token, base_token, 3);
        meta.delta_tail_address = 8192;
        meta.num_entries = 100;

        meta.write_to_file(&file_path).unwrap();
        let restored = DeltaLogMetadata::read_from_file(&file_path).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.get_token().unwrap(), token);
        assert_eq!(meta.get_base_token().unwrap(), base_token);
    }

    #[test]
    fn test_incremental_checkpoint_chain() {
        let base_token = Uuid::new_v4();
        let mut chain = IncrementalCheckpointChain::new(base_token);

        assert_eq!(chain.num_incrementals(), 0);
        assert!(chain.is_complete);
        assert_eq!(chain.latest_token().unwrap(), base_token);

        let incr1 = Uuid::new_v4();
        chain.add_incremental(incr1, 50);

        assert_eq!(chain.num_incrementals(), 1);
        assert_eq!(chain.total_delta_entries, 50);
        assert_eq!(chain.latest_token().unwrap(), incr1);

        let incr2 = Uuid::new_v4();
        chain.add_incremental(incr2, 30);

        assert_eq!(chain.num_incrementals(), 2);
        assert_eq!(chain.total_delta_entries, 80);
        assert_eq!(chain.latest_token().unwrap(), incr2);
    }

    #[test]
    fn test_incremental_checkpoint_chain_file_io() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("incremental.info");

        let base_token = Uuid::new_v4();
        let mut chain = IncrementalCheckpointChain::new(base_token);
        chain.add_incremental(Uuid::new_v4(), 100);
        chain.add_incremental(Uuid::new_v4(), 200);

        chain.write_to_file(&file_path).unwrap();
        let restored = IncrementalCheckpointChain::read_from_file(&file_path).unwrap();

        assert_eq!(chain.num_incrementals(), restored.num_incrementals());
        assert_eq!(chain.total_delta_entries, restored.total_delta_entries);
        assert_eq!(chain.is_complete, restored.is_complete);
    }

    #[test]
    fn test_log_metadata_incremental_serialization() {
        let mut meta = LogMetadata::with_token(Uuid::new_v4());
        let prev_token = Uuid::new_v4();

        meta.initialize_incremental(5, Address::new(0, 100), prev_token);
        meta.delta_tail_address = 2048;
        meta.snapshot_final_address = Address::new(10, 500);

        let json = meta.serialize_json().unwrap();
        let restored = LogMetadata::deserialize_json(&json).unwrap();

        assert!(restored.is_incremental);
        assert_eq!(restored.delta_tail_address, 2048);
        assert_eq!(restored.prev_snapshot_token, Some(prev_token));
        assert!(restored.is_incremental_checkpoint());
        assert_eq!(restored.base_snapshot_token(), Some(prev_token));
    }
}
