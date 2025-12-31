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
    /// Hash table size
    pub table_size: u64,
    /// Number of overflow buckets
    pub num_buckets: u64,
    /// Number of overflow entries
    pub num_entries: u64,
}

impl SerializableIndexMetadata {
    /// Create from IndexMetadata
    pub fn from_metadata(meta: &super::IndexMetadata) -> Self {
        Self {
            token: meta.token.to_string(),
            table_size: meta.table_size,
            num_buckets: meta.num_buckets,
            num_entries: meta.num_entries,
        }
    }

    /// Convert to IndexMetadata
    pub fn to_metadata(&self) -> io::Result<super::IndexMetadata> {
        let token = self.token.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UUID: {}", e))
        })?;
        
        Ok(super::IndexMetadata {
            token,
            table_size: self.table_size,
            num_buckets: self.num_buckets,
            num_entries: self.num_entries,
        })
    }
}

/// Serializable version of LogMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableLogMetadata {
    /// Checkpoint token (UUID as string)
    pub token: String,
    /// Version at checkpoint
    pub version: u32,
    /// Begin address (as u64)
    pub begin_address: u64,
    /// Final address at checkpoint (as u64)
    pub final_address: u64,
    /// Flushed until address (as u64)
    pub flushed_until_address: u64,
    /// Object log exists
    pub use_object_log: bool,
}

impl SerializableLogMetadata {
    /// Create from LogMetadata
    pub fn from_metadata(meta: &super::LogMetadata) -> Self {
        Self {
            token: meta.token.to_string(),
            version: meta.version,
            begin_address: meta.begin_address.control(),
            final_address: meta.final_address.control(),
            flushed_until_address: meta.flushed_until_address.control(),
            use_object_log: meta.use_object_log,
        }
    }

    /// Convert to LogMetadata
    pub fn to_metadata(&self) -> io::Result<super::LogMetadata> {
        let token = self.token.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UUID: {}", e))
        })?;
        
        Ok(super::LogMetadata {
            token,
            version: self.version,
            begin_address: Address::from_control(self.begin_address),
            final_address: Address::from_control(self.final_address),
            flushed_until_address: Address::from_control(self.flushed_until_address),
            use_object_log: self.use_object_log,
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
        bincode::serialize(&serializable)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
        bincode::serialize(&serializable)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
pub fn create_checkpoint_directory(base_dir: &Path, token: CheckpointToken) -> io::Result<std::path::PathBuf> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{IndexMetadata, LogMetadata};
    use uuid::Uuid;

    #[test]
    fn test_index_metadata_json_serialization() {
        let meta = IndexMetadata {
            token: Uuid::new_v4(),
            table_size: 1024,
            num_buckets: 100,
            num_entries: 500,
        };

        let json = meta.serialize_json().unwrap();
        let restored = IndexMetadata::deserialize_json(&json).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.table_size, restored.table_size);
        assert_eq!(meta.num_buckets, restored.num_buckets);
        assert_eq!(meta.num_entries, restored.num_entries);
    }

    #[test]
    fn test_index_metadata_binary_serialization() {
        let meta = IndexMetadata {
            token: Uuid::new_v4(),
            table_size: 2048,
            num_buckets: 200,
            num_entries: 1000,
        };

        let binary = meta.serialize_binary().unwrap();
        let restored = IndexMetadata::deserialize_binary(&binary).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.table_size, restored.table_size);
        assert_eq!(meta.num_buckets, restored.num_buckets);
        assert_eq!(meta.num_entries, restored.num_entries);
    }

    #[test]
    fn test_log_metadata_json_serialization() {
        let meta = LogMetadata {
            token: Uuid::new_v4(),
            version: 5,
            begin_address: Address::new(0, 0),
            final_address: Address::new(10, 1024),
            flushed_until_address: Address::new(8, 512),
            use_object_log: false,
        };

        let json = meta.serialize_json().unwrap();
        let restored = LogMetadata::deserialize_json(&json).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.version, restored.version);
        assert_eq!(meta.begin_address, restored.begin_address);
        assert_eq!(meta.final_address, restored.final_address);
        assert_eq!(meta.flushed_until_address, restored.flushed_until_address);
        assert_eq!(meta.use_object_log, restored.use_object_log);
    }

    #[test]
    fn test_log_metadata_binary_serialization() {
        let meta = LogMetadata {
            token: Uuid::new_v4(),
            version: 10,
            begin_address: Address::new(1, 100),
            final_address: Address::new(20, 2048),
            flushed_until_address: Address::new(15, 1024),
            use_object_log: true,
        };

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

        let meta = IndexMetadata {
            token: Uuid::new_v4(),
            table_size: 4096,
            num_buckets: 50,
            num_entries: 250,
        };

        meta.write_to_file(&file_path).unwrap();
        let restored = IndexMetadata::read_from_file(&file_path).unwrap();

        assert_eq!(meta.token, restored.token);
        assert_eq!(meta.table_size, restored.table_size);
    }

    #[test]
    fn test_log_metadata_file_io() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("log.meta");

        let meta = LogMetadata {
            token: Uuid::new_v4(),
            version: 3,
            begin_address: Address::new(0, 0),
            final_address: Address::new(5, 512),
            flushed_until_address: Address::new(4, 256),
            use_object_log: false,
        };

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
    }
}

