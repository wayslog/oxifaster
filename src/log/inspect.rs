//! Log inspection utilities
//!
//! Provides tools for inspecting log metadata and statistics without opening the log.

use std::path::Path;

use crate::device::StorageDevice;
use crate::log::format::LogMetadata;
use crate::log::io::IoExecutor;
use crate::log::types::{LogError, LogErrorKind};
use crate::utility::AlignedBuffer;

/// Statistics about a log file
#[derive(Debug, Clone)]
pub struct LogStats {
    /// Format version
    pub version: u32,
    /// Page size in bytes
    pub page_size: u32,
    /// Segment size in bytes
    pub segment_size: u64,
    /// Data offset in bytes
    pub data_offset: u64,
    /// Begin address (after truncation)
    pub begin_address: u64,
    /// Committed address
    pub committed_address: u64,
    /// Tail address
    pub tail_address: u64,
    /// Estimated number of entries
    pub estimated_entries: u64,
    /// Estimated size in bytes
    pub estimated_size_bytes: u64,
}

impl LogStats {
    /// Get the committed range (begin to committed)
    pub fn committed_range(&self) -> u64 {
        self.committed_address.saturating_sub(self.begin_address)
    }

    /// Get the uncommitted range (committed to tail)
    pub fn uncommitted_range(&self) -> u64 {
        self.tail_address.saturating_sub(self.committed_address)
    }
}

/// Inspector for log metadata
pub struct LogInspector;

impl LogInspector {
    /// Read metadata from a log file without opening the log.
    ///
    /// # Arguments
    /// - `device`: Storage device containing the log
    /// - `page_size`: Expected page size
    ///
    /// # Returns
    /// The log metadata if present and valid
    pub fn read_metadata<D: StorageDevice>(
        device: &D,
        page_size: usize,
    ) -> Result<LogMetadata, LogError> {
        let io_exec =
            IoExecutor::new().map_err(|e| LogError::new(LogErrorKind::Io, e.to_string()))?;

        let alignment = device.alignment();
        let mut buffer = AlignedBuffer::zeroed(alignment, page_size).ok_or_else(|| {
            LogError::new(LogErrorKind::Io, "buffer allocation failed".to_string())
        })?;

        let size = device
            .size()
            .map_err(|e| LogError::new(LogErrorKind::Io, e.to_string()))?;

        if size < page_size as u64 {
            return Err(LogError::new(
                LogErrorKind::Metadata,
                "file too small".to_string(),
            ));
        }

        let read = io_exec
            .block_on(device.read(0, buffer.as_mut_slice()))
            .map_err(|e| LogError::new(LogErrorKind::Io, e.to_string()))?;

        if read < LogMetadata::ENCODED_SIZE {
            return Err(LogError::new(
                LogErrorKind::Metadata,
                "incomplete metadata".to_string(),
            ));
        }

        LogMetadata::decode(&buffer.as_slice()[..LogMetadata::ENCODED_SIZE])
            .map_err(|e| LogError::new(LogErrorKind::Metadata, e.to_string()))
    }

    /// Check if a file is a valid FasterLog.
    ///
    /// # Arguments
    /// - `device`: Storage device to check
    /// - `page_size`: Expected page size (default: 4096)
    ///
    /// # Returns
    /// `true` if the file contains valid FasterLog metadata
    pub fn is_valid_log<D: StorageDevice>(device: &D, page_size: usize) -> bool {
        Self::read_metadata(device, page_size).is_ok()
    }

    /// Get log statistics without opening the log.
    ///
    /// # Arguments
    /// - `device`: Storage device containing the log
    /// - `page_size`: Expected page size
    ///
    /// # Returns
    /// Statistics about the log
    pub fn get_stats<D: StorageDevice>(device: &D, page_size: usize) -> Result<LogStats, LogError> {
        let meta = Self::read_metadata(device, page_size)?;

        // Rough estimate of entries (assuming average entry size of 64 bytes)
        let estimated_entries = (meta.committed_address - meta.begin_address) / 64;

        Ok(LogStats {
            version: meta.version,
            page_size: meta.page_size,
            segment_size: meta.segment_size,
            data_offset: meta.data_offset,
            begin_address: meta.begin_address,
            committed_address: meta.committed_address,
            tail_address: meta.tail_address,
            estimated_entries,
            estimated_size_bytes: meta.committed_address - meta.begin_address,
        })
    }

    /// Check if a log at the given path exists and is valid.
    ///
    /// This is a convenience method for file-based logs.
    pub fn check_log_file(path: &Path) -> Result<LogStats, LogError> {
        use crate::device::FileSystemDisk;

        let device = FileSystemDisk::single_file(path)
            .map_err(|e| LogError::new(LogErrorKind::Io, e.to_string()))?;

        // Try common page sizes
        for &page_size in &[4096, 8192, 16384] {
            if let Ok(stats) = Self::get_stats(&device, page_size) {
                return Ok(stats);
            }
        }

        Err(LogError::new(
            LogErrorKind::Metadata,
            "No valid metadata found",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::FileSystemDisk;
    use crate::log::{FasterLog, FasterLogConfig};
    use tempfile::tempdir;

    #[test]
    fn test_inspect_valid_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a log
        let config = FasterLogConfig::default();
        let page_size = config.page_size;
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        let log = FasterLog::new(config, device).unwrap();

        // Write some data
        for i in 0..100 {
            log.append(format!("entry-{}", i).as_bytes()).unwrap();
        }
        log.commit().unwrap();

        drop(log);

        // Inspect the log
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        let metadata = LogInspector::read_metadata(&device, page_size).unwrap();

        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.page_size, page_size as u32);
        assert!(metadata.committed_address > 0);
    }

    #[test]
    fn test_is_valid_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a log
        let config = FasterLogConfig::default();
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        let log = FasterLog::new(config, device).unwrap();
        drop(log);

        // Check if valid
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        assert!(LogInspector::is_valid_log(&device, 4096));
    }

    #[test]
    fn test_get_stats() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a log
        let config = FasterLogConfig::default();
        let page_size = config.page_size;
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        let log = FasterLog::new(config, device).unwrap();

        // Write data
        for i in 0..1000 {
            log.append(format!("entry-{:06}", i).as_bytes()).unwrap();
        }
        let committed = log.commit().unwrap();
        log.wait_for_commit(committed).unwrap();

        drop(log);

        // Get stats
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        let stats = LogInspector::get_stats(&device, page_size).unwrap();

        println!("Stats: {:?}", stats);
        assert_eq!(stats.version, 1);
        assert_eq!(stats.page_size, page_size as u32);
        assert!(stats.committed_address > 0);
        assert!(stats.estimated_entries > 0);
        assert_eq!(
            stats.estimated_size_bytes,
            stats.committed_address - stats.begin_address
        );
    }

    #[test]
    fn test_check_log_file() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a log
        let config = FasterLogConfig::default();
        let device = FileSystemDisk::single_file(&log_path).unwrap();
        let log = FasterLog::new(config, device).unwrap();
        log.append(b"test data").unwrap();
        log.commit().unwrap();
        drop(log);

        // Check log file
        let stats = LogInspector::check_log_file(&log_path).unwrap();
        assert_eq!(stats.version, 1);
        assert!(stats.committed_address > 0);
    }

    #[test]
    fn test_invalid_log_detection() {
        let dir = tempdir().unwrap();
        let invalid_path = dir.path().join("invalid.log");

        // Create an invalid file
        std::fs::write(&invalid_path, b"not a log file").unwrap();

        // Should fail validation
        let result = LogInspector::check_log_file(&invalid_path);
        assert!(result.is_err());
    }
}
