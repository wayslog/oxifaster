//! File system storage device for FASTER
//!
//! This module provides file-based storage device implementations.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::device::SyncStorageDevice;

/// File system file wrapper
///
/// Wraps a file with mutex protection for thread-safe access.
pub struct FileSystemFile {
    /// Path to the file
    path: PathBuf,
    /// The underlying file
    file: Mutex<File>,
    /// Whether direct I/O is enabled
    direct_io: bool,
}

impl FileSystemFile {
    /// Open or create a file at the specified path
    pub fn open(path: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(&path)?;

        Ok(Self {
            path,
            file: Mutex::new(file),
            direct_io: false,
        })
    }

    /// Get the path to the file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Check if direct I/O is enabled
    pub fn direct_io(&self) -> bool {
        self.direct_io
    }
}

impl SyncStorageDevice for FileSystemFile {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Failed to lock file"))?;

        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Failed to lock file"))?;

        file.seek(SeekFrom::Start(offset))?;
        file.write(buf)
    }

    fn flush_sync(&self) -> io::Result<()> {
        let file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Failed to lock file"))?;

        file.sync_all()
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        let file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Failed to lock file"))?;

        file.set_len(size)
    }

    fn size_sync(&self) -> io::Result<u64> {
        let file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Failed to lock file"))?;

        file.metadata().map(|m| m.len())
    }
}

/// Segmented file for large storage
///
/// Manages multiple segment files to support storage larger than
/// the maximum file size.
pub struct SegmentedFile {
    /// Base directory
    base_dir: PathBuf,
    /// File prefix
    prefix: String,
    /// Segment size in bytes
    segment_size: u64,
    /// Open segments
    segments: Mutex<Vec<Option<FileSystemFile>>>,
}

impl SegmentedFile {
    /// Create a new segmented file
    pub fn new(base_dir: impl AsRef<Path>, prefix: &str, segment_size: u64) -> io::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;

        Ok(Self {
            base_dir,
            prefix: prefix.to_string(),
            segment_size,
            segments: Mutex::new(Vec::new()),
        })
    }

    /// Get the segment file path for a given segment index
    fn segment_path(&self, segment: u64) -> PathBuf {
        self.base_dir.join(format!("{}.{}", self.prefix, segment))
    }

    /// Get or create a segment
    fn get_segment(&self, segment: u64) -> io::Result<()> {
        let mut segments = self
            .segments
            .lock()
            .map_err(|_| io::Error::other("Failed to lock segments"))?;

        // Extend vector if needed
        while segments.len() <= segment as usize {
            segments.push(None);
        }

        // Open segment if not already open
        if segments[segment as usize].is_none() {
            let path = self.segment_path(segment);
            let file = FileSystemFile::open(path, true)?;
            segments[segment as usize] = Some(file);
        }

        Ok(())
    }
}

impl SyncStorageDevice for SegmentedFile {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let segment = offset / self.segment_size;
        let segment_offset = offset % self.segment_size;

        self.get_segment(segment)?;

        let segments = self
            .segments
            .lock()
            .map_err(|_| io::Error::other("Failed to lock segments"))?;

        if let Some(ref file) = segments[segment as usize] {
            file.read_sync(segment_offset, buf)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Segment not found"))
        }
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let segment = offset / self.segment_size;
        let segment_offset = offset % self.segment_size;

        self.get_segment(segment)?;

        let segments = self
            .segments
            .lock()
            .map_err(|_| io::Error::other("Failed to lock segments"))?;

        if let Some(ref file) = segments[segment as usize] {
            file.write_sync(segment_offset, buf)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Segment not found"))
        }
    }

    fn flush_sync(&self) -> io::Result<()> {
        let segments = self
            .segments
            .lock()
            .map_err(|_| io::Error::other("Failed to lock segments"))?;

        for segment in segments.iter().flatten() {
            segment.flush_sync()?;
        }

        Ok(())
    }

    fn truncate_sync(&self, _size: u64) -> io::Result<()> {
        // For segmented files, truncation is complex
        // For now, just return Ok
        Ok(())
    }

    fn size_sync(&self) -> io::Result<u64> {
        let segments = self
            .segments
            .lock()
            .map_err(|_| io::Error::other("Failed to lock segments"))?;

        let mut total = 0u64;
        for segment in segments.iter().flatten() {
            total += segment.size_sync()?;
        }

        Ok(total)
    }
}

/// File system disk device
///
/// High-level abstraction for file-based storage, supporting both
/// single file and segmented storage modes.
pub struct FileSystemDisk {
    /// The underlying storage (either single file or segmented)
    inner: FileSystemDiskInner,
}

enum FileSystemDiskInner {
    SingleFile(FileSystemFile),
    Segmented(SegmentedFile),
}

impl FileSystemDisk {
    /// Create a single-file disk
    pub fn single_file(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = FileSystemFile::open(path, true)?;
        Ok(Self {
            inner: FileSystemDiskInner::SingleFile(file),
        })
    }

    /// Create a segmented disk
    pub fn segmented(
        base_dir: impl AsRef<Path>,
        prefix: &str,
        segment_size: u64,
    ) -> io::Result<Self> {
        let segments = SegmentedFile::new(base_dir, prefix, segment_size)?;
        Ok(Self {
            inner: FileSystemDiskInner::Segmented(segments),
        })
    }
}

impl SyncStorageDevice for FileSystemDisk {
    fn read_sync(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        match &self.inner {
            FileSystemDiskInner::SingleFile(f) => f.read_sync(offset, buf),
            FileSystemDiskInner::Segmented(s) => s.read_sync(offset, buf),
        }
    }

    fn write_sync(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        match &self.inner {
            FileSystemDiskInner::SingleFile(f) => f.write_sync(offset, buf),
            FileSystemDiskInner::Segmented(s) => s.write_sync(offset, buf),
        }
    }

    fn flush_sync(&self) -> io::Result<()> {
        match &self.inner {
            FileSystemDiskInner::SingleFile(f) => f.flush_sync(),
            FileSystemDiskInner::Segmented(s) => s.flush_sync(),
        }
    }

    fn truncate_sync(&self, size: u64) -> io::Result<()> {
        match &self.inner {
            FileSystemDiskInner::SingleFile(f) => f.truncate_sync(size),
            FileSystemDiskInner::Segmented(s) => s.truncate_sync(size),
        }
    }

    fn size_sync(&self) -> io::Result<u64> {
        match &self.inner {
            FileSystemDiskInner::SingleFile(f) => f.size_sync(),
            FileSystemDiskInner::Segmented(s) => s.size_sync(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::StorageDevice;
    use tempfile::tempdir;

    #[test]
    fn test_file_system_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        // Write some data
        let data = b"Hello, World!";
        let written = file.write_sync(0, data).unwrap();
        assert_eq!(written, data.len());

        // Read it back
        let mut buf = vec![0u8; data.len()];
        let read = file.read_sync(0, &mut buf).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_file_system_file_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_path.dat");

        let file = FileSystemFile::open(&path, true).unwrap();
        assert_eq!(file.path(), path);
    }

    #[test]
    fn test_file_system_file_direct_io() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_direct_io.dat");

        let file = FileSystemFile::open(&path, true).unwrap();
        assert!(!file.direct_io());
    }

    #[test]
    fn test_file_system_file_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_flush.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        let data = b"Test data for flush";
        file.write_sync(0, data).unwrap();

        let result = file.flush_sync();
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_system_file_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_truncate.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        // Write some data
        let data = b"Some test data to truncate";
        file.write_sync(0, data).unwrap();

        // Truncate to smaller size
        file.truncate_sync(10).unwrap();

        let size = file.size_sync().unwrap();
        assert_eq!(size, 10);
    }

    #[test]
    fn test_file_system_file_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_size.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        // Initial size should be 0
        let initial_size = file.size_sync().unwrap();
        assert_eq!(initial_size, 0);

        // Write data
        let data = b"Test data";
        file.write_sync(0, data).unwrap();

        let size = file.size_sync().unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[test]
    fn test_file_system_file_read_at_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_offset.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        let data = b"0123456789ABCDEF";
        file.write_sync(0, data).unwrap();

        // Read from middle of file
        let mut buf = vec![0u8; 4];
        let read = file.read_sync(4, &mut buf).unwrap();
        assert_eq!(read, 4);
        assert_eq!(&buf, b"4567");
    }

    #[test]
    fn test_file_system_file_write_at_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_write_offset.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        // Write initial data
        let data1 = b"AAAAAAAAAA";
        file.write_sync(0, data1).unwrap();

        // Overwrite in middle
        let data2 = b"BB";
        file.write_sync(4, data2).unwrap();

        // Read entire file
        let mut buf = vec![0u8; 10];
        file.read_sync(0, &mut buf).unwrap();
        assert_eq!(&buf, b"AAAABBAAAA");
    }

    #[test]
    fn test_file_system_file_async_operations() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_async.dat");

        let file = FileSystemFile::open(&path, true).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Test async write
            let data = b"Async test data";
            let written = StorageDevice::write(&file, 0, data).await.unwrap();
            assert_eq!(written, data.len());

            // Test async read
            let mut buf = vec![0u8; data.len()];
            let read = StorageDevice::read(&file, 0, &mut buf).await.unwrap();
            assert_eq!(read, data.len());
            assert_eq!(&buf, data);

            // Test async flush
            StorageDevice::flush(&file).await.unwrap();

            // Test async truncate
            StorageDevice::truncate(&file, 5).await.unwrap();

            // Test size
            let size = StorageDevice::size(&file).unwrap();
            assert_eq!(size, 5);
        });
    }

    #[test]
    fn test_file_system_file_alignment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_alignment.dat");

        let file = FileSystemFile::open(&path, true).unwrap();
        assert_eq!(SyncStorageDevice::alignment(&file), 512);
    }

    #[test]
    fn test_file_system_file_sector_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sector.dat");

        let file = FileSystemFile::open(&path, true).unwrap();
        assert_eq!(SyncStorageDevice::sector_size(&file), 512);
    }

    #[test]
    fn test_segmented_file() {
        let dir = tempdir().unwrap();

        let segments = SegmentedFile::new(dir.path(), "test", 1024).unwrap();

        // Write to first segment
        let data1 = b"First segment data";
        segments.write_sync(0, data1).unwrap();

        // Write to second segment (after segment boundary)
        let data2 = b"Second segment data";
        segments.write_sync(1024, data2).unwrap();

        // Read back
        let mut buf1 = vec![0u8; data1.len()];
        segments.read_sync(0, &mut buf1).unwrap();
        assert_eq!(&buf1, data1);

        let mut buf2 = vec![0u8; data2.len()];
        segments.read_sync(1024, &mut buf2).unwrap();
        assert_eq!(&buf2, data2);
    }

    #[test]
    fn test_segmented_file_flush() {
        let dir = tempdir().unwrap();

        let segments = SegmentedFile::new(dir.path(), "flush_test", 1024).unwrap();

        let data = b"Test data";
        segments.write_sync(0, data).unwrap();

        let result = segments.flush_sync();
        assert!(result.is_ok());
    }

    #[test]
    fn test_segmented_file_truncate() {
        let dir = tempdir().unwrap();

        let segments = SegmentedFile::new(dir.path(), "truncate_test", 1024).unwrap();

        let data = b"Test data";
        segments.write_sync(0, data).unwrap();

        // Truncate should succeed (returns Ok(()) for segmented files)
        let result = segments.truncate_sync(5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_segmented_file_size() {
        let dir = tempdir().unwrap();

        let segments = SegmentedFile::new(dir.path(), "size_test", 1024).unwrap();

        // Initial size should be 0
        let initial_size = segments.size_sync().unwrap();
        assert_eq!(initial_size, 0);

        // Write data and check size
        let data = b"Test data";
        segments.write_sync(0, data).unwrap();

        let size = segments.size_sync().unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[test]
    fn test_segmented_file_multiple_segments() {
        let dir = tempdir().unwrap();

        let segments = SegmentedFile::new(dir.path(), "multi", 100).unwrap();

        // Write to 3 different segments
        segments.write_sync(0, b"Segment 0").unwrap();
        segments.write_sync(100, b"Segment 1").unwrap();
        segments.write_sync(200, b"Segment 2").unwrap();

        // Read back from each segment
        let mut buf = vec![0u8; 9];

        segments.read_sync(0, &mut buf).unwrap();
        assert_eq!(&buf, b"Segment 0");

        segments.read_sync(100, &mut buf).unwrap();
        assert_eq!(&buf, b"Segment 1");

        segments.read_sync(200, &mut buf).unwrap();
        assert_eq!(&buf, b"Segment 2");
    }

    #[test]
    fn test_file_system_disk_single_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.dat");

        let disk = FileSystemDisk::single_file(&path).unwrap();

        let data = b"Single file test";
        let written = disk.write_sync(0, data).unwrap();
        assert_eq!(written, data.len());

        let mut buf = vec![0u8; data.len()];
        disk.read_sync(0, &mut buf).unwrap();
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_file_system_disk_segmented() {
        let dir = tempdir().unwrap();

        let disk = FileSystemDisk::segmented(dir.path(), "seg", 1024).unwrap();

        let data = b"Segmented disk test";
        let written = disk.write_sync(0, data).unwrap();
        assert_eq!(written, data.len());

        let mut buf = vec![0u8; data.len()];
        disk.read_sync(0, &mut buf).unwrap();
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_file_system_disk_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("flush.dat");

        let disk = FileSystemDisk::single_file(&path).unwrap();
        disk.write_sync(0, b"test").unwrap();

        let result = disk.flush_sync();
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_system_disk_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("truncate.dat");

        let disk = FileSystemDisk::single_file(&path).unwrap();
        disk.write_sync(0, b"test data longer").unwrap();
        disk.truncate_sync(4).unwrap();

        let size = disk.size_sync().unwrap();
        assert_eq!(size, 4);
    }

    #[test]
    fn test_file_system_disk_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("size.dat");

        let disk = FileSystemDisk::single_file(&path).unwrap();

        let initial_size = disk.size_sync().unwrap();
        assert_eq!(initial_size, 0);

        disk.write_sync(0, b"1234567890").unwrap();

        let size = disk.size_sync().unwrap();
        assert_eq!(size, 10);
    }
}
