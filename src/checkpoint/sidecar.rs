//! Sidecar `.crc` files for checkpoint artifact integrity verification.
//!
//! Each checkpoint artifact (index.meta, log.meta, delta.meta, log.snapshot)
//! gets a companion `.crc` file written alongside it:
//!
//! - For metadata files: the `.crc` file contains a single 8-byte XOR checksum
//!   of the entire file content.
//! - For snapshot files: the `.crc` file contains per-page checksums as
//!   `[num_pages: u64][page_num: u64, checksum: u64]...` in little-endian format.
//!
//! On read, if the `.crc` sidecar exists its checksum(s) are verified.
//! If the sidecar is missing (old checkpoint), a warning is emitted but
//! processing continues.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::format::compute_xor_checksum;

/// Get the sidecar path for a given artifact file.
fn crc_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".crc");
    PathBuf::from(p)
}

/// Write a whole-file XOR checksum sidecar for a metadata file.
///
/// Reads the file content, computes the XOR checksum, and writes it
/// as 8 bytes (u64 LE) to `<path>.crc`.
pub fn write_file_checksum(path: &Path) -> io::Result<()> {
    let content = std::fs::read(path)?;
    let checksum = compute_xor_checksum(&content);
    let crc = crc_path(path);
    let mut file = File::create(&crc)?;
    file.write_all(&checksum.to_le_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Verify the sidecar checksum for a metadata file.
///
/// If the `.crc` sidecar exists, reads it and verifies. If it does not
/// exist (legacy checkpoint), emits a warning to stderr and returns Ok.
pub fn verify_file_checksum(path: &Path) -> io::Result<()> {
    let crc = crc_path(path);
    if !crc.exists() {
        eprintln!(
            "warning: no sidecar checksum for {} (legacy checkpoint)",
            path.display()
        );
        return Ok(());
    }

    let mut buf = [0u8; 8];
    let mut file = File::open(&crc)?;
    file.read_exact(&mut buf)?;
    let stored = u64::from_le_bytes(buf);

    let content = std::fs::read(path)?;
    let computed = compute_xor_checksum(&content);

    if stored != computed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: checksum mismatch (stored={:#x}, computed={:#x})",
                path.display(),
                stored,
                computed
            ),
        ));
    }

    Ok(())
}

/// Write per-page checksums for a snapshot file to `<path>.crc`.
///
/// Format (all little-endian):
///   num_pages : u64
///   (page_num : u64, checksum : u64) * num_pages
pub fn write_snapshot_checksums(path: &Path, checksums: &[(u64, u64)]) -> io::Result<()> {
    let crc = crc_path(path);
    let mut file = File::create(&crc)?;
    let count = checksums.len() as u64;
    file.write_all(&count.to_le_bytes())?;
    for &(page_num, checksum) in checksums {
        file.write_all(&page_num.to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
    }
    file.sync_all()?;
    Ok(())
}

/// Read per-page checksums from a snapshot sidecar file.
///
/// Returns `Ok(Some(map))` if the sidecar exists, `Ok(None)` if it does
/// not (legacy checkpoint -- a warning is emitted to stderr).
pub fn read_snapshot_checksums(path: &Path) -> io::Result<Option<HashMap<u64, u64>>> {
    let crc = crc_path(path);
    if !crc.exists() {
        eprintln!(
            "warning: no sidecar checksum for {} (legacy checkpoint)",
            path.display()
        );
        return Ok(None);
    }

    let data = std::fs::read(&crc)?;
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: sidecar too short", crc.display()),
        ));
    }

    let count = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    let expected_len = 8 + count * 16;
    if data.len() != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: sidecar size mismatch (expected {} bytes, got {})",
                crc.display(),
                expected_len,
                data.len()
            ),
        ));
    }

    let mut map = HashMap::with_capacity(count);
    for i in 0..count {
        let offset = 8 + i * 16;
        let page_num = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let checksum = u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());
        if map.insert(page_num, checksum).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: duplicate page entry {}", crc.display(), page_num),
            ));
        }
    }

    Ok(Some(map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_verify_file_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.meta");
        std::fs::write(&path, b"hello world").unwrap();

        write_file_checksum(&path).unwrap();

        assert!(crc_path(&path).exists());
        verify_file_checksum(&path).unwrap();
    }

    #[test]
    fn test_verify_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.meta");
        std::fs::write(&path, b"hello world").unwrap();

        write_file_checksum(&path).unwrap();

        // Corrupt the file
        std::fs::write(&path, b"corrupted!!").unwrap();

        let result = verify_file_checksum(&path);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("checksum mismatch"));
    }

    #[test]
    fn test_verify_missing_sidecar_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.meta");
        std::fs::write(&path, b"hello world").unwrap();

        // No .crc file -- legacy format, should succeed with warning
        verify_file_checksum(&path).unwrap();
    }

    #[test]
    fn test_snapshot_checksums_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.snapshot");
        std::fs::write(&path, b"dummy").unwrap();

        let checksums = vec![(0u64, 0xDEADBEEF_u64), (1, 0xCAFEBABE), (5, 0x12345678)];
        write_snapshot_checksums(&path, &checksums).unwrap();

        let loaded = read_snapshot_checksums(&path).unwrap().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[&0], 0xDEADBEEF);
        assert_eq!(loaded[&1], 0xCAFEBABE);
        assert_eq!(loaded[&5], 0x12345678);
    }

    #[test]
    fn test_snapshot_checksums_missing_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.snapshot");
        std::fs::write(&path, b"dummy").unwrap();

        let result = read_snapshot_checksums(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_snapshot_checksums_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.snapshot");
        std::fs::write(&path, b"dummy").unwrap();

        write_snapshot_checksums(&path, &[]).unwrap();

        let loaded = read_snapshot_checksums(&path).unwrap().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_snapshot_checksums_reject_extra_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.snapshot");
        std::fs::write(&path, b"dummy").unwrap();

        let crc = crc_path(&path);
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0xABCDu64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&crc, data).unwrap();

        let err = read_snapshot_checksums(&path).unwrap_err();
        assert!(err.to_string().contains("size mismatch"));
    }

    #[test]
    fn test_snapshot_checksums_reject_duplicate_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.snapshot");
        std::fs::write(&path, b"dummy").unwrap();

        let crc = crc_path(&path);
        let mut data = Vec::new();
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&0x1111u64.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        data.extend_from_slice(&0x2222u64.to_le_bytes());
        std::fs::write(&crc, data).unwrap();

        let err = read_snapshot_checksums(&path).unwrap_err();
        assert!(err.to_string().contains("duplicate page entry"));
    }

    #[test]
    fn test_crc_path() {
        let p = Path::new("/tmp/checkpoint/index.meta");
        assert_eq!(crc_path(p), Path::new("/tmp/checkpoint/index.meta.crc"));
    }
}
