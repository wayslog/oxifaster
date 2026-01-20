# Phase 3: Complete Workstream D (FasterLog Enhancements)

**Priority**: Optional
**Estimated Effort**: 0.5-1 day
**Status**: Ready for implementation

## Overview

FasterLog is already 90% complete with full durability. This phase adds space reclamation (truncation) and documents the format versioning strategy for future compatibility.

---

## Task 3.1: Add Log Truncation API

**File**: `src/log/faster_log.rs`
**Estimated Time**: 3-4 hours
**Difficulty**: Medium

### Purpose

Allow users to reclaim space from old log entries that are no longer needed.

### Current State

FasterLog appends indefinitely. There's no way to remove old entries and reclaim space.

### Implementation

```rust
// TODO: Add to src/log/faster_log.rs

impl<D: StorageDevice> FasterLog<D> {
    /// Truncate the log before the specified address.
    ///
    /// This removes all log entries before `before_address` and updates
    /// the log's begin address. The space may not be immediately reclaimed
    /// depending on the storage device.
    ///
    /// # Arguments
    /// - `before_address`: Entries before this address will be removed
    ///
    /// # Returns
    /// The new begin address after truncation
    ///
    /// # Errors
    /// Returns an error if:
    /// - `before_address` is beyond the committed address
    /// - Device I/O fails
    /// - Metadata update fails
    pub fn truncate_before(&self, before_address: Address) -> Result<Address, Status> {
        // Validate address
        let committed = self.shared.committed_until.load(Ordering::Acquire);
        if before_address > committed {
            return Err(Status::InvalidArgument);
        }

        let begin = self.shared.begin_address.load(Ordering::Acquire);
        if before_address <= begin {
            return Ok(begin); // Already truncated
        }

        // Update begin address
        self.shared.begin_address.store(before_address, Ordering::Release);

        // Persist updated metadata
        let io_exec = IoExecutor::new()
            .map_err(|e| {
                self.shared.record_error(LogError::new(LogErrorKind::Io, e.to_string()));
                Status::IoError
            })?;

        let mut meta = self.shared.metadata.lock()
            .map_err(|_| Status::IoError)?;
        meta.begin_address = u64::from(before_address);

        Self::persist_metadata(
            &self.shared.device,
            &io_exec,
            self.shared.config.page_size,
            self.shared.device.alignment(),
            &meta,
        ).map_err(|e| {
            self.shared.record_error(e.clone());
            status_from_error(&e)
        })?;

        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(
                from = %begin,
                to = %before_address,
                "log truncated"
            );
        }

        Ok(before_address)
    }

    /// Get the current begin address.
    ///
    /// Entries before this address have been truncated.
    pub fn get_begin_address(&self) -> Address {
        self.shared.begin_address.load(Ordering::Acquire)
    }

    /// Get the reclaimable space in bytes.
    ///
    /// This is an estimate of space that could be reclaimed by truncating
    /// to the current committed address.
    pub fn get_reclaimable_space(&self) -> u64 {
        let begin = self.shared.begin_address.load(Ordering::Acquire);
        let committed = self.shared.committed_until.load(Ordering::Acquire);

        if committed <= begin {
            return 0;
        }

        // Estimate based on page size
        let begin_page = begin.page();
        let committed_page = committed.page();
        let pages = committed_page.saturating_sub(begin_page);
        pages as u64 * self.shared.config.page_size as u64
    }
}
```

### Usage Example

```rust
// TODO: Add to examples/faster_log.rs or create examples/log_truncation.rs

use oxifaster::log::FasterLog;

fn example_truncation() {
    let log = FasterLog::open(config, device).unwrap();

    // Append some data
    for i in 0..1000 {
        log.append(format!("entry-{}", i).as_bytes()).unwrap();
    }
    let committed = log.commit().unwrap();
    log.wait_for_commit(committed).unwrap();

    // Later, truncate old entries
    let keep_after = Address::from(100_000u64); // Keep only recent entries
    log.truncate_before(keep_after).unwrap();

    println!("Reclaimable space: {} bytes", log.get_reclaimable_space());
}
```

### Testing

```rust
// TODO: Add to tests/faster_log.rs

#[test]
fn test_log_truncation_basic() {
    // 1. Append 1000 entries
    // 2. Commit
    // 3. Truncate first 500
    // 4. Verify begin_address updated
    // 5. Verify scanning skips truncated entries
    // 6. Verify metadata persisted correctly
}

#[test]
fn test_truncation_validation() {
    // Test error cases:
    // - Truncate beyond committed address (should fail)
    // - Truncate before begin (should be no-op)
    // - Truncate at committed exactly (should succeed)
}

#[test]
fn test_truncation_with_recovery() {
    // 1. Append data
    // 2. Commit
    // 3. Truncate
    // 4. Close log
    // 5. Reopen
    // 6. Verify begin_address restored correctly
    // 7. Verify truncated data not accessible
}

#[test]
fn test_reclaimable_space_calculation() {
    // Verify get_reclaimable_space() returns correct estimates
}
```

---

## Task 3.2: Document Format Versioning Strategy

**File**: `docs/fasterlog_format.md`
**Estimated Time**: 2-3 hours
**Difficulty**: Low

### Purpose

Document how future format changes will be handled without breaking compatibility.

### What to Add

```markdown
// TODO: Add to docs/fasterlog_format.md

## Format Versioning and Migration Strategy

### Current Version

- **Format Version**: 1
- **Magic**: `OXFLOG1\0`
- **Introduced**: 2026-01-20
- **Status**: Stable

### Version Detection

The format version is stored in the metadata header at offset 8 (after magic):

```rust
pub fn detect_version(device: &D) -> Result<u32, LogError> {
    let mut buf = vec![0u8; LogMetadata::ENCODED_SIZE];
    // Read metadata from device
    // Check magic
    // Return version field (u32 at offset 8)
}
```

### Supported Versions

| Version | Status | Notes |
|---------|--------|-------|
| 1 | Current | Initial stable format |
| 2+ | Future | Reserved for future use |

### Migration Strategy

When introducing a new format version:

1. **Version 2 Implementation**:
   - Add `LogMetadata::VERSION_2` constant
   - Update `encode()` to support both versions
   - Update `decode()` to detect and handle both versions
   - Add migration function: `migrate_v1_to_v2()`

2. **Reading Old Formats**:
   ```rust
   impl LogMetadata {
       pub fn decode(buf: &[u8]) -> Result<Self, LogMetadataError> {
           // Check magic
           // Read version
           match version {
               1 => Self::decode_v1(buf),
               2 => Self::decode_v2(buf),
               _ => Err(LogMetadataError::UnsupportedVersion(version)),
           }
       }
   }
   ```

3. **Writing New Format**:
   - Always write newest supported version
   - Or provide option to write old format for compatibility

4. **Migration Tool**:
   ```rust
   // Tool to migrate v1 logs to v2
   pub fn migrate_log(path: &Path, from_version: u32, to_version: u32) {
       // Read with old format
       // Write with new format
       // Preserve all data
   }
   ```

### Backward Compatibility Policy

- **Minor versions (1.x)**: Must read all previous minor versions
- **Major versions (x.0)**: May drop support for old versions after migration tool is provided
- **Deprecation**: Minimum 2 major versions before dropping support

### Format Change Guidelines

When changing the format:

1. **Metadata changes**: Increase version number
2. **Entry format changes**: Consider new entry flag instead of version bump
3. **Checksum algorithm changes**: Version bump required
4. **Page size constraints**: Version bump required

### Example: Adding Compression (Hypothetical v2)

```rust
// Version 2 might add:
// - compression_algorithm field (u8) in metadata
// - compressed_size field in entry header
// - ENTRY_FLAG_COMPRESSED = 1 << 1

impl LogMetadata {
    pub const VERSION_2: u32 = 2;

    // New field
    pub compression_algorithm: u8, // 0 = none, 1 = lz4, etc.
}
```

### Testing Strategy

- Test reading v1 logs with v2 code (backward compat)
- Test migration from v1 to v2
- Test that v1 readers reject v2 logs gracefully
- Fuzz old version formats for robustness
```

---

## Task 3.3: Add Metadata Inspection Tools

**File**: `src/log/inspect.rs` (NEW)
**Estimated Time**: 1-2 hours
**Difficulty**: Low

### Purpose

Provide tools for inspecting log metadata without opening the log.

```rust
// TODO: Create src/log/inspect.rs

use std::path::Path;
use crate::device::StorageDevice;
use crate::log::format::{LogMetadata, LogMetadataError};

/// Inspect log metadata without opening the log.
pub struct LogInspector;

impl LogInspector {
    /// Read metadata from a log file.
    pub fn read_metadata<D: StorageDevice>(
        device: &D,
        page_size: usize,
    ) -> Result<LogMetadata, LogMetadataError> {
        // Read first page
        // Decode metadata
        // Return
        todo!()
    }

    /// Check if a file is a valid FasterLog.
    pub fn is_valid_log<D: StorageDevice>(device: &D) -> bool {
        Self::read_metadata(device, 4096).is_ok()
    }

    /// Get log statistics without opening.
    pub fn get_stats<D: StorageDevice>(device: &D) -> Result<LogStats, LogMetadataError> {
        let meta = Self::read_metadata(device, 4096)?;

        Ok(LogStats {
            version: meta.version,
            page_size: meta.page_size,
            begin_address: meta.begin_address,
            committed_address: meta.committed_address,
            tail_address: meta.tail_address,
            estimated_entries: (meta.committed_address - meta.begin_address) / 64, // Rough estimate
            estimated_size_bytes: meta.committed_address - meta.begin_address,
        })
    }
}

#[derive(Debug)]
pub struct LogStats {
    pub version: u32,
    pub page_size: u32,
    pub begin_address: u64,
    pub committed_address: u64,
    pub tail_address: u64,
    pub estimated_entries: u64,
    pub estimated_size_bytes: u64,
}
```

### CLI Tool (Optional)

```rust
// TODO: Optional - create examples/log_inspect.rs

use std::env;
use oxifaster::device::FileSystemDisk;
use oxifaster::log::inspect::LogInspector;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: log_inspect <log_file>");
        return;
    }

    let path = &args[1];
    let device = FileSystemDisk::single_file(path).unwrap();

    match LogInspector::get_stats(&device) {
        Ok(stats) => {
            println!("Log Statistics:");
            println!("  Version: {}", stats.version);
            println!("  Page Size: {} bytes", stats.page_size);
            println!("  Begin: {}", stats.begin_address);
            println!("  Committed: {}", stats.committed_address);
            println!("  Tail: {}", stats.tail_address);
            println!("  Estimated Entries: {}", stats.estimated_entries);
            println!("  Estimated Size: {} bytes", stats.estimated_size_bytes);
        }
        Err(e) => {
            eprintln!("Error reading log: {}", e);
        }
    }
}
```

---

## Success Criteria

Phase 3 is complete when:

✅ `truncate_before()` API implemented and tested
✅ Format versioning strategy documented
✅ Migration strategy defined
✅ Log inspection tools available
✅ Examples demonstrate new features
✅ All tests passing

---

## Estimated Breakdown

| Task | Time | Difficulty |
|------|------|-----------|
| 3.1 Truncation API | 3-4h | Medium |
| 3.2 Format docs | 2-3h | Low |
| 3.3 Inspection tools | 1-2h | Low |
| **Total** | **6-9h** | **~1 day** |

---

## Notes for Implementer

- Truncation doesn't need to physically delete data (depends on device)
- Focus on updating metadata correctly
- Ensure recovery after truncation works correctly
- Consider adding metrics for truncation events
- Format versioning is mostly documentation

---

## Optional Enhancements

Beyond the core requirements, consider:

- **Automatic truncation**: Based on age or size threshold
- **Compression**: For old entries before truncation
- **Archival**: Move truncated entries to cold storage
- **Monitoring**: Metrics for space usage and reclamation

---

## References

- Current implementation: `src/log/faster_log.rs`
- Format specification: `docs/fasterlog_format.md`
- Existing tests: `tests/faster_log.rs`
