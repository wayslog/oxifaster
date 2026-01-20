# FasterLog Format Description

## Scope and Compatibility

- This format is **not 100% binary compatible with FASTER**, but follows similar concepts (append-only log, commit points, recovery scan). FASTER can be used as a reference for mental models, not a wire-compatibility target.
- The log is a single append-only stream addressed by `(page, offset)`; entries never span pages.
- Durability is defined by explicit `commit()` plus `wait_for_commit()` and by persisted metadata.
- Corruption detection is checksum-based and enforced on recovery and reads.

## Physical Layout (Device-Level)

- Page 0 holds persistent metadata; data begins at `data_offset = page_size`.
- Data pages are addressed from `(page=0, offset=0)` where page 0 corresponds to the first data page after metadata.
- File offsets use `file_offset = data_offset + (page * page_size) + offset`.
- Each entry is stored as `header + payload` and is contained within a single page.
- If an entry would cross a page boundary, a padding record is written to the remaining bytes of that page.
- The metadata encodes `page_size`, `segment_size`, and the current addresses used for recovery.
- `segment_size` is primarily a device concern (segmented file), not an on-disk entry boundary.

## Addressing and Configuration Constraints

- Addresses are 48-bit logical values with `PAGE_BITS = 23` and `OFFSET_BITS = 25`.
- `MAX_PAGE = (1 << PAGE_BITS) - 1`, `MAX_OFFSET = (1 << OFFSET_BITS) - 1`.
- `page_size` must be non-zero, must not exceed `MAX_OFFSET`, and must be at least the entry header size (16 bytes).
- `page_size` must be aligned to the device alignment; device alignment must be a power of two.

## Metadata Layout (64 bytes, little-endian)

- `magic` (8 bytes): ASCII `OXFLOG1\0`; mismatch is treated as "metadata missing".
- `version` (u32): currently `1`; unsupported values are errors.
- `page_size` (u32): must match the open config; mismatch is an error.
- `segment_size` (u64): persisted from config for format context.
- `data_offset` (u64): start of data region (typically `page_size`).
- `begin_address` (u64): logical begin address for truncation.
- `committed_address` (u64): latest committed durable address.
- `tail_address` (u64): latest observed tail address.
- `checksum` (u64): XOR checksum of bytes `[0..56)` (all fields before checksum).

## Entry Layout and Padding

- Entry header (16 bytes, little-endian):
  - `length` (u32): payload length in bytes.
  - `flags` (u32): entry flags; padding uses `LOG_ENTRY_FLAG_PADDING = 1 << 0`.
  - `checksum` (u64): XOR checksum of `(length, flags)` and payload bytes.
- Payload:
  - Exactly `length` bytes; checksum uses 8-byte XOR chunks with zero padding of the tail chunk.
- Padding record:
  - Written when a new entry cannot fit in the remaining page space.
  - `flags` includes `LOG_ENTRY_FLAG_PADDING`; `length` is the remaining payload size.
  - Payload bytes are zeroed; checksum is computed from header bytes and zero payload.
  - Readers and recovery skip padding by advancing to the next address.
- End-of-log:
  - A header with `length == 0` and no padding flag indicates end of valid data during scans.

## Durability Semantics (`commit` and `wait_for_commit`)

- `append()` writes to in-memory page buffers; it does not persist data on its own.
- `commit()`:
  - Sets `committed_until` to the current tail (CAS update).
  - Enqueues a background flush to persist pages up to the committed address.
  - Returns the commit address but **does not guarantee durability** until the flush completes.
- `wait_for_commit(address)`:
  - Waits until `flushed_until >= address`.
  - Guarantees all log data up to `address` is written, `StorageDevice::flush()` has completed, and metadata is updated with the committed address.
- Background flushing is allowed; `wait_for_commit()` is the durability barrier.

## Recovery Flow and Self-Check Differences

- Recovery scan order (shared concept):
  - Load metadata if present.
  - Determine scan range: `[begin_address, committed_address]` or `[begin_address, device_end]`.
  - Scan entries sequentially, validating entry checksum and page boundaries; skip padding.
  - On corruption, capture the last valid address as `scan_end`.
  - Trim (truncate) to `scan_end` only when repair is allowed.
  - Persist updated metadata (committed/tail/begin) after successful repair.
- `open()` with options:
  - `recover = true` runs the scan and can repair on corruption if `truncate_on_corruption = true`.
  - `recover = false` skips scanning; metadata is trusted as-is and corruption is not detected at open time.
  - If metadata is missing and `create_if_missing = true`, a new metadata page is created.
  - If metadata is missing and `recover = true`, a scan is attempted and metadata may be created from the scan result when permitted.
  - If corruption is detected and `truncate_on_corruption = false`, `open()` returns `Status::Corruption`.
- `self_check_with_options()`:
  - Runs the same scan without opening the log.
  - `repair = true` applies truncation and metadata updates.
  - `dry_run = true` reports what would happen without writing.
  - `create_if_missing = true` can create metadata when missing during repair.
- Default behavior:
  - `open()` defaults to `recover = true`, `create_if_missing = true`, `truncate_on_corruption = true`.
  - `self_check()` defaults to `repair = false`, `dry_run = false`, `create_if_missing = false`.

## Error Reporting and Status Mapping

- `last_error()` returns the most recent `LogError` recorded internally (e.g., recovery, flush, read).
- `LogErrorKind` to `Status` mapping:
  - `Io` -> `Status::IoError`
  - `Metadata` -> `Status::Corruption`
  - `Entry` -> `Status::Corruption`
  - `Corruption` -> `Status::Corruption`
  - `Config` -> `Status::InvalidArgument`
- `open()` returns `Status::NotFound` when metadata is missing and `create_if_missing = false`.

## Format Versioning and Migration Strategy

### Current Version

- **Format Version**: 1
- **Magic**: `OXFLOG1\0`
- **Introduced**: 2026-01-20
- **Status**: Stable
- **Features**: Basic append-only log with commit points, checksums, and truncation

### Version Detection

The format version is encoded in the metadata header at offset 8 (after the 8-byte magic string):

```rust
pub fn detect_version(device: &D) -> Result<u32, LogError> {
    let mut buf = vec![0u8; LogMetadata::ENCODED_SIZE];
    // Read metadata from device
    device.read(0, &mut buf)?;
    // Check magic
    let magic = &buf[0..8];
    if magic != b"OXFLOG1\0" {
        return Err(LogError::new(LogErrorKind::Metadata, "Invalid magic"));
    }
    // Return version field (u32 at offset 8)
    let version = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    Ok(version)
}
```

### Supported Versions

| Version | Status | Date | Notes |
|---------|--------|------|-------|
| 1 | Current | 2026-01-20 | Initial stable format with truncation support |
| 2+ | Future | TBD | Reserved for future use |

### Migration Strategy

When introducing a new format version:

#### 1. Version 2 Implementation Guidelines

- Add `LogMetadata::VERSION_2` constant
- Update `encode()` to support both versions (write new version)
- Update `decode()` to detect and handle all supported versions
- Add migration function: `migrate_v1_to_v2()`
- Provide clear migration path in documentation

#### 2. Reading Old Formats (Backward Compatibility)

```rust
impl LogMetadata {
    pub fn decode(buf: &[u8]) -> Result<Self, LogMetadataError> {
        // Check magic
        let magic = &buf[0..8];
        if magic != Self::MAGIC {
            return Err(LogMetadataError::MagicMismatch);
        }

        // Read version
        let version = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);

        // Dispatch to version-specific decoder
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => Err(LogMetadataError::UnsupportedVersion(version)),
        }
    }

    fn decode_v1(buf: &[u8]) -> Result<Self, LogMetadataError> {
        // Current format decoder
        // ...
    }

    fn decode_v2(buf: &[u8]) -> Result<Self, LogMetadataError> {
        // Future format decoder with additional fields
        // ...
    }
}
```

#### 3. Writing New Format

By default, always write the newest supported version:

```rust
impl LogMetadata {
    pub fn encode(&self, buf: &mut [u8]) -> Result<(), LogMetadataError> {
        // Always write newest version
        let version = Self::VERSION_2;
        // ... encode with version 2 format
    }

    // Optional: Support writing old formats for compatibility
    pub fn encode_with_version(&self, buf: &mut [u8], version: u32) -> Result<(), LogMetadataError> {
        match version {
            1 => self.encode_v1(buf),
            2 => self.encode_v2(buf),
            _ => Err(LogMetadataError::UnsupportedVersion(version)),
        }
    }
}
```

#### 4. Migration Tool

Provide a standalone migration utility:

```rust
/// Migrate a log from one version to another
pub fn migrate_log(
    path: &Path,
    from_version: u32,
    to_version: u32,
) -> Result<(), LogError> {
    // 1. Open log with old format (read-only)
    let old_log = FasterLog::open_with_version(config, device, from_version)?;

    // 2. Create new log with new format
    let new_log = FasterLog::create_with_version(config, new_device, to_version)?;

    // 3. Copy all entries
    for (addr, entry) in old_log.scan(old_log.get_begin_address(), old_log.get_committed_until()) {
        new_log.append(&entry)?;
    }

    // 4. Commit and close
    new_log.commit()?;
    new_log.wait_for_commit(new_log.get_tail_address())?;

    Ok(())
}
```

### Backward Compatibility Policy

1. **Minor versions (1.x)**: Must read all previous minor versions
   - Can write new format
   - Must support reading old format for at least 2 minor versions

2. **Major versions (x.0)**: May drop support for old versions
   - Must provide migration tool before dropping support
   - Deprecation notice for at least 2 major versions

3. **Deprecation Timeline**:
   - Version N-2: Announce deprecation
   - Version N-1: Warn on old format detection
   - Version N: Drop support (with migration tool)

### Format Change Guidelines

When considering format changes:

#### Requires Version Bump

- Metadata structure changes (adding/removing/reordering fields)
- Entry header structure changes
- Checksum algorithm changes
- Page size or addressing changes
- Magic number changes

#### May Not Require Version Bump (Use Entry Flags)

- New entry types (use new flag values)
- Optional entry metadata (backward compatible if ignored)
- Performance optimizations that don't change format

#### Example: Add new field without version bump

If the new field can be optional and ignored by old readers:

```rust
// Use reserved bytes in metadata
// Old readers ignore, new readers use
let new_field = if buf.len() > 64 {
    u32::from_le_bytes([buf[64], buf[65], buf[66], buf[67]])
} else {
    0 // Default value
};
```

### Example: Adding Compression (Hypothetical v2)

Suppose version 2 adds optional compression:

```rust
// Version 2 metadata adds:
pub struct LogMetadataV2 {
    // ... all v1 fields ...
    pub compression_algorithm: u8, // 0 = none, 1 = lz4, 2 = zstd
    pub compression_level: u8,
    pub reserved: [u8; 6], // For future use
}

// Entry flag for compressed entries
const ENTRY_FLAG_COMPRESSED: u32 = 1 << 1;

// Entry header for v2 (if entry is compressed)
pub struct CompressedEntryHeader {
    pub uncompressed_length: u32, // Original size
    pub compressed_length: u32,   // Stored size
    pub flags: u32,               // Includes ENTRY_FLAG_COMPRESSED
    pub checksum: u64,            // Checksum of compressed data
}
```

Migration from v1 to v2:
- All v1 entries remain uncompressed
- New entries can be compressed
- v2 readers handle both compressed and uncompressed entries
- v1 readers cannot open v2 logs (version check fails gracefully)

### Testing Strategy

For format versioning:

1. **Backward Compatibility Tests**:
   - Read v1 logs with v2 code
   - Verify all data is accessible
   - Verify metadata is correctly interpreted

2. **Forward Compatibility Tests**:
   - Attempt to read v2 logs with v1 code
   - Verify graceful error (not corruption)
   - Verify clear error message about version

3. **Migration Tests**:
   - Migrate v1 log to v2
   - Verify all entries preserved
   - Verify metadata correctly updated
   - Verify v2 log is valid

4. **Fuzzing**:
   - Fuzz old version formats for robustness
   - Verify version detection is reliable
   - Verify corruption detection still works

### Upgrade Path Recommendation

For users upgrading:

1. **Before Upgrade**:
   - Backup existing logs
   - Note current format version
   - Read release notes for breaking changes

2. **During Upgrade**:
   - Use migration tool if format changed
   - Or: keep old format (if supported)
   - Or: start fresh log (if truncation acceptable)

3. **After Upgrade**:
   - Verify logs are accessible
   - Monitor for any format-related errors
   - Keep old binary available as fallback

### Version History

| Version | Date | Changes |
|---------|------|---------|
| 1 | 2026-01-20 | Initial stable format |
| | | - 64-byte metadata header |
| | | - 16-byte entry header |
| | | - XOR checksums |
| | | - Truncation support |
| | | - Padding records |

