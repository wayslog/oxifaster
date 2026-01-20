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
