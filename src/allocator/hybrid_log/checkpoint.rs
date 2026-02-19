use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::address::Address;
use crate::checkpoint::LogMetadata;
use crate::device::StorageDevice;

use super::PersistentMemoryMalloc;

fn rename_tmp_overwrite(src: &Path, dst: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        match std::fs::rename(src, dst) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(dst)?;
                std::fs::rename(src, dst)
            }
            Err(e) => Err(e),
        }
    }

    #[cfg(not(windows))]
    {
        std::fs::rename(src, dst)
    }
}

impl<D: StorageDevice> PersistentMemoryMalloc<D> {
    /// Create checkpoint metadata from the current log state.
    pub fn checkpoint_metadata(
        &self,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        use_snapshot: bool,
    ) -> LogMetadata {
        self.checkpoint_metadata_at(
            token,
            version,
            self.get_begin_address(),
            self.get_tail_address(),
            self.get_flushed_until_address(),
            use_snapshot,
        )
    }

    /// Build checkpoint metadata from explicit addresses.
    ///
    /// This is used by cooperative CPR so that all artifacts (metadata + snapshot/fold-over)
    /// reflect a single coherent `(begin, final, flushed_until)` view even as the store
    /// continues accepting writes.
    pub fn checkpoint_metadata_at(
        &self,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        begin_address: Address,
        final_address: Address,
        flushed_until_address: Address,
        use_snapshot: bool,
    ) -> LogMetadata {
        let mut metadata = LogMetadata::with_token(token);
        metadata.use_snapshot_file = use_snapshot;
        metadata.version = version;
        metadata.begin_address = begin_address;
        metadata.final_address = final_address;
        metadata.flushed_until_address = flushed_until_address;
        metadata.use_object_log = false;
        metadata
    }

    /// Flush the log and write checkpoint data to disk.
    ///
    /// This is a convenience method that uses snapshot mode by default.
    pub fn checkpoint(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
    ) -> io::Result<LogMetadata> {
        self.checkpoint_with_options(checkpoint_dir, token, version, true)
    }

    /// Flush the log and write checkpoint data to disk with options.
    pub fn checkpoint_with_options(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        use_snapshot: bool,
    ) -> io::Result<LogMetadata> {
        let tail_address = self.get_tail_address();

        self.flush_until(tail_address)?;

        let metadata = self.checkpoint_metadata_at(
            token,
            version,
            self.get_begin_address(),
            tail_address,
            self.get_flushed_until_address(),
            use_snapshot,
        );

        let meta_path = checkpoint_dir.join("log.meta");
        metadata.write_to_file(&meta_path)?;

        let snapshot_path = checkpoint_dir.join("log.snapshot");
        self.write_log_snapshot(&snapshot_path, metadata.begin_address, tail_address)?;

        Ok(metadata)
    }

    /// Flush the log and write checkpoint data to disk with session states.
    pub fn checkpoint_with_sessions(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
        version: u32,
        session_states: Vec<crate::checkpoint::SessionState>,
    ) -> io::Result<LogMetadata> {
        let tail_address = self.get_tail_address();

        self.flush_until(tail_address)?;

        let mut metadata = self.checkpoint_metadata_at(
            token,
            version,
            self.get_begin_address(),
            tail_address,
            self.get_flushed_until_address(),
            true,
        );
        metadata.session_states = session_states;
        metadata.num_threads = metadata.session_states.len() as u32;

        let meta_path = checkpoint_dir.join("log.meta");
        metadata.write_to_file(&meta_path)?;

        let snapshot_path = checkpoint_dir.join("log.snapshot");
        self.write_log_snapshot(&snapshot_path, metadata.begin_address, tail_address)?;

        Ok(metadata)
    }

    /// Write an in-memory snapshot of log pages in `[begin, final_address)`.
    ///
    /// The format is compatible with `read_log_snapshot` and is used by snapshot checkpoints.
    pub fn write_log_snapshot(
        &self,
        path: &Path,
        begin: Address,
        final_address: Address,
    ) -> io::Result<()> {
        let parent = path.parent().unwrap_or(Path::new("."));
        let file_name = path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing file name"))?
            .to_string_lossy();
        let tmp_path = parent.join(format!(".{file_name}.tmp"));

        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::with_capacity(1 << 20, file);

        let tail = final_address;
        let page_size = self.config.page_size as u64;

        writer.write_all(&begin.control().to_le_bytes())?;
        writer.write_all(&tail.control().to_le_bytes())?;
        writer.write_all(&page_size.to_le_bytes())?;
        writer.write_all(&(self.buffer_size as u64).to_le_bytes())?;

        let begin_page = begin.page();
        let mut tail_page = tail.page();
        if tail.offset() == 0 {
            if tail_page == 0 {
                writer.write_all(&u64::MAX.to_le_bytes())?;
                let file = writer.into_inner()?;
                file.sync_all()?;
                return rename_tmp_overwrite(&tmp_path, path);
            }
            tail_page = tail_page.saturating_sub(1);
        }

        for page in begin_page..=tail_page {
            if let Some(page_data) = self.pages.get_page(page) {
                writer.write_all(&(page as u64).to_le_bytes())?;
                writer.write_all(page_data)?;
            }
        }

        writer.write_all(&u64::MAX.to_le_bytes())?;

        let file = writer.into_inner()?;
        file.sync_all()?;
        rename_tmp_overwrite(&tmp_path, path)
    }

    /// Recover the log from a checkpoint.
    pub fn recover(
        &mut self,
        checkpoint_dir: &Path,
        metadata: Option<&LogMetadata>,
    ) -> io::Result<()> {
        let meta_path = checkpoint_dir.join("log.meta");
        let loaded_metadata;
        let _metadata = match metadata {
            Some(m) => m,
            None => {
                loaded_metadata = LogMetadata::read_from_file(&meta_path)?;
                &loaded_metadata
            }
        };

        let (snapshot_begin, snapshot_tail) = if _metadata.use_snapshot_file {
            let snapshot_path = checkpoint_dir.join("log.snapshot");
            if !snapshot_path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Log snapshot file missing: {}. Checkpoint appears incomplete or corrupted.",
                        snapshot_path.display()
                    ),
                ));
            }
            self.read_log_snapshot(&snapshot_path)?
        } else {
            let begin = _metadata.begin_address;
            let tail = _metadata.final_address;
            self.read_log_from_device(begin, tail)?;
            (begin, tail)
        };

        self.begin_address.store(snapshot_begin, Ordering::Release);
        self.head_address.store(snapshot_begin, Ordering::Release);
        self.safe_head_address
            .store(snapshot_begin, Ordering::Release);
        self.read_only_address
            .store(snapshot_tail, Ordering::Release);
        self.safe_read_only_address
            .store(snapshot_tail, Ordering::Release);
        self.flushed_until_address
            .store(snapshot_tail, Ordering::Release);
        self.tail_page_offset
            .store_address(snapshot_tail, Ordering::Release);

        Ok(())
    }

    fn read_log_from_device(&mut self, begin: Address, final_address: Address) -> io::Result<()> {
        let page_size = self.config.page_size;
        let begin_page = begin.page();
        let mut tail_page = final_address.page();
        if final_address.offset() == 0 {
            if tail_page == 0 {
                return Ok(());
            }
            tail_page = tail_page.saturating_sub(1);
        }

        let mut reads = Vec::new();
        for page in begin_page..=tail_page {
            if !self.pages.allocate_page(page, page_size) {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("Failed to allocate page {page} during recovery"),
                ));
            }

            let Some(dst) = self.pages.get_page_mut(page) else {
                return Err(io::Error::other(format!(
                    "Page {page} not available after allocation"
                )));
            };

            let offset = Address::new(page, 0).control();
            reads.push((page, offset, dst.as_mut_ptr(), dst.len()));
        }

        let device = self.device.clone();
        let fut = async move {
            for (page, offset, ptr, len) in reads {
                // SAFETY: ptr/len come from AlignedBuffer which remains valid for the async block's lifetime.
                let dst = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                let n = device.read(offset, dst).await?;
                if n != len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("short read for page {page}: expected {len}, got {n}"),
                    ));
                }
            }

            Ok(())
        };

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                    return Err(io::Error::other(
                        "Cannot recover within a current-thread Tokio runtime. \
                         Call recovery from a blocking thread (e.g., spawn_blocking) or outside Tokio.",
                    ));
                }

                tokio::task::block_in_place(|| handle.block_on(fut))
            }
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(fut)
            }
        }
    }

    fn read_log_snapshot(&mut self, path: &Path) -> io::Result<(Address, Address)> {
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(1 << 20, file);

        let mut buf = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let begin_address = Address::from_control(u64::from_le_bytes(buf));

        reader.read_exact(&mut buf)?;
        let tail_address = Address::from_control(u64::from_le_bytes(buf));

        reader.read_exact(&mut buf)?;
        let page_size = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let checkpoint_buffer_size = u64::from_le_bytes(buf);

        if page_size != self.config.page_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Page size mismatch: file has {}, allocator has {}",
                    page_size, self.config.page_size
                ),
            ));
        }

        let recovery_buffer_size = self.buffer_size as u64;
        if recovery_buffer_size < checkpoint_buffer_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Buffer size mismatch: checkpoint has {checkpoint_buffer_size} pages, recovery allocator has {recovery_buffer_size} pages. \
                     Recovery buffer size must be >= checkpoint buffer size to prevent data corruption."
                ),
            ));
        }

        let mut page_data = vec![0u8; self.config.page_size];
        loop {
            reader.read_exact(&mut buf)?;
            let page_num = u64::from_le_bytes(buf);

            if page_num == u64::MAX {
                break;
            }

            reader.read_exact(&mut page_data)?;

            let page = page_num as u32;
            if !self.pages.allocate_page(page, self.config.page_size) {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("Failed to allocate page {page} during recovery"),
                ));
            }

            if let Some(dest) = self.pages.get_page_mut(page) {
                dest.copy_from_slice(&page_data);
            } else {
                return Err(io::Error::other(format!(
                    "Page {page} not available after allocation"
                )));
            }
        }

        Ok((begin_address, tail_address))
    }
}
