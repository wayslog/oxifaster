use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::checkpoint::IndexMetadata;
use crate::index::{HashBucket, HashBucketEntry, HashBucketOverflowEntry};
use crate::status::Status;

use super::{MemHashIndex, MemHashIndexConfig};

impl MemHashIndex {
    /// Create a checkpoint of the hash index to disk.
    pub fn checkpoint(
        &self,
        checkpoint_dir: &Path,
        token: crate::checkpoint::CheckpointToken,
    ) -> io::Result<IndexMetadata> {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        if !self.tables[version].is_initialized() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Hash index not initialized",
            ));
        }

        let stats = self.dump_distribution();

        let mut metadata = IndexMetadata::with_token(token);
        metadata.version = self.version.load(Ordering::Acquire) as u32;
        metadata.table_size = table_size;
        metadata.num_buckets = stats.buckets_with_entries;
        metadata.num_entries = stats.used_entries;
        metadata.num_ht_bytes = table_size * HashBucket::NUM_ENTRIES as u64 * 8;

        let data_path = checkpoint_dir.join("index.dat");
        self.write_index_data(&data_path)?;

        let meta_path = checkpoint_dir.join("index.meta");
        metadata.write_to_file(&meta_path)?;

        Ok(metadata)
    }

    fn write_index_data(&self, path: &Path) -> io::Result<()> {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(1 << 20, file);

        // index.dat v1:
        // - table_size: u64
        // - overflow_bucket_count: u64
        // - main buckets: (7 entries + 1 overflow ptr) * table_size
        // - overflow buckets: (7 entries + 1 overflow ptr) * overflow_bucket_count
        //
        // To be consistent under concurrent overflow bucket allocation, we write a placeholder
        // overflow count and patch it at the end based on a single overflow bucket snapshot.
        writer.write_all(&table_size.to_le_bytes())?;
        writer.write_all(&0u64.to_le_bytes())?;

        for idx in 0..table_size {
            let bucket = self.tables[version].bucket_at(idx);

            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load(Ordering::Relaxed);
                writer.write_all(&entry.control().to_le_bytes())?;
            }

            let overflow = bucket.overflow_entry.load(Ordering::Relaxed);
            writer.write_all(&overflow.control().to_le_bytes())?;
        }

        let overflow_snapshot = self.overflow_pools[version].snapshot_ptrs();
        let overflow_count = overflow_snapshot.len() as u64;

        for bucket_ptr in overflow_snapshot {
            // SAFETY: snapshot_ptrs returns pointers from Box<HashBucket>, stable for pool lifetime.
            let bucket = unsafe { &*bucket_ptr };

            for i in 0..HashBucket::NUM_ENTRIES {
                let entry = bucket.entries[i].load(Ordering::Relaxed);
                writer.write_all(&entry.control().to_le_bytes())?;
            }

            let overflow = bucket.overflow_entry.load(Ordering::Relaxed);
            writer.write_all(&overflow.control().to_le_bytes())?;
        }

        writer.flush()?;
        writer.seek(SeekFrom::Start(8))?;
        writer.write_all(&overflow_count.to_le_bytes())?;
        writer.flush()?;
        Ok(())
    }

    /// Recover the hash index from a checkpoint.
    pub fn recover(
        &mut self,
        checkpoint_dir: &Path,
        metadata: Option<&IndexMetadata>,
    ) -> io::Result<()> {
        let meta_path = checkpoint_dir.join("index.meta");
        let loaded_metadata;
        let metadata = match metadata {
            Some(m) => m,
            None => {
                loaded_metadata = IndexMetadata::read_from_file(&meta_path)?;
                &loaded_metadata
            }
        };

        let config = MemHashIndexConfig::new(metadata.table_size);
        let status = self.initialize(&config);
        if status != Status::Ok {
            return Err(io::Error::other(format!(
                "Failed to initialize hash index: {status:?}"
            )));
        }

        let data_path = checkpoint_dir.join("index.dat");
        self.read_index_data(&data_path)?;

        self.clear_tentative_entries();

        Ok(())
    }

    fn read_index_data(&mut self, path: &Path) -> io::Result<()> {
        let version = self.version.load(Ordering::Acquire) as usize;

        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(1 << 20, file);

        let mut size_buf = [0u8; 8];
        reader.read_exact(&mut size_buf)?;
        let file_table_size = u64::from_le_bytes(size_buf);

        reader.read_exact(&mut size_buf)?;
        let overflow_count = u64::from_le_bytes(size_buf);

        if file_table_size != self.tables[version].size() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Table size mismatch: file has {}, index has {}",
                    file_table_size,
                    self.tables[version].size()
                ),
            ));
        }

        let mut entry_buf = [0u8; 8];
        for idx in 0..file_table_size {
            let bucket = self.tables[version].bucket_at(idx);

            for i in 0..HashBucket::NUM_ENTRIES {
                reader.read_exact(&mut entry_buf)?;
                let control = u64::from_le_bytes(entry_buf);
                bucket.entries[i].store(HashBucketEntry::from_control(control), Ordering::Release);
            }

            reader.read_exact(&mut entry_buf)?;
            let overflow_control = u64::from_le_bytes(entry_buf);
            bucket.overflow_entry.store(
                HashBucketOverflowEntry::from_control(overflow_control),
                Ordering::Release,
            );
        }

        self.overflow_pools[version].clear();
        for _ in 0..overflow_count {
            let _ = self.overflow_pools[version].allocate();
        }

        for i in 0..overflow_count {
            let addr = crate::index::hash_bucket::FixedPageAddress::new(i + 1);
            let bucket_ptr = self.overflow_pools[version]
                .bucket_ptr(addr)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "overflow bucket missing")
                })?;

            // SAFETY: bucket_ptr points to a valid overflow bucket.
            let bucket = unsafe { &*bucket_ptr };

            for j in 0..HashBucket::NUM_ENTRIES {
                reader.read_exact(&mut entry_buf)?;
                let control = u64::from_le_bytes(entry_buf);
                bucket.entries[j].store(HashBucketEntry::from_control(control), Ordering::Release);
            }

            reader.read_exact(&mut entry_buf)?;
            let overflow_control = u64::from_le_bytes(entry_buf);
            bucket.overflow_entry.store(
                HashBucketOverflowEntry::from_control(overflow_control),
                Ordering::Release,
            );
        }

        Ok(())
    }

    /// Verify that the recovered index is consistent.
    pub fn verify_recovery(&self) -> io::Result<()> {
        let version = self.version.load(Ordering::Acquire) as usize;
        let table_size = self.tables[version].size();

        let mut errors = 0u64;

        for idx in 0..table_size {
            let base_bucket = self.tables[version].bucket_at(idx);
            let mut bucket_ptr: *const HashBucket = base_bucket as *const _;

            loop {
                // SAFETY: `bucket_ptr` points to a valid bucket.
                let bucket = unsafe { &*bucket_ptr };

                for i in 0..HashBucket::NUM_ENTRIES {
                    let entry = bucket.entries[i].load_index(Ordering::Relaxed);
                    if entry.is_tentative() {
                        errors += 1;
                    }
                }

                let overflow = bucket.overflow_entry.load(Ordering::Relaxed);
                if overflow.is_unused() {
                    break;
                }
                let next_ptr = self.overflow_pools[version].bucket_ptr(overflow.address());
                match next_ptr {
                    Some(p) => bucket_ptr = p,
                    None => break,
                }
            }
        }

        if errors > 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Found {errors} corrupted entries during recovery verification"),
            ))
        } else {
            Ok(())
        }
    }
}
