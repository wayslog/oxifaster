use crate::address::Address;
use crate::checkpoint::IndexMetadata;
use crate::index::{
    AtomicHashBucketEntry, ColdIndex, ColdIndexConfig, FindResult, HashBucketEntry,
    IndexHashBucketEntry, KeyHash, MemHashIndex, MemHashIndexConfig,
};
use crate::status::Status;

/// Unified index interface for F2 stores
///
/// This enum allows switching between in-memory and cold indexes
/// depending on the store configuration.
pub enum StoreIndex {
    /// In-memory hash index
    Memory(Box<MemHashIndex>),
    /// On-disk cold index
    Cold(Box<ColdIndex>),
}

impl StoreIndex {
    /// Create a new memory index
    pub fn new_memory(table_size: u64) -> Self {
        let mut index = MemHashIndex::new();
        let config = MemHashIndexConfig::new(table_size);
        index.initialize(&config);
        Self::Memory(Box::new(index))
    }

    /// Create a new cold index
    pub fn new_cold(config: ColdIndexConfig) -> Result<Self, Status> {
        let mut index = ColdIndex::new(config);
        index.initialize()?;
        Ok(Self::Cold(Box::new(index)))
    }

    /// Check if this is a memory index
    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// Check if this is a cold index
    pub fn is_cold(&self) -> bool {
        matches!(self, Self::Cold(_))
    }

    /// Get as memory index reference
    pub fn as_memory(&self) -> Option<&MemHashIndex> {
        match self {
            Self::Memory(idx) => Some(idx.as_ref()),
            _ => None,
        }
    }

    /// Get as cold index reference
    pub fn as_cold(&self) -> Option<&ColdIndex> {
        match self {
            Self::Cold(idx) => Some(idx.as_ref()),
            _ => None,
        }
    }

    /// Get as mutable cold index reference
    pub fn as_cold_mut(&mut self) -> Option<&mut ColdIndex> {
        match self {
            Self::Cold(idx) => Some(idx.as_mut()),
            _ => None,
        }
    }

    /// Find entry by key hash
    pub fn find_entry(&self, hash: KeyHash) -> FindResult {
        match self {
            Self::Memory(idx) => idx.find_entry(hash),
            Self::Cold(idx) => {
                // Convert ColdIndexFindResult to FindResult
                // NOTE: ColdIndex returns HashBucketEntry (address only, no tag)
                // We must reconstruct IndexHashBucketEntry with the correct tag from hash
                let result = idx.find_entry(hash);
                let entry = if result.entry.is_unused() {
                    IndexHashBucketEntry::INVALID
                } else {
                    // Preserve tag from the key hash for proper collision detection
                    IndexHashBucketEntry::new(result.entry.address(), hash.tag(), false)
                };
                FindResult {
                    entry,
                    atomic_entry: None, // Cold index doesn't provide atomic entry pointer
                }
            }
        }
    }

    /// Find or create entry by key hash
    pub fn find_or_create_entry(&self, hash: KeyHash) -> FindResult {
        match self {
            Self::Memory(idx) => idx.find_or_create_entry(hash),
            Self::Cold(idx) => {
                // Convert ColdIndexFindResult to FindResult
                // NOTE: ColdIndex returns HashBucketEntry (address only, no tag)
                // We must reconstruct IndexHashBucketEntry with the correct tag from hash
                let result = idx.find_or_create_entry(hash);
                let entry = if result.entry.is_unused() {
                    IndexHashBucketEntry::INVALID
                } else {
                    // Preserve tag from the key hash for proper collision detection
                    IndexHashBucketEntry::new(result.entry.address(), hash.tag(), false)
                };
                FindResult {
                    entry,
                    atomic_entry: None, // Cold index doesn't provide atomic entry pointer
                }
            }
        }
    }

    /// Garbage collect entries before the given address
    pub fn garbage_collect(&self, new_begin_address: Address) {
        match self {
            Self::Memory(idx) => {
                let _ = idx.garbage_collect(new_begin_address);
            }
            Self::Cold(idx) => {
                let _ = idx.garbage_collect(new_begin_address);
            }
        }
    }

    /// Try to update an entry atomically（对 MemoryIndex 使用 CAS；对 ColdIndex 使用 chunk CAS）。
    pub fn try_update_entry(
        &self,
        atomic_entry: Option<*const AtomicHashBucketEntry>,
        expected: IndexHashBucketEntry,
        new_address: Address,
        hash: KeyHash,
        read_cache: bool,
    ) -> Status {
        match self {
            Self::Memory(idx) => {
                let Some(ptr) = atomic_entry else {
                    return Status::OutOfMemory;
                };
                idx.try_update_entry(
                    ptr,
                    expected.to_hash_bucket_entry(),
                    new_address,
                    hash.tag(),
                    read_cache,
                )
            }
            Self::Cold(idx) => {
                let expected_entry =
                    if expected.is_unused() || expected.address() == Address::INVALID {
                        HashBucketEntry::INVALID
                    } else {
                        HashBucketEntry::new(expected.address())
                    };
                idx.try_update_entry(hash, expected_entry, new_address)
            }
        }
    }

    /// Checkpoint the index
    pub fn checkpoint(
        &self,
        dir: &std::path::Path,
        token: uuid::Uuid,
    ) -> Result<IndexMetadata, Status> {
        match self {
            Self::Memory(idx) => idx.checkpoint(dir, token).map_err(|_| Status::Corruption),
            Self::Cold(idx) => {
                // For cold index, we start checkpoint and return basic metadata
                if idx.start_checkpoint() {
                    idx.complete_checkpoint(true);
                    let mut metadata = IndexMetadata::with_token(token);
                    metadata.table_size = idx.size();
                    Ok(metadata)
                } else {
                    Err(Status::Aborted)
                }
            }
        }
    }

    /// Recover the index
    pub fn recover(
        &mut self,
        dir: &std::path::Path,
        metadata: Option<&IndexMetadata>,
    ) -> Result<IndexMetadata, Status> {
        match self {
            Self::Memory(idx) => {
                idx.recover(dir, metadata).map_err(|_| Status::Corruption)?;
                let recovered_metadata = if let Some(meta) = metadata {
                    meta.clone()
                } else {
                    let meta_path = dir.join("index.meta");
                    IndexMetadata::read_from_file(&meta_path).map_err(|_| Status::Corruption)?
                };
                Ok(recovered_metadata)
            }
            Self::Cold(idx) => {
                let token = metadata.map(|m| m.token).unwrap_or_default();
                let mut recovered_metadata = IndexMetadata::with_token(token);
                recovered_metadata.table_size = idx.size();
                Ok(recovered_metadata)
            }
        }
    }
}
