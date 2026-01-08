use crate::address::Address;

/// Store type identifier for internal operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreType {
    /// Hot store (frequently accessed data)
    Hot,
    /// Cold store (infrequently accessed data)
    Cold,
}

/// Read operation stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOperationStage {
    /// Reading from hot log
    HotLogRead,
    /// Reading from cold log
    ColdLogRead,
}

/// RMW operation stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmwOperationStage {
    /// RMW on hot log
    HotLogRmw,
    /// Reading from cold log
    ColdLogRead,
    /// Conditional insert to hot log
    HotLogConditionalInsert,
}

/// Index type for a store
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexType {
    /// In-memory hash index (for hot store)
    #[default]
    MemoryIndex,
    /// On-disk cold index (for cold store in F2 mode)
    ColdIndex,
}

/// Statistics for a single store
#[derive(Debug, Clone)]
pub struct StoreStats {
    /// Current log size
    pub size: u64,
    /// Begin address
    pub begin_address: Address,
    /// Tail address
    pub tail_address: Address,
    /// Safe read-only address
    pub safe_read_only_address: Address,
}
