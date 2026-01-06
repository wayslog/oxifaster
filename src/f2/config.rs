//! F2 configuration
//!
//! Configuration for the F2 hot-cold separation architecture.

use crate::cache::ReadCacheConfig;
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the hot store
#[derive(Debug, Clone)]
pub struct HotStoreConfig {
    /// Index size (number of buckets)
    pub index_size: u64,
    /// Log memory size in bytes
    pub log_mem_size: u64,
    /// Log file path
    pub log_path: PathBuf,
    /// Mutable fraction of the log (0.0 to 1.0)
    pub mutable_fraction: f64,
    /// Read cache configuration
    pub read_cache: Option<ReadCacheConfig>,
}

impl Default for HotStoreConfig {
    fn default() -> Self {
        Self {
            index_size: 1 << 20,             // 1M buckets
            log_mem_size: 512 * 1024 * 1024, // 512 MB
            log_path: PathBuf::from("hot_store.log"),
            mutable_fraction: 0.6,
            read_cache: Some(ReadCacheConfig::default()),
        }
    }
}

impl HotStoreConfig {
    /// Create a new hot store configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index size
    pub fn with_index_size(mut self, size: u64) -> Self {
        self.index_size = size;
        self
    }

    /// Set the log memory size
    pub fn with_log_mem_size(mut self, size: u64) -> Self {
        self.log_mem_size = size;
        self
    }

    /// Set the log file path
    pub fn with_log_path(mut self, path: PathBuf) -> Self {
        self.log_path = path;
        self
    }

    /// Set the mutable fraction
    pub fn with_mutable_fraction(mut self, fraction: f64) -> Self {
        self.mutable_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// Set the read cache configuration
    pub fn with_read_cache(mut self, config: Option<ReadCacheConfig>) -> Self {
        self.read_cache = config;
        self
    }
}

/// Configuration for the cold store
#[derive(Debug, Clone)]
pub struct ColdStoreConfig {
    /// Index size (number of buckets)
    pub index_size: u64,
    /// Log memory size in bytes
    pub log_mem_size: u64,
    /// Log file path
    pub log_path: PathBuf,
    /// Mutable fraction of the log (0.0 to 1.0)
    pub mutable_fraction: f64,
}

impl Default for ColdStoreConfig {
    fn default() -> Self {
        Self {
            index_size: 1 << 22,             // 4M buckets
            log_mem_size: 256 * 1024 * 1024, // 256 MB
            log_path: PathBuf::from("cold_store.log"),
            mutable_fraction: 0.0, // Cold store is read-only in memory
        }
    }
}

impl ColdStoreConfig {
    /// Create a new cold store configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index size
    pub fn with_index_size(mut self, size: u64) -> Self {
        self.index_size = size;
        self
    }

    /// Set the log memory size
    pub fn with_log_mem_size(mut self, size: u64) -> Self {
        self.log_mem_size = size;
        self
    }

    /// Set the log file path
    pub fn with_log_path(mut self, path: PathBuf) -> Self {
        self.log_path = path;
        self
    }
}

/// Configuration for F2 compaction
#[derive(Debug, Clone)]
pub struct F2CompactionConfig {
    /// Whether automatic compaction is enabled for hot store
    pub hot_store_enabled: bool,
    /// Whether automatic compaction is enabled for cold store
    pub cold_store_enabled: bool,
    /// Hot store log size budget
    pub hot_log_size_budget: u64,
    /// Cold store log size budget
    pub cold_log_size_budget: u64,
    /// Trigger compaction when log size exceeds this percentage of budget
    pub trigger_percentage: f64,
    /// Percentage of log to compact per round
    pub compact_percentage: f64,
    /// Maximum size to compact per round
    pub max_compact_size: u64,
    /// Check interval for compaction
    pub check_interval: Duration,
    /// Number of compaction threads
    pub num_threads: usize,
}

impl Default for F2CompactionConfig {
    fn default() -> Self {
        Self {
            hot_store_enabled: true,
            cold_store_enabled: true,
            hot_log_size_budget: 1 << 30,   // 1 GB
            cold_log_size_budget: 10 << 30, // 10 GB
            trigger_percentage: 0.9,
            compact_percentage: 0.2,
            max_compact_size: 256 * 1024 * 1024, // 256 MB
            check_interval: Duration::from_secs(1),
            num_threads: 1,
        }
    }
}

impl F2CompactionConfig {
    /// Create a new compaction configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable hot store compaction
    pub fn with_hot_store_enabled(mut self, enabled: bool) -> Self {
        self.hot_store_enabled = enabled;
        self
    }

    /// Enable or disable cold store compaction
    pub fn with_cold_store_enabled(mut self, enabled: bool) -> Self {
        self.cold_store_enabled = enabled;
        self
    }

    /// Set the hot store log size budget
    pub fn with_hot_log_size_budget(mut self, size: u64) -> Self {
        self.hot_log_size_budget = size;
        self
    }

    /// Set the cold store log size budget
    pub fn with_cold_log_size_budget(mut self, size: u64) -> Self {
        self.cold_log_size_budget = size;
        self
    }

    /// Set the trigger percentage
    pub fn with_trigger_percentage(mut self, pct: f64) -> Self {
        self.trigger_percentage = pct.clamp(0.0, 1.0);
        self
    }

    /// Set the check interval
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }
}

/// Main F2 configuration
#[derive(Debug, Clone, Default)]
pub struct F2Config {
    /// Hot store configuration
    pub hot_store: HotStoreConfig,
    /// Cold store configuration
    pub cold_store: ColdStoreConfig,
    /// Compaction configuration
    pub compaction: F2CompactionConfig,
}

impl F2Config {
    /// Create a new F2 configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the hot store configuration
    pub fn with_hot_store(mut self, config: HotStoreConfig) -> Self {
        self.hot_store = config;
        self
    }

    /// Set the cold store configuration
    pub fn with_cold_store(mut self, config: ColdStoreConfig) -> Self {
        self.cold_store = config;
        self
    }

    /// Set the compaction configuration
    pub fn with_compaction(mut self, config: F2CompactionConfig) -> Self {
        self.compaction = config;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        // Minimum log size budget
        const MIN_LOG_SIZE: u64 = 64 * 1024 * 1024; // 64 MB

        if self.compaction.hot_store_enabled && self.compaction.hot_log_size_budget < MIN_LOG_SIZE {
            return Err(format!(
                "Hot log size budget too small (min: {} MB)",
                MIN_LOG_SIZE / (1024 * 1024)
            ));
        }

        if self.compaction.cold_store_enabled && self.compaction.cold_log_size_budget < MIN_LOG_SIZE
        {
            return Err(format!(
                "Cold log size budget too small (min: {} MB)",
                MIN_LOG_SIZE / (1024 * 1024)
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_store_config_default() {
        let config = HotStoreConfig::default();
        assert_eq!(config.index_size, 1 << 20);
        assert_eq!(config.mutable_fraction, 0.6);
        assert!(config.read_cache.is_some());
    }

    #[test]
    fn test_cold_store_config_default() {
        let config = ColdStoreConfig::default();
        assert_eq!(config.index_size, 1 << 22);
        assert_eq!(config.mutable_fraction, 0.0);
    }

    #[test]
    fn test_compaction_config_default() {
        let config = F2CompactionConfig::default();
        assert!(config.hot_store_enabled);
        assert!(config.cold_store_enabled);
        assert_eq!(config.trigger_percentage, 0.9);
    }

    #[test]
    fn test_f2_config_validation() {
        let config = F2Config::default();
        assert!(config.validate().is_ok());

        // Invalid config
        let mut config = F2Config::default();
        config.compaction.hot_log_size_budget = 1 << 20; // 1 MB - too small
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_builder() {
        let config = F2Config::new()
            .with_hot_store(
                HotStoreConfig::new()
                    .with_index_size(1 << 18)
                    .with_log_mem_size(128 * 1024 * 1024),
            )
            .with_compaction(F2CompactionConfig::new().with_hot_store_enabled(false));

        assert_eq!(config.hot_store.index_size, 1 << 18);
        assert!(!config.compaction.hot_store_enabled);
    }
}
