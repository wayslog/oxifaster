//! Read cache configuration
//!
//! Based on C++ FASTER's read_cache_utils.h ReadCacheConfig.

/// Configuration for the read cache
#[derive(Debug, Clone)]
pub struct ReadCacheConfig {
    /// Size of the read cache in bytes
    pub mem_size: u64,
    /// Fraction of the cache that is mutable (0.0 to 1.0)
    /// The rest is read-only and subject to eviction
    pub mutable_fraction: f64,
    /// Whether to pre-allocate memory for the cache
    pub pre_allocate: bool,
    /// Whether to copy records to tail on read (Copy-to-Tail optimization)
    pub copy_to_tail: bool,
}

impl Default for ReadCacheConfig {
    fn default() -> Self {
        Self {
            mem_size: 256 * 1024 * 1024, // 256 MB
            mutable_fraction: 0.9,
            pre_allocate: false,
            copy_to_tail: true,
        }
    }
}

impl ReadCacheConfig {
    /// Create a new read cache configuration
    pub fn new(mem_size: u64) -> Self {
        Self {
            mem_size,
            ..Default::default()
        }
    }

    /// Set the mutable fraction
    pub fn with_mutable_fraction(mut self, fraction: f64) -> Self {
        self.mutable_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// Set whether to pre-allocate memory
    pub fn with_pre_allocate(mut self, pre_allocate: bool) -> Self {
        self.pre_allocate = pre_allocate;
        self
    }

    /// Set whether to copy records to tail on read
    pub fn with_copy_to_tail(mut self, copy_to_tail: bool) -> Self {
        self.copy_to_tail = copy_to_tail;
        self
    }

    /// Calculate the read-only region size
    pub fn read_only_size(&self) -> u64 {
        ((self.mem_size as f64) * (1.0 - self.mutable_fraction)) as u64
    }

    /// Calculate the mutable region size
    pub fn mutable_size(&self) -> u64 {
        ((self.mem_size as f64) * self.mutable_fraction) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReadCacheConfig::default();
        assert_eq!(config.mem_size, 256 * 1024 * 1024);
        assert_eq!(config.mutable_fraction, 0.9);
        assert!(!config.pre_allocate);
        assert!(config.copy_to_tail);
    }

    #[test]
    fn test_config_builder() {
        let config = ReadCacheConfig::new(512 * 1024 * 1024)
            .with_mutable_fraction(0.8)
            .with_pre_allocate(true)
            .with_copy_to_tail(false);

        assert_eq!(config.mem_size, 512 * 1024 * 1024);
        assert_eq!(config.mutable_fraction, 0.8);
        assert!(config.pre_allocate);
        assert!(!config.copy_to_tail);
    }

    #[test]
    fn test_region_sizes() {
        let config = ReadCacheConfig::new(100 * 1024 * 1024).with_mutable_fraction(0.9);

        // Allow for floating point rounding
        let mutable = config.mutable_size();
        let read_only = config.read_only_size();

        assert!(mutable >= 89 * 1024 * 1024 && mutable <= 91 * 1024 * 1024);
        assert!(read_only >= 9 * 1024 * 1024 && read_only <= 11 * 1024 * 1024);
        assert!(mutable + read_only <= 100 * 1024 * 1024);
    }
}
