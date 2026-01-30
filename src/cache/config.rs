//! Read cache configuration
//!
//! Based on C++ FASTER's read_cache_utils.h ReadCacheConfig.

use std::sync::Arc;

/// Callback type invoked when a cache entry is evicted.
///
/// The callback receives references to the raw encoded key and value bytes.
/// This allows users to perform cleanup, logging, or other actions when
/// entries are removed from the read cache.
///
/// # Example
/// ```ignore
/// use std::sync::Arc;
/// use oxifaster::cache::EvictCallback;
///
/// let callback: EvictCallback = Arc::new(|key_bytes, value_bytes| {
///     println!("Evicted entry: key={} bytes, value={} bytes",
///              key_bytes.len(), value_bytes.len());
/// });
/// ```
pub type EvictCallback = Arc<dyn Fn(&[u8], &[u8]) + Send + Sync>;

/// Configuration for the read cache
#[derive(Clone)]
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
    /// Optional callback invoked when entries are evicted from the cache
    pub evict_callback: Option<EvictCallback>,
}

impl std::fmt::Debug for ReadCacheConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadCacheConfig")
            .field("mem_size", &self.mem_size)
            .field("mutable_fraction", &self.mutable_fraction)
            .field("pre_allocate", &self.pre_allocate)
            .field("copy_to_tail", &self.copy_to_tail)
            .field("evict_callback", &self.evict_callback.is_some())
            .finish()
    }
}

impl Default for ReadCacheConfig {
    fn default() -> Self {
        Self {
            mem_size: 256 * 1024 * 1024, // 256 MB
            mutable_fraction: 0.9,
            pre_allocate: false,
            copy_to_tail: true,
            evict_callback: None,
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

    /// Set the eviction callback.
    ///
    /// The callback will be invoked when entries are evicted from the cache,
    /// receiving references to the raw encoded key and value bytes.
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    ///
    /// let config = ReadCacheConfig::new(1024 * 1024)
    ///     .with_evict_callback(Arc::new(|key_bytes, value_bytes| {
    ///         println!("Evicted: key={} bytes", key_bytes.len());
    ///     }));
    /// ```
    pub fn with_evict_callback(mut self, callback: EvictCallback) -> Self {
        self.evict_callback = Some(callback);
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
        assert!(config.evict_callback.is_none());
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
    fn test_evict_callback() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let call_count = Arc::new(AtomicU64::new(0));
        let count_clone = call_count.clone();

        let callback: EvictCallback = Arc::new(move |_key, _value| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        let config = ReadCacheConfig::new(1024 * 1024).with_evict_callback(callback);
        assert!(config.evict_callback.is_some());

        // Invoke the callback manually to test it works
        if let Some(cb) = &config.evict_callback {
            cb(b"key", b"value");
        }
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_region_sizes() {
        let config = ReadCacheConfig::new(100 * 1024 * 1024).with_mutable_fraction(0.9);

        // Allow for floating point rounding
        let mutable = config.mutable_size();
        let read_only = config.read_only_size();

        assert!((89 * 1024 * 1024..=91 * 1024 * 1024).contains(&mutable));
        assert!((9 * 1024 * 1024..=11 * 1024 * 1024).contains(&read_only));
        assert!(mutable + read_only <= 100 * 1024 * 1024);
    }
}
