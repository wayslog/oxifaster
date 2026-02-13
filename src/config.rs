//! Configuration loading helpers.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::cache::ReadCacheConfig;
use crate::compaction::CompactionConfig;
use crate::device::FileSystemDisk;
use crate::store::FasterKvConfig;

/// Errors returned by configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// I/O error while reading config files.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// TOML parse error.
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),
    /// Invalid value for a key.
    #[error("invalid value for {key}: {value}")]
    InvalidValue {
        /// Configuration key.
        key: String,
        /// Raw value string.
        value: String,
    },
    /// Unknown configuration key.
    #[error("unknown config key: {0}")]
    UnknownKey(String),
    /// Missing required configuration field.
    #[error("missing required field: {0}")]
    MissingField(String),
}

/// Top-level configuration schema.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OxifasterConfig {
    /// Store configuration.
    pub store: Option<StoreConfig>,
    /// Compaction configuration.
    pub compaction: Option<CompactionConfigSpec>,
    /// Read cache configuration.
    pub cache: Option<ReadCacheConfigSpec>,
    /// Device configuration.
    pub device: Option<DeviceConfigSpec>,
}

impl OxifasterConfig {
    /// Load configuration from a TOML file.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        Ok(toml::from_str(&contents)?)
    }

    /// Load configuration from the `OXIFASTER_CONFIG` env var (if set),
    /// then apply `OXIFASTER__section__field` overrides.
    pub fn load_from_env() -> Result<Self, ConfigError> {
        let config_path = env::var("OXIFASTER_CONFIG").ok();
        let mut config = match config_path {
            Some(path) => Self::load_from_path(path)?,
            None => Self::default(),
        };
        config.apply_env_overrides()?;
        Ok(config)
    }

    /// Apply environment overrides in-place.
    pub fn apply_env_overrides(&mut self) -> Result<(), ConfigError> {
        for (key, value) in env::vars() {
            if !key.starts_with("OXIFASTER__") {
                continue;
            }
            let path = key["OXIFASTER__".len()..].to_ascii_lowercase();
            let parts: Vec<&str> = path.split("__").collect();
            let value = value.trim().to_string();

            match parts.as_slice() {
                ["store", "table_size"] => {
                    self.store_mut().table_size = Some(parse_value(&key, &value)?);
                }
                ["store", "log_memory_size"] => {
                    self.store_mut().log_memory_size = Some(parse_value(&key, &value)?);
                }
                ["store", "page_size_bits"] => {
                    self.store_mut().page_size_bits = Some(parse_value(&key, &value)?);
                }
                ["store", "mutable_fraction"] => {
                    self.store_mut().mutable_fraction = Some(parse_value(&key, &value)?);
                }
                ["compaction", "target_utilization"] => {
                    self.compaction_mut().target_utilization = Some(parse_value(&key, &value)?);
                }
                ["compaction", "min_compact_bytes"] => {
                    self.compaction_mut().min_compact_bytes = Some(parse_value(&key, &value)?);
                }
                ["compaction", "max_compact_bytes"] => {
                    self.compaction_mut().max_compact_bytes = Some(parse_value(&key, &value)?);
                }
                ["compaction", "num_threads"] => {
                    self.compaction_mut().num_threads = Some(parse_value(&key, &value)?);
                }
                ["compaction", "compact_tombstones"] => {
                    self.compaction_mut().compact_tombstones = Some(parse_value(&key, &value)?);
                }
                ["cache", "enabled"] => {
                    self.cache_mut().enabled = Some(parse_value(&key, &value)?);
                }
                ["cache", "mem_size"] => {
                    self.cache_mut().mem_size = Some(parse_value(&key, &value)?);
                }
                ["cache", "mutable_fraction"] => {
                    self.cache_mut().mutable_fraction = Some(parse_value(&key, &value)?);
                }
                ["cache", "pre_allocate"] => {
                    self.cache_mut().pre_allocate = Some(parse_value(&key, &value)?);
                }
                ["cache", "copy_to_tail"] => {
                    self.cache_mut().copy_to_tail = Some(parse_value(&key, &value)?);
                }
                ["device", "kind"] => {
                    self.device_mut().kind = Some(value.to_string());
                }
                ["device", "path"] => {
                    self.device_mut().path = Some(PathBuf::from(value));
                }
                ["device", "base_dir"] => {
                    self.device_mut().base_dir = Some(PathBuf::from(value));
                }
                ["device", "prefix"] => {
                    self.device_mut().prefix = Some(value.to_string());
                }
                ["device", "segment_size"] => {
                    self.device_mut().segment_size = Some(parse_value(&key, &value)?);
                }
                _ => return Err(ConfigError::UnknownKey(key)),
            }
        }

        Ok(())
    }

    /// Build a `FasterKvConfig` using defaults plus overrides.
    pub fn to_faster_kv_config(&self) -> FasterKvConfig {
        let mut config = FasterKvConfig::default();
        if let Some(store) = &self.store {
            store.apply_to(&mut config);
        }
        config
    }

    /// Build a `CompactionConfig` using defaults plus overrides.
    pub fn to_compaction_config(&self) -> CompactionConfig {
        let mut config = CompactionConfig::default();
        if let Some(compaction) = &self.compaction {
            compaction.apply_to(&mut config);
        }
        config
    }

    /// Build a `ReadCacheConfig` when cache is enabled.
    pub fn to_read_cache_config(&self) -> Option<ReadCacheConfig> {
        let cache = self.cache.as_ref()?;
        if cache.enabled != Some(true) {
            return None;
        }

        let mut config = ReadCacheConfig::default();
        cache.apply_to(&mut config);
        Some(config)
    }

    /// Resolve a device configuration, if present.
    pub fn device_config(&self) -> Result<Option<DeviceConfig>, ConfigError> {
        match self.device.as_ref() {
            Some(spec) => Ok(Some(spec.resolve()?)),
            None => Ok(None),
        }
    }

    /// Open a file-based device from the configuration, if present.
    pub fn open_device(&self) -> Result<Option<FileSystemDisk>, ConfigError> {
        match self.device_config()? {
            Some(device) => Ok(Some(device.open()?)),
            None => Ok(None),
        }
    }

    fn store_mut(&mut self) -> &mut StoreConfig {
        if self.store.is_none() {
            self.store = Some(StoreConfig::default());
        }
        self.store.as_mut().expect("store config")
    }

    fn compaction_mut(&mut self) -> &mut CompactionConfigSpec {
        if self.compaction.is_none() {
            self.compaction = Some(CompactionConfigSpec::default());
        }
        self.compaction.as_mut().expect("compaction config")
    }

    fn cache_mut(&mut self) -> &mut ReadCacheConfigSpec {
        if self.cache.is_none() {
            self.cache = Some(ReadCacheConfigSpec::default());
        }
        self.cache.as_mut().expect("cache config")
    }

    fn device_mut(&mut self) -> &mut DeviceConfigSpec {
        if self.device.is_none() {
            self.device = Some(DeviceConfigSpec::default());
        }
        self.device.as_mut().expect("device config")
    }
}

/// Store configuration overrides.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StoreConfig {
    /// Initial hash table size.
    pub table_size: Option<u64>,
    /// Log memory size in bytes.
    pub log_memory_size: Option<u64>,
    /// Log page size bits.
    pub page_size_bits: Option<u32>,
    /// Mutable fraction of log memory.
    pub mutable_fraction: Option<f64>,
}

impl StoreConfig {
    fn apply_to(&self, config: &mut FasterKvConfig) {
        if let Some(value) = self.table_size {
            config.table_size = value;
        }
        if let Some(value) = self.log_memory_size {
            config.log_memory_size = value;
        }
        if let Some(value) = self.page_size_bits {
            config.page_size_bits = value;
        }
        if let Some(value) = self.mutable_fraction {
            config.mutable_fraction = value;
        }
    }
}

/// Compaction configuration overrides.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CompactionConfigSpec {
    /// Target utilization ratio.
    pub target_utilization: Option<f64>,
    /// Minimum bytes to compact.
    pub min_compact_bytes: Option<u64>,
    /// Maximum bytes to compact.
    pub max_compact_bytes: Option<u64>,
    /// Number of threads.
    pub num_threads: Option<usize>,
    /// Whether to compact tombstones.
    pub compact_tombstones: Option<bool>,
}

impl CompactionConfigSpec {
    fn apply_to(&self, config: &mut CompactionConfig) {
        if let Some(value) = self.target_utilization {
            config.target_utilization = value.clamp(0.0, 1.0);
        }
        if let Some(value) = self.min_compact_bytes {
            config.min_compact_bytes = value;
        }
        if let Some(value) = self.max_compact_bytes {
            config.max_compact_bytes = value;
        }
        if let Some(value) = self.num_threads {
            config.num_threads = value.max(1);
        }
        if let Some(value) = self.compact_tombstones {
            config.compact_tombstones = value;
        }
    }
}

/// Read cache configuration overrides.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReadCacheConfigSpec {
    /// Whether to enable read cache.
    pub enabled: Option<bool>,
    /// Cache size in bytes.
    pub mem_size: Option<u64>,
    /// Mutable fraction.
    pub mutable_fraction: Option<f64>,
    /// Whether to pre-allocate cache memory.
    pub pre_allocate: Option<bool>,
    /// Whether to copy records to tail on read.
    pub copy_to_tail: Option<bool>,
}

impl ReadCacheConfigSpec {
    fn apply_to(&self, config: &mut ReadCacheConfig) {
        if let Some(value) = self.mem_size {
            config.mem_size = value;
        }
        if let Some(value) = self.mutable_fraction {
            config.mutable_fraction = value.clamp(0.0, 1.0);
        }
        if let Some(value) = self.pre_allocate {
            config.pre_allocate = value;
        }
        if let Some(value) = self.copy_to_tail {
            config.copy_to_tail = value;
        }
    }
}

/// Device configuration from TOML/env.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DeviceConfigSpec {
    /// Device kind: "single_file" or "segmented".
    pub kind: Option<String>,
    /// Path for single-file device.
    pub path: Option<PathBuf>,
    /// Base directory for segmented device.
    pub base_dir: Option<PathBuf>,
    /// File prefix for segmented device.
    pub prefix: Option<String>,
    /// Segment size for segmented device.
    pub segment_size: Option<u64>,
}

impl DeviceConfigSpec {
    fn resolve(&self) -> Result<DeviceConfig, ConfigError> {
        let kind = self.kind.as_deref().map(|v| v.to_ascii_lowercase());

        match kind.as_deref() {
            Some("single_file") => {
                let path = self
                    .path
                    .clone()
                    .ok_or_else(|| ConfigError::MissingField("device.path".into()))?;
                Ok(DeviceConfig::SingleFile { path })
            }
            Some("segmented") => {
                let base_dir = self
                    .base_dir
                    .clone()
                    .ok_or_else(|| ConfigError::MissingField("device.base_dir".into()))?;
                let prefix = self
                    .prefix
                    .clone()
                    .ok_or_else(|| ConfigError::MissingField("device.prefix".into()))?;
                let segment_size = self
                    .segment_size
                    .ok_or_else(|| ConfigError::MissingField("device.segment_size".into()))?;
                Ok(DeviceConfig::Segmented {
                    base_dir,
                    prefix,
                    segment_size,
                })
            }
            None => {
                if let Some(path) = &self.path {
                    return Ok(DeviceConfig::SingleFile { path: path.clone() });
                }
                if self.base_dir.is_some() || self.prefix.is_some() || self.segment_size.is_some() {
                    let base_dir = self
                        .base_dir
                        .clone()
                        .ok_or_else(|| ConfigError::MissingField("device.base_dir".into()))?;
                    let prefix = self
                        .prefix
                        .clone()
                        .ok_or_else(|| ConfigError::MissingField("device.prefix".into()))?;
                    let segment_size = self
                        .segment_size
                        .ok_or_else(|| ConfigError::MissingField("device.segment_size".into()))?;
                    return Ok(DeviceConfig::Segmented {
                        base_dir,
                        prefix,
                        segment_size,
                    });
                }
                Err(ConfigError::MissingField("device.kind".into()))
            }
            Some(other) => Err(ConfigError::InvalidValue {
                key: "device.kind".into(),
                value: other.into(),
            }),
        }
    }
}

/// Resolved device configuration.
#[derive(Debug, Clone)]
pub enum DeviceConfig {
    /// Single file device.
    SingleFile {
        /// Path to the device file.
        path: PathBuf,
    },
    /// Segmented device.
    Segmented {
        /// Base directory for segments.
        base_dir: PathBuf,
        /// Segment filename prefix.
        prefix: String,
        /// Segment size in bytes.
        segment_size: u64,
    },
}

impl DeviceConfig {
    /// Open the file-based device described by this config.
    pub fn open(&self) -> Result<FileSystemDisk, ConfigError> {
        match self {
            DeviceConfig::SingleFile { path } => Ok(FileSystemDisk::single_file(path)?),
            DeviceConfig::Segmented {
                base_dir,
                prefix,
                segment_size,
            } => Ok(FileSystemDisk::segmented(base_dir, prefix, *segment_size)?),
        }
    }
}

fn parse_value<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, ConfigError> {
    value.parse().map_err(|_| ConfigError::InvalidValue {
        key: key.to_string(),
        value: value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_env_overrides_store_and_cache() {
        let _guard = ENV_LOCK.lock().unwrap();

        unsafe {
            env::set_var("OXIFASTER__store__table_size", "2048");
            env::set_var("OXIFASTER__cache__enabled", "true");
            env::set_var("OXIFASTER__cache__mem_size", "1048576");
        }

        let mut config = OxifasterConfig::default();
        config.apply_env_overrides().unwrap();

        unsafe {
            env::remove_var("OXIFASTER__store__table_size");
            env::remove_var("OXIFASTER__cache__enabled");
            env::remove_var("OXIFASTER__cache__mem_size");
        }

        let store = config.store.unwrap();
        assert_eq!(store.table_size, Some(2048));

        let cache = config.cache.unwrap();
        assert_eq!(cache.enabled, Some(true));
        assert_eq!(cache.mem_size, Some(1048576));
    }

    #[test]
    fn test_device_config_resolve_single_file() {
        let spec = DeviceConfigSpec {
            kind: Some("single_file".to_string()),
            path: Some(PathBuf::from("/tmp/oxifaster.db")),
            base_dir: None,
            prefix: None,
            segment_size: None,
        };

        let resolved = spec.resolve().unwrap();
        match resolved {
            DeviceConfig::SingleFile { path } => {
                assert!(path.ends_with("oxifaster.db"));
            }
            DeviceConfig::Segmented { .. } => panic!("expected single file config"),
        }
    }

    #[test]
    fn test_config_default() {
        let config = OxifasterConfig::default();
        assert!(config.store.is_none());
        assert!(config.compaction.is_none());
        assert!(config.cache.is_none());
        assert!(config.device.is_none());
    }

    #[test]
    fn test_to_faster_kv_config_default() {
        let config = OxifasterConfig::default();
        let kv_config = config.to_faster_kv_config();
        assert!(kv_config.table_size > 0);
    }

    #[test]
    fn test_to_compaction_config_default() {
        let config = OxifasterConfig::default();
        let compaction_config = config.to_compaction_config();
        assert!(compaction_config.target_utilization > 0.0);
    }

    #[test]
    fn test_to_read_cache_config_disabled() {
        let config = OxifasterConfig::default();
        let cache_config = config.to_read_cache_config();
        assert!(cache_config.is_none());
    }

    #[test]
    fn test_to_read_cache_config_enabled() {
        let config = OxifasterConfig {
            store: None,
            compaction: None,
            cache: Some(ReadCacheConfigSpec {
                enabled: Some(true),
                mem_size: Some(1024 * 1024),
                mutable_fraction: None,
                pre_allocate: None,
                copy_to_tail: None,
            }),
            device: None,
        };
        let cache_config = config.to_read_cache_config();
        assert!(cache_config.is_some());
    }

    #[test]
    fn test_device_config_none() {
        let config = OxifasterConfig::default();
        let device_config = config.device_config().unwrap();
        assert!(device_config.is_none());
    }

    #[test]
    fn test_device_config_resolve_segmented() {
        let spec = DeviceConfigSpec {
            kind: Some("segmented".to_string()),
            path: None,
            base_dir: Some(PathBuf::from("/tmp")),
            prefix: Some("oxifaster".to_string()),
            segment_size: Some(1 << 30),
        };

        let resolved = spec.resolve().unwrap();
        match resolved {
            DeviceConfig::Segmented {
                base_dir,
                prefix,
                segment_size,
            } => {
                assert!(base_dir.ends_with("tmp"));
                assert_eq!(prefix, "oxifaster");
                assert_eq!(segment_size, 1 << 30);
            }
            DeviceConfig::SingleFile { .. } => panic!("expected segmented config"),
        }
    }

    #[test]
    fn test_device_config_resolve_error() {
        let spec = DeviceConfigSpec {
            kind: Some("unknown".to_string()),
            path: None,
            base_dir: None,
            prefix: None,
            segment_size: None,
        };

        let result = spec.resolve();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_value_integer() {
        let result: Result<u64, ConfigError> = parse_value("test_key", "12345");
        assert_eq!(result.unwrap(), 12345);
    }

    #[test]
    fn test_parse_value_float() {
        let result: Result<f64, ConfigError> = parse_value("test_key", "0.75");
        assert_eq!(result.unwrap(), 0.75);
    }

    #[test]
    fn test_parse_value_bool() {
        let result: Result<bool, ConfigError> = parse_value("test_key", "true");
        assert!(result.unwrap());

        let result: Result<bool, ConfigError> = parse_value("test_key", "false");
        assert!(!result.unwrap());
    }

    #[test]
    fn test_env_overrides_compaction() {
        let _guard = ENV_LOCK.lock().unwrap();

        unsafe {
            env::set_var("OXIFASTER__compaction__target_utilization", "0.8");
            env::set_var("OXIFASTER__compaction__min_compact_bytes", "1024");
        }

        let mut config = OxifasterConfig::default();
        config.apply_env_overrides().unwrap();

        unsafe {
            env::remove_var("OXIFASTER__compaction__target_utilization");
            env::remove_var("OXIFASTER__compaction__min_compact_bytes");
        }

        let compaction = config.compaction.unwrap();
        assert_eq!(compaction.target_utilization, Some(0.8));
        assert_eq!(compaction.min_compact_bytes, Some(1024));
    }

    #[test]
    fn test_env_overrides_device() {
        let _guard = ENV_LOCK.lock().unwrap();

        unsafe {
            env::set_var("OXIFASTER__device__kind", "single_file");
            env::set_var("OXIFASTER__device__path", "/tmp/test.db");
        }

        let mut config = OxifasterConfig::default();
        config.apply_env_overrides().unwrap();

        unsafe {
            env::remove_var("OXIFASTER__device__kind");
            env::remove_var("OXIFASTER__device__path");
        }

        let device = config.device.unwrap();
        assert_eq!(device.kind, Some("single_file".to_string()));
        assert_eq!(device.path, Some(PathBuf::from("/tmp/test.db")));
    }

    #[test]
    fn test_store_config_apply_to() {
        let store_config = StoreConfig {
            table_size: Some(2048),
            log_memory_size: Some(1 << 20),
            page_size_bits: Some(14),
            mutable_fraction: Some(0.8),
        };

        let mut kv_config = FasterKvConfig::default();
        store_config.apply_to(&mut kv_config);

        assert_eq!(kv_config.table_size, 2048);
        assert_eq!(kv_config.log_memory_size, 1 << 20);
        assert_eq!(kv_config.page_size_bits, 14);
        assert_eq!(kv_config.mutable_fraction, 0.8);
    }

    #[test]
    fn test_compaction_config_apply_to() {
        let compaction_spec = CompactionConfigSpec {
            target_utilization: Some(0.75),
            min_compact_bytes: Some(2048),
            max_compact_bytes: Some(1 << 20),
            num_threads: Some(4),
            compact_tombstones: Some(true),
        };

        let mut compaction_config = CompactionConfig::default();
        compaction_spec.apply_to(&mut compaction_config);

        assert_eq!(compaction_config.target_utilization, 0.75);
        assert_eq!(compaction_config.min_compact_bytes, 2048);
        assert_eq!(compaction_config.max_compact_bytes, 1 << 20);
        assert_eq!(compaction_config.num_threads, 4);
        assert!(compaction_config.compact_tombstones);
    }

    #[test]
    fn test_read_cache_config_apply_to() {
        let cache_spec = ReadCacheConfigSpec {
            enabled: Some(true),
            mem_size: Some(1 << 22),
            mutable_fraction: Some(0.9),
            pre_allocate: Some(true),
            copy_to_tail: Some(false),
        };

        let mut cache_config = ReadCacheConfig::default();
        cache_spec.apply_to(&mut cache_config);

        assert_eq!(cache_config.mem_size, 1 << 22);
        assert_eq!(cache_config.mutable_fraction, 0.9);
        assert!(cache_config.pre_allocate);
    }

    #[test]
    fn test_load_from_path_nonexistent() {
        let result = OxifasterConfig::load_from_path("/nonexistent/config.toml");
        assert!(result.is_err());
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::InvalidValue {
            key: "test_key".to_string(),
            value: "bad_value".to_string(),
        };
        let display = format!("{}", err);
        assert!(display.contains("invalid value"));
        assert!(display.contains("test_key"));
        assert!(display.contains("bad_value"));
    }
}
