//! FASTER Log implementation
//!
//! This module provides the FASTER Log - a high-performance
//! concurrent persistent recoverable log.
//!
//! # Overview
//!
//! FasterLog is an append-only log designed for high-throughput logging
//! with persistence and recovery support. Unlike FasterKV, it does not
//! maintain an index and is optimized purely for sequential append/read.
//!
//! # Key Features
//!
//! - **High-throughput Append**: Lock-free concurrent appends
//! - **Persistence**: Automatic flushing to disk
//! - **Recovery**: Restore log state from disk after restart
//! - **Commit**: Explicit commit points for durability
//! - **Scan**: Sequential iteration through log entries
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::log::faster_log::{FasterLog, FasterLogConfig};
//! use oxifaster::device::NullDisk;
//!
//! let config = FasterLogConfig::default();
//! let device = NullDisk::new();
//! let log = FasterLog::new(config, device);
//!
//! // Append data
//! let addr = log.append(b"entry data")?;
//!
//! // Commit for durability
//! log.commit()?;
//!
//! // Read entry
//! let data = log.read_entry(addr)?;
//!
//! // Scan all entries
//! for (addr, data) in log.scan_all() {
//!     println!("Entry at {}: {:?}", addr, data);
//! }
//! ```

pub mod faster_log;

pub use faster_log::{FasterLog, FasterLogConfig, LogStats};
