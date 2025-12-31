//! FASTER Log implementation
//!
//! This module provides the FASTER Log - a high-performance
//! concurrent persistent recoverable log.

pub mod faster_log;

pub use faster_log::{FasterLog, FasterLogConfig, LogStats};

