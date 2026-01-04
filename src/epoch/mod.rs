//! Epoch-based memory reclamation for FASTER
//!
//! This module implements the LightEpoch mechanism for safe memory reclamation
//! in lock-free data structures. It provides a lightweight epoch protection
//! framework that allows threads to safely access shared data.

mod light_epoch;

pub use light_epoch::{get_thread_id, EpochAction, EpochGuard, LightEpoch};

