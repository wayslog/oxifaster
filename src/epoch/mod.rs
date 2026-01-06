//! Epoch-based memory reclamation for FASTER
//!
//! This module implements the LightEpoch mechanism for safe memory reclamation
//! in lock-free data structures. It provides a lightweight epoch protection
//! framework that allows threads to safely access shared data.
//!
//! # Overview
//!
//! The epoch mechanism ensures that memory is not reclaimed while any thread
//! might still be accessing it. Each thread "protects" itself by announcing
//! which epoch it is currently in. Memory can only be reclaimed when all
//! threads have moved past the epoch in which that memory was freed.
//!
//! # Key Concepts
//!
//! - **Epoch**: A monotonically increasing counter that defines points in time
//! - **Protection**: A thread protects itself before accessing shared data
//! - **Safe-to-reclaim epoch**: The oldest epoch that might still be accessed
//! - **Drain list**: Deferred actions waiting for epochs to become safe
//!
//! # Usage
//!
//! ```rust,ignore
//! use oxifaster::epoch::{LightEpoch, get_thread_id};
//!
//! let epoch = LightEpoch::new();
//! let tid = get_thread_id();
//!
//! // Protect before accessing shared data
//! epoch.protect(tid);
//!
//! // ... perform operations on shared data ...
//!
//! // Unprotect when done
//! epoch.unprotect(tid);
//!
//! // Or use the RAII guard
//! {
//!     let _guard = epoch.protect_guard(tid);
//!     // ... automatically unprotects when dropped ...
//! }
//! ```
//!
//! # Thread Safety
//!
//! The epoch mechanism is designed for concurrent use. Multiple threads can
//! safely protect/unprotect and trigger epoch advances simultaneously.

mod light_epoch;

pub use light_epoch::{get_thread_id, EpochAction, EpochGuard, LightEpoch};
