//! Garbage collection state management for FASTER
//!
//! This module provides the GC state machine for log truncation and memory reclamation.
//! Based on C++ FASTER's gc_state.h implementation.
//!
//! The GC process runs in two phases:
//! - GC_IO_PENDING: Wait for outstanding I/Os to complete before truncation
//! - GC_IN_PROGRESS: Clean up hash table entries pointing to truncated regions

mod gc_state;

pub use gc_state::{GcPhase, GcState};
