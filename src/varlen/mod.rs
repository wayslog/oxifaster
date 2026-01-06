//! Variable Length Records
//!
//! This module provides support for variable-length keys and values in FASTER.
//!
//! ## Overview
//!
//! Variable-length records are essential for storing data where the size
//! is not known at compile time, such as strings, byte arrays, or serialized objects.
//!
//! ## Key Types
//!
//! - `SpanByte`: A pinned variable-length byte array that can be viewed as a slice
//! - `VarLenStruct`: Trait for variable-length structs
//! - `VarLenKey`: Trait for variable-length keys
//! - `VarLenValue`: Trait for variable-length values
//!
//! ## Example
//!
//! ```ignore
//! use oxifaster::varlen::SpanByte;
//!
//! // Create a SpanByte from a slice
//! let data = b"hello world";
//! let span = SpanByte::from_slice(data);
//! assert_eq!(span.len(), 11);
//! ```

mod span_byte;
mod traits;

pub use span_byte::{SpanByte, SpanByteBuilder};
pub use traits::{VarLenKey, VarLenStruct, VarLenValue};
