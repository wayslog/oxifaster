//! Variable Length Records
//!
//! This module provides support for variable-length keys and values in FASTER.
//!
//! # Overview
//!
//! Variable-length records are essential for storing data where the size
//! is not known at compile time, such as strings, byte arrays, or serialized objects.
//!
//! # Key Types
//!
//! - [`SpanByte`]: A pinned variable-length byte array that can be viewed as a slice
//! - [`VarLenStruct`]: Trait for variable-length structs
//! - [`VarLenKey`]: Trait for variable-length keys  
//! - [`VarLenValue`]: Trait for variable-length values
//!
//! # Memory Layout
//!
//! Variable-length records use a prefix length encoding:
//!
//! ```text
//! +---------------+------------------+
//! | Length (4B)   |  Data (variable) |
//! +---------------+------------------+
//! ```
//!
//! # Usage
//!
//! ## SpanByte
//!
//! ```rust,ignore
//! use oxifaster::varlen::SpanByte;
//!
//! // Create a SpanByte from a slice
//! let data = b"hello world";
//! let span = SpanByte::from_slice(data);
//! assert_eq!(span.len(), 11);
//! assert_eq!(span.as_slice(), data);
//! ```
//!
//! ## Custom Variable-Length Types
//!
//! ```rust,ignore
//! use oxifaster::varlen::{VarLenKey, VarLenValue};
//!
//! struct MyKey {
//!     data: Vec<u8>,
//! }
//!
//! impl VarLenKey for MyKey {
//!     fn serialized_len(&self) -> usize {
//!         4 + self.data.len() // 4 bytes for length prefix
//!     }
//!     
//!     // ... other trait methods
//! }
//! ```

mod span_byte;
mod traits;

pub use span_byte::{SpanByte, SpanByteBuilder};
pub use traits::{VarLenKey, VarLenStruct, VarLenValue};
