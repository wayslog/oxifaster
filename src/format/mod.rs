//! 格式识别和版本管理
//!
//! 提供统一的格式头和格式检测机制

pub mod detector;
pub mod header;

pub use detector::{DetectionResult, FormatDetector};
pub use header::{
    FormatFlags, FormatHeaderError, FormatType, UniversalFormatHeader, compute_xor_checksum,
};
