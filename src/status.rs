//! Status codes and operation results for FASTER operations
//!
//! This module defines the various status codes that can be returned by FASTER operations.

use std::fmt;

/// Status code returned by FASTER operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    /// Operation completed successfully
    Ok = 0,
    /// Operation is pending (async continuation required)
    Pending = 1,
    /// Key was not found
    NotFound = 2,
    /// Out of memory
    OutOfMemory = 3,
    /// I/O error occurred
    IoError = 4,
    /// Data corruption detected
    Corruption = 5,
    /// Operation was aborted
    Aborted = 6,
}

impl Status {
    /// Check if the status indicates success
    #[inline]
    pub const fn is_ok(&self) -> bool {
        matches!(self, Status::Ok)
    }

    /// Check if the operation is pending
    #[inline]
    pub const fn is_pending(&self) -> bool {
        matches!(self, Status::Pending)
    }

    /// Check if the key was not found
    #[inline]
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Status::NotFound)
    }

    /// Check if the status indicates an error
    #[inline]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Status::OutOfMemory | Status::IoError | Status::Corruption | Status::Aborted
        )
    }

    /// Get the status as a string
    pub const fn as_str(&self) -> &'static str {
        match self {
            Status::Ok => "Ok",
            Status::Pending => "Pending",
            Status::NotFound => "NotFound",
            Status::OutOfMemory => "OutOfMemory",
            Status::IoError => "IoError",
            Status::Corruption => "Corruption",
            Status::Aborted => "Aborted",
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for Status {
    fn default() -> Self {
        Status::Ok
    }
}

/// Internal operation status used within FASTER
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationStatus {
    /// Operation succeeded
    Success,
    /// Key not found
    NotFound,
    /// Should retry the operation immediately
    RetryNow,
    /// Should retry the operation later
    RetryLater,
    /// Record is on disk, needs async I/O
    RecordOnDisk,
    /// Index entry is on disk (for cold index)
    IndexEntryOnDisk,
    /// Async operation to cold store
    AsyncToColdStore,
    /// Success but need to unmark
    SuccessUnmark,
    /// Not found but need to unmark
    NotFoundUnmark,
    /// CPR shift detected during operation
    CprShiftDetected,
    /// Operation was aborted
    Aborted,
    /// Aborted but need to unmark
    AbortedUnmark,
}

impl OperationStatus {
    /// Check if the operation succeeded
    #[inline]
    pub const fn is_success(&self) -> bool {
        matches!(self, OperationStatus::Success | OperationStatus::SuccessUnmark)
    }

    /// Check if the key was not found
    #[inline]
    pub const fn is_not_found(&self) -> bool {
        matches!(self, OperationStatus::NotFound | OperationStatus::NotFoundUnmark)
    }

    /// Check if a retry is needed
    #[inline]
    pub const fn needs_retry(&self) -> bool {
        matches!(self, OperationStatus::RetryNow | OperationStatus::RetryLater)
    }

    /// Check if the record is on disk
    #[inline]
    pub const fn is_on_disk(&self) -> bool {
        matches!(
            self,
            OperationStatus::RecordOnDisk | OperationStatus::IndexEntryOnDisk
        )
    }

    /// Convert to external Status
    pub const fn to_status(&self) -> Status {
        match self {
            OperationStatus::Success | OperationStatus::SuccessUnmark => Status::Ok,
            OperationStatus::NotFound | OperationStatus::NotFoundUnmark => Status::NotFound,
            OperationStatus::RetryNow
            | OperationStatus::RetryLater
            | OperationStatus::RecordOnDisk
            | OperationStatus::IndexEntryOnDisk
            | OperationStatus::AsyncToColdStore => Status::Pending,
            OperationStatus::CprShiftDetected => Status::Pending,
            OperationStatus::Aborted | OperationStatus::AbortedUnmark => Status::Aborted,
        }
    }
}

impl Default for OperationStatus {
    fn default() -> Self {
        OperationStatus::Success
    }
}

/// Internal status for index operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum InternalStatus {
    /// Operation succeeded
    Ok,
    /// Should retry now
    RetryNow,
    /// Should retry later
    RetryLater,
    /// Record is on disk
    RecordOnDisk,
    /// Success but need to unmark
    SuccessUnmark,
    /// CPR shift detected
    CprShiftDetected,
}

impl InternalStatus {
    /// Convert to OperationStatus
    pub const fn to_operation_status(&self) -> OperationStatus {
        match self {
            InternalStatus::Ok => OperationStatus::Success,
            InternalStatus::RetryNow => OperationStatus::RetryNow,
            InternalStatus::RetryLater => OperationStatus::RetryLater,
            InternalStatus::RecordOnDisk => OperationStatus::RecordOnDisk,
            InternalStatus::SuccessUnmark => OperationStatus::SuccessUnmark,
            InternalStatus::CprShiftDetected => OperationStatus::CprShiftDetected,
        }
    }
}

/// Type of operation being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationType {
    /// Read operation
    Read,
    /// Read-Modify-Write operation
    Rmw,
    /// Upsert (insert or update) operation
    Upsert,
    /// Insert operation
    Insert,
    /// Delete operation
    Delete,
    /// Conditional insert operation
    ConditionalInsert,
    /// Recovery operation
    Recovery,
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationType::Read => write!(f, "Read"),
            OperationType::Rmw => write!(f, "RMW"),
            OperationType::Upsert => write!(f, "Upsert"),
            OperationType::Insert => write!(f, "Insert"),
            OperationType::Delete => write!(f, "Delete"),
            OperationType::ConditionalInsert => write!(f, "ConditionalInsert"),
            OperationType::Recovery => write!(f, "Recovery"),
        }
    }
}

/// Type of index operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum IndexOperationType {
    /// No operation
    #[default]
    None,
    /// Retrieve entry
    Retrieve,
    /// Update entry
    Update,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_checks() {
        assert!(Status::Ok.is_ok());
        assert!(!Status::Ok.is_error());
        
        assert!(Status::Pending.is_pending());
        
        assert!(Status::NotFound.is_not_found());
        
        assert!(Status::IoError.is_error());
        assert!(Status::Corruption.is_error());
        assert!(Status::Aborted.is_error());
    }

    #[test]
    fn test_operation_status_conversion() {
        assert_eq!(OperationStatus::Success.to_status(), Status::Ok);
        assert_eq!(OperationStatus::NotFound.to_status(), Status::NotFound);
        assert_eq!(OperationStatus::RetryNow.to_status(), Status::Pending);
        assert_eq!(OperationStatus::Aborted.to_status(), Status::Aborted);
    }
}

