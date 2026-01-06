//! Status codes and operation results for FASTER operations
//!
//! This module defines the various status codes that can be returned by FASTER operations.

use std::fmt;

/// Status code returned by FASTER operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Status {
    /// Operation completed successfully
    #[default]
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
    /// Invalid argument provided
    InvalidArgument = 7,
    /// Invalid operation in current state
    InvalidOperation = 8,
    /// Feature or operation not supported
    NotSupported = 9,
    /// Overflow buckets were skipped during index growth (potential data loss)
    OverflowBucketsSkipped = 10,
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
            Status::OutOfMemory
                | Status::IoError
                | Status::Corruption
                | Status::Aborted
                | Status::InvalidArgument
                | Status::InvalidOperation
                | Status::NotSupported
                | Status::OverflowBucketsSkipped
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
            Status::InvalidArgument => "InvalidArgument",
            Status::InvalidOperation => "InvalidOperation",
            Status::NotSupported => "NotSupported",
            Status::OverflowBucketsSkipped => "OverflowBucketsSkipped",
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Internal operation status used within FASTER
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum OperationStatus {
    /// Operation succeeded
    #[default]
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
        matches!(
            self,
            OperationStatus::Success | OperationStatus::SuccessUnmark
        )
    }

    /// Check if the key was not found
    #[inline]
    pub const fn is_not_found(&self) -> bool {
        matches!(
            self,
            OperationStatus::NotFound | OperationStatus::NotFoundUnmark
        )
    }

    /// Check if a retry is needed
    #[inline]
    pub const fn needs_retry(&self) -> bool {
        matches!(
            self,
            OperationStatus::RetryNow | OperationStatus::RetryLater
        )
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

    #[test]
    fn test_status_as_str() {
        assert_eq!(Status::Ok.as_str(), "Ok");
        assert_eq!(Status::Pending.as_str(), "Pending");
        assert_eq!(Status::NotFound.as_str(), "NotFound");
        assert_eq!(Status::OutOfMemory.as_str(), "OutOfMemory");
        assert_eq!(Status::IoError.as_str(), "IoError");
        assert_eq!(Status::Corruption.as_str(), "Corruption");
        assert_eq!(Status::Aborted.as_str(), "Aborted");
        assert_eq!(Status::InvalidArgument.as_str(), "InvalidArgument");
        assert_eq!(Status::InvalidOperation.as_str(), "InvalidOperation");
        assert_eq!(Status::NotSupported.as_str(), "NotSupported");
        assert_eq!(
            Status::OverflowBucketsSkipped.as_str(),
            "OverflowBucketsSkipped"
        );
    }

    #[test]
    fn test_status_display() {
        assert_eq!(format!("{}", Status::Ok), "Ok");
        assert_eq!(format!("{}", Status::Pending), "Pending");
        assert_eq!(format!("{}", Status::NotFound), "NotFound");
        assert_eq!(format!("{}", Status::OutOfMemory), "OutOfMemory");
        assert_eq!(format!("{}", Status::IoError), "IoError");
        assert_eq!(format!("{}", Status::Corruption), "Corruption");
        assert_eq!(format!("{}", Status::Aborted), "Aborted");
        assert_eq!(format!("{}", Status::InvalidArgument), "InvalidArgument");
        assert_eq!(format!("{}", Status::InvalidOperation), "InvalidOperation");
        assert_eq!(format!("{}", Status::NotSupported), "NotSupported");
        assert_eq!(
            format!("{}", Status::OverflowBucketsSkipped),
            "OverflowBucketsSkipped"
        );
    }

    #[test]
    fn test_status_default() {
        assert_eq!(Status::default(), Status::Ok);
    }

    #[test]
    fn test_status_all_error_types() {
        // All error types should return true for is_error()
        assert!(Status::OutOfMemory.is_error());
        assert!(Status::IoError.is_error());
        assert!(Status::Corruption.is_error());
        assert!(Status::Aborted.is_error());
        assert!(Status::InvalidArgument.is_error());
        assert!(Status::InvalidOperation.is_error());
        assert!(Status::NotSupported.is_error());
        assert!(Status::OverflowBucketsSkipped.is_error());

        // Non-error types
        assert!(!Status::Ok.is_error());
        assert!(!Status::Pending.is_error());
        assert!(!Status::NotFound.is_error());
    }

    #[test]
    fn test_operation_status_is_success() {
        assert!(OperationStatus::Success.is_success());
        assert!(OperationStatus::SuccessUnmark.is_success());
        assert!(!OperationStatus::NotFound.is_success());
        assert!(!OperationStatus::RetryNow.is_success());
        assert!(!OperationStatus::Aborted.is_success());
    }

    #[test]
    fn test_operation_status_is_not_found() {
        assert!(OperationStatus::NotFound.is_not_found());
        assert!(OperationStatus::NotFoundUnmark.is_not_found());
        assert!(!OperationStatus::Success.is_not_found());
        assert!(!OperationStatus::RetryNow.is_not_found());
    }

    #[test]
    fn test_operation_status_needs_retry() {
        assert!(OperationStatus::RetryNow.needs_retry());
        assert!(OperationStatus::RetryLater.needs_retry());
        assert!(!OperationStatus::Success.needs_retry());
        assert!(!OperationStatus::NotFound.needs_retry());
        assert!(!OperationStatus::RecordOnDisk.needs_retry());
    }

    #[test]
    fn test_operation_status_is_on_disk() {
        assert!(OperationStatus::RecordOnDisk.is_on_disk());
        assert!(OperationStatus::IndexEntryOnDisk.is_on_disk());
        assert!(!OperationStatus::Success.is_on_disk());
        assert!(!OperationStatus::NotFound.is_on_disk());
    }

    #[test]
    fn test_operation_status_default() {
        assert_eq!(OperationStatus::default(), OperationStatus::Success);
    }

    #[test]
    fn test_operation_status_all_conversions() {
        assert_eq!(OperationStatus::Success.to_status(), Status::Ok);
        assert_eq!(OperationStatus::SuccessUnmark.to_status(), Status::Ok);
        assert_eq!(OperationStatus::NotFound.to_status(), Status::NotFound);
        assert_eq!(OperationStatus::NotFoundUnmark.to_status(), Status::NotFound);
        assert_eq!(OperationStatus::RetryNow.to_status(), Status::Pending);
        assert_eq!(OperationStatus::RetryLater.to_status(), Status::Pending);
        assert_eq!(OperationStatus::RecordOnDisk.to_status(), Status::Pending);
        assert_eq!(
            OperationStatus::IndexEntryOnDisk.to_status(),
            Status::Pending
        );
        assert_eq!(
            OperationStatus::AsyncToColdStore.to_status(),
            Status::Pending
        );
        assert_eq!(
            OperationStatus::CprShiftDetected.to_status(),
            Status::Pending
        );
        assert_eq!(OperationStatus::Aborted.to_status(), Status::Aborted);
        assert_eq!(OperationStatus::AbortedUnmark.to_status(), Status::Aborted);
    }

    #[test]
    fn test_internal_status_to_operation_status() {
        assert_eq!(
            InternalStatus::Ok.to_operation_status(),
            OperationStatus::Success
        );
        assert_eq!(
            InternalStatus::RetryNow.to_operation_status(),
            OperationStatus::RetryNow
        );
        assert_eq!(
            InternalStatus::RetryLater.to_operation_status(),
            OperationStatus::RetryLater
        );
        assert_eq!(
            InternalStatus::RecordOnDisk.to_operation_status(),
            OperationStatus::RecordOnDisk
        );
        assert_eq!(
            InternalStatus::SuccessUnmark.to_operation_status(),
            OperationStatus::SuccessUnmark
        );
        assert_eq!(
            InternalStatus::CprShiftDetected.to_operation_status(),
            OperationStatus::CprShiftDetected
        );
    }

    #[test]
    fn test_operation_type_display() {
        assert_eq!(format!("{}", OperationType::Read), "Read");
        assert_eq!(format!("{}", OperationType::Rmw), "RMW");
        assert_eq!(format!("{}", OperationType::Upsert), "Upsert");
        assert_eq!(format!("{}", OperationType::Insert), "Insert");
        assert_eq!(format!("{}", OperationType::Delete), "Delete");
        assert_eq!(
            format!("{}", OperationType::ConditionalInsert),
            "ConditionalInsert"
        );
        assert_eq!(format!("{}", OperationType::Recovery), "Recovery");
    }

    #[test]
    fn test_index_operation_type_default() {
        assert_eq!(IndexOperationType::default(), IndexOperationType::None);
    }

    #[test]
    fn test_status_clone_copy() {
        let status = Status::IoError;
        let cloned = status.clone();
        let copied = status;
        assert_eq!(status, cloned);
        assert_eq!(status, copied);
    }

    #[test]
    fn test_operation_status_clone_copy() {
        let status = OperationStatus::RetryNow;
        let cloned = status.clone();
        let copied = status;
        assert_eq!(status, cloned);
        assert_eq!(status, copied);
    }

    #[test]
    fn test_internal_status_clone_copy() {
        let status = InternalStatus::RecordOnDisk;
        let cloned = status.clone();
        let copied = status;
        assert_eq!(status, cloned);
        assert_eq!(status, copied);
    }

    #[test]
    fn test_operation_type_clone_copy() {
        let op_type = OperationType::Rmw;
        let cloned = op_type.clone();
        let copied = op_type;
        assert_eq!(op_type, cloned);
        assert_eq!(op_type, copied);
    }

    #[test]
    fn test_index_operation_type_clone_copy() {
        let op_type = IndexOperationType::Update;
        let cloned = op_type.clone();
        let copied = op_type;
        assert_eq!(op_type, cloned);
        assert_eq!(op_type, copied);
    }

    #[test]
    fn test_status_debug() {
        let debug_str = format!("{:?}", Status::InvalidArgument);
        assert!(debug_str.contains("InvalidArgument"));
    }

    #[test]
    fn test_operation_status_debug() {
        let debug_str = format!("{:?}", OperationStatus::AsyncToColdStore);
        assert!(debug_str.contains("AsyncToColdStore"));
    }

    #[test]
    fn test_internal_status_debug() {
        let debug_str = format!("{:?}", InternalStatus::CprShiftDetected);
        assert!(debug_str.contains("CprShiftDetected"));
    }

    #[test]
    fn test_operation_type_debug() {
        let debug_str = format!("{:?}", OperationType::ConditionalInsert);
        assert!(debug_str.contains("ConditionalInsert"));
    }

    #[test]
    fn test_index_operation_type_debug() {
        let debug_str = format!("{:?}", IndexOperationType::Retrieve);
        assert!(debug_str.contains("Retrieve"));
    }
}
