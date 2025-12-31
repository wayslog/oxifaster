//! Page allocator and status management for FASTER
//!
//! This module provides page-level status tracking for the hybrid log allocator.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Flush status for a page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FlushStatus {
    /// Page is not being flushed
    Flushed = 0,
    /// Page is currently being flushed
    InProgress = 1,
}

impl From<u8> for FlushStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => FlushStatus::Flushed,
            _ => FlushStatus::InProgress,
        }
    }
}

/// Close status for a page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CloseStatus {
    /// Page is open
    Open = 0,
    /// Page is closing
    Closing = 1,
    /// Page is closed
    Closed = 2,
}

impl From<u8> for CloseStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => CloseStatus::Open,
            1 => CloseStatus::Closing,
            _ => CloseStatus::Closed,
        }
    }
}

/// Combined flush and close status for a page
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct FullPageStatus {
    /// Flush status
    pub flush_status: u8,
    /// Close status
    pub close_status: u8,
    /// Padding
    _padding: u16,
}

impl FullPageStatus {
    /// Create a new status (flushed, open)
    pub const fn new() -> Self {
        Self {
            flush_status: FlushStatus::Flushed as u8,
            close_status: CloseStatus::Open as u8,
            _padding: 0,
        }
    }

    /// Create from raw control value
    pub fn from_control(control: u32) -> Self {
        Self {
            flush_status: (control & 0xFF) as u8,
            close_status: ((control >> 8) & 0xFF) as u8,
            _padding: 0,
        }
    }

    /// Convert to raw control value
    pub fn to_control(&self) -> u32 {
        (self.flush_status as u32) | ((self.close_status as u32) << 8)
    }

    /// Get flush status
    pub fn flush(&self) -> FlushStatus {
        FlushStatus::from(self.flush_status)
    }

    /// Get close status
    pub fn close(&self) -> CloseStatus {
        CloseStatus::from(self.close_status)
    }

    /// Check if the page is flushed
    pub fn is_flushed(&self) -> bool {
        self.flush() == FlushStatus::Flushed
    }

    /// Check if the page is open
    pub fn is_open(&self) -> bool {
        self.close() == CloseStatus::Open
    }

    /// Check if the page is closed
    pub fn is_closed(&self) -> bool {
        self.close() == CloseStatus::Closed
    }
}

/// Atomic full page status
#[repr(transparent)]
pub struct AtomicFullPageStatus {
    control: AtomicU32,
}

impl std::fmt::Debug for AtomicFullPageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = self.load(Ordering::Relaxed);
        f.debug_struct("AtomicFullPageStatus")
            .field("flush", &status.flush())
            .field("close", &status.close())
            .finish()
    }
}

impl AtomicFullPageStatus {
    /// Create a new atomic status
    pub fn new(status: FullPageStatus) -> Self {
        Self {
            control: AtomicU32::new(status.to_control()),
        }
    }

    /// Create a new status with default values
    pub fn new_default() -> Self {
        Self {
            control: AtomicU32::new(0),
        }
    }

    /// Load the status atomically
    pub fn load(&self, ordering: Ordering) -> FullPageStatus {
        FullPageStatus::from_control(self.control.load(ordering))
    }

    /// Store a status atomically
    pub fn store(&self, status: FullPageStatus, ordering: Ordering) {
        self.control.store(status.to_control(), ordering);
    }

    /// Compare and exchange
    pub fn compare_exchange(
        &self,
        current: FullPageStatus,
        new: FullPageStatus,
        success: Ordering,
        failure: Ordering,
    ) -> Result<FullPageStatus, FullPageStatus> {
        self.control
            .compare_exchange(current.to_control(), new.to_control(), success, failure)
            .map(FullPageStatus::from_control)
            .map_err(FullPageStatus::from_control)
    }
}

impl Default for AtomicFullPageStatus {
    fn default() -> Self {
        Self::new_default()
    }
}

impl Clone for AtomicFullPageStatus {
    fn clone(&self) -> Self {
        Self::new(self.load(Ordering::Relaxed))
    }
}

/// Page header for additional metadata
#[repr(C)]
#[derive(Debug, Default)]
pub struct PageHeader {
    /// Number of valid entries in the page
    pub num_entries: AtomicU32,
    /// Flags for the page
    pub flags: AtomicU32,
}

impl PageHeader {
    /// Create a new page header
    pub const fn new() -> Self {
        Self {
            num_entries: AtomicU32::new(0),
            flags: AtomicU32::new(0),
        }
    }
}

/// Page metadata entry for tracking page info
#[derive(Debug)]
pub struct PageInfo {
    /// Status of the page
    pub status: AtomicFullPageStatus,
    /// Start of the dirty region (for flush)
    pub dirty_start: AtomicU64,
    /// End of the dirty region (for flush)
    pub dirty_end: AtomicU64,
}

impl PageInfo {
    /// Create a new page info
    pub fn new() -> Self {
        Self {
            status: AtomicFullPageStatus::default(),
            dirty_start: AtomicU64::new(0),
            dirty_end: AtomicU64::new(0),
        }
    }

    /// Reset the page info
    pub fn reset(&self) {
        self.status.store(FullPageStatus::new(), Ordering::Release);
        self.dirty_start.store(0, Ordering::Release);
        self.dirty_end.store(0, Ordering::Release);
    }
}

impl Default for PageInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PageInfo {
    fn clone(&self) -> Self {
        Self {
            status: self.status.clone(),
            dirty_start: AtomicU64::new(self.dirty_start.load(Ordering::Relaxed)),
            dirty_end: AtomicU64::new(self.dirty_end.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flush_status() {
        assert_eq!(FlushStatus::from(0), FlushStatus::Flushed);
        assert_eq!(FlushStatus::from(1), FlushStatus::InProgress);
    }

    #[test]
    fn test_close_status() {
        assert_eq!(CloseStatus::from(0), CloseStatus::Open);
        assert_eq!(CloseStatus::from(1), CloseStatus::Closing);
        assert_eq!(CloseStatus::from(2), CloseStatus::Closed);
    }

    #[test]
    fn test_full_page_status() {
        let status = FullPageStatus::new();
        assert!(status.is_flushed());
        assert!(status.is_open());
        assert!(!status.is_closed());
    }

    #[test]
    fn test_atomic_full_page_status() {
        let atomic = AtomicFullPageStatus::default();
        
        let loaded = atomic.load(Ordering::Relaxed);
        assert!(loaded.is_flushed());
        assert!(loaded.is_open());
        
        let mut new_status = FullPageStatus::new();
        new_status.flush_status = FlushStatus::InProgress as u8;
        atomic.store(new_status, Ordering::Relaxed);
        
        let loaded2 = atomic.load(Ordering::Relaxed);
        assert!(!loaded2.is_flushed());
        assert_eq!(loaded2.flush(), FlushStatus::InProgress);
    }

    #[test]
    fn test_page_info() {
        let info = PageInfo::new();
        
        let status = info.status.load(Ordering::Relaxed);
        assert!(status.is_flushed());
        
        info.dirty_start.store(100, Ordering::Relaxed);
        info.dirty_end.store(200, Ordering::Relaxed);
        
        assert_eq!(info.dirty_start.load(Ordering::Relaxed), 100);
        assert_eq!(info.dirty_end.load(Ordering::Relaxed), 200);
        
        info.reset();
        assert_eq!(info.dirty_start.load(Ordering::Relaxed), 0);
    }
}

