//! GC state machine implementation

use std::sync::atomic::{AtomicU8, Ordering};

use crate::address::Address;

/// GC phase
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GcPhase {
    /// No GC in progress
    Rest = 0,
    /// Waiting for outstanding I/Os to complete before truncation
    IoPending = 1,
    /// Hash table cleanup in progress
    InProgress = 2,
}

impl GcPhase {
    /// Get the phase as a string
    pub const fn as_str(&self) -> &'static str {
        match self {
            GcPhase::Rest => "Rest",
            GcPhase::IoPending => "IoPending",
            GcPhase::InProgress => "InProgress",
        }
    }
}

impl From<u8> for GcPhase {
    fn from(value: u8) -> Self {
        match value {
            0 => GcPhase::Rest,
            1 => GcPhase::IoPending,
            2 => GcPhase::InProgress,
            _ => GcPhase::Rest,
        }
    }
}

impl std::fmt::Display for GcPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Callback type for truncate notification.
/// Called when log truncation is about to happen.
/// The Address parameter is the new begin address.
pub type TruncateCallback = Box<dyn FnOnce(Address) + Send>;

/// Callback type for GC completion notification.
/// Called when the entire GC cycle completes.
pub type CompleteCallback = Box<dyn FnOnce(Address) + Send>;

/// GC state machine for log truncation and memory reclamation
///
/// Based on C++ FASTER's GcState class. Tracks the current GC phase
/// and provides callback hooks for truncation and completion events.
pub struct GcState {
    /// Current GC phase
    phase: AtomicU8,
    /// Target address for truncation (new begin address)
    new_begin_address: Address,
    /// Truncation callback (called before truncation)
    truncate_callback: Option<TruncateCallback>,
    /// Completion callback (called after GC completes)
    complete_callback: Option<CompleteCallback>,
}

impl GcState {
    /// Create a new GC state in the Rest phase
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(GcPhase::Rest as u8),
            new_begin_address: Address::INVALID,
            truncate_callback: None,
            complete_callback: None,
        }
    }

    /// Get the current GC phase
    pub fn phase(&self) -> GcPhase {
        GcPhase::from(self.phase.load(Ordering::Acquire))
    }

    /// Check if GC is in progress (any phase other than Rest)
    pub fn is_active(&self) -> bool {
        self.phase() != GcPhase::Rest
    }

    /// Get the target begin address for truncation
    pub fn new_begin_address(&self) -> Address {
        self.new_begin_address
    }

    /// Initialize a new GC cycle
    ///
    /// Transitions from Rest to IoPending phase.
    /// Returns true if the transition succeeded, false if GC is already active.
    pub fn initialize(
        &mut self,
        new_begin_address: Address,
        truncate_callback: Option<TruncateCallback>,
        complete_callback: Option<CompleteCallback>,
    ) -> bool {
        if self.is_active() {
            return false;
        }

        self.new_begin_address = new_begin_address;
        self.truncate_callback = truncate_callback;
        self.complete_callback = complete_callback;
        self.phase
            .store(GcPhase::IoPending as u8, Ordering::Release);
        true
    }

    /// Transition from IoPending to InProgress phase
    ///
    /// Should be called when all outstanding I/Os have completed.
    /// Issues the truncate callback if one was provided.
    /// Returns true if the transition succeeded.
    pub fn advance_to_in_progress(&mut self) -> bool {
        let current = self.phase();
        if current != GcPhase::IoPending {
            return false;
        }

        // Issue truncate callback before transitioning
        if let Some(callback) = self.truncate_callback.take() {
            callback(self.new_begin_address);
        }

        self.phase
            .store(GcPhase::InProgress as u8, Ordering::Release);
        true
    }

    /// Complete the GC cycle
    ///
    /// Transitions from InProgress to Rest phase.
    /// Issues the complete callback if one was provided.
    /// Returns true if the transition succeeded.
    pub fn complete(&mut self) -> bool {
        let current = self.phase();
        if current != GcPhase::InProgress {
            return false;
        }

        // Issue complete callback
        if let Some(callback) = self.complete_callback.take() {
            callback(self.new_begin_address);
        }

        self.new_begin_address = Address::INVALID;
        self.phase.store(GcPhase::Rest as u8, Ordering::Release);
        true
    }

    /// Reset the GC state to Rest, discarding any pending callbacks
    pub fn reset(&mut self) {
        self.truncate_callback = None;
        self.complete_callback = None;
        self.new_begin_address = Address::INVALID;
        self.phase.store(GcPhase::Rest as u8, Ordering::Release);
    }
}

impl Default for GcState {
    fn default() -> Self {
        Self::new()
    }
}

// GcState contains callback boxes which are Send but not Sync.
// The state machine is designed to be used from a single coordinator thread.
// SAFETY: GcState is only accessed from the store's coordination paths,
// which are serialized by the epoch protection mechanism.
unsafe impl Send for GcState {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_state_new() {
        let state = GcState::new();
        assert_eq!(state.phase(), GcPhase::Rest);
        assert!(!state.is_active());
        assert!(state.new_begin_address().is_invalid());
    }

    #[test]
    fn test_gc_phase_transitions() {
        let mut state = GcState::new();
        let addr = Address::new(1, 0);

        // Rest -> IoPending
        assert!(state.initialize(addr, None, None));
        assert_eq!(state.phase(), GcPhase::IoPending);
        assert!(state.is_active());
        assert_eq!(state.new_begin_address(), addr);

        // IoPending -> InProgress
        assert!(state.advance_to_in_progress());
        assert_eq!(state.phase(), GcPhase::InProgress);

        // InProgress -> Rest
        assert!(state.complete());
        assert_eq!(state.phase(), GcPhase::Rest);
        assert!(!state.is_active());
    }

    #[test]
    fn test_gc_cannot_initialize_when_active() {
        let mut state = GcState::new();
        let addr = Address::new(1, 0);

        assert!(state.initialize(addr, None, None));
        // Cannot initialize again while active
        assert!(!state.initialize(addr, None, None));
    }

    #[test]
    fn test_gc_invalid_phase_transitions() {
        let mut state = GcState::new();

        // Cannot advance from Rest
        assert!(!state.advance_to_in_progress());
        assert!(!state.complete());

        // Initialize to IoPending
        let addr = Address::new(1, 0);
        state.initialize(addr, None, None);

        // Cannot complete from IoPending (must go through InProgress)
        assert!(!state.complete());
    }

    #[test]
    fn test_gc_truncate_callback() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        let addr = Address::new(5, 0);

        let mut state = GcState::new();
        state.initialize(
            addr,
            Some(Box::new(move |a| {
                assert_eq!(a, Address::new(5, 0));
                called_clone.store(true, Ordering::SeqCst);
            })),
            None,
        );

        assert!(!called.load(Ordering::SeqCst));
        state.advance_to_in_progress();
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_gc_complete_callback() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        let addr = Address::new(10, 0);

        let mut state = GcState::new();
        state.initialize(
            addr,
            None,
            Some(Box::new(move |a| {
                assert_eq!(a, Address::new(10, 0));
                called_clone.store(true, Ordering::SeqCst);
            })),
        );

        state.advance_to_in_progress();
        assert!(!called.load(Ordering::SeqCst));

        state.complete();
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_gc_reset() {
        let mut state = GcState::new();
        let addr = Address::new(1, 0);

        state.initialize(addr, None, None);
        assert!(state.is_active());

        state.reset();
        assert!(!state.is_active());
        assert_eq!(state.phase(), GcPhase::Rest);
    }

    #[test]
    fn test_gc_phase_display() {
        assert_eq!(GcPhase::Rest.as_str(), "Rest");
        assert_eq!(GcPhase::IoPending.as_str(), "IoPending");
        assert_eq!(GcPhase::InProgress.as_str(), "InProgress");
        assert_eq!(format!("{}", GcPhase::Rest), "Rest");
    }

    #[test]
    fn test_gc_phase_from_u8() {
        assert_eq!(GcPhase::from(0), GcPhase::Rest);
        assert_eq!(GcPhase::from(1), GcPhase::IoPending);
        assert_eq!(GcPhase::from(2), GcPhase::InProgress);
        assert_eq!(GcPhase::from(255), GcPhase::Rest); // unknown defaults to Rest
    }
}
