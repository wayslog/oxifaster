use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free EMA-based router that switches F2 read order between hot-first
/// and cold-first based on observed hit distribution.
///
/// Score in [0, SCALE]: high = hot-dominant, low = cold-dominant.
/// EMA update: score' = score - score/DECAY + (hot_hit ? SCALE/DECAY : 0)
#[derive(Debug)]
pub(super) struct ReadRouter {
    score: AtomicU64,
}

impl Default for ReadRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRouter {
    const SCALE: u64 = 1024;
    const DECAY_SHIFT: u32 = 6;
    const COLD_FIRST_THRESHOLD: u64 = Self::SCALE * 30 / 100;
    const SAMPLE_MASK: u64 = 15;

    pub(super) fn new() -> Self {
        Self {
            score: AtomicU64::new(Self::SCALE / 2),
        }
    }

    #[inline]
    pub(super) fn cold_first(&self) -> bool {
        self.score.load(Ordering::Relaxed) < Self::COLD_FIRST_THRESHOLD
    }

    #[inline]
    pub(super) fn record_hot_hit(&self, sample_tick: u64) {
        if (sample_tick & Self::SAMPLE_MASK) != 0 {
            return;
        }
        let old = self.score.load(Ordering::Relaxed);
        let new = old
            .saturating_sub(old >> Self::DECAY_SHIFT)
            .saturating_add(Self::SCALE >> Self::DECAY_SHIFT);
        self.score.store(new.min(Self::SCALE), Ordering::Relaxed);
    }

    #[inline]
    pub(super) fn record_cold_hit(&self, sample_tick: u64) {
        if (sample_tick & Self::SAMPLE_MASK) != 0 {
            return;
        }
        let old = self.score.load(Ordering::Relaxed);
        let new = old.saturating_sub(old >> Self::DECAY_SHIFT);
        self.score.store(new, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_is_hot_first() {
        let router = ReadRouter::new();
        assert!(!router.cold_first());
    }

    #[test]
    fn test_converges_to_cold_first_on_cold_hits() {
        let router = ReadRouter::new();
        for i in 0..10_000u64 {
            router.record_cold_hit(i & !ReadRouter::SAMPLE_MASK);
        }
        assert!(router.cold_first());
    }

    #[test]
    fn test_converges_to_hot_first_on_hot_hits() {
        let router = ReadRouter::new();
        for i in 0..10_000u64 {
            router.record_cold_hit(i & !ReadRouter::SAMPLE_MASK);
        }
        assert!(router.cold_first());

        for i in 0..10_000u64 {
            router.record_hot_hit(i & !ReadRouter::SAMPLE_MASK);
        }
        assert!(!router.cold_first());
    }

    #[test]
    fn test_score_bounded() {
        let router = ReadRouter::new();
        for _ in 0..100_000u64 {
            router.record_hot_hit(0);
        }
        assert!(router.score.load(Ordering::Relaxed) <= ReadRouter::SCALE);
    }
}
