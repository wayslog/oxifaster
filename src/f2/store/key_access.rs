use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free key access tracker using Count-Min Sketch.
/// D=4 rows x W=4096 columns of AtomicU64 counters (~128 KB).
#[derive(Debug)]
pub(super) struct KeyAccessTracker {
    counters: Box<[[AtomicU64; Self::WIDTH]; Self::DEPTH]>,
    sample_log2: u8,
    sample_mask: u64,
    sample_counter: AtomicU64,
}

impl Default for KeyAccessTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyAccessTracker {
    const DEPTH: usize = 4;
    const WIDTH: usize = 4096;
    const DEFAULT_SAMPLE_LOG2: u8 = 4;

    const HASH_MULTS: [u64; Self::DEPTH] = [
        1,
        0x9E3779B97F4A7C15,
        0x517CC1B727220A95,
        0x6C62272E07BB0142,
    ];

    pub(super) fn new() -> Self {
        let sample_log2 = Self::DEFAULT_SAMPLE_LOG2;
        let sample_mask = if sample_log2 == 0 {
            0
        } else {
            (1u64 << sample_log2) - 1
        };

        let counters: Box<[[AtomicU64; Self::WIDTH]; Self::DEPTH]> =
            Box::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }));

        Self {
            counters,
            sample_log2,
            sample_mask,
            sample_counter: AtomicU64::new(0),
        }
    }

    #[inline]
    fn column_index(row_hash: u64) -> usize {
        ((row_hash >> 32) as usize) % Self::WIDTH
    }

    pub(super) fn record(&self, key_hash: u64) {
        if self.sample_mask != 0 {
            let tick = self.sample_counter.fetch_add(1, Ordering::Relaxed);
            if (tick & self.sample_mask) != 0 {
                return;
            }
        }

        for (row, &mult) in Self::HASH_MULTS.iter().enumerate() {
            let row_hash = key_hash.wrapping_mul(mult);
            let col = Self::column_index(row_hash);
            self.counters[row][col].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn get(&self, key_hash: u64) -> u64 {
        let mut min_count = u64::MAX;
        for (row, &mult) in Self::HASH_MULTS.iter().enumerate() {
            let row_hash = key_hash.wrapping_mul(mult);
            let col = Self::column_index(row_hash);
            let count = self.counters[row][col].load(Ordering::Relaxed);
            min_count = min_count.min(count);
        }
        min_count
    }

    pub(super) fn get_estimated(&self, key_hash: u64) -> u64 {
        let sampled = self.get(key_hash);
        let multiplier = 1u64
            .checked_shl(self.sample_log2 as u32)
            .unwrap_or(u64::MAX);
        sampled.saturating_mul(multiplier)
    }

    pub(super) fn remove(&self, key_hash: u64) {
        for (row, &mult) in Self::HASH_MULTS.iter().enumerate() {
            let row_hash = key_hash.wrapping_mul(mult);
            let col = Self::column_index(row_hash);
            let _ =
                self.counters[row][col].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
        }
    }

    pub(super) fn decay_shift(&self, shift: u8) {
        if shift == 0 {
            return;
        }

        for row in self.counters.iter() {
            for counter in row.iter() {
                if shift >= 64 {
                    counter.store(0, Ordering::Relaxed);
                } else {
                    let _ = counter
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v >> shift));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KeyAccessTracker;

    #[test]
    fn test_key_access_tracker_sampling_is_deterministic() {
        let tracker = KeyAccessTracker::new();
        let key_hash = 42u64;

        for _ in 0..64 {
            tracker.record(key_hash);
        }

        assert!(tracker.get(key_hash) >= 4);
        assert!(tracker.get_estimated(key_hash) >= 64);
    }

    #[test]
    fn test_key_access_decay_shift_ge_64_clears() {
        let tracker = KeyAccessTracker::new();
        let key_hash = 7u64;

        for _ in 0..128 {
            tracker.record(key_hash);
        }
        assert!(tracker.get(key_hash) > 0);

        tracker.decay_shift(64);
        assert_eq!(tracker.get(key_hash), 0);
    }
}
