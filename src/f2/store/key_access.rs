use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// 按 key(hash) 的访问次数统计（用于按访问率迁移策略）。
#[derive(Debug)]
pub(super) struct KeyAccessTracker {
    shards: [Mutex<HashMap<u64, u64>>; Self::NUM_SHARDS],
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
    const NUM_SHARDS: usize = 64;
    // 热路径默认做采样，避免每次访问都抢锁更新 HashMap。
    // 1 / 2^DEFAULT_SAMPLE_LOG2 的访问会进入计数逻辑。
    const DEFAULT_SAMPLE_LOG2: u8 = 4; // 1/16
                                       // 每个分片的最大容量限制，防止内存无限增长
    const SHARD_CAPACITY_LIMIT: usize = 4096;

    pub(super) fn new() -> Self {
        let sample_log2 = Self::DEFAULT_SAMPLE_LOG2;
        let sample_mask = if sample_log2 == 0 {
            0
        } else {
            // sample_log2 由内部常量控制，保证 < 64
            (1u64 << sample_log2) - 1
        };

        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            sample_log2,
            sample_mask,
            sample_counter: AtomicU64::new(0),
        }
    }

    fn shard_idx(key_hash: u64) -> usize {
        // NUM_SHARDS 约定为 2 的幂，便于用位运算替代取模。
        debug_assert!(Self::NUM_SHARDS.is_power_of_two());
        (key_hash as usize) & (Self::NUM_SHARDS - 1)
    }

    pub(super) fn record(&self, key_hash: u64) {
        if self.sample_mask != 0 {
            // Relaxed：仅用于节流采样，不参与正确性同步。
            let tick = self.sample_counter.fetch_add(1, Ordering::Relaxed);
            if (tick & self.sample_mask) != 0 {
                return;
            }
        }

        let idx = Self::shard_idx(key_hash);
        // 热路径避免阻塞：竞争激烈时直接丢弃本次计数，统计值用于“冷热倾向”即可。
        if let Some(mut shard) = self.shards[idx].try_lock() {
            // 简单的容量保护：如果分片过大，直接清空（类似 massive decay）
            if shard.len() >= Self::SHARD_CAPACITY_LIMIT && !shard.contains_key(&key_hash) {
                shard.clear();
            }
            let entry = shard.entry(key_hash).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

    pub(super) fn get(&self, key_hash: u64) -> u64 {
        let idx = Self::shard_idx(key_hash);
        self.shards[idx].lock().get(&key_hash).copied().unwrap_or(0)
    }

    /// 获取“估算访问次数”：把采样计数按采样倍率放大，用于与阈值比较。
    pub(super) fn get_estimated(&self, key_hash: u64) -> u64 {
        // compaction/migration 线程读计数时采用“尽力而为”：避免阻塞在 shard 锁上，降低对前台的影响。
        let idx = Self::shard_idx(key_hash);
        let sampled = self.shards[idx]
            .try_lock()
            .and_then(|shard| shard.get(&key_hash).copied())
            .unwrap_or(0);
        let multiplier = 1u64
            .checked_shl(self.sample_log2 as u32)
            .unwrap_or(u64::MAX);
        sampled.saturating_mul(multiplier)
    }

    pub(super) fn remove(&self, key_hash: u64) {
        let idx = Self::shard_idx(key_hash);
        self.shards[idx].lock().remove(&key_hash);
    }

    pub(super) fn decay_shift(&self, shift: u8) {
        if shift == 0 {
            return;
        }

        // Rust 对 `u64 >> shift` 在 shift >= 64 时会 panic；此处把“衰减到 0”视为清空。
        if shift >= 64 {
            for shard in &self.shards {
                shard.lock().clear();
            }
            return;
        }

        for shard in &self.shards {
            let mut counts = shard.lock();
            counts.retain(|_, v| {
                *v >>= shift;
                *v != 0
            });
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

        // DEFAULT_SAMPLE_LOG2=4 => 1/16 采样，tick 从 0 开始：0,16,32,... 会记录
        for _ in 0..64 {
            tracker.record(key_hash);
        }

        assert_eq!(tracker.get(key_hash), 4);
        assert_eq!(tracker.get_estimated(key_hash), 64);
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
