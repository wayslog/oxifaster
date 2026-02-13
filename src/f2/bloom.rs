use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free Bloom filter for cold store existence check.
/// Uses double hashing: h(i) = h1 + i * h2, where h1/h2 are derived from input hash.
pub(super) struct ColdBloomFilter {
    bits: Vec<AtomicU64>,
    num_bits: u64,
    num_hashes: u8,
}

impl ColdBloomFilter {
    pub(super) fn new(expected_keys: u64, bits_per_key: u8) -> Self {
        let bits_per_key = bits_per_key.max(1);
        let num_bits = (expected_keys.saturating_mul(bits_per_key as u64)).max(64);
        let num_words = num_bits.div_ceil(64);
        let num_bits = num_words * 64;

        // K = bits_per_key * ln(2) ~ bits_per_key * 69 / 100
        let num_hashes = ((bits_per_key as u32 * 69) / 100).clamp(1, 30) as u8;

        let bits = (0..num_words).map(|_| AtomicU64::new(0)).collect();

        Self {
            bits,
            num_bits,
            num_hashes,
        }
    }

    pub(super) fn insert(&self, hash: u64) {
        let (h1, h2) = Self::split_hash(hash);
        for i in 0..self.num_hashes as u64 {
            let bit_index = self.get_bit_index(h1, h2, i);
            let word_index = (bit_index / 64) as usize;
            let bit_offset = bit_index % 64;
            self.bits[word_index].fetch_or(1u64 << bit_offset, Ordering::Relaxed);
        }
    }

    pub(super) fn may_contain(&self, hash: u64) -> bool {
        let (h1, h2) = Self::split_hash(hash);
        for i in 0..self.num_hashes as u64 {
            let bit_index = self.get_bit_index(h1, h2, i);
            let word_index = (bit_index / 64) as usize;
            let bit_offset = bit_index % 64;
            let word = self.bits[word_index].load(Ordering::Relaxed);
            if (word & (1u64 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    pub(super) fn clear(&self) {
        for word in &self.bits {
            word.store(0, Ordering::Relaxed);
        }
    }

    pub(super) fn estimated_false_positive_rate(&self) -> f64 {
        let set_bits: u64 = self
            .bits
            .iter()
            .map(|w| w.load(Ordering::Relaxed).count_ones() as u64)
            .sum();
        let fill_ratio = set_bits as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    #[inline]
    fn split_hash(hash: u64) -> (u64, u64) {
        (hash, (hash >> 32) | 1)
    }

    #[inline]
    fn get_bit_index(&self, h1: u64, h2: u64, i: u64) -> u64 {
        h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let bloom = ColdBloomFilter::new(1000, 10);
        bloom.insert(12345);
        bloom.insert(67890);
        bloom.insert(11111);

        assert!(bloom.may_contain(12345));
        assert!(bloom.may_contain(67890));
        assert!(bloom.may_contain(11111));

        let mut false_positives = 0;
        for i in 100000..100100 {
            if bloom.may_contain(i) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 20,
            "Too many false positives: {}",
            false_positives
        );
    }

    #[test]
    fn test_bloom_filter_clear() {
        let bloom = ColdBloomFilter::new(100, 10);
        bloom.insert(12345);
        assert!(bloom.may_contain(12345));
        bloom.clear();
        assert!(!bloom.may_contain(12345));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let bloom = ColdBloomFilter::new(1000, 10);
        assert_eq!(bloom.estimated_false_positive_rate(), 0.0);

        for i in 0..1000u64 {
            bloom.insert(i * 7919);
        }
        let fpr = bloom.estimated_false_positive_rate();
        assert!(fpr < 0.05, "FPR too high: {}", fpr);
    }

    #[test]
    fn test_bloom_filter_concurrent_insert() {
        use std::sync::Arc;
        use std::thread;

        let bloom = Arc::new(ColdBloomFilter::new(10000, 10));
        let mut handles = vec![];

        for t in 0..4 {
            let bloom = Arc::clone(&bloom);
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    bloom.insert(t * 10000 + i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for t in 0..4u64 {
            for i in 0..1000u64 {
                assert!(bloom.may_contain(t * 10000 + i));
            }
        }
    }

    #[test]
    fn test_bloom_filter_minimum_size() {
        let bloom = ColdBloomFilter::new(0, 10);
        assert!(bloom.num_bits >= 64);
        bloom.insert(42);
        assert!(bloom.may_contain(42));
    }
}
