mod tests {
    use super::*;
    use crate::compaction::AutoCompactionConfig;
    use crate::device::NullDisk;
    use crate::store::{Action, Phase};
    use std::time::{Duration, Instant};

    fn create_test_store() -> Arc<FasterKv<u64, u64, NullDisk>> {
        let config = FasterKvConfig {
            table_size: 1024,
            log_memory_size: 1 << 20, // 1 MB
            page_size_bits: 12,       // 4 KB pages
            mutable_fraction: 0.9,
        };
        let device = NullDisk::new();
        Arc::new(FasterKv::new(config, device))
    }


    include!("tests/part1.rs");
    include!("tests/part2.rs");
}
