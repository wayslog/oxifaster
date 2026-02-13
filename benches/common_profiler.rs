// Shared Criterion profiler hook for dumping `oxifaster::index::profile` counters.
//
// This is only used when running Criterion with `--profile-time`.

use criterion::Criterion;

pub fn configured_criterion() -> Criterion {
    #[cfg(feature = "index-profile")]
    {
        Criterion::default().with_profiler(IndexProfileProfiler)
    }

    #[cfg(not(feature = "index-profile"))]
    {
        Criterion::default()
    }
}

#[cfg(feature = "index-profile")]
struct IndexProfileProfiler;

#[cfg(feature = "index-profile")]
use std::{fs, path::Path};

#[cfg(feature = "index-profile")]
impl criterion::profiler::Profiler for IndexProfileProfiler {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        oxifaster::index::profile::INDEX_INSERT_PROFILE.reset();
    }

    fn stop_profiling(&mut self, benchmark_id: &str, benchmark_dir: &Path) {
        let snapshot = oxifaster::index::profile::INDEX_INSERT_PROFILE.snapshot();
        let contents = format!("benchmark_id: {benchmark_id}\n{snapshot}\n");

        // Best-effort write; profiling should not fail the benchmark run.
        let _ = fs::create_dir_all(benchmark_dir);
        let _ = fs::write(benchmark_dir.join("index_insert_profile.txt"), contents);
    }
}
