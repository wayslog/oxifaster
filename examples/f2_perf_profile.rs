#[cfg(not(feature = "f2"))]
fn main() {
    eprintln!(
        "This example requires the `f2` feature. Run:\n  cargo run --example f2_perf_profile --features f2 -- [args]"
    );
}

#[cfg(feature = "f2")]
fn main() {
    app::run();
}

#[cfg(feature = "f2")]
#[allow(clippy::collapsible_if)]
mod app {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use oxifaster::Address;
    use oxifaster::device::NullDisk;
    use oxifaster::f2::{F2Config, F2Kv};

    #[derive(Clone, Copy, Debug)]
    enum Workload {
        HotDominant,
        ColdDominant,
        Balanced,
        ReadOnly,
        Mixed,
        WriteOnly,
    }

    impl Workload {
        fn parse(s: &str) -> Option<Self> {
            match s {
                "hot_dominant" => Some(Self::HotDominant),
                "cold_dominant" => Some(Self::ColdDominant),
                "balanced" => Some(Self::Balanced),
                "read_only" => Some(Self::ReadOnly),
                "mixed" => Some(Self::Mixed),
                "write_only" => Some(Self::WriteOnly),
                _ => None,
            }
        }

        fn default_hot_ratio(self) -> f64 {
            match self {
                Self::HotDominant => 0.9,
                Self::ColdDominant => 0.1,
                Self::Balanced | Self::ReadOnly | Self::Mixed | Self::WriteOnly => 0.5,
            }
        }

        fn read_probability(self) -> f64 {
            match self {
                Self::ReadOnly | Self::HotDominant | Self::ColdDominant | Self::Balanced => 1.0,
                Self::Mixed => 0.5,
                Self::WriteOnly => 0.0,
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct Args {
        keys: u64,
        hot_ratio: Option<f64>,
        migrate_fraction: f64,
        threads: usize,
        seconds: u64,
        workload: Workload,
    }

    impl Default for Args {
        fn default() -> Self {
            Self {
                keys: 100_000,
                hot_ratio: None,
                migrate_fraction: 0.5,
                threads: 1,
                seconds: 5,
                workload: Workload::Balanced,
            }
        }
    }

    #[derive(Default, Clone, Copy)]
    struct ThreadStats {
        ops: u64,
        reads: u64,
        writes: u64,
        read_hits: u64,
        hot_read_targets: u64,
        cold_read_targets: u64,
        hot_hits: u64,
        cold_hits: u64,
    }

    impl ThreadStats {
        fn merge(self, rhs: Self) -> Self {
            Self {
                ops: self.ops.saturating_add(rhs.ops),
                reads: self.reads.saturating_add(rhs.reads),
                writes: self.writes.saturating_add(rhs.writes),
                read_hits: self.read_hits.saturating_add(rhs.read_hits),
                hot_read_targets: self.hot_read_targets.saturating_add(rhs.hot_read_targets),
                cold_read_targets: self.cold_read_targets.saturating_add(rhs.cold_read_targets),
                hot_hits: self.hot_hits.saturating_add(rhs.hot_hits),
                cold_hits: self.cold_hits.saturating_add(rhs.cold_hits),
            }
        }
    }

    fn parse_args() -> Args {
        let mut args = Args::default();
        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            match flag.as_str() {
                "--keys" => {
                    if let Some(v) = it.next() {
                        if let Ok(n) = v.parse::<u64>() {
                            args.keys = n.max(1);
                        }
                    }
                }
                "--hot-ratio" => {
                    if let Some(v) = it.next() {
                        if let Ok(r) = v.parse::<f64>() {
                            args.hot_ratio = Some(r.clamp(0.0, 1.0));
                        }
                    }
                }
                "--migrate-fraction" => {
                    if let Some(v) = it.next() {
                        if let Ok(r) = v.parse::<f64>() {
                            args.migrate_fraction = r.clamp(0.0, 1.0);
                        }
                    }
                }
                "--threads" => {
                    if let Some(v) = it.next() {
                        if let Ok(n) = v.parse::<usize>() {
                            args.threads = n.max(1);
                        }
                    }
                }
                "--seconds" => {
                    if let Some(v) = it.next() {
                        if let Ok(n) = v.parse::<u64>() {
                            args.seconds = n.max(1);
                        }
                    }
                }
                "--workload" => {
                    if let Some(v) = it.next() {
                        if let Some(w) = Workload::parse(&v) {
                            args.workload = w;
                        }
                    }
                }
                _ => {}
            }
        }
        args
    }

    fn xorshift64star(seed: &mut u64) -> u64 {
        *seed ^= *seed >> 12;
        *seed ^= *seed << 25;
        *seed ^= *seed >> 27;
        *seed = seed.wrapping_mul(0x2545F4914F6CDD1D);
        *seed
    }

    fn build_compaction_until(begin: Address, tail: Address, fraction: f64) -> Address {
        let span = tail.control().saturating_sub(begin.control());
        if span == 0 {
            return tail;
        }
        let delta = ((span as f64) * fraction.clamp(0.0, 1.0)) as u64;
        Address::from_control(begin.control().saturating_add(delta).min(tail.control()))
    }

    pub fn run() {
        let args = parse_args();
        let hot_ratio = args
            .hot_ratio
            .unwrap_or_else(|| args.workload.default_hot_ratio())
            .clamp(0.0, 1.0);
        let read_probability = args.workload.read_probability();

        let mut config = F2Config::default();
        config.compaction.hot_log_size_budget = 4 << 30;
        let f2 = Arc::new(
            F2Kv::<u64, u64, NullDisk>::new(config, NullDisk::new(), NullDisk::new())
                .expect("failed to create F2Kv"),
        );
        f2.start_session().expect("failed to start session");

        for i in 0..args.keys {
            f2.upsert(i, i)
                .unwrap_or_else(|e| panic!("populate upsert failed at key {i}: {e:?}"));
        }
        f2.refresh();

        let hot_before = f2.hot_store_stats();
        let cold_before = f2.cold_store_stats();
        // TODO: if compaction needs tighter boundary control, add public F2Kv::hot_tail_address().
        let compact_until = build_compaction_until(
            hot_before.begin_address,
            hot_before.tail_address,
            args.migrate_fraction,
        );
        let compaction_result = f2
            .compact_hot_log(compact_until)
            .expect("hot compaction failed");
        f2.refresh();
        let hot_after = f2.hot_store_stats();
        let cold_after = f2.cold_store_stats();

        let cold_key_count = ((args.keys as f64) * args.migrate_fraction.clamp(0.0, 1.0)) as u64;
        let cold_key_count = cold_key_count.min(args.keys);
        let hot_key_count = args.keys.saturating_sub(cold_key_count);

        let stop = Arc::new(AtomicBool::new(false));
        let timer_stop = Arc::clone(&stop);
        let run_for = Duration::from_secs(args.seconds);
        let start = Instant::now();
        let timer = std::thread::spawn(move || {
            std::thread::sleep(run_for);
            timer_stop.store(true, Ordering::Release);
        });

        let mut total = ThreadStats::default();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(args.threads);
            for thread_id in 0..args.threads {
                let f2 = Arc::clone(&f2);
                let stop = Arc::clone(&stop);
                handles.push(scope.spawn(move || {
                    let mut seed = 0x1234_5678_9abc_def0u64 ^ (thread_id as u64);
                    let mut local = ThreadStats::default();
                    while !stop.load(Ordering::Acquire) {
                        let do_read = (xorshift64star(&mut seed) as f64) / (u64::MAX as f64)
                            < read_probability;
                        if do_read {
                            local.reads = local.reads.saturating_add(1);
                            let target_hot =
                                (xorshift64star(&mut seed) as f64) / (u64::MAX as f64) < hot_ratio;
                            let key = if target_hot {
                                if hot_key_count == 0 {
                                    0
                                } else {
                                    cold_key_count + (xorshift64star(&mut seed) % hot_key_count)
                                }
                            } else if cold_key_count == 0 {
                                0
                            } else {
                                xorshift64star(&mut seed) % cold_key_count
                            };

                            if target_hot {
                                local.hot_read_targets = local.hot_read_targets.saturating_add(1);
                            } else {
                                local.cold_read_targets = local.cold_read_targets.saturating_add(1);
                            }

                            if let Ok(Some(_)) = f2.read(&key) {
                                local.read_hits = local.read_hits.saturating_add(1);
                                if target_hot {
                                    local.hot_hits = local.hot_hits.saturating_add(1);
                                } else {
                                    local.cold_hits = local.cold_hits.saturating_add(1);
                                }
                            }
                        } else {
                            local.writes = local.writes.saturating_add(1);
                            let key = xorshift64star(&mut seed) % args.keys;
                            let value = xorshift64star(&mut seed);
                            let _ = f2.upsert(key, value);
                        }

                        local.ops = local.ops.saturating_add(1);
                        if local.ops & 1023 == 0 {
                            f2.refresh();
                        }
                    }
                    local
                }));
            }
            for handle in handles {
                total = total.merge(handle.join().expect("worker thread panicked"));
            }
        });

        let _ = timer.join();
        let elapsed = start.elapsed();
        f2.stop_session();

        let throughput = if elapsed.as_secs_f64() > 0.0 {
            total.ops as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let avg_latency_us = if total.ops > 0 {
            elapsed.as_secs_f64() * 1_000_000.0 / (total.ops as f64)
        } else {
            0.0
        };
        let read_hit_rate = if total.reads > 0 {
            100.0 * (total.read_hits as f64) / (total.reads as f64)
        } else {
            0.0
        };
        let hot_hit_rate = if total.hot_read_targets > 0 {
            100.0 * (total.hot_hits as f64) / (total.hot_read_targets as f64)
        } else {
            0.0
        };
        let cold_hit_rate = if total.cold_read_targets > 0 {
            100.0 * (total.cold_hits as f64) / (total.cold_read_targets as f64)
        } else {
            0.0
        };

        println!("=== f2_perf_profile ===");
        println!("workload         : {:?}", args.workload);
        println!("keys             : {}", args.keys);
        println!("threads          : {}", args.threads);
        println!("seconds          : {}", args.seconds);
        println!("hot_ratio        : {:.2}", hot_ratio);
        println!("migrate_fraction : {:.2}", args.migrate_fraction);
        println!();

        println!("Compaction");
        println!("+----------------------+----------------------+");
        println!("| metric               | value                |");
        println!("+----------------------+----------------------+");
        println!("| until_address        | {:>20} |", compact_until.control());
        println!(
            "| records_compacted    | {:>20} |",
            compaction_result.stats.records_compacted
        );
        println!(
            "| bytes_reclaimed      | {:>20} |",
            compaction_result.stats.bytes_reclaimed
        );
        println!("| hot_size_before      | {:>20} |", hot_before.size);
        println!("| hot_size_after       | {:>20} |", hot_after.size);
        println!("| cold_size_before     | {:>20} |", cold_before.size);
        println!("| cold_size_after      | {:>20} |", cold_after.size);
        println!("+----------------------+----------------------+");
        println!();

        println!("Workload Results");
        println!("+----------------------+----------------------+");
        println!("| metric               | value                |");
        println!("+----------------------+----------------------+");
        println!("| elapsed_s            | {:>20.3} |", elapsed.as_secs_f64());
        println!("| total_ops            | {:>20} |", total.ops);
        println!("| reads                | {:>20} |", total.reads);
        println!("| writes               | {:>20} |", total.writes);
        println!("| throughput_ops_s     | {:>20.2} |", throughput);
        println!("| avg_latency_us       | {:>20.2} |", avg_latency_us);
        println!("| read_hit_rate_pct    | {:>20.2} |", read_hit_rate);
        println!("| hot_hit_rate_pct     | {:>20.2} |", hot_hit_rate);
        println!("| cold_hit_rate_pct    | {:>20.2} |", cold_hit_rate);
        println!("+----------------------+----------------------+");
        println!(
            "store_sizes_bytes: hot={} cold={} total={}",
            f2.hot_store_size(),
            f2.cold_store_size(),
            f2.size()
        );
    }
}
