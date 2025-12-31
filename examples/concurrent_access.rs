//! 并发访问示例
//!
//! 演示多线程环境下使用 FasterKV
//!
//! 运行: cargo run --example concurrent_access

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use oxifaster::device::NullDisk;
use oxifaster::status::Status;
use oxifaster::store::{FasterKv, FasterKvConfig};

fn main() {
    println!("=== oxifaster 并发访问示例 ===\n");

    // 配置
    let num_threads = 4;
    let ops_per_thread = 10_000;
    let key_range = 1000u64;

    // 创建存储
    let config = FasterKvConfig {
        table_size: 1 << 16,
        log_memory_size: 1 << 26, // 64 MB
        page_size_bits: 20,
        mutable_fraction: 0.9,
    };
    let device = NullDisk::new();
    let store = Arc::new(FasterKv::<u64, u64, _>::new(config, device));

    // 预填充数据
    println!("预填充 {} 个键...", key_range);
    {
        let mut session = store.start_session();
        for i in 1..=key_range {
            session.upsert(i, i);
        }
    }
    println!("预填充完成\n");

    // 统计计数器
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));
    let successful_reads = Arc::new(AtomicU64::new(0));

    println!("启动 {} 个线程，每个执行 {} 次操作...\n", num_threads, ops_per_thread);
    let start = Instant::now();

    // 创建工作线程
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let store = store.clone();
            let total_reads = total_reads.clone();
            let total_writes = total_writes.clone();
            let successful_reads = successful_reads.clone();

            thread::spawn(move || {
                let mut session = store.start_session();
                let mut local_reads = 0u64;
                let mut local_writes = 0u64;
                let mut local_success = 0u64;

                for i in 0..ops_per_thread {
                    let key = ((thread_id as u64 * ops_per_thread as u64 + i as u64) % key_range) + 1;

                    // 混合读写: 80% 读, 20% 写
                    if i % 5 == 0 {
                        // 写操作
                        let value = key * 10 + thread_id as u64;
                        if session.upsert(key, value) == Status::Ok {
                            local_writes += 1;
                        }
                    } else {
                        // 读操作
                        local_reads += 1;
                        if let Ok(Some(_)) = session.read(&key) {
                            local_success += 1;
                        }
                    }
                }

                total_reads.fetch_add(local_reads, Ordering::Relaxed);
                total_writes.fetch_add(local_writes, Ordering::Relaxed);
                successful_reads.fetch_add(local_success, Ordering::Relaxed);

                println!(
                    "  线程 {} 完成: {} 读, {} 写",
                    thread_id, local_reads, local_writes
                );
            })
        })
        .collect();

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    // 打印结果
    println!("\n--- 结果统计 ---");
    let reads = total_reads.load(Ordering::Relaxed);
    let writes = total_writes.load(Ordering::Relaxed);
    let success = successful_reads.load(Ordering::Relaxed);
    let total_ops = reads + writes;

    println!("总操作数: {}", total_ops);
    println!("  读操作: {} (成功: {})", reads, success);
    println!("  写操作: {}", writes);
    println!("耗时: {:.2?}", elapsed);
    println!(
        "吞吐量: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    // 显示最终索引统计
    println!("\n--- 索引统计 ---");
    let stats = store.index_stats();
    println!("已用条目: {}", stats.used_entries);
    println!("负载因子: {:.2}%", stats.load_factor * 100.0);

    println!("\n=== 示例完成 ===");
}

