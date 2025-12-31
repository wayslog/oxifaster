//! Epoch 保护机制示例
//!
//! 演示 LightEpoch 的基本用法
//!
//! 运行: cargo run --example epoch_protection

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use oxifaster::epoch::{EpochGuard, LightEpoch};

fn main() {
    println!("=== oxifaster Epoch 保护机制示例 ===\n");

    // 创建 Epoch 实例
    let epoch = Arc::new(LightEpoch::new());

    println!("初始状态:");
    println!("  当前 Epoch: {}", epoch.current_epoch.load(Ordering::Relaxed));
    println!(
        "  安全回收 Epoch: {}",
        epoch.safe_to_reclaim_epoch.load(Ordering::Relaxed)
    );

    // 示例 1: 基本的保护和取消保护
    println!("\n--- 示例 1: 基本保护 ---");
    {
        let thread_id = 0;
        
        println!("  线程 {} 是否受保护: {}", thread_id, epoch.is_protected(thread_id));
        
        let protected_epoch = epoch.protect(thread_id);
        println!("  线程 {} 进入保护, Epoch: {}", thread_id, protected_epoch);
        println!("  线程 {} 是否受保护: {}", thread_id, epoch.is_protected(thread_id));
        
        epoch.unprotect(thread_id);
        println!("  线程 {} 退出保护", thread_id);
        println!("  线程 {} 是否受保护: {}", thread_id, epoch.is_protected(thread_id));
    }

    // 示例 2: 使用 EpochGuard (RAII)
    println!("\n--- 示例 2: EpochGuard (RAII) ---");
    {
        let thread_id = 1;
        
        {
            let _guard = EpochGuard::new(&epoch, thread_id);
            println!("  线程 {} 通过 Guard 进入保护", thread_id);
            println!("  线程 {} 是否受保护: {}", thread_id, epoch.is_protected(thread_id));
            
            // Guard 作用域内是受保护的
        }
        
        // Guard 离开作用域后自动取消保护
        println!("  Guard 离开作用域");
        println!("  线程 {} 是否受保护: {}", thread_id, epoch.is_protected(thread_id));
    }

    // 示例 3: 递增 Epoch
    println!("\n--- 示例 3: 递增 Epoch ---");
    {
        let initial = epoch.current_epoch.load(Ordering::Relaxed);
        println!("  当前 Epoch: {}", initial);
        
        let new_epoch = epoch.bump_current_epoch();
        println!("  递增后 Epoch: {}", new_epoch);
        
        let new_epoch2 = epoch.bump_current_epoch();
        println!("  再次递增后 Epoch: {}", new_epoch2);
    }

    // 示例 4: 带回调的 Epoch 递增
    println!("\n--- 示例 4: 带回调的 Epoch 递增 ---");
    {
        let callback_called = Arc::new(AtomicU64::new(0));
        let callback_called_clone = callback_called.clone();
        
        let epoch_value = epoch.bump_current_epoch_with_action(move || {
            callback_called_clone.fetch_add(1, Ordering::SeqCst);
            println!("  [回调] Epoch 已变为安全!");
        });
        
        println!("  注册回调于 Epoch: {}", epoch_value - 1);
        
        // 计算安全 Epoch 以触发回调
        epoch.compute_new_safe_to_reclaim_epoch(epoch_value + 10);
        
        // 短暂等待回调执行
        thread::sleep(Duration::from_millis(10));
        
        println!("  回调执行次数: {}", callback_called.load(Ordering::Relaxed));
    }

    // 示例 5: 多线程场景
    println!("\n--- 示例 5: 多线程 Epoch 使用 ---");
    {
        let epoch_clone = epoch.clone();
        let num_threads = 4;
        
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let epoch = epoch_clone.clone();
                thread::spawn(move || {
                    // 每个线程进入保护
                    let protected = epoch.protect(thread_id);
                    println!("  线程 {} 进入保护, Epoch: {}", thread_id, protected);
                    
                    // 模拟一些工作
                    thread::sleep(Duration::from_millis(10));
                    
                    // 退出保护
                    epoch.unprotect(thread_id);
                    println!("  线程 {} 退出保护", thread_id);
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    // 示例 6: 安全回收检查
    println!("\n--- 示例 6: 安全回收检查 ---");
    {
        let thread_id = 0;
        
        // 保护线程
        epoch.protect(thread_id);
        let protected_epoch = epoch.current_epoch.load(Ordering::Relaxed);
        println!("  线程 {} 受保护于 Epoch: {}", thread_id, protected_epoch);
        
        // 递增几次 Epoch
        for _ in 0..5 {
            epoch.bump_current_epoch();
        }
        
        // 计算安全回收 Epoch
        let current = epoch.current_epoch.load(Ordering::Relaxed);
        let safe = epoch.compute_new_safe_to_reclaim_epoch(current);
        
        println!("  当前 Epoch: {}", current);
        println!("  安全回收 Epoch: {}", safe);
        println!(
            "  Epoch {} 是否安全回收: {}",
            protected_epoch - 1,
            epoch.is_safe_to_reclaim(protected_epoch - 1)
        );
        println!(
            "  Epoch {} 是否安全回收: {}",
            protected_epoch,
            epoch.is_safe_to_reclaim(protected_epoch)
        );
        
        // 取消保护
        epoch.unprotect(thread_id);
        
        // 重新计算
        let safe2 = epoch.compute_new_safe_to_reclaim_epoch(current);
        println!("\n  取消保护后:");
        println!("  安全回收 Epoch: {}", safe2);
        println!(
            "  Epoch {} 是否安全回收: {}",
            protected_epoch,
            epoch.is_safe_to_reclaim(protected_epoch)
        );
    }

    println!("\n=== 示例完成 ===");
}

