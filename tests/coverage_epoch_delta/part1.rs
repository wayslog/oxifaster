#[test]
fn cov_light_epoch_protect_and_drain_no_pending() {
    let epoch = LightEpoch::new();
    // With no pending drain actions the fast path should just protect.
    let e = epoch.protect_and_drain(0);
    assert_eq!(e, 1);
    assert!(epoch.is_protected(0));
    epoch.unprotect(0);
}

#[test]
fn cov_light_epoch_protect_and_drain_with_pending() {
    let epoch = Arc::new(LightEpoch::new());
    let executed = Arc::new(AtomicBool::new(false));
    let executed_clone = executed.clone();

    // Register an action so drain_count > 0.
    epoch.bump_current_epoch_with_action(move || {
        executed_clone.store(true, Ordering::Release);
    });

    // Now protect_and_drain should attempt to drain.
    // First make sure epoch is high enough for the action to trigger.
    for _ in 0..5 {
        epoch.bump_current_epoch();
    }
    epoch.compute_new_safe_to_reclaim_epoch(epoch.current_epoch.load(Ordering::Acquire));

    let e = epoch.protect_and_drain(1);
    assert!(e > 0);
    epoch.unprotect(1);

    // The action should have been executed by now.
    assert!(executed.load(Ordering::Acquire));
}

#[test]
fn cov_light_epoch_reentrant_protect_and_drain_no_pending() {
    let epoch = LightEpoch::new();
    let e = epoch.reentrant_protect_and_drain(0);
    assert_eq!(e, 1);
    assert!(epoch.is_protected(0));
    epoch.reentrant_unprotect(0);
    assert!(!epoch.is_protected(0));
}

#[test]
fn cov_light_epoch_reentrant_protect_and_drain_nested() {
    let epoch = LightEpoch::new();
    let e1 = epoch.reentrant_protect_and_drain(0);
    let e2 = epoch.reentrant_protect_and_drain(0);
    // Nested calls should return the same epoch.
    assert_eq!(e1, e2);
    assert!(epoch.is_protected(0));

    epoch.reentrant_unprotect(0);
    assert!(epoch.is_protected(0)); // Still protected (one level remaining).

    epoch.reentrant_unprotect(0);
    assert!(!epoch.is_protected(0));
}

#[test]
fn cov_light_epoch_reentrant_protect_and_drain_with_pending() {
    let epoch = Arc::new(LightEpoch::new());
    let executed = Arc::new(AtomicBool::new(false));
    let executed_clone = executed.clone();

    epoch.bump_current_epoch_with_action(move || {
        executed_clone.store(true, Ordering::Release);
    });

    for _ in 0..5 {
        epoch.bump_current_epoch();
    }
    epoch.compute_new_safe_to_reclaim_epoch(epoch.current_epoch.load(Ordering::Acquire));

    let _ = epoch.reentrant_protect_and_drain(2);
    epoch.reentrant_unprotect(2);

    assert!(executed.load(Ordering::Acquire));
}

#[test]
fn cov_light_epoch_bump_current_epoch_with_action_multithread() {
    let epoch = Arc::new(LightEpoch::new());
    let counter = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let ep = epoch.clone();
        let ctr = counter.clone();
        let handle = std::thread::spawn(move || {
            let tid = get_thread_id();
            for _ in 0..10 {
                let c = ctr.clone();
                ep.bump_current_epoch_with_action(move || {
                    c.fetch_add(1, Ordering::AcqRel);
                });
                ep.protect_and_drain(tid);
                ep.unprotect(tid);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Drain remaining actions by bumping the epoch significantly and draining.
    for _ in 0..20 {
        epoch.bump_current_epoch();
    }
    epoch.compute_new_safe_to_reclaim_epoch(epoch.current_epoch.load(Ordering::Acquire));

    // Trigger drain on a thread.
    let tid = get_thread_id();
    epoch.protect_and_drain(tid);
    epoch.unprotect(tid);

    // All 40 actions should have been executed.
    assert_eq!(counter.load(Ordering::Acquire), 40);
}

#[test]
fn cov_light_epoch_spin_wait_for_safe_to_reclaim_immediate() {
    let epoch = LightEpoch::new();
    // No threads are protected, so epoch 5 should immediately be safe.
    epoch.current_epoch.store(10, Ordering::Relaxed);
    epoch.spin_wait_for_safe_to_reclaim(10, 5);
    assert!(epoch.is_safe_to_reclaim(5));
}

#[test]
fn cov_light_epoch_spin_wait_for_safe_to_reclaim_with_protected_thread() {
    let epoch = Arc::new(LightEpoch::new());
    // Set epoch to 10.
    epoch.current_epoch.store(10, Ordering::Relaxed);
    // Protect thread 0 at epoch 10.
    epoch.protect(0);

    let ep = epoch.clone();
    let handle = std::thread::spawn(move || {
        // After a short delay, unprotect thread 0.
        std::thread::sleep(std::time::Duration::from_millis(20));
        ep.unprotect(0);
    });

    // This should spin until thread 0 unprotects.
    epoch.spin_wait_for_safe_to_reclaim(10, 9);
    assert!(epoch.is_safe_to_reclaim(9));

    handle.join().unwrap();
}

#[test]
fn cov_light_epoch_reset_phase_finished() {
    let epoch = LightEpoch::new();

    // Mark a few threads as finished for phase 1.
    epoch.protect(0);
    epoch.protect(1);
    epoch.finish_thread_phase(0, 1);
    epoch.finish_thread_phase(1, 1);

    assert!(epoch.has_thread_finished_phase(0, 1));
    assert!(epoch.has_thread_finished_phase(1, 1));

    // Reset should clear all.
    epoch.reset_phase_finished();

    assert!(!epoch.has_thread_finished_phase(0, 1));
    assert!(!epoch.has_thread_finished_phase(1, 1));

    epoch.unprotect(0);
    epoch.unprotect(1);
}

#[test]
fn cov_light_epoch_finish_thread_phase_all_done() {
    let epoch = LightEpoch::new();

    // Protect two threads.
    epoch.protect(0);
    epoch.protect(1);

    // Thread 0 finishes phase 2 - not all done yet.
    let all_done = epoch.finish_thread_phase(0, 2);
    assert!(!all_done);

    // Thread 1 finishes phase 2 - now all done.
    let all_done = epoch.finish_thread_phase(1, 2);
    assert!(all_done);

    epoch.unprotect(0);
    epoch.unprotect(1);
}

#[test]
fn cov_light_epoch_finish_thread_phase_single_thread() {
    let epoch = LightEpoch::new();

    // Only one thread protected.
    epoch.protect(0);
    let all_done = epoch.finish_thread_phase(0, 3);
    // With a single protected thread, finishing should return true.
    assert!(all_done);
    epoch.unprotect(0);
}

#[test]
fn cov_light_epoch_has_thread_finished_phase_not_finished() {
    let epoch = LightEpoch::new();
    // Thread 0 never called finish_thread_phase, so phase_finished is 0.
    assert!(!epoch.has_thread_finished_phase(0, 1));
    // But checking against phase 0 should be true (default value).
    assert!(epoch.has_thread_finished_phase(0, 0));
}

#[test]
fn cov_epoch_guard_thread_id() {
    let epoch = Arc::new(LightEpoch::new());
    let guard = EpochGuard::new(epoch.clone(), 5);
    assert_eq!(guard.thread_id(), 5);
    assert!(epoch.is_protected(5));
    drop(guard);
    assert!(!epoch.is_protected(5));
}

#[test]
fn cov_epoch_guard_nested() {
    let epoch = Arc::new(LightEpoch::new());
    {
        let g1 = EpochGuard::new(epoch.clone(), 3);
        assert!(epoch.is_protected(3));
        {
            let g2 = EpochGuard::new(epoch.clone(), 3);
            assert_eq!(g2.thread_id(), 3);
        }
        // Still protected because g1 is alive.
        assert!(epoch.is_protected(3));
        drop(g1);
    }
    assert!(!epoch.is_protected(3));
}

#[test]
fn cov_get_thread_id_stable() {
    // Calling get_thread_id multiple times on the same thread should return the same id.
    let id1 = get_thread_id();
    let id2 = get_thread_id();
    assert_eq!(id1, id2);
}

#[test]
fn cov_get_thread_id_different_threads() {
    let id_main = get_thread_id();

    let handle = std::thread::spawn(get_thread_id);
    let id_other = handle.join().unwrap();

    // Both should be valid thread IDs.
    assert!(id_main < oxifaster::constants::MAX_THREADS);
    assert!(id_other < oxifaster::constants::MAX_THREADS);
}

#[test]
fn cov_get_thread_tag_returns_value() {
    let _id = get_thread_id();
    let tag = get_thread_tag();
    // Tag is a generation counter; for the first use of this slot it should be 0.
    // For reused slots it may be > 0. We just verify it doesn't panic.
    let _ = tag;
}

#[test]
fn cov_get_thread_tag_changes_on_reuse() {
    // Spawn and join threads to force slot reuse, then check generation changes.
    let mut seen_tags = std::collections::HashMap::new();
    for _ in 0..10 {
        let (id, tag) = std::thread::spawn(|| {
            let id = get_thread_id();
            let tag = get_thread_tag();
            (id, tag)
        })
        .join()
        .unwrap();

        seen_tags.entry(id).or_insert_with(Vec::new).push(tag);
    }

    // At least one slot should have been reused with a different generation tag.
    let any_reused = seen_tags
        .values()
        .any(|tags| tags.len() >= 2 && tags.windows(2).any(|w| w[0] != w[1]));
    assert!(any_reused);
}

#[test]
fn cov_light_epoch_default() {
    let epoch = LightEpoch::default();
    assert_eq!(epoch.current_epoch.load(Ordering::Relaxed), 1);
}

#[test]
fn cov_light_epoch_bump_current_epoch_drains() {
    let epoch = Arc::new(LightEpoch::new());
    let executed = Arc::new(AtomicBool::new(false));
    let executed_clone = executed.clone();

    epoch.bump_current_epoch_with_action(move || {
        executed_clone.store(true, Ordering::Release);
    });

    // Bump epoch many times to push safe_to_reclaim forward.
    for _ in 0..10 {
        epoch.bump_current_epoch();
    }

    // The drain inside bump_current_epoch should have triggered the action.
    assert!(executed.load(Ordering::Acquire));
}

// ============================================================
