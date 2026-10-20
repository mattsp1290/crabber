use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn caller_drop_and_late_creator_close_wait_for_literal_original_disposal() {
    for (typed, pending, failed) in [
        (false, true, false),
        (true, true, false),
        (false, false, false),
        (true, false, false),
        (false, false, true),
        (true, false, true),
    ] {
        let seen = Seen::new();
        *seen.future_drop.open.lock().unwrap() = false;
        seen.mode.store(
            if pending {
                2
            } else if failed {
                4
            } else {
                0
            },
            Ordering::SeqCst,
        );
        let release = Release(vec![seen.future_drop.clone()]);
        let (provider, host, _) = provider(&seen, typed, FactoryScope::Session, "original");
        let plan = provider.acquire_plan_in(&session()).await.unwrap();
        let callbacks = plan.prompt_contributors.clone();
        drop(plan);
        let parent = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
        let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
            collect_prompt_contributions(&callbacks, context()).await
        }));
        if pending {
            tokio::time::timeout(BOUND, seen.entered.notified())
                .await
                .unwrap();
            waiter.abort();
        }
        seen.future_drop.entered().await;
        drop(owner);
        parent.close();
        let snapshot = parent.drain(Duration::from_millis(20)).await;
        let close = provider.close_all(Duration::from_millis(20)).await;
        let before = seen.drops.load(Ordering::SeqCst);
        drop(release);
        let outcome = waiter.await;
        let joined = parent.drain(BOUND).await;
        let closed = provider.close_all(BOUND).await;
        let node = host.0.lock().unwrap()[0].prompt_product(0).unwrap();
        let disposed = node.disposed(BOUND).await;
        assert_eq!(snapshot.pending, 1, "{snapshot:?}");
        assert!(close.is_err());
        assert_eq!(before, 0);
        assert_eq!(joined.pending, 0);
        assert_eq!(!joined.failures.is_empty(), failed);
        assert_eq!(closed.is_err(), failed);
        assert_eq!(disposed.is_err(), failed);
        assert_eq!(seen.futures.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        if pending {
            assert!(outcome.unwrap_err().is_cancelled());
        } else {
            assert!(matches!(
                outcome.unwrap(),
                PromptContributionOutcome::Failed { .. }
            ));
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn installed_box_drop_unblocks_original_ready_constructor_future_drop() {
    for typed in [false, true] {
        let seen = Seen::new();
        *seen.constructor_drop.open.lock().unwrap() = false;
        seen.cross.store(true, Ordering::SeqCst);
        let _release = Release(vec![seen.constructor_drop.clone()]);
        let (provider, host, _) = provider(&seen, typed, FactoryScope::Session, "original");
        let actual = provider.clone();
        let waiter = tokio::spawn(async move { actual.acquire_plan_in(&session()).await });
        seen.constructor_drop.entered().await;
        waiter.abort();
        let _ = waiter.await;
        let graph = host.0.lock().unwrap()[0].clone();
        graph.request_close().unwrap();
        graph
            .prompt_product(0)
            .unwrap()
            .disposed(BOUND)
            .await
            .unwrap();
        provider.close_all(BOUND).await.unwrap();
        assert_eq!(seen.made.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_refusal_and_original_blocked_or_failed_box_disposal_are_sticky() {
    for (name, panic) in [("", false), ("original", false), ("original", true)] {
        let seen = Seen::new();
        seen.box_panic.store(panic, Ordering::SeqCst);
        *seen.box_drop.open.lock().unwrap() = false;
        let release = Release(vec![seen.box_drop.clone()]);
        let (provider, host, _) = provider(&seen, true, FactoryScope::Session, name);
        let acquired = provider.acquire_plan_in(&session()).await;
        let invalid = name.is_empty();
        assert_eq!(acquired.is_err(), invalid);
        drop(acquired);
        let graph = host.0.lock().unwrap()[0].clone();
        graph.request_close().unwrap();
        seen.box_drop.entered().await;
        let node = graph.prompt_product(0).unwrap();
        let pending = node.disposed(Duration::from_millis(20)).await;
        drop(release);
        let disposed = node.disposed(BOUND).await;
        let repeated = node.disposed(BOUND).await;
        let closed = provider.close_all(BOUND).await;
        assert!(pending.is_err());
        assert_eq!(disposed.is_err(), invalid || panic);
        assert_eq!(repeated.is_err(), invalid || panic);
        assert_eq!(closed.is_err(), invalid || panic);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn metadata_fingerprint_at_full_reference_capacity_is_pure_and_stable() {
    for typed in [false, true] {
        let seen = Seen::new();
        let (provider, _, registry) = provider(&seen, typed, FactoryScope::Session, "original");
        let context = session();
        let mut plans = Vec::new();
        plans.push(provider.acquire_plan_in(&context).await.unwrap());
        let original = registry.registration_fingerprint(&context).unwrap();
        for _ in 1..64 {
            plans.push(provider.acquire_plan_in(&context).await.unwrap());
        }
        let refused = provider.acquire_plan_in(&context).await;
        let full = registry.registration_fingerprint(&context).unwrap();
        let full_again = registry.registration_fingerprint(&context).unwrap();
        let before = seen.drops.load(Ordering::SeqCst);
        drop(plans);
        provider.close_all(BOUND).await.unwrap();
        assert!(refused.is_err());
        assert_eq!(full, original);
        assert_eq!(full_again, original);
        assert_eq!(before, 0);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
        assert_eq!(seen.made.load(Ordering::SeqCst), 1);
    }
}
