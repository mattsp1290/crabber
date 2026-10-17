use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_tool_destructor_is_retained_after_observation_waiter_abort() {
    let seen = Seen::new(Call::Ready);
    *seen.tool_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.tool_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    drop(provider.acquire_plan_in(&context()).await.unwrap());
    let graph = graph(&host);
    let node = graph.tool_product(0).unwrap();
    graph.request_close().unwrap();
    entered(&seen.tool_drop).await;
    let waiter = tokio::spawn({
        let node = node.clone();
        async move { node.disposed(Duration::from_secs(3)).await }
    });
    waiter.abort();
    let aborted = waiter.await;
    let pending = node.disposed(Duration::from_millis(20)).await;
    let before = seen.drops.load(Ordering::SeqCst);
    seen.tool_drop.release();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(matches!(aborted,Err(error) if error.is_cancelled()));
    assert!(pending.is_err());
    assert_eq!(before, 0);
    assert!(disposed.is_ok());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_callback_or_tool_drop_failure_is_sticky_but_actual_cleanup_runs() {
    for callback in [true, false] {
        let seen = Seen::new(Call::Ready);
        seen.callback_panic.store(callback, Ordering::SeqCst);
        seen.tool_panic.store(!callback, Ordering::SeqCst);
        let (provider, host) = setup(seen.clone(), FactoryScope::Session);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        if callback {
            assert!(plan.tools[0].executor.execute(Value::Null).await.is_err());
        }
        drop(plan);
        let graph = graph(&host);
        let node = graph.tool_product(0).unwrap();
        graph.request_close().unwrap();
        let first = node.disposed(Duration::from_secs(3)).await;
        let second = node.disposed(Duration::from_secs(3)).await;
        let close = provider.close_all(Duration::from_secs(3)).await;
        assert!(first.is_err() && second.is_err() && close.is_err());
        assert_eq!(
            seen.drops.load(Ordering::SeqCst),
            1,
            "actual Tool cleanup is driven even after callback failure"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tool_box_drop_keeps_original_initializer_self_close_frame() {
    let seen = Seen::new(Call::Ready);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    *seen.provider.lock().unwrap() = Some(provider.clone());
    drop(provider.acquire_plan_in(&context()).await.unwrap());
    let graph = graph(&host);
    graph.request_close().unwrap();
    let disposed = graph
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    seen.provider.lock().unwrap().take();
    assert!(disposed.is_ok());
    assert_eq!(
        *seen.self_close.lock().unwrap(),
        vec![Err(ExtensionError::SelfClose)]
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_or_changed_resumability_is_refused_instead_of_hidden() {
    for before in [true, false] {
        let seen = Seen::new(Call::Ready);
        seen.declaration.store(before, Ordering::SeqCst);
        let (provider, host) = setup(seen.clone(), FactoryScope::Session);
        let acquired = provider.acquire_plan_in(&context()).await;
        let refused = if before {
            acquired.is_err()
        } else {
            let plan = acquired.unwrap();
            seen.declaration.store(true, Ordering::SeqCst);
            let result = plan.tools[0].executor.execute(Value::Null).await;
            drop(plan);
            result.is_err()
        };
        let graph = graph(&host);
        let _ = graph.request_close();
        let disposed = graph
            .tool_product(0)
            .unwrap()
            .disposed(Duration::from_secs(3))
            .await;
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert!(refused);
        assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
        assert!(disposed.is_ok());
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tool_callback_and_destructor_keep_distinct_original_self_close_frames() {
    let seen = Seen::new(Call::Ready);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    *seen.provider.lock().unwrap() = Some(provider.clone());
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let output = plan.tools[0].executor.execute(Value::Null).await;
    drop(plan);
    let graph = graph(&host);
    graph.request_close().unwrap();
    let disposed = graph
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    seen.provider.lock().unwrap().take();
    assert!(output.is_ok() && disposed.is_ok());
    assert_eq!(
        *seen.self_close.lock().unwrap(),
        vec![
            Err(ExtensionError::SelfClose),
            Err(ExtensionError::SelfClose)
        ]
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleaned_original_node_cannot_close_or_supply_replacement_generation() {
    let seen = Seen::new(Call::Ready);
    seen.declaration.store(true, Ordering::SeqCst);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let first = provider.acquire_plan_in(&context()).await;
    assert!(first.is_err());
    let original = graph(&host);
    let original_node = original.tool_product(0).unwrap();
    let first_disposed = original_node.disposed(Duration::from_secs(3)).await;
    seen.declaration.store(false, Ordering::SeqCst);
    let next = provider.acquire_plan_in(&context()).await.unwrap();
    let replacement = graph(&host);
    let wrong = original.request_close();
    let output = next.tools[0].executor.execute(Value::Null).await;
    let original_again = original_node.disposed(Duration::from_secs(3)).await;
    let before = seen.drops.load(Ordering::SeqCst);
    drop(next);
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    let replaced_disposed = replacement
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    assert!(first_disposed.is_ok() && original_again.is_ok() && replaced_disposed.is_ok());
    assert!(matches!(
        wrong,
        Err(ExtensionError::ContextIdentityMismatch)
    ));
    assert_ne!(original.generation().id(), replacement.generation().id());
    assert!(output.is_ok());
    assert_eq!(before, 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatched_live_capability_is_never_hidden_by_cached_ordinary_facade() {
    for before in [true, false] {
        let seen = Seen::new(Call::Ready);
        seen.capability.store(before, Ordering::SeqCst);
        let (provider, host) = setup(seen.clone(), FactoryScope::Session);
        let acquired = provider.acquire_plan_in(&context()).await;
        let refused = if before {
            acquired.is_err()
        } else {
            let plan = acquired.unwrap();
            seen.capability.store(true, Ordering::SeqCst);
            let result = plan.tools[0].executor.execute(Value::Null).await;
            drop(plan);
            result.is_err()
        };
        let graph = graph(&host);
        let _ = graph.request_close();
        let disposed = graph
            .tool_product(0)
            .unwrap()
            .disposed(Duration::from_secs(3))
            .await;
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert!(refused && disposed.is_ok());
        assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
