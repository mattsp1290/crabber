use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_original_borrow_disposal_blocks_phase_mount_and_box_join() {
    let seen = Seen::new(Call::Pending);
    *seen.callback_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.callback_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let executor = plan.tools[0].executor.clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let cap = owner.capability();
    let call = tokio::spawn(async move {
        with_callback_invocations(cap, async move { executor.execute(Value::Null).await }).await
    });
    seen.called.notified().await;
    call.abort();
    let _ = call.await;
    entered(&seen.callback_drop).await;
    drop(owner);
    resources.close();
    let pending = resources.drain(Duration::from_millis(20)).await;
    let graph = graph(&host);
    graph.request_close().unwrap();
    let node = graph.tool_product(0).unwrap();
    let pending_node = node.disposed(Duration::from_millis(20)).await;
    let before = seen.drops.load(Ordering::SeqCst);
    seen.callback_drop.release();
    let joined = resources.drain(Duration::from_secs(3)).await;
    let disposed = node.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(pending.pending > 0);
    assert!(pending_node.is_err());
    assert_eq!(before, 0);
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
    assert!(disposed.is_ok());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_box_drop_starts_before_held_create_future_disposal() {
    let seen = Seen::new(Call::Ready);
    *seen.create_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.create_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context()).await }
    });
    entered(&seen.create_drop).await;
    let close = tokio::spawn({
        let provider = provider.clone();
        async move { provider.close_all(Duration::from_secs(3)).await }
    });
    let started =
        tokio::time::timeout(Duration::from_secs(1), seen.tool_drop.entered.notified()).await;
    let graph = graph(&host);
    let node = graph.tool_product(0).unwrap();
    let pending = node.disposed(Duration::from_millis(20)).await;
    seen.create_drop.release();
    let acquired = acquire.await.unwrap();
    drop(acquired);
    close.await.unwrap().unwrap();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    assert!(started.is_ok());
    assert!(pending.is_err());
    assert!(disposed.is_ok());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_before_ready_still_installs_original_box_before_future_disposal() {
    let seen = Seen::new(Call::Ready);
    seen.ready.forget_permits(1);
    *seen.create_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.create_drop.clone()]);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context()).await }
    });
    seen.creating.notified().await;
    let graph = graph(&host);
    let node = graph.tool_product(0).unwrap();
    let close = tokio::spawn({
        let provider = provider.clone();
        async move { provider.close_all(Duration::from_secs(3)).await }
    });
    let no_return = node.disposed(Duration::from_millis(20)).await;
    seen.ready.add_permits(1);
    entered(&seen.create_drop).await;
    let started =
        tokio::time::timeout(Duration::from_secs(1), seen.tool_drop.entered.notified()).await;
    let waiting = node.disposed(Duration::from_millis(20)).await;
    seen.create_drop.release();
    let acquired = acquire.await.unwrap();
    drop(acquired);
    let closed = close.await.unwrap();
    let disposed = node.disposed(Duration::from_secs(3)).await;
    assert!(no_return.is_err() && started.is_ok() && waiting.is_err());
    assert!(closed.is_ok() && disposed.is_ok());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_session_reference_cannot_close_sibling_original_node() {
    let seen = Seen::new(Call::Ready);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let a = context();
    let b = SessionContext::new(SessionId("sibling".into()), "workspace", "/workspace");
    let plan_a = provider.acquire_plan_in(&a).await.unwrap();
    let graph_a = graph(&host);
    let executor_a = plan_a.tools[0].executor.clone();
    drop(plan_a);
    let plan_b = provider.acquire_plan_in(&b).await.unwrap();
    let graph_b = graph(&host);
    let held = provider.close_session(&a, Duration::from_millis(20)).await;
    let denied = executor_a.execute(Value::Null).await;
    let sibling = plan_b.tools[0].executor.execute(Value::Null).await;
    let before = seen.drops.load(Ordering::SeqCst);
    drop(executor_a);
    provider
        .close_session(&a, Duration::from_secs(3))
        .await
        .unwrap();
    let disposed_a = graph_a
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    let sibling_held = graph_b
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_millis(20))
        .await;
    drop(plan_b);
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    let disposed_b = graph_b
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    assert!(
        held.is_err()
            && denied.is_err()
            && sibling.is_ok()
            && disposed_a.is_ok()
            && sibling_held.is_err()
            && disposed_b.is_ok()
    );
    assert_eq!(before, 0);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 2);
}
