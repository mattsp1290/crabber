use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_inline_body_and_panic_payload_keep_both_owners_and_original_box_pending() {
    for mode in [1, 2] {
        for contextual_call in [false, true] {
            let seen = Seen::new();
            seen.mode.store(mode, Ordering::SeqCst);
            *seen.call.open.lock().unwrap() = false;
            let _release = Release(vec![seen.call.clone()]);
            let (provider, host) = setup(&seen, FactoryScope::Session);
            let plan = provider.acquire_plan_in(&context()).await.unwrap();
            let guard = plan.guards[0].clone();
            drop(plan);
            let resources = ToolResourceOwner::new();
            let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
            let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
                if contextual_call {
                    contextual(guard.as_ref())
                } else {
                    guard.check("tool", &Value::Null)
                }
            }));
            seen.call.entered().await;
            waiter.abort();
            drop(owner);
            resources.close();
            graph(&host).request_close().unwrap();
            let pending = resources.drain(Duration::from_millis(20)).await;
            let observed = graph(&host).tool_guard_product(0).unwrap();
            let closed = observed.disposed(Duration::from_millis(20)).await;
            let dropped = seen.drops.load(Ordering::SeqCst);
            seen.call.release();
            let _actual = waiter.await;
            let joined = resources.drain(BOUND).await;
            let disposed = observed.disposed(BOUND).await;
            let closed_final = provider.close_all(BOUND).await;
            seen.registry.lock().unwrap().take();
            assert_ne!(pending.pending, 0);
            assert!(closed.is_err());
            assert_eq!(dropped, 0);
            assert_eq!(joined.pending, 0);
            assert_eq!(joined.failures.is_empty(), mode == 1);
            assert_eq!(disposed.is_err(), mode == 2);
            assert_eq!(closed_final.is_err(), mode == 2);
            assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creator_seal_while_original_id_blocks_prevents_later_check_effect() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen, FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let guard = plan.guards[0].clone();
    drop(plan);
    seen.block_id.store(true, Ordering::SeqCst);
    *seen.id_gate.open.lock().unwrap() = false;
    let _release = Release(vec![seen.id_gate.clone()]);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        guard.check("tool", &Value::Null)
    }));
    seen.id_gate.entered().await;
    drop(owner);
    resources.close();
    let pending = resources.drain(Duration::from_millis(20)).await;
    seen.id_gate.release();
    let result = waiter.await.unwrap();
    let joined = resources.drain(BOUND).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    seen.registry.lock().unwrap().take();
    assert_eq!(result, GuardDecision::Deny);
    assert_ne!(pending.pending, 0);
    assert_eq!(joined.pending, 0);
    assert_eq!(seen.ordinary.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creator_seal_during_original_check_refuses_late_allow_after_real_return() {
    let seen = Seen::new();
    seen.mode.store(1, Ordering::SeqCst);
    *seen.call.open.lock().unwrap() = false;
    let _release = Release(vec![seen.call.clone()]);
    let (provider, host) = setup(&seen, FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let guard = plan.guards[0].clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        guard.check("tool", &Value::Null)
    }));
    seen.call.entered().await;
    drop(owner);
    resources.close();
    let held = resources.drain(Duration::from_millis(20)).await;
    seen.call.release();
    let result = waiter.await.unwrap();
    let joined = resources.drain(BOUND).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .tool_guard_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    seen.registry.lock().unwrap().take();
    assert_eq!(result, GuardDecision::Deny);
    assert_ne!(held.pending, 0);
    assert_eq!(joined.pending, 0);
    assert_eq!(seen.ordinary.load(Ordering::SeqCst), 1);
}
