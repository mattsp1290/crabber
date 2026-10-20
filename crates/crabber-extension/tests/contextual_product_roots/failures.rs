use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_root_destructor_self_close_retains_exact_owner_frame() {
    let seen = Seen::new(false, false, Failure::None);
    let host = Arc::new(Host::default());
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    *seen.provider.lock().unwrap() = Some(provider.clone());
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    drop(plan);
    let graph = graph(&host);
    let root = graph.product_root(0).unwrap();
    graph.request_close().unwrap();
    let result = root.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    seen.provider.lock().unwrap().take();
    assert!(result.is_ok());
    assert_eq!(
        *seen.self_close.lock().unwrap(),
        vec![Err(ExtensionError::SelfClose)]
    );
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_and_create_future_disposal_failures_stay_sticky_without_repeating_drop() {
    for failure in [Failure::Root, Failure::Future] {
        let seen = Seen::new(false, false, failure);
        let host = Arc::new(Host::default());
        let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
        let acquired = provider.acquire_plan_in(&context()).await;
        drop(acquired);
        let graph = graph(&host);
        let root = graph.product_root(0).unwrap();
        graph.request_close().unwrap();
        let first = root.disposed(Duration::from_secs(3)).await;
        let second = root.disposed(Duration::from_secs(3)).await;
        let retried = provider.acquire_plan_in(&context()).await;
        drop(retried);
        let close = provider.close_all(Duration::from_secs(3)).await;
        assert!(first.is_err() && second.is_err() && close.is_err());
        assert_eq!(seen.created.load(Ordering::SeqCst), 1);
        assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
    }
}

struct NoRoot;
impl ContextualProductFactory for NoRoot {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "no-root".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "no-root".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_root(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<Box<dyn ContextualProductRoot>, ExtensionError>> {
        Box::pin(async { Err(ExtensionError::Plan("normal constructor error".into())) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entered_member_without_returned_root_never_mints_root_disposed() {
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_product_roots(
        Registry::new(),
        vec![Arc::new(NoRoot)],
        Duration::from_secs(3),
        host.clone(),
    )
    .unwrap();
    let result = provider.acquire_plan_in(&context()).await;
    let graph = graph(&host);
    let root = graph.product_root(0).unwrap();
    let joined = graph
        .registered_graph_join(Duration::from_secs(3))
        .await
        .unwrap();
    let disposed = root.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(result.is_err());
    assert_eq!(
        joined.dispositions(),
        &[ContextualGenerationMemberDisposition::CleanupJoined]
    );
    assert!(matches!(
        disposed,
        Err(ExtensionError::ContextInitializationIncomplete)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_payload_disposal_keeps_original_self_close_frame_and_failed_join() {
    let seen = Seen::new(false, false, Failure::Root);
    seen.panic_payload.store(true, Ordering::SeqCst);
    let host = Arc::new(Host::default());
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    *seen.provider.lock().unwrap() = Some(provider.clone());
    drop(provider.acquire_plan_in(&context()).await.unwrap());
    let graph = graph(&host);
    let root = graph.product_root(0).unwrap();
    graph.request_close().unwrap();
    let result = root.disposed(Duration::from_secs(3)).await;
    let retry = root.disposed(Duration::from_secs(3)).await;
    let close = provider.close_all(Duration::from_secs(3)).await;
    seen.provider.lock().unwrap().take();
    assert!(result.is_err() && retry.is_err() && close.is_err());
    assert_eq!(
        *seen.self_close.lock().unwrap(),
        vec![
            Err(ExtensionError::SelfClose),
            Err(ExtensionError::SelfClose)
        ]
    );
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
}
