use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_and_control_teardown_never_runs_user_drop_inline() {
    let seen = Seen::new(true, false, Failure::None);
    let _release = Release(vec![seen.root_drop.clone()]);
    let host = Arc::new(Host::default());
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    drop(plan);
    let (returned, dropped_owner) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        drop(provider);
        drop(host);
        let _ = returned.send(());
    });
    tokio::time::timeout(Duration::from_secs(3), dropped_owner)
        .await
        .unwrap()
        .unwrap();
    entered(&seen.root_drop).await;
    let before = seen.dropped.load(Ordering::SeqCst);
    seen.root_drop.release();
    tokio::time::timeout(Duration::from_secs(3), async {
        while seen.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        before, 0,
        "owner teardown returns while original blocking Drop is still held"
    );
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_only_factory_role_scope_and_config_are_frozen_into_empty_plan() {
    let mut fingerprints = Vec::new();
    for (scope, config) in [
        (FactoryScope::Session, "a"),
        (FactoryScope::Session, "b"),
        (FactoryScope::Workspace, "a"),
    ] {
        let seen = Seen::new(false, false, Failure::None);
        let host = Arc::new(Host::default());
        let provider = ContextualPlanProvider::new_with_product_roots(
            Registry::new(),
            vec![Arc::new(Factory {
                seen,
                scope,
                config,
            })],
            Duration::from_secs(3),
            host,
        )
        .unwrap();
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        fingerprints.push(plan.fingerprint.clone());
        assert!(plan.tools.is_empty() && plan.providers.is_empty() && plan.guards.is_empty());
        drop(plan);
        provider.close_all(Duration::from_secs(3)).await.unwrap();
    }
    assert_ne!(fingerprints[0], fingerprints[1]);
    assert_ne!(fingerprints[0], fingerprints[2]);
    let ordinary_empty = Registry::new().acquire(context().session_id());
    assert_ne!(fingerprints[0], ordinary_empty.fingerprint);
}
