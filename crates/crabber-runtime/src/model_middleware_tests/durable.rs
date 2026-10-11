//! Backend-neutral durable middleware reconstruction assertions.
use super::*;

pub(super) async fn verify<F, Fut>(connect: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Arc<dyn Store>>,
{
    // Persist a paused run, then let every runtime-owned object used to admit it
    // fall out of scope. Resume uses a new pool, registry, extension instance,
    // resolver instance, and orchestrator.
    let run_id = {
        let admission_provider = FakeProvider::scripted(vec![call_script()]);
        let store = connect().await;
        let registry = paused_registry(AgentsMdConfig::default()).await;
        let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
        let runtime = paused_runtime(registry, &backend, &admission_provider, store);
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        assert_eq!(admission_provider.requests().len(), 1);
        run_id
    };
    let resume_provider = FakeProvider::scripted(vec![text_script("done")]);
    let store = connect().await;
    let registry = paused_registry(AgentsMdConfig::default()).await;
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v2");
    let runtime = paused_runtime(registry, &backend, &resume_provider, store);
    assert_eq!(
        runtime.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(resume_provider.requests().len(), 1);
    assert!(
        resume_provider.requests()[0]
            .system
            .as_deref()
            .unwrap()
            .contains("\nv2\n")
    );

    // A second durable run proves a changed reconstruction is refused before
    // claiming the row or dispatching another provider request.
    let changed_run_id = {
        let admission_provider = FakeProvider::scripted(vec![call_script()]);
        let store = connect().await;
        let registry = paused_registry(AgentsMdConfig::default()).await;
        let backend = WorkspaceBackend::with_file("AGENTS.md", "original");
        let runtime = paused_runtime(registry, &backend, &admission_provider, store);
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        assert_eq!(admission_provider.requests().len(), 1);
        run_id
    };
    let resume_provider = FakeProvider::scripted(vec![text_script("must-not-run")]);
    let changed_store = connect().await;
    let before = changed_store
        .get_run(&changed_run_id)
        .await
        .unwrap()
        .unwrap();
    let changed_registry =
        paused_registry(AgentsMdConfig::new(vec!["DIFFERENT.md".into()], false).unwrap()).await;
    let changed_backend = WorkspaceBackend::with_file("DIFFERENT.md", "changed");
    let changed_runtime = paused_runtime(
        changed_registry,
        &changed_backend,
        &resume_provider,
        changed_store.clone(),
    );
    assert!(matches!(
        changed_runtime.resume(&changed_run_id).await,
        Err(RuntimeError::PlanChanged)
    ));
    assert_eq!(
        changed_store
            .get_run(&changed_run_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(resume_provider.requests().len(), 0);
    assert_eq!(changed_backend.resolves.lock().unwrap().len(), 0);
}
