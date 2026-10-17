use super::*;

fn original(host: &Host) -> ContextualGenerationObservation {
    host.observations.lock().unwrap().last().unwrap().clone()
}
#[tokio::test]
async fn original_graph_survives_ticket_and_operational_record_cleanup() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let host = Host::new(Mode::Admit, false);
        let seen = Seen::new(false, false, false);
        let provider = provider(host.clone(), seen.clone(), scope);
        let context = context("graph");
        let plan = provider.acquire_plan_in(&context).await.unwrap();
        let observation = original(&host);
        let generation = observation.generation().clone();
        assert!(
            observation
                .registered_graph_join(Duration::from_millis(10))
                .await
                .is_err()
        );
        drop(plan);
        if scope == FactoryScope::Workspace {
            provider
                .close_workspace(&context, Duration::from_secs(1))
                .await
                .unwrap();
        } else {
            provider
                .close_session(&context, Duration::from_secs(1))
                .await
                .unwrap();
        }
        let joined = observation
            .registered_graph_join(Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(joined.generation(), &generation);
        assert_eq!(joined.members().len(), 1);
        assert!(joined.mount_join(0).is_some());
        assert!(joined.mount_join(1).is_none());
        assert_eq!(
            joined.dispositions(),
            &[ContextualGenerationMemberDisposition::MountedJoined]
        );
        assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
        provider.close_all(Duration::from_secs(1)).await.unwrap();
    }
}
#[tokio::test]
async fn graph_join_waits_original_host_ack_after_waiter_drop_and_close() {
    let host = Host::new(Mode::Admit, true);
    let seen = Seen::new(false, false, false);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let task = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context("host")).await }
    });
    host.entered.notified().await;
    let observation = original(&host);
    task.abort();
    observation.request_close().unwrap();
    let waiting = observation
        .registered_graph_join(Duration::from_millis(20))
        .await;
    host.release.add_permits(1);
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    let joined = observation
        .registered_graph_join(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(waiting.is_err());
    assert_eq!(seen.created.load(Ordering::SeqCst), 0);
    assert_eq!(
        joined.dispositions(),
        &[ContextualGenerationMemberDisposition::NotEntered]
    );
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}
#[tokio::test]
async fn graph_join_keeps_original_constructor_pending_after_cleanup_returns() {
    let host = Host::new(Mode::Admit, false);
    let seen = Seen::new(true, true, false);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let task = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context("constructor")).await }
    });
    seen.entered.notified().await;
    let observation = original(&host);
    observation.request_close().unwrap();
    seen.cleanup_entered.notified().await;
    seen.cleanup_release.add_permits(1);
    while seen.stopped.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let waiting = observation
        .registered_graph_join(Duration::from_millis(20))
        .await;
    seen.create_release.add_permits(1);
    assert!(task.await.unwrap().is_err());
    let joined = observation
        .registered_graph_join(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(waiting.is_err());
    assert_eq!(
        joined.dispositions(),
        &[ContextualGenerationMemberDisposition::CleanupJoined]
    );
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}
#[tokio::test]
async fn old_graph_cannot_close_reselected_generation() {
    let host = Host::new(Mode::Admit, false);
    let seen = Seen::new(false, false, true);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let context = context("retry");
    assert!(provider.acquire_plan_in(&context).await.is_err());
    let old = original(&host);
    let first = old
        .registered_graph_join(Duration::from_secs(1))
        .await
        .unwrap();
    seen.panic_create.store(false, Ordering::SeqCst);
    host.release.add_permits(1);
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let current = original(&host);
    let refused = old.request_close();
    let another = provider.acquire_plan_in(&context).await.unwrap();
    drop(another);
    drop(plan);
    current.request_close().unwrap();
    let second = current
        .registered_graph_join(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(refused, Err(ExtensionError::ContextIdentityMismatch));
    assert_ne!(first.generation(), second.generation());
    assert_eq!(
        first.dispositions(),
        &[ContextualGenerationMemberDisposition::CleanupJoined]
    );
    assert_eq!(
        second.dispositions(),
        &[ContextualGenerationMemberDisposition::MountedJoined]
    );
    assert_eq!(
        old.registered_graph_join(Duration::from_secs(1))
            .await
            .unwrap()
            .generation(),
        first.generation()
    );
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

struct RosterHost(Mutex<Option<ContextualGenerationObservation>>);
impl ContextualGenerationHost for RosterHost {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        *self.0.lock().unwrap() = Some(work.observation().unwrap());
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
struct NeverEntered(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl ExtensionFactory for NeverEntered {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "never-entered".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "never-entered.v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(ExtensionError::ContextCleanupIncomplete)
    }
}
#[tokio::test]
async fn frozen_roster_distinguishes_entered_cleanup_from_never_entered_constructor() {
    let host = Arc::new(RosterHost(Mutex::new(None)));
    let seen = Seen::new(false, false, true);
    let second = Arc::new(AtomicUsize::new(0));
    let provider = ContextualPlanProvider::new_with_generation_host(
        Registry::new(),
        vec![],
        vec![
            Arc::new(Factory {
                seen: seen.clone(),
                scope: FactoryScope::Session,
            }),
            Arc::new(NeverEntered(second.clone())),
        ],
        Duration::from_secs(1),
        host.clone(),
    )
    .unwrap();
    let result = provider.acquire_plan_in(&context("roster")).await;
    let observation = host.0.lock().unwrap().clone().unwrap();
    let joined = observation
        .registered_graph_join(Duration::from_secs(1))
        .await
        .unwrap();
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert!(result.is_err());
    assert_eq!(seen.created.load(Ordering::SeqCst), 1);
    assert_eq!(second.load(Ordering::SeqCst), 0);
    assert_eq!(joined.members().len(), 2);
    assert_eq!(
        joined.dispositions(),
        &[
            ContextualGenerationMemberDisposition::CleanupJoined,
            ContextualGenerationMemberDisposition::NotEntered,
        ]
    );
}

struct RetainedRefusal {
    calls: AtomicUsize,
    original: Mutex<Option<ContextualGenerationWork>>,
}
impl ContextualGenerationHost for RetainedRefusal {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        if first {
            *self.original.lock().unwrap() = Some(work);
            Box::pin(async { ContextualGenerationAdmission::Refused })
        } else {
            Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
        }
    }
}
#[tokio::test]
async fn retained_refused_work_never_uses_a_reconstructed_generation() {
    let host = Arc::new(RetainedRefusal {
        calls: AtomicUsize::new(0),
        original: Mutex::new(None),
    });
    let make = || {
        ContextualPlanProvider::new_with_generation_host(
            Registry::new(),
            vec![],
            vec![Arc::new(Factory {
                seen: Seen::new(false, false, false),
                scope: FactoryScope::Session,
            })],
            Duration::from_secs(1),
            host.clone(),
        )
        .unwrap()
    };
    let old = make();
    let context = context("retained-work");
    let refused = old.acquire_plan_in(&context).await;
    old.close_all(Duration::from_secs(1)).await.unwrap();
    drop(old);
    let replacement = make();
    let plan = replacement.acquire_plan_in(&context).await.unwrap();
    let original = host.original.lock().unwrap().take().unwrap();
    let observed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| original.observation()));
    drop(plan);
    replacement.close_all(Duration::from_secs(1)).await.unwrap();
    assert!(refused.is_err());
    assert!(
        matches!(
            observed,
            Ok(Err(ExtensionError::ContextInitializationIncomplete))
        ),
        "an expired genuine carrier must return a typed refusal instead of panicking"
    );
}
