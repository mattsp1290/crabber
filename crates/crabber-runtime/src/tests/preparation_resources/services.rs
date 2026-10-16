//! The real Runtime registrar owns actual contextual dependency task lifetimes.
use super::*;
#[path = "services/ack.rs"]
mod ack;
#[path = "services/cleanup.rs"]
mod cleanup;
#[path = "services/faults.rs"]
mod faults;
#[path = "services/provider.rs"]
mod provider;
#[path = "services/recovery.rs"]
mod recovery;
#[path = "services/retention.rs"]
mod retention;
use crabber_extension::{
    ContextualPlanProvider, Extension, ExtensionFactory, FactoryContext, FactoryScope, Registrar,
    Registry,
};
use crabber_session::session_owner::service::{
    SessionServiceCleanupPolicy, SessionServiceGeneration, SessionServiceGenerationScope,
    SessionServiceLink,
};

struct ContextProvider {
    inner: ContextualPlanProvider,
    seen: Arc<Seen>,
    contexts: Mutex<Vec<PreparationResourceContext>>,
    abort_acquisition: Notify,
}
#[async_trait]
impl RunPlanProvider for ContextProvider {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.inner.registration_fingerprint(context)
    }
    fn preparation_resource_contract(
        &self,
        _: &SessionContext,
    ) -> Result<Option<PreparationResourceContract>, ExtensionError> {
        // An explicit fixture bridge, not an automatic Contextual declaration or
        // a certificate for arbitrary callbacks/whole Runtime coverage.
        Ok(Some(PreparationResourceContract::new(
            ComponentIdentity {
                id: "native-service-fixture".into(),
                version: "1".into(),
            },
            InputFingerprint::new("c".repeat(64)).unwrap(),
        )?))
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("owned fixture cannot fall back")
    }
    async fn acquire_plan_in_preparation(
        &self,
        context: &PreparationResourceContext,
    ) -> Result<RunPlan, ExtensionError> {
        self.contexts.lock().unwrap().push(context.clone());
        let decorated = cleanup::decorate(context, self.seen.clone());
        tokio::select! {
            result = self.inner.acquire_plan_in_preparation(&decorated) => result,
            () = self.abort_acquisition.notified() => Err(ExtensionError::ContextInitializationIncomplete),
        }
    }
}
struct Seen {
    created: AtomicUsize,
    stopped: AtomicUsize,
    shutdown: AtomicUsize,
    resource_wait: std::sync::atomic::AtomicBool,
    resource_failure: AtomicUsize,
    resource_started: AtomicUsize,
    resource_finished: AtomicUsize,
    resource_release: tokio::sync::Semaphore,
}
impl Default for Seen {
    fn default() -> Self {
        Self {
            created: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            shutdown: AtomicUsize::new(0),
            resource_wait: std::sync::atomic::AtomicBool::new(false),
            resource_failure: AtomicUsize::new(0),
            resource_started: AtomicUsize::new(0),
            resource_finished: AtomicUsize::new(0),
            resource_release: tokio::sync::Semaphore::new(0),
        }
    }
}
struct Factory(Arc<Seen>);
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "native-workspace-service".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "fixture.v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Workspace
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        assert!(context.session_id().is_none());
        self.0.created.fetch_add(1, Ordering::SeqCst);
        let closing = context.cleanup().closing();
        let seen = self.0.clone();
        context
            .cleanup()
            .spawn_checked(async move {
                closing.await;
                seen.stopped.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        Ok(Arc::new(Instance(self.0.clone())))
    }
}
struct Instance(Arc<Seen>);
#[async_trait]
impl Extension for Instance {
    fn id(&self) -> &'static str {
        "native-workspace-service"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture.v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
    async fn shutdown(&self) {
        self.0.shutdown.fetch_add(1, Ordering::SeqCst);
    }
}
async fn ids(
    store: &dyn Store,
    runtime: &Orchestrator,
    input: &Request,
    provider: &ContextProvider,
) -> Vec<crabber_core::SessionOwnerId> {
    let session = input.session_id.as_ref().unwrap();
    let root = runtime
        .lookup_preparation_operation(session, &PreparationKey::Admission(options().key))
        .await
        .unwrap()
        .unwrap();
    let original = store
        .lookup_session_owner(session, &root.reserve_id)
        .await
        .unwrap()
        .unwrap();
    let link = PreparationResourceLink {
        session_id: session.clone(),
        root: root.owner,
        root_reserve: root.reserve_id,
        root_begin: root.begin_id,
        root_digest: original.semantic_digest,
    };
    provider
        .inner
        .service_generations()
        .iter()
        .map(|generation| {
            let scope = match generation.scope() {
                crabber_extension::ServiceGenerationScope::Shared => {
                    SessionServiceGenerationScope::Shared
                }
                crabber_extension::ServiceGenerationScope::Workspace {
                    workspace_id,
                    directory,
                } => SessionServiceGenerationScope::Workspace {
                    workspace_id: workspace_id.clone(),
                    directory: directory.clone(),
                },
                crabber_extension::ServiceGenerationScope::Session { .. } => panic!("fixture has no session factory"),
            };
            SessionServiceLink {
                root: link.clone(),
                generation: SessionServiceGeneration {
                    id: generation.id().as_str().into(),
                    scope,
                    registration_fingerprint: generation.registration_fingerprint().clone(),
                    cleanup_policy: match generation.cleanup_policy() {
                        crabber_extension::ServiceGenerationCleanupPolicy::LegacySharedMount => {
                            SessionServiceCleanupPolicy::LegacySharedMount
                        }
                        crabber_extension::ServiceGenerationCleanupPolicy::FullySealedFactory => {
                            SessionServiceCleanupPolicy::FullySealedFactory
                        }
                        crabber_extension::ServiceGenerationCleanupPolicy::SealedFactoryLegacyMount => panic!("owned workspace fixture"),
                    },
                },
            }
            .identities()
            .unwrap()
            .owner
        })
        .collect()
}
async fn states(
    store: &dyn Store,
    session: &SessionId,
    ids: &[crabber_core::SessionOwnerId],
    expected: SessionOwnerState,
) {
    for id in ids {
        assert_eq!(
            store.get_session_owner(session, id).await.unwrap().state,
            expected
        );
    }
}
struct ServiceHarness {
    seen: Arc<Seen>,
    provider: Arc<ContextProvider>,
    fake: FakeProvider,
    runtime: Orchestrator,
    a: Request,
    b: Request,
}
impl ServiceHarness {
    fn new(store: &Arc<dyn Store>) -> Self {
        let seen = Arc::new(Seen::default());
        let provider = Arc::new(ContextProvider {
            seen: seen.clone(),
            contexts: Mutex::new(Vec::new()),
            abort_acquisition: Notify::new(),
            inner: ContextualPlanProvider::new(
                Registry::new(),
                vec![],
                vec![Arc::new(Factory(seen.clone()))],
                // Initialization includes real native registration/proof I/O.
                // This fixture tests ownership, not a two-second I/O deadline.
                Duration::from_secs(10),
            )
            .unwrap(),
        });
        let fake = FakeProvider::scripted(vec![
            text_script("one"),
            text_script("two"),
            text_script("three"),
        ]);
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(options().behavior_fingerprint)
            .store(store.clone())
            .resolver(Arc::new(fake.clone()))
            .plan_provider(provider.clone())
            .build()
            .unwrap();
        let mut a = request();
        a.session_id = Some(SessionId::new());
        let mut b = a.clone();
        b.session_id = Some(SessionId::new());
        Self {
            seen,
            provider,
            fake,
            runtime,
            a,
            b,
        }
    }
}
async fn admit_initial(
    runtime: &Orchestrator,
    a: &Request,
    b: &Request,
) -> Option<crabber_core::AdmissionReceipt> {
    let mut original_receipt = None;
    for input in [a, b] {
        let crate::Admission::Started { handle, receipt } = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.start_keyed(input.clone(), options()),
        )
        .await
        .unwrap()
        .unwrap() else {
            panic!("fresh admission")
        };
        handle.done().await.unwrap();
        if original_receipt.is_none() {
            original_receipt = Some(receipt);
        }
    }
    original_receipt
}
async fn reconstructed_manager_cannot_reinitialize_live_generation(
    store: &Arc<dyn Store>,
    input: &Request,
) {
    let other = ServiceHarness::new(store);
    let mut admission = options();
    admission.key = AdmissionKey::new("reconstructed-runtime").unwrap();
    assert!(
        other
            .runtime
            .start_keyed(input.clone(), admission)
            .await
            .is_err()
    );
    assert_eq!(other.seen.created.load(Ordering::SeqCst), 0);
    assert_eq!(
        other.fake.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    // Closing this empty local manager does not close the independent original
    // workspace or certify that the session has no other native owners.
    other
        .provider
        .inner
        .close_all(Duration::from_secs(2))
        .await
        .unwrap();
}
async fn journey(store: Arc<dyn Store>) {
    let ServiceHarness {
        seen,
        provider,
        fake,
        runtime,
        a,
        b,
    } = ServiceHarness::new(&store);
    let original_receipt = admit_initial(&runtime, &a, &b).await;
    assert_eq!(seen.created.load(Ordering::SeqCst), 1);
    reconstructed_manager_cannot_reinitialize_live_generation(&store, &a).await;
    let a_ids = ids(&*store, &runtime, &a, &provider).await;
    let b_ids = ids(&*store, &runtime, &b, &provider).await;
    assert_eq!(a_ids.len(), 2);
    states(
        &*store,
        a.session_id.as_ref().unwrap(),
        &a_ids,
        SessionOwnerState::Active,
    )
    .await;
    states(
        &*store,
        b.session_id.as_ref().unwrap(),
        &b_ids,
        SessionOwnerState::Active,
    )
    .await;
    let context = |input: &Request| {
        SessionContext::new(
            input.session_id.clone().unwrap(),
            input.workspace_id.clone(),
            input.directory.clone(),
        )
    };
    provider
        .inner
        .close_session(&context(&a), Duration::from_secs(2))
        .await
        .unwrap();
    states(
        &*store,
        a.session_id.as_ref().unwrap(),
        &a_ids,
        SessionOwnerState::Joined,
    )
    .await;
    states(
        &*store,
        b.session_id.as_ref().unwrap(),
        &b_ids,
        SessionOwnerState::Active,
    )
    .await;
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 0);
    let mut later = options();
    later.key = AdmissionKey::new("second-root").unwrap();
    let crate::Admission::Started { handle, .. } =
        runtime.start_keyed(b.clone(), later).await.unwrap()
    else {
        panic!("new root same live service")
    };
    handle.done().await.unwrap();
    assert_eq!(seen.created.load(Ordering::SeqCst), 1);
    provider
        .inner
        .close_session(&context(&b), Duration::from_secs(2))
        .await
        .unwrap();
    states(
        &*store,
        b.session_id.as_ref().unwrap(),
        &b_ids,
        SessionOwnerState::Joined,
    )
    .await;
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 0);
    provider
        .inner
        .close_workspace(&context(&b), Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
    assert!(
        matches!(runtime.start_keyed(a, options()).await.unwrap(), crate::Admission::Replayed(receipt) if Some(&receipt) == original_receipt.as_ref())
    );
    assert_eq!(fake.requests().len(), 3);
    provider
        .inner
        .close_all(Duration::from_secs(2))
        .await
        .unwrap();
}
#[tokio::test]
async fn memory_native_services_release_only_exact_session_and_keep_workspace_alive() {
    journey(Arc::new(MemoryStore::new())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_native_services_release_only_exact_session_and_keep_workspace_alive() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime-services.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    journey(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_native_services_release_only_exact_session_and_keep_workspace_alive() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "required PostgreSQL URL missing"
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    journey(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
