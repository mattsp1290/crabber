//! Actual Contextual producer, neutral provider consumer and native dependency.
use super::*;
use crabber_providers::{
    BoundProvider, ProviderBindingError, ProviderError, ProviderResourceContext,
    ProviderResourceContract, ProviderServiceSource, Resolver, Selection, Streamer,
};
#[path = "provider/acquisition.rs"]
mod acquisition;
type Held = Arc<Mutex<Vec<(SessionId, Arc<dyn Streamer>)>>>;
#[derive(Default)]
struct FactoryGate {
    hold: std::sync::atomic::AtomicBool,
    entered: Notify,
    release: Notify,
    abort: Notify,
    finished: AtomicUsize,
}
type Source = Arc<Mutex<Option<Arc<dyn ProviderServiceSource>>>>;
struct BorrowFactory {
    inner: Factory,
    source: Source,
}
#[async_trait]
impl ExtensionFactory for BorrowFactory {
    fn identity(&self) -> ComponentIdentity {
        self.inner.identity()
    }
    fn config_hash(&self) -> String {
        self.inner.config_hash()
    }
    fn scope(&self) -> FactoryScope {
        self.inner.scope()
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        *self.source.lock().unwrap() = Some(context.provider_service_source());
        self.inner.create(context).await
    }
}
struct BorrowResolver {
    fake: FakeProvider,
    source: Source,
    constructors: Arc<AtomicUsize>,
    held: Held,
    contexts: Mutex<Vec<ProviderResourceContext>>,
    gate: Arc<FactoryGate>,
}
#[async_trait]
impl Resolver for BorrowResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber_core::ProviderBindingDescriptor, ProviderBindingError> {
        self.fake.binding_descriptor(selection)
    }
    fn binding_resource_contract(
        &self,
        _: &Selection,
    ) -> Result<Option<ProviderResourceContract>, ProviderBindingError> {
        Ok(Some(ProviderResourceContract::new(
            "contextual-borrow-test".into(),
            "1".into(),
            InputFingerprint::new("e".repeat(64)).unwrap(),
        )?))
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        panic!("borrowed provider cannot use legacy resolve")
    }
    async fn bind_owned(
        &self,
        selection: &Selection,
        context: &ProviderResourceContext,
    ) -> Result<BoundProvider, ProviderBindingError> {
        self.contexts.lock().unwrap().push(context.clone());
        let source = self.source.lock().unwrap().clone().unwrap();
        let selection = selection.clone();
        let fake = self.fake.clone();
        let constructors = self.constructors.clone();
        let gate = self.gate.clone();
        let acquisition = context.acquire_service_binding(source, move || async move {
            constructors.fetch_add(1, Ordering::SeqCst);
            if gate.hold.load(Ordering::SeqCst) {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            gate.finished.fetch_add(1, Ordering::SeqCst);
            fake.bind(&selection).await
        });
        let binding = tokio::select! {
            result = acquisition => result?,
            () = self.gate.abort.notified() => return Err(ProviderBindingError::VersionConflict),
        };
        self.held
            .lock()
            .unwrap()
            .push((context.session().session_id().clone(), binding.streamer()));
        Ok(binding)
    }
}
struct BorrowHarness {
    services: ServiceHarness,
    resolver: Arc<BorrowResolver>,
}
impl BorrowHarness {
    fn new(store: &Arc<dyn Store>) -> Self {
        let source = Arc::new(Mutex::new(None));
        let mut h = ServiceHarness::new(store);
        h.provider = Arc::new(ContextProvider {
            seen: h.seen.clone(),
            contexts: Mutex::new(Vec::new()),
            abort_acquisition: Notify::new(),
            inner: ContextualPlanProvider::new(
                Registry::new(),
                vec![],
                vec![Arc::new(BorrowFactory {
                    inner: Factory(h.seen.clone()),
                    source: source.clone(),
                })],
                Duration::from_secs(10),
            )
            .unwrap(),
        });
        let resolver = Arc::new(BorrowResolver {
            fake: h.fake.clone(),
            source,
            constructors: Arc::new(AtomicUsize::new(0)),
            held: Arc::new(Mutex::new(Vec::new())),
            contexts: Mutex::new(Vec::new()),
            gate: Arc::new(FactoryGate::default()),
        });
        h.runtime = Orchestrator::builder()
            .behavior_fingerprint(options().behavior_fingerprint)
            .store(store.clone())
            .resolver(resolver.clone())
            .plan_provider(h.provider.clone())
            .build()
            .unwrap();
        Self {
            services: h,
            resolver,
        }
    }
    fn release(&self, input: &Request) {
        self.resolver
            .held
            .lock()
            .unwrap()
            .retain(|(id, _)| Some(id) != input.session_id.as_ref());
    }
    async fn close(&self, input: &Request, timeout: Duration) -> Result<(), ExtensionError> {
        self.services
            .provider
            .inner
            .close_session(
                &SessionContext::new(
                    input.session_id.clone().unwrap(),
                    input.workspace_id.clone(),
                    input.directory.clone(),
                ),
                timeout,
            )
            .await
    }
}
async fn contract(store: Arc<dyn Store>) {
    let h = BorrowHarness::new(&store);
    let s = &h.services;
    let original = admit_initial(&s.runtime, &s.a, &s.b).await.unwrap();
    assert_eq!(h.resolver.constructors.load(Ordering::SeqCst), 2);
    assert_eq!(s.seen.created.load(Ordering::SeqCst), 1);
    let a_ids = ids(&*store, &s.runtime, &s.a, &s.provider).await;
    assert!(h.close(&s.a, Duration::from_millis(30)).await.is_err());
    for (generation, id) in s.provider.inner.service_generations().iter().zip(&a_ids) {
        if matches!(
            generation.scope(),
            crabber_extension::ServiceGenerationScope::Workspace { .. }
        ) {
            states(
                &*store,
                s.a.session_id.as_ref().unwrap(),
                std::slice::from_ref(id),
                SessionOwnerState::Active,
            )
            .await;
        }
    }
    h.release(&s.b);
    h.close(&s.b, Duration::from_secs(5)).await.unwrap();
    assert_eq!(s.seen.shutdown.load(Ordering::SeqCst), 0);
    h.release(&s.a);
    h.close(&s.a, Duration::from_secs(5)).await.unwrap();
    states(
        &*store,
        s.a.session_id.as_ref().unwrap(),
        &a_ids,
        SessionOwnerState::Joined,
    )
    .await;
    let crate::Admission::Replayed(receipt) =
        s.runtime.start_keyed(s.a.clone(), options()).await.unwrap()
    else {
        panic!("original metadata replay")
    };
    assert_eq!(receipt, original);
    assert_eq!(h.resolver.constructors.load(Ordering::SeqCst), 2);
    assert_eq!(s.fake.requests().len(), 2);
    s.provider
        .inner
        .close_all(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(s.seen.shutdown.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn memory_provider_service_borrow_lifecycle() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_provider_service_borrow_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider-borrow.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_provider_service_borrow_lifecycle() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        contract(Arc::new(
            crabber_session::PostgresStore::connect(&url).await.unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}
