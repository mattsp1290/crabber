//! A committed receipt must remain usable when setup is unavailable or closed.
use async_trait::async_trait;
use crabber::extension::{
    ComponentIdentity, Extension, ExtensionError, ExtensionFactory, FactoryContext, FactoryScope,
    Registrar,
};
use crabber::providers::{DeltaStream, ModelRequest, ProviderError, Resolver, Streamer};
use crabber::session::{MemoryStore, Store, StoreError};
use crabber::{
    Admission, AdmissionKey, AdmissionOptions, AdmissionReceipt, Agent, AgentConfig, FakeProvider,
    InputFingerprint, RuntimeError, Selection, SessionId, StreamDelta,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, Semaphore};

fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("retained").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
#[derive(Clone)]
struct Provider {
    fake: FakeProvider,
    resolves: Arc<AtomicUsize>,
    streams: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    gate: bool,
}
impl Provider {
    fn new(gate: bool) -> Self {
        Self {
            fake: FakeProvider::scripted(vec![vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ]]),
            resolves: Arc::default(),
            streams: Arc::default(),
            entered: Arc::default(),
            release: Arc::new(Semaphore::new(0)),
            gate,
        }
    }
}
#[async_trait]
impl Resolver for Provider {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        self.fake.binding_descriptor(selection)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for Provider {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.streams.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        if self.gate {
            self.release.acquire().await.unwrap().forget();
        }
        self.fake.stream(request).await
    }
}
struct Factory {
    version: &'static str,
    unavailable: bool,
    calls: Arc<AtomicUsize>,
}
struct Instance;
#[async_trait]
impl Extension for Instance {
    fn id(&self) -> &'static str {
        "replay-instance"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "replay-factory".into(),
            version: self.version.into(),
        }
    }
    fn config_hash(&self) -> String {
        "declared-configuration".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.unavailable {
            return Err(ExtensionError::ContextInitializationFailed {
                component: "unavailable-fixture".into(),
            });
        }
        Ok(Arc::new(Instance))
    }
}
fn agent(
    store: Arc<dyn Store>,
    provider: &Provider,
    version: &'static str,
    unavailable: bool,
    calls: Arc<AtomicUsize>,
) -> Agent {
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "replay-workspace".into();
    config.directory = "/admitted/replay".into();
    Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .provider(Arc::new(provider.clone()))
        .config(config)
        .extension_factory(Arc::new(Factory {
            version,
            unavailable,
            calls,
        }))
        .build()
        .unwrap()
}
async fn replay(agent: &Agent, session: &SessionId, expected: &AdmissionReceipt) {
    assert!(
        matches!(agent.prompt_keyed(session.clone(), "original", options()).await.unwrap(), Admission::Replayed(receipt) if receipt == *expected)
    );
    assert!(
        matches!(agent.recover_admission(session.clone(), "original", options()).await.unwrap(), Admission::Replayed(receipt) if receipt == *expected)
    );
}
async fn run_contract(store: Arc<dyn Store>) {
    for terminal in [false, true] {
        let original_provider = Provider::new(!terminal);
        let created = Arc::new(AtomicUsize::new(0));
        let original = agent(
            store.clone(),
            &original_provider,
            "1",
            false,
            created.clone(),
        );
        let session = SessionId::new();
        let Admission::Started { receipt, handle } = original
            .prompt_keyed(session.clone(), "original", options())
            .await
            .unwrap()
        else {
            panic!("first request must start");
        };
        let pending = if terminal {
            handle.done().await.unwrap();
            None
        } else {
            original_provider.entered.notified().await;
            Some(handle)
        };
        assert_eq!(created.load(Ordering::SeqCst), 1);
        let unavailable_provider = Provider::new(false);
        let recreated = Arc::new(AtomicUsize::new(0));
        let unavailable = agent(
            store.clone(),
            &unavailable_provider,
            "1",
            true,
            recreated.clone(),
        );
        let revision = store.session_revision(&session).await.unwrap();
        replay(&unavailable, &session, &receipt).await;
        assert_eq!(store.session_revision(&session).await.unwrap(), revision);
        unavailable.close_extensions().await.unwrap();
        replay(&unavailable, &session, &receipt).await;
        assert!(matches!(
            unavailable
                .prompt_keyed(session.clone(), "changed", options())
                .await,
            Err(RuntimeError::Store(StoreError::AdmissionConflict))
        ));
        let mut new_key = options();
        new_key.key = AdmissionKey::new("new-request").unwrap();
        assert!(matches!(
            unavailable
                .prompt_keyed(session.clone(), "original", new_key)
                .await,
            Err(RuntimeError::HostClosed)
        ));
        let changed = agent(
            store.clone(),
            &unavailable_provider,
            "2",
            true,
            recreated.clone(),
        );
        assert!(matches!(
            changed
                .prompt_keyed(session.clone(), "original", options())
                .await,
            Err(RuntimeError::Store(StoreError::AdmissionConflict))
        ));
        assert_eq!(recreated.load(Ordering::SeqCst), 0);
        assert_eq!(unavailable_provider.resolves.load(Ordering::SeqCst), 0);
        assert_eq!(unavailable_provider.streams.load(Ordering::SeqCst), 0);
        assert_eq!(store.session_revision(&session).await.unwrap(), revision);
        if let Some(handle) = pending {
            original_provider.release.add_permits(1);
            handle.done().await.unwrap();
        }
        original.close_extensions().await.unwrap();
        changed.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn memory_replay_never_initializes_unavailable_or_closed_components() {
    run_contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_replay_never_initializes_unavailable_or_closed_components() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("replay.sqlite3");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    run_contract(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_replay_never_initializes_unavailable_or_closed_components() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(
            std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none(),
            "required PostgreSQL fixture missing"
        );
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    run_contract(Arc::new(
        crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap(),
    ))
    .await;
}
