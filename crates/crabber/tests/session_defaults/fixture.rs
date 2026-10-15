use async_trait::async_trait;
use crabber::core::{
    ControlId, ProviderBindingDescriptor, ProviderBindingLimits, SessionId, SessionRevision,
};
use crabber::extension::{ComponentIdentity, Extension, Registrar};
use crabber::providers::{ProviderBindingError, ProviderError, Resolver, Streamer};
use crabber::session::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber::{
    Agent, AgentConfig, ExtensionError, ExtensionFactory, FactoryContext, FactoryScope,
    FakeProvider, InputFingerprint, Selection, StreamDelta,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Clone)]
pub(super) struct Probe {
    pub fake: FakeProvider,
    pub resolutions: Arc<AtomicUsize>,
    pub factories: Arc<AtomicUsize>,
}
impl Probe {
    pub fn new(turns: usize) -> Self {
        Self {
            fake: FakeProvider::scripted(
                (0..turns)
                    .map(|_| {
                        vec![
                            StreamDelta::TextDelta("done".into()),
                            StreamDelta::Completed,
                        ]
                    })
                    .collect(),
            ),
            resolutions: Arc::new(AtomicUsize::new(0)),
            factories: Arc::new(AtomicUsize::new(0)),
        }
    }
}
#[async_trait]
impl Resolver for Probe {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "session-defaults-fixture",
            "1",
            InputFingerprint::new("c".repeat(64)).unwrap(),
            ProviderBindingLimits {
                context_tokens: Some(8192),
                output_tokens: Some(64),
            },
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(self.fake.clone()))
    }
}
struct Factory(Arc<AtomicUsize>);
struct Empty;
#[async_trait]
impl Extension for Empty {
    fn config_hash(&self) -> String {
        "v1".into()
    }
    fn id(&self) -> &'static str {
        "defaults-empty"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "defaults-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Empty))
    }
}
pub(super) fn selection(provider: &str, model: &str) -> Selection {
    Selection {
        provider_id: provider.into(),
        model_id: model.into(),
    }
}
pub(super) fn agent(store: Arc<dyn Store>, probe: &Probe) -> Agent {
    agent_with_selection(store, probe, selection("host-provider", "host-model"))
}
pub(super) fn agent_with_selection(
    store: Arc<dyn Store>,
    probe: &Probe,
    fallback: Selection,
) -> Agent {
    let mut config = AgentConfig::new(fallback);
    config.workspace_id = "defaults-workspace".into();
    config.directory = "/defaults".into();
    Agent::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .provider(Arc::new(probe.clone()))
        .config(config)
        .extension_factory(Arc::new(Factory(probe.factories.clone())))
        .build()
        .unwrap()
}
pub(super) async fn create(store: &dyn Store, defaults: SessionDefaults) -> SessionId {
    let session = SessionId::new();
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "defaults-workspace".into(),
                directory: "/defaults".into(),
                title: "defaults".into(),
                defaults,
            },
        })
        .await
        .unwrap();
    session
}
pub(super) async fn update(store: &dyn Store, session: &SessionId, defaults: SessionDefaults) {
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: store.session_revision(session).await.unwrap(),
            change: SessionChange::SetDefaults { defaults },
        })
        .await
        .unwrap();
}
pub(super) fn defaults(provider: Option<&str>, model: Option<&str>) -> SessionDefaults {
    SessionDefaults {
        provider_id: provider.map(str::to_owned),
        model_id: model.map(str::to_owned),
    }
}
pub(super) fn options() -> crabber::AdmissionOptions {
    crabber::AdmissionOptions {
        key: crabber::AdmissionKey::new(format!("defaults-{}", ControlId::new())).unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
