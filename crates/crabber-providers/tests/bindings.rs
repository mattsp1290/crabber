use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, RunId, SessionId, TurnId,
};
use crabber_providers::{
    DeltaStream, ModelRequest, ProviderBindingError, ProviderError, RequestIdentity, Resolver,
    Selection, StreamDelta, Streamer,
};
use futures::StreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Routing {
    version: AtomicUsize,
    credential: Arc<AtomicUsize>,
    acquisitions: AtomicUsize,
}
fn selection() -> Selection {
    Selection {
        provider_id: "provider".into(),
        model_id: "model".into(),
    }
}
#[async_trait]
impl Resolver for Routing {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "host-backend",
            format!("v{}", self.version.load(Ordering::SeqCst)),
            InputFingerprint::new("a".repeat(64)).unwrap(),
            ProviderBindingLimits {
                context_tokens: Some(8192),
                output_tokens: Some(1024),
            },
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.acquisitions.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Captured {
            version: self.version.load(Ordering::SeqCst),
            credential: self.credential.clone(),
        }))
    }
}
struct Captured {
    version: usize,
    credential: Arc<AtomicUsize>,
}
#[async_trait]
impl Streamer for Captured {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        Ok(Box::pin(futures::stream::iter(vec![
            StreamDelta::TextDelta(format!(
                "v{} credential-generation={}",
                self.version,
                self.credential.load(Ordering::SeqCst)
            )),
            StreamDelta::Completed,
        ])))
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        identity: RequestIdentity {
            session_id: SessionId::new(),
            run_id: RunId::new(),
            turn_id: TurnId::new(),
        },
        selection: selection(),
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        tool_choice: None,
    }
}

#[tokio::test]
async fn held_binding_keeps_backend_while_credentials_refresh_and_restoration_is_exact() {
    let resolver = Routing {
        version: AtomicUsize::new(1),
        credential: Arc::new(AtomicUsize::new(1)),
        acquisitions: AtomicUsize::new(0),
    };
    let held = resolver.bind(&selection()).await.unwrap();
    let original = held.descriptor().clone();
    resolver.credential.store(2, Ordering::SeqCst);
    let restored = resolver.restore_binding(&original).await.unwrap();
    assert_eq!(restored.descriptor(), &original);
    assert!(
        !serde_json::to_string(&original)
            .unwrap()
            .contains("credential")
    );
    resolver.version.store(2, Ordering::SeqCst);
    assert!(matches!(
        resolver.restore_binding(&original).await,
        Err(ProviderBindingError::VersionConflict)
    ));
    assert_eq!(resolver.acquisitions.load(Ordering::SeqCst), 2);
    let output = held
        .streamer()
        .stream(request())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        output[0],
        StreamDelta::TextDelta("v1 credential-generation=2".into())
    );
    assert_ne!(
        resolver.bind(&selection()).await.unwrap().descriptor(),
        &original
    );
}

struct Undeclared;
#[async_trait]
impl Resolver for Undeclared {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        panic!("unavailable binding must not acquire a backend")
    }
}
#[tokio::test]
async fn undeclared_native_binding_fails_without_acquiring() {
    assert!(matches!(
        Undeclared.bind(&selection()).await,
        Err(ProviderBindingError::Unavailable)
    ));
}

#[tokio::test]
async fn explicit_host_routes_reject_duplicates_and_unknown_models() {
    let resolver = Arc::new(Routing {
        version: AtomicUsize::new(1),
        credential: Arc::new(AtomicUsize::new(1)),
        acquisitions: AtomicUsize::new(0),
    });
    let descriptor = resolver.binding_descriptor(&selection()).unwrap();
    assert!(matches!(
        crabber_providers::VersionedResolver::new(
            resolver.clone(),
            vec![descriptor.clone(), descriptor.clone()]
        ),
        Err(ProviderBindingError::InvalidDescriptor)
    ));
    let routes =
        crabber_providers::VersionedResolver::new(resolver.clone(), vec![descriptor]).unwrap();
    let mut unknown = selection();
    unknown.model_id = "undeclared".into();
    assert!(matches!(
        routes.bind(&unknown).await,
        Err(ProviderBindingError::Unavailable)
    ));
    assert!(routes.resolve(&unknown).await.is_err());
    assert_eq!(resolver.acquisitions.load(Ordering::SeqCst), 0);
    routes.bind(&selection()).await.unwrap();
    assert_eq!(resolver.acquisitions.load(Ordering::SeqCst), 1);
}

struct Mismatched;
#[async_trait]
impl Resolver for Mismatched {
    fn binding_descriptor(
        &self,
        _: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        Routing {
            version: AtomicUsize::new(1),
            credential: Arc::new(AtomicUsize::new(1)),
            acquisitions: AtomicUsize::new(0),
        }
        .binding_descriptor(&Selection {
            provider_id: "other".into(),
            model_id: "model".into(),
        })
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        panic!("mismatched metadata must fail before acquisition")
    }
}
#[tokio::test]
async fn mismatched_descriptor_fails_before_acquisition() {
    assert!(matches!(
        Mismatched.bind(&selection()).await,
        Err(ProviderBindingError::InvalidDescriptor)
    ));
}

#[tokio::test]
async fn versioned_extension_adapter_freezes_catalog_and_rejects_other_provider_identity() {
    use crabber_providers::{FakeProvider, ProviderAdapter, VersionedAdapter};
    let inner = Arc::new(FakeProvider::default());
    let selected = Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    };
    let descriptor = ProviderAdapter::binding_descriptor(inner.as_ref(), &selected).unwrap();
    assert!(matches!(
        VersionedAdapter::new(
            inner.clone(),
            vec![
                Routing {
                    version: AtomicUsize::new(1),
                    credential: Arc::new(AtomicUsize::new(1)),
                    acquisitions: AtomicUsize::new(0)
                }
                .binding_descriptor(&selection())
                .unwrap()
            ]
        ),
        Err(ProviderBindingError::InvalidDescriptor)
    ));
    let adapter = VersionedAdapter::new(inner, vec![descriptor.clone()]).unwrap();
    let models = adapter.models().await.unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(
        models[0].context_limit,
        descriptor.limits().context_tokens.unwrap()
    );
    assert_eq!(
        adapter
            .restore_binding(&descriptor)
            .await
            .unwrap()
            .descriptor(),
        &descriptor
    );
    assert!(matches!(
        adapter
            .bind(&Selection {
                provider_id: "fake".into(),
                model_id: "undeclared".into()
            })
            .await,
        Err(ProviderBindingError::Unavailable)
    ));
}
