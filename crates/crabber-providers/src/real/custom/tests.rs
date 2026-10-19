//! Custom callbacks require an explicit host declaration on every route.
use super::{CredentialSource, CustomHttpAdapter};
use crate::{
    HttpAdapter, HttpResolver, ModelRequest, Protocol, ProviderAdapter, ProviderBindingError,
    ProviderError, ProviderResourceContext, ProviderResourceHost,
    ProviderResourceRegistrationError, ProviderResourceTask, Resolver, Selection, VersionedAdapter,
};
use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, SessionContext, SessionId,
};
use futures::future::BoxFuture;
use std::sync::Arc;

fn selection() -> Selection {
    Selection {
        provider_id: "custom".into(),
        model_id: "model".into(),
    }
}
fn adapter() -> CustomHttpAdapter {
    HttpAdapter::custom("custom", "http://127.0.0.1:1/v1", Protocol::Responses)
        .with_credential_source(Arc::new(NoCredentialAcquisition))
}
struct NoCredentialAcquisition;
#[async_trait]
impl CredentialSource for NoCredentialAcquisition {
    async fn credential(&self, _: &ModelRequest) -> Result<String, ProviderError> {
        panic!("binding must not acquire credentials or send an HTTP request")
    }
    async fn invalidate(&self, _: &str) {
        panic!("binding must not invalidate credentials")
    }
}
struct NoResourceAcquisition;
impl ProviderResourceHost for NoResourceAcquisition {
    fn register(&self, _: ProviderResourceTask) -> Result<(), ProviderResourceRegistrationError> {
        panic!("an undeclared owned bridge must refuse before resource registration")
    }
    fn closing(&self) -> BoxFuture<'static, ()> {
        panic!("an undeclared owned bridge must not acquire a host closing future")
    }
}
fn context() -> ProviderResourceContext {
    ProviderResourceContext::from_host(
        SessionContext::new(SessionId::new(), "binding-fixture", "/binding-fixture"),
        Arc::new(NoResourceAcquisition),
    )
}
#[tokio::test]
async fn undeclared_custom_binding_refuses_direct_inner_and_resolver_routes() {
    let custom = adapter();
    let inner = &custom.inner;
    let resolver = HttpResolver::new().with_adapter(custom.clone());
    let selection = selection();
    assert_eq!(
        custom.binding_descriptor(&selection),
        Err(ProviderBindingError::Unavailable)
    );
    assert_eq!(
        inner.binding_descriptor(&selection),
        Err(ProviderBindingError::Unavailable)
    );
    assert_eq!(
        resolver.binding_descriptor(&selection),
        Err(ProviderBindingError::Unavailable)
    );
    assert!(matches!(
        custom.bind(&selection).await,
        Err(ProviderBindingError::Unavailable)
    ));
    assert!(matches!(
        inner.bind(&selection).await,
        Err(ProviderBindingError::Unavailable)
    ));
    assert!(matches!(
        resolver.bind(&selection).await,
        Err(ProviderBindingError::Unavailable)
    ));
    assert_eq!(custom.binding_resource_contract(&selection).unwrap(), None);
    assert_eq!(inner.binding_resource_contract(&selection).unwrap(), None);
    assert_eq!(
        resolver.binding_resource_contract(&selection).unwrap(),
        None
    );
    let context = context();
    assert!(matches!(
        custom.bind_owned(&selection, &context).await,
        Err(ProviderBindingError::ResourcesUnsupported)
    ));
    assert!(matches!(
        inner.bind_owned(&selection, &context).await,
        Err(ProviderBindingError::ResourcesUnsupported)
    ));
    assert!(matches!(
        resolver.bind_owned(&selection, &context).await,
        Err(ProviderBindingError::ResourcesUnsupported)
    ));
}
#[tokio::test]
async fn explicit_versioned_custom_adapter_binds_without_inventing_owned_resources() {
    let original = Arc::new(adapter());
    let selection = selection();
    let descriptor = ProviderBindingDescriptor::new(
        "custom",
        "model",
        "host-declared-custom",
        "1",
        InputFingerprint::new("a".repeat(64)).unwrap(),
        ProviderBindingLimits {
            context_tokens: None,
            output_tokens: None,
        },
    )
    .unwrap();
    let declared = VersionedAdapter::new(original.clone(), vec![descriptor.clone()]).unwrap();
    assert_eq!(declared.binding_descriptor(&selection).unwrap(), descriptor);
    assert_eq!(
        declared.bind(&selection).await.unwrap().descriptor(),
        &descriptor
    );
    assert_eq!(
        declared
            .restore_binding(&descriptor)
            .await
            .unwrap()
            .descriptor(),
        &descriptor
    );
    assert!(matches!(
        declared
            .bind(&Selection {
                provider_id: "custom".into(),
                model_id: "undeclared-model".into(),
            })
            .await,
        Err(ProviderBindingError::Unavailable)
    ));
    assert_eq!(
        original.binding_descriptor(&selection),
        Err(ProviderBindingError::Unavailable)
    );
    assert_eq!(
        declared.binding_resource_contract(&selection).unwrap(),
        None
    );
    assert!(matches!(
        declared.bind_owned(&selection, &context()).await,
        Err(ProviderBindingError::ResourcesUnsupported)
    ));
}
