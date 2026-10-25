//! A large declared context keeps this history test independent of compaction.
use async_trait::async_trait;
use crabber::{
    FakeProvider, Selection,
    core::{InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits},
    providers::{ProviderBindingError, ProviderError, Resolver, Streamer},
};
use std::sync::Arc;
pub(super) struct LongContext(pub FakeProvider);
#[async_trait]
impl Resolver for LongContext {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "newest-history",
            "1",
            InputFingerprint::new("c".repeat(64)).unwrap(),
            ProviderBindingLimits {
                context_tokens: Some(1_000_000),
                output_tokens: Some(64),
            },
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.0.resolve(selection).await
    }
}
