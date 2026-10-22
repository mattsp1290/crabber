//! Replace scripts between runs without changing the frozen provider binding.
use crabber::{
    FakeProvider, Selection, StreamDelta,
    core::ProviderBindingDescriptor,
    providers::{ProviderBindingError, ProviderError, Resolver, Streamer},
};
use std::sync::{Arc, Mutex};
#[derive(Default)]
pub struct Scripts(Mutex<Arc<FakeProvider>>);
impl Scripts {
    pub fn set(&self, scripts: Vec<Vec<StreamDelta>>) -> Arc<FakeProvider> {
        let provider = Arc::new(FakeProvider::scripted(scripts));
        *self.0.lock().unwrap() = provider.clone();
        provider
    }
}
#[async_trait::async_trait]
impl Resolver for Scripts {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        Resolver::binding_descriptor(&FakeProvider::default(), selection)
    }
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        let provider = self.0.lock().unwrap().clone();
        provider.resolve(selection).await
    }
}
