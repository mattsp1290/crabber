use async_trait::async_trait;
use crabber::core::ProviderBindingDescriptor;
use crabber::providers::ModelRequest;
use crabber::providers::{DeltaStream, ProviderBindingError, ProviderError, Resolver, Streamer};
use crabber::{FakeProvider, Selection};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Clone)]
pub(super) struct Gate {
    pub fake: FakeProvider,
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
    pub resolutions: Arc<AtomicUsize>,
    streams: Arc<AtomicUsize>,
}
impl Gate {
    pub fn new(turns: usize) -> Self {
        Self {
            fake: FakeProvider::scripted(
                (0..turns)
                    .map(|_| {
                        vec![
                            crabber::StreamDelta::TextDelta("done".into()),
                            crabber::StreamDelta::Completed,
                        ]
                    })
                    .collect(),
            ),
            entered: Arc::default(),
            release: Arc::default(),
            resolutions: Arc::default(),
            streams: Arc::default(),
        }
    }
}
#[async_trait]
impl Resolver for Gate {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        Resolver::binding_descriptor(&self.fake, selection)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for Gate {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let stream = self.fake.stream(request).await?;
        if self.streams.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(stream)
    }
}
