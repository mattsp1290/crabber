//! A fixed retry, tool call and final response exercise every live signal.
use async_trait::async_trait;
use crabber::{
    ExtensionError, FakeProvider, JsonToolExecutor, Selection, StreamDelta,
    core::{InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, ToolCallId},
    providers::{
        DeltaStream, ModelRequest, ProviderBindingError, ProviderError, ProviderErrorKind,
        Resolver, Streamer,
    },
};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
#[derive(Clone)]
pub struct Provider {
    first: Arc<AtomicBool>,
    inner: FakeProvider,
}
impl Provider {
    pub fn new() -> Self {
        let id = ToolCallId::new();
        Self {
            first: Arc::new(AtomicBool::new(true)),
            inner: FakeProvider::scripted(vec![
                vec![
                    StreamDelta::ToolCallStart {
                        call_id: id.clone(),
                        name: "echo".into(),
                    },
                    StreamDelta::ToolCallArgsDelta {
                        call_id: id.clone(),
                        text: r#"{"text":"TOOL_ARGUMENT_SECRET"}"#.into(),
                    },
                    StreamDelta::ToolCallDone { call_id: id },
                    StreamDelta::Completed,
                ],
                vec![
                    StreamDelta::TextDelta("done".into()),
                    StreamDelta::Completed,
                ],
            ]),
        }
    }
}
#[async_trait]
impl Resolver for Provider {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "observability-example",
            "1",
            InputFingerprint::new("a".repeat(64)).unwrap(),
            ProviderBindingLimits::default(),
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for Provider {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        if self.first.swap(false, Ordering::SeqCst) {
            return Err(ProviderError {
                kind: ProviderErrorKind::Server,
                message: "SECRET response credential header".into(),
                retryable: true,
            });
        }
        let next = self.inner.resolve(&request.selection).await?;
        next.stream(request).await
    }
}
pub struct Echo;
#[async_trait]
impl JsonToolExecutor for Echo {
    async fn execute(&self, args: Value) -> Result<Value, ExtensionError> {
        Ok(args)
    }
}
