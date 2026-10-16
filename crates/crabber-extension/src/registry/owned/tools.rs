//! Invocation facades; held object references do not prove object disposal.
use super::InvocationSource;
use crate::{ExtensionError, ResumableTool, ToolContext, ToolDefinition, ToolExecutor};
use async_trait::async_trait;
use crabber_core::{
    AcceptedToolResponse, ResumableToolIdentity, ToolCheckpoint, ToolOutput, ToolPhaseOutcome,
};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub(in crate::registry) struct Metadata {
    pub(in crate::registry) declaration: Option<ResumableToolIdentity>,
    capability: Option<ResumableToolIdentity>,
}
impl Metadata {
    pub(in crate::registry) fn capture(inner: &Arc<dyn ToolExecutor>) -> Self {
        Self {
            declaration: inner.resumable_identity(),
            capability: inner.resumable().map(ResumableTool::identity),
        }
    }
}

pub(in crate::registry) fn project(
    definition: &Arc<ToolDefinition>,
    metadata: &Metadata,
    source: InvocationSource,
) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: definition.info.clone(),
        executor: Arc::new(Executor {
            inner: definition.executor.clone(),
            source: source.clone(),
            metadata: metadata.clone(),
            resumable: metadata.capability.as_ref().map(|identity| Resumable {
                inner: definition.executor.clone(),
                source,
                metadata: metadata.clone(),
                identity: identity.clone(),
            }),
        }),
    })
}
struct Executor {
    inner: Arc<dyn ToolExecutor>,
    source: InvocationSource,
    metadata: Metadata,
    resumable: Option<Resumable>,
}
#[async_trait]
impl ToolExecutor for Executor {
    fn resumable_identity(&self) -> Option<ResumableToolIdentity> {
        self.metadata.declaration.clone()
    }
    fn resumable(&self) -> Option<&dyn ResumableTool> {
        self.resumable
            .as_ref()
            .map(|tool| tool as &dyn ResumableTool)
    }
    async fn execute(&self, arguments: Value) -> Result<ToolOutput, ExtensionError> {
        let inner = self.inner.clone();
        self.source
            .invoke(move || Box::pin(async move { inner.execute(arguments).await }))
            .await?
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolOutput, ExtensionError> {
        let inner = self.inner.clone();
        self.source
            .invoke(move || {
                Box::pin(async move { inner.execute_with_context(context, arguments).await })
            })
            .await?
    }
}
struct Resumable {
    inner: Arc<dyn ToolExecutor>,
    source: InvocationSource,
    metadata: Metadata,
    identity: ResumableToolIdentity,
}
impl Resumable {
    fn original<'a>(
        inner: &'a dyn ToolExecutor,
        metadata: &Metadata,
    ) -> Result<&'a dyn ResumableTool, ExtensionError> {
        let declaration = inner.resumable_identity();
        let capability = inner.resumable();
        let identity = capability.map(ResumableTool::identity);
        if declaration != metadata.declaration
            || identity != metadata.capability
            || declaration != identity
        {
            return Err(ExtensionError::Tool(
                "resumable tool identity changed".into(),
            ));
        }
        capability.ok_or_else(|| ExtensionError::Tool("resumable tool unavailable".into()))
    }
}
#[async_trait]
impl ResumableTool for Resumable {
    fn identity(&self) -> ResumableToolIdentity {
        self.identity.clone()
    }
    async fn start(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        let inner = self.inner.clone();
        let metadata = self.metadata.clone();
        self.source
            .invoke(move || {
                Box::pin(async move {
                    Self::original(inner.as_ref(), &metadata)?
                        .start(context, arguments)
                        .await
                })
            })
            .await?
    }
    async fn resume(
        &self,
        context: ToolContext,
        arguments: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        let inner = self.inner.clone();
        let metadata = self.metadata.clone();
        self.source
            .invoke(move || {
                Box::pin(async move {
                    Self::original(inner.as_ref(), &metadata)?
                        .resume(context, arguments, checkpoint, responses)
                        .await
                })
            })
            .await?
    }
}
