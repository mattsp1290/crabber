//! The original native source commit survives a lost response, without another write.
use super::super::{
    Error, call, done, selection,
    store::{AuditedStore, SourceAck},
};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, JsonToolExecutor, PermissionDecision,
    StaticPolicy, ToolDefinition,
    core::{ControlId, PublicSourceChange, RunStatus, ToolInfo},
    extension::{PublicSourceError, PublicSourceWriter, ToolContext},
    session::Store,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

struct Original {
    writer: PublicSourceWriter,
    id: ControlId,
    expected: crabber::core::SessionRevision,
    change: PublicSourceChange,
    revision: crabber::core::SessionRevision,
}
struct Publish(Arc<Mutex<Option<Original>>>);
#[async_trait]
impl JsonToolExecutor for Publish {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Rejected("actual source context required"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let writer = context.public_source().clone();
        let snapshot = writer.snapshot().await.map_err(failure)?;
        let id = ControlId::new();
        let change = PublicSourceChange::StateSnapshot {
            state: json!({"display":"ACK_LOSS_DISPLAY_ONLY"}),
        };
        assert_eq!(
            writer
                .publish(id.clone(), snapshot.revision, change.clone())
                .await,
            Err(PublicSourceError::UnknownOutcome)
        );
        let committed = writer.snapshot().await.map_err(failure)?;
        assert_eq!(committed.revision, snapshot.revision.next().unwrap());
        let revision = writer
            .publish(id.clone(), snapshot.revision, change.clone())
            .await
            .map_err(failure)?;
        assert_eq!(revision, committed.revision);
        assert_eq!(writer.snapshot().await.map_err(failure)?, committed);
        *self.0.lock().unwrap() = Some(Original {
            writer,
            id,
            expected: snapshot.revision,
            change,
            revision,
        });
        Ok(json!({"completed":true}))
    }
}
fn failure(error: PublicSourceError) -> ExtensionError {
    ExtensionError::Tool(error.to_string())
}
pub(super) async fn run(inner: Arc<dyn Store>) -> Result<(), Error> {
    let ack = Arc::new(SourceAck::default());
    let original = Arc::new(Mutex::new(None));
    let provider = FakeProvider::scripted(vec![call("unknown_source"), done()]);
    let host = Agent::builder()
        .behavior_version("source-original-response-loss.v1")
        .store(Arc::new(AuditedStore::with_source_ack_loss(
            inner.clone(),
            ack.clone(),
        )))
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "unknown_source".into(),
                description: "native ACK loss probe".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Publish(original.clone())),
        }))
        .build()?;
    let handle = host
        .prompt(None, "publish once and reconcile the original")
        .await?;
    let session = handle.session_id().clone();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), handle.done())
            .await??
            .status,
        RunStatus::Completed
    );
    host.close_extensions().await?;
    let original = original.lock().unwrap().take().unwrap();
    verify(&*inner, &session, original, &ack, &provider).await
}
async fn verify(
    inner: &dyn Store,
    session: &crabber::core::SessionId,
    original: Original,
    ack: &SourceAck,
    provider: &FakeProvider,
) -> Result<(), Error> {
    let (mutation, receipt) = ack.committed.lock().unwrap().take().unwrap();
    assert_eq!(mutation.id, original.id);
    assert_eq!(mutation.change, original.change);
    assert_eq!(mutation.expected_revision, original.expected);
    assert_eq!(receipt.revision, original.revision);
    assert_eq!(ack.calls.load(Ordering::SeqCst), 2);
    let events = inner.list_events(session, None, 1000).await?;
    let state = inner.public_state(session).await?;
    let publication = inner
        .lookup_public_source_publication(session, &original.id)
        .await?
        .unwrap();
    assert_eq!(publication.receipt, receipt);
    assert_eq!(publication.events.len(), 1);
    for event in &publication.events {
        assert_eq!(events.iter().filter(|saved| *saved == event).count(), 1);
    }
    assert_eq!(
        inner
            .lookup_public_state_mutation(session, &original.id)
            .await?,
        Some(receipt)
    );
    assert_eq!(
        original
            .writer
            .publish(original.id, original.expected, original.change)
            .await?,
        original.revision
    );
    assert_eq!(
        ack.calls.load(Ordering::SeqCst),
        2,
        "sealed replay must be read-only"
    );
    assert_eq!(inner.public_state(session).await?, state);
    assert_eq!(inner.list_events(session, None, 1000).await?, events);
    assert_eq!(provider.requests().len(), 2);
    for request in provider.requests() {
        assert!(
            !request
                .system
                .as_deref()
                .unwrap_or_default()
                .contains("ACK_LOSS_DISPLAY_ONLY")
        );
        assert!(
            !serde_json::to_string(&(&request.messages, &request.context, &request.tools))?
                .contains("ACK_LOSS_DISPLAY_ONLY")
        );
    }
    assert!(
        !serde_json::to_string(&inner.list_all_messages(session).await?)?
            .contains("ACK_LOSS_DISPLAY_ONLY")
    );
    Ok(())
}
