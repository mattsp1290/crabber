//! A real native tool phase publishes display state without changing conversation.
use super::{Error, call, done, selection};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, JsonToolExecutor, PermissionDecision, StaticPolicy,
    ToolDefinition,
    core::{
        ControlId, PublicActivity, PublicPatchOperation, PublicSourceChange, PublicSourceEvent,
        RunStatus, SessionRevision, ToolCallId, ToolInfo,
    },
    extension::{PublicSourceError, PublicSourceWriter, ToolContext},
    session::Store,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

const DISPLAY: &str = "PUBLIC_DISPLAY_ONLY_NOT_MODEL_CONVERSATION";
struct Original {
    id: ControlId,
    expected: SessionRevision,
    change: PublicSourceChange,
    revision: SessionRevision,
}
struct Observed {
    writer: PublicSourceWriter,
    call: ToolCallId,
    originals: Vec<Original>,
}
struct DisplayTool(Arc<Mutex<Option<Observed>>>);
#[async_trait]
impl JsonToolExecutor for DisplayTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Rejected("actual tool context required"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let writer = context.public_source().clone();
        let originals = updates(&writer)
            .await
            .map_err(|error| ExtensionError::Tool(error.to_string()))?;
        *self.0.lock().unwrap() = Some(Observed {
            writer,
            call: context.call_id,
            originals,
        });
        Ok(json!({"tool":"completed"}))
    }
}
fn changes() -> Result<Vec<PublicSourceChange>, PublicSourceError> {
    let patch = |value| PublicPatchOperation::new(value).map_err(|_| PublicSourceError::Rejected);
    Ok(vec![
        PublicSourceChange::StateSnapshot {
            state: json!({"count":0,"display":DISPLAY}),
        },
        PublicSourceChange::StateDelta {
            delta: vec![
                patch(json!({"op":"test","path":"/count","value":0}))?,
                patch(json!({"op":"replace","path":"/count","value":1}))?,
            ],
        },
        PublicSourceChange::ActivitySnapshot {
            activity: PublicActivity::new(
                "probe-progress",
                "status",
                json!({"phase":"working","display":DISPLAY}),
            )
            .map_err(|_| PublicSourceError::Rejected)?,
        },
        PublicSourceChange::ActivityDelta {
            activity_id: "probe-progress".into(),
            activity_type: "status".into(),
            delta: vec![patch(
                json!({"op":"replace","path":"/phase","value":"done"}),
            )?],
        },
    ])
}
async fn updates(writer: &PublicSourceWriter) -> Result<Vec<Original>, PublicSourceError> {
    let initial = writer.snapshot().await?;
    assert!(initial.contents.state().is_null());
    let mut expected = initial.revision;
    let mut originals = Vec::new();
    for change in changes()? {
        // Retain the exact payload and original CAS before its first write.
        let id = ControlId::new();
        let revision = writer.publish(id.clone(), expected, change.clone()).await?;
        assert_eq!(revision, expected.next().unwrap());
        assert_eq!(
            writer.publish(id.clone(), expected, change.clone()).await?,
            revision
        );
        originals.push(Original {
            id,
            expected,
            change,
            revision,
        });
        expected = revision;
    }
    let current = writer.snapshot().await?;
    assert_eq!(current.revision, expected);
    assert_eq!(
        current.contents.state(),
        &json!({"count":1,"display":DISPLAY})
    );
    assert_eq!(current.contents.activities().len(), 1);
    assert_eq!(
        current.contents.activities()[0].content(),
        &json!({"phase":"done","display":DISPLAY})
    );
    Ok(originals)
}

pub(crate) async fn run(store: Arc<dyn Store>) -> Result<usize, Error> {
    let observed = Arc::new(Mutex::new(None));
    let provider = crabber::FakeProvider::scripted(vec![call("public_display"), done()]);
    let host = Agent::builder()
        .behavior_version("public-source-probe.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "public_display".into(),
                description: "Publish explicit display data".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(DisplayTool(observed.clone())),
        }))
        .build()?;
    let handle = host.prompt(None, "publish display state").await?;
    let (session, run) = (handle.session_id().clone(), handle.run_id().clone());
    assert_eq!(handle.done().await?.status, RunStatus::Completed);
    host.close_extensions().await?;
    let observed = observed
        .lock()
        .unwrap()
        .take()
        .expect("actual tool published display state");
    assert_eq!(observed.originals.len(), 4);
    verify_publications(&*store, &session, &run, &observed).await?;
    reconcile_after_close(&*store, &session, &observed).await?;
    assert_eq!(provider.requests().len(), 2);
    for request in provider.requests() {
        assert!(
            !request
                .system
                .as_deref()
                .unwrap_or_default()
                .contains(DISPLAY)
        );
        assert!(
            !serde_json::to_string(&(&request.messages, &request.context, &request.tools))?
                .contains(DISPLAY)
        );
    }
    assert!(!serde_json::to_string(&store.list_all_messages(&session).await?)?.contains(DISPLAY));
    Ok(observed.originals.len())
}

async fn reconcile_after_close(
    store: &dyn Store,
    session: &crabber::core::SessionId,
    observed: &Observed,
) -> Result<(), Error> {
    let before = store.list_events(session, None, 1000).await?;
    let state = store.public_state(session).await?;
    for original in &observed.originals {
        assert_eq!(
            observed
                .writer
                .publish(
                    original.id.clone(),
                    original.expected,
                    original.change.clone()
                )
                .await?,
            original.revision
        );
    }
    assert_eq!(
        observed
            .writer
            .publish(
                ControlId::new(),
                state.revision,
                PublicSourceChange::StateSnapshot {
                    state: json!({"late":"must refuse"})
                }
            )
            .await,
        Err(PublicSourceError::StaleOwner)
    );
    assert_eq!(store.public_state(session).await?, state);
    assert_eq!(store.list_events(session, None, 1000).await?, before);
    Ok(())
}

async fn verify_publications(
    store: &dyn Store,
    session: &crabber::core::SessionId,
    run: &crabber::core::RunId,
    observed: &Observed,
) -> Result<(), Error> {
    let current = store.public_state(session).await?;
    assert_eq!(
        current.contents.state(),
        &json!({"count":1,"display":DISPLAY})
    );
    assert_eq!(current.contents.activities().len(), 1);
    assert_eq!(
        current.contents.activities()[0].content(),
        &json!({"phase":"done","display":DISPLAY})
    );
    for original in &observed.originals {
        let saved = store
            .lookup_public_state_mutation(session, &original.id)
            .await?
            .expect("committed receipt");
        assert_eq!(&saved.run_id, run);
        assert_eq!(saved.revision, original.revision);
        let publication = store
            .lookup_public_source_publication(session, &original.id)
            .await?
            .expect("native original publication");
        assert_eq!(publication.receipt, saved);
        let event = publication.events.last().expect("actual source event");
        let source: PublicSourceEvent = serde_json::from_value(event.payload.clone())?;
        assert_eq!(&source.source.session_id, session);
        assert_eq!(&source.source.run_id, run);
        assert_eq!(source.source.call_id.as_ref(), Some(&observed.call));
        assert!(source.source.invocation.is_none());
        assert_eq!(source.change, original.change);
        assert_eq!(event.cursor, Some(saved.event_cursor));
    }
    Ok(())
}
