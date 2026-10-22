//! Recorded interaction controls and restart against real durable storage.
use crabber::{
    Agent, AgentConfig, ContinueTool, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolContinuation,
    core::{
        AuthorityRequirement, ControlId, RunStatus, SessionRevision, ToolCallId, ToolCallStatus,
    },
    extension::Scope,
    runtime::ExecutionMode,
    session::{
        ApprovalResolverClaim, MemoryStore, SqliteStore, Store, ToolContinuationSnapshot,
        ToolInteractionAnswer, ToolInteractionReply,
    },
};
use crabber_tools_catalog::prelude::*;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
type Error = Box<dyn std::error::Error>;
fn authority() -> AuthorityRequirement {
    AuthorityRequirement::new("consumer-operator", "1").unwrap()
}
pub(super) fn turn(name: &str, args: Value) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: args.to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}
pub(super) fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
fn policy() -> UserInteractPolicy {
    UserInteractPolicy::durable(authority())
}
fn host(
    root: &Path,
    store: Arc<dyn Store>,
    provider: Arc<FakeProvider>,
    interaction: UserInteractPolicy,
) -> Result<Agent, Error> {
    let root = WorkspaceRoot::open(root)?;
    let policies = Policies {
        enabled: EnabledSet::only([ToolId::FileWrite, ToolId::UserInteract]),
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 65536,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())?,
        url_fetch: None,
        tracker: None,
        user_interact: Some(interaction),
        limits: Limits {
            max_in_flight: 2,
            max_blocking_wait: Duration::from_secs(2),
        },
    };
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "consumer".into();
    config.directory = root.path().to_string_lossy().into_owned();
    Ok(Agent::builder()
        .behavior_version("consumer-durable/0.4.0")
        .store(store)
        .provider(provider)
        .config(config)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 3 })
        .shared_extension(Arc::new(StandardTools::new(root, policies)?), Scope::Global)
        .build()?)
}
async fn paused(agent: &Agent) -> Result<ToolContinuationSnapshot, Error> {
    let handle = agent.prompt(None, "ask").await?;
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await?.status, RunStatus::Paused);
    Ok(agent
        .tool_continuation_snapshot(&session, &run)
        .await?
        .ok_or("missing continuation")?)
}
fn answer(
    snapshot: &ToolContinuationSnapshot,
    index: usize,
    reply: ToolInteractionReply,
) -> ToolInteractionAnswer {
    ToolInteractionAnswer {
        id: ControlId::new(),
        expected_revision: snapshot.revision,
        session_id: snapshot.session_id.clone(),
        run_id: snapshot.run_id.clone(),
        continuation: snapshot.continuation.clone(),
        interaction_id: snapshot.interactions[index].id.clone(),
        resolver: ApprovalResolverClaim {
            resolver_id: "consumer-host".into(),
            authority: authority(),
            authority_claim: "scripted fixture".into(),
        },
        reply,
    }
}
fn continuation(snapshot: &ToolContinuationSnapshot, revision: SessionRevision) -> ContinueTool {
    ContinueTool {
        id: ControlId::new(),
        session_id: snapshot.session_id.clone(),
        run_id: snapshot.run_id.clone(),
        continuation: snapshot.continuation.clone(),
        expected_revision: revision,
    }
}
async fn finish(
    agent: &Agent,
    snapshot: &ToolContinuationSnapshot,
    revision: SessionRevision,
) -> Result<(), Error> {
    let request = continuation(snapshot, revision);
    let ToolContinuation::Started { handle, .. } = agent.continue_tool(request.clone()).await?
    else {
        return Err("fresh continuation replayed".into());
    };
    assert_eq!(handle.done().await?.status, RunStatus::Completed);
    assert!(matches!(
        agent.continue_tool(request).await?,
        ToolContinuation::Replayed(_)
    ));
    Ok(())
}
pub(super) async fn results(
    store: &dyn Store,
    session: &crabber::core::SessionId,
) -> Result<Vec<crabber::core::ToolCallRecord>, Error> {
    use crabber::session::{SnapshotLimits, SnapshotOutcome, SnapshotRequest};
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 1_000_000,
                encoded_bytes: 2_000_000,
            },
            continuation: None,
        })
        .await?
    else {
        return Err("snapshot page required".into());
    };
    Ok(page.tool_calls)
}
pub(super) fn value(record: &crabber::core::ToolCallRecord) -> Value {
    match &record.result.as_ref().unwrap().content[0] {
        crabber::core::ContentBlock::Text { text } => serde_json::from_str(text).unwrap(),
        _ => panic!("JSON tool result"),
    }
}
fn check(name: &str) {
    println!("{}", json!({"check":name,"ok":true}));
}
pub(super) async fn run() -> Result<(), Error> {
    controls::decisions().await?;
    controls::controls().await?;
    controls::model_answer().await?;
    restart::restart(false).await?;
    restart::restart(true).await?;
    Ok(())
}

mod controls;
mod restart;
