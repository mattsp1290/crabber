//! Actual pre-effect approval termination after the original worker joined.
use super::{Error, call, selection};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, JsonToolExecutor, PermissionDecision,
    StaticPolicy, ToolDefinition,
    core::{AuthorityRequirement, ControlId, EventKind, RunStatus, ToolInfo},
    session::{ApprovalResolverClaim, ApprovalTermination, ApprovalTerminationKind, Store},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct Tool(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Tool {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(value)
    }
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<usize, Error> {
    let effects = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![call("approval_wait")]);
    let host = Agent::builder()
        .behavior_version("public-approval-termination-probe.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "approval_wait".into(),
                description: "Must not execute".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Tool(effects.clone())),
        }))
        .build()?;
    let handle = host.prompt(None, "wait for host approval").await?;
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await?.status, RunStatus::Paused);
    host.close_extensions().await?;
    let waiting = host.approval_snapshot(&session, &run).await?.unwrap();
    let request = ApprovalTermination {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: waiting.continuation,
        expected_revision: waiting.revision,
        resolver: ApprovalResolverClaim {
            resolver_id: "host-admin".into(),
            authority: AuthorityRequirement::new("administration", "1")?,
            authority_claim: "offline host authenticated cancellation".into(),
        },
        kind: ApprovalTerminationKind::Cancel,
    };
    let receipt = host.terminate_approval(request.clone()).await?;
    assert_eq!(
        store.get_run(&run).await?.unwrap().status,
        RunStatus::Interrupted
    );
    assert_eq!(store.list_unfinished_tool_calls(&run).await?.len(), 0);
    let revision = store.session_revision(&session).await?;
    let events = store.list_events(&session, None, 1000).await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::RunSettled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::TurnCompleted)
            .count(),
        1
    );
    assert_eq!(host.terminate_approval(request).await?, receipt);
    assert_eq!(store.session_revision(&session).await?, revision);
    assert_eq!(store.list_events(&session, None, 1000).await?, events);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
    Ok(1)
}
