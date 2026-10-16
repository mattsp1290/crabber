#[path = "fork/admitted.rs"]
mod admitted;
#[path = "fork/snapshot.rs"]
mod snapshot;
// Copied native results become presentation history, never executable calls.
use super::*;
use crabber::core::{ContentBlock, ControlId, InputEnvelope, SessionRevision};
use crabber::session::{
    ForkBoundary, ForkOutcome, ForkSession, ManageSession, SessionChange, SnapshotLimits,
};

struct Effect(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Effect {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!("closed protected result"))
    }
}
fn worker(store: Arc<dyn Store>, provider: FakeProvider, effects: Arc<AtomicUsize>) -> Agent {
    Agent::builder()
        .behavior_version("safe-fork-fixture.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "identity".into(),
                description: "native effect".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Effect(effects)),
        }))
        .build()
        .unwrap()
}
async fn fork_request(agent: &Agent, store: &dyn Store, source: &SessionId) -> ForkSession {
    let summary = agent.session_summary(source).await.unwrap();
    let messages = store.list_all_messages(source).await.unwrap();
    ForkSession {
        id: ControlId::new(),
        source_session_id: source.clone(),
        workspace_id: summary.workspace_id,
        directory: summary.directory,
        expected_source_revision: summary.revision,
        boundary: ForkBoundary::AfterMessage {
            message_id: messages.last().unwrap().id.clone(),
        },
        target_session_id: SessionId::new(),
        title: "forked history".into(),
        defaults: summary.defaults,
        limits: SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 256,
            text_bytes: 2_097_152,
            encoded_bytes: 4_194_304,
        },
        max_proof_bytes: 32 * 1024 * 1024,
    }
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // Actual source→fork→refork→fresh target admission.
async fn memory_safe_fork_never_reexecutes_imported_native_calls() {
    let store = Arc::new(MemoryStore::new());
    let source_provider = FakeProvider::scripted(vec![call("source-effect"), done()]);
    let effects = Arc::new(AtomicUsize::new(0));
    let agent = worker(store.clone(), source_provider.clone(), effects.clone());
    let source = SessionId::new();
    let execution = context(&source.0, "fork-workspace", "/fork-workspace", "scripted");
    let opts = AdmissionOptions {
        key: AdmissionKey::new("source-input").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    };
    let Admission::Started { handle, .. } = agent
        .prompt_input_keyed_in(
            &execution,
            InputEnvelope::text("source", SessionRevision::INITIAL).unwrap(),
            opts,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("source admission")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let request = fork_request(&agent, store.as_ref(), &source).await;
    let original = store.list_all_messages(&source).await.unwrap();
    agent.close_extensions().await.unwrap();
    let ForkOutcome::Applied(receipt) = agent.fork_session(request.clone()).await.unwrap() else {
        panic!("fresh fork")
    };
    let copied = store
        .list_all_messages(&request.target_session_id)
        .await
        .unwrap();
    assert_eq!(copied.len(), original.len());
    assert_eq!(receipt.imported_calls, 1);
    snapshot::check(
        store.as_ref(),
        &request.target_session_id,
        crabber::core::ImportedCallSource::NativeExecution,
        original.len(),
    )
    .await;
    assert!(
        copied
            .iter()
            .all(|message| message.run_id.is_none()
                && message.session_id == request.target_session_id)
    );
    assert!(
        copied
            .iter()
            .all(|message| !original.iter().any(|source| source.id == message.id))
    );
    let call = copied
        .iter()
        .flat_map(|message| &message.parts)
        .find_map(|part| match &part.content {
            ContentBlock::ToolCall { call_id, .. } => Some(call_id),
            _ => None,
        })
        .unwrap();
    assert_ne!(call, &ToolCallId::from("source-effect"));
    assert!(copied.iter().flat_map(|message| &message.parts).any(
        |part| matches!(&part.content, ContentBlock::ToolResult { call_id, .. } if call_id == call)
    ));
    assert!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .iter()
            .all(|run| run.session_id != request.target_session_id)
    );
    let source_summary = agent.session_summary(&source).await.unwrap();
    agent
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: source.clone(),
            expected_revision: source_summary.revision,
            change: SessionChange::SetArchived { archived: true },
        })
        .await
        .unwrap();
    let mut changed_request = request.clone();
    changed_request.title.push_str(" changed");
    assert!(matches!(
        agent.fork_session(changed_request).await,
        Err(crabber::session::ForkError::SemanticConflict)
    ));
    assert!(
        matches!(agent.fork_session(request.clone()).await.unwrap(), ForkOutcome::Replayed(saved) if saved == receipt)
    );
    // The first copy itself is valid source provenance, without a fictitious Run.
    let second = fork_request(&agent, store.as_ref(), &request.target_session_id).await;
    assert!(
        matches!(agent.fork_session(second).await.unwrap(), ForkOutcome::Applied(saved) if saved.imported_calls == 1)
    );
    assert_eq!(source_provider.requests().len(), 2);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let target_provider = FakeProvider::scripted(vec![done()]);
    let target = worker(store.clone(), target_provider.clone(), effects.clone());
    let execution = context(
        &request.target_session_id.0,
        "fork-workspace",
        "/fork-workspace",
        "scripted",
    );
    let handle = target
        .prompt_in(&execution, "continue copied conversation", None)
        .await
        .unwrap();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(target_provider.requests().len(), 1);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    target.close_extensions().await.unwrap();
    assert!(
        matches!(agent.fork_session(request.clone()).await.unwrap(), ForkOutcome::Replayed(saved) if saved == receipt)
    );
    // A fresh model call cannot promote a copied presentation identity into a
    // native executable call, even when its tool name and arguments are valid.
    let reused_provider = FakeProvider::scripted(vec![super::call(&call.0), done()]);
    let reused = worker(store.clone(), reused_provider, effects.clone());
    let handle = reused
        .prompt_in(&execution, "attempt reused identity", None)
        .await
        .unwrap();
    let outcome = handle.done().await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(matches!(outcome, Err(RuntimeError::Store(_))));
    reused.close_extensions().await.unwrap();
}
