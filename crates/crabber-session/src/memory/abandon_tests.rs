use super::*;
use crabber_core::{AbandonAuthority, AbandonError, AbandonRequest, EventKind, Role};
use serde_json::json;
use std::time::Duration;

fn request() -> AdmitRequest {
    let session = SessionId::new();
    AdmitRequest {
        session_id: None,
        workspace_id: "w".into(),
        directory: ".".into(),
        title: "pending fixture".into(),
        config_hash: "missing".into(),
        plan_fingerprint: "missing".into(),
        owner: "owner".into(),
        lease: Duration::from_secs(30),
        user_message: Message {
            id: MessageId::new(),
            session_id: session,
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![],
            created_at: SystemClock.now(),
        },
    }
}
fn event(run: &Run) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::ToolCallPending,
        payload: json!({}),
        correlation: None,
        live_only: false,
        created_at: run.created_at,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pending_atomic_rollback_preserves_calls_usage_history_fence_and_inbox() {
    let now = SystemClock.now();
    let clock = Arc::new(crabber_core::ManualClock::new(now));
    let store = MemoryStore::with_clock(clock.clone());
    let admitted = store.admit_run(request()).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: admitted.run.id.clone(),
        name: "missing".into(),
        arguments: json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    execution
        .create_tool_call(call.clone(), event(&admitted.run))
        .await
        .unwrap();
    let running = ToolCallRecord {
        id: ToolCallId::new(),
        ..call.clone()
    };
    execution
        .create_tool_call(running.clone(), event(&admitted.run))
        .await
        .unwrap();
    execution
        .claim_tool_call(&running.id, event(&admitted.run))
        .await
        .unwrap();
    let completed = ToolCallRecord {
        id: ToolCallId::new(),
        ..call.clone()
    };
    execution
        .create_tool_call(completed.clone(), event(&admitted.run))
        .await
        .unwrap();
    execution
        .claim_tool_call(&completed.id, event(&admitted.run))
        .await
        .unwrap();
    let mut completed_message = request().user_message;
    completed_message.session_id = admitted.run.session_id.clone();
    completed_message.run_id = Some(admitted.run.id.clone());
    completed_message.role = Role::Tool;
    completed_message.parts.push(Part {
        id: crabber_core::PartId::new(),
        message_id: completed_message.id.clone(),
        ordinal: 0,
        kind: PartKind::FunctionToolResult,
        content: crabber_core::ContentBlock::ToolResult {
            call_id: completed.id.clone(),
            content: vec![],
            is_error: false,
        },
    });
    execution
        .settle_tool_call(
            &completed.id,
            ToolResult {
                status: ToolResultStatus::Completed,
                content: vec![],
            },
            completed_message,
            event(&admitted.run),
        )
        .await
        .unwrap();
    let original_calls = store.state.lock().unwrap().calls.clone();
    let mut inbox = request().user_message;
    inbox.session_id = admitted.run.session_id.clone();
    store
        .enqueue_inbox(&admitted.run.session_id, InboxKind::Steer, inbox.clone())
        .await
        .unwrap();
    let mut followup = inbox.clone();
    followup.id = MessageId::new();
    store
        .enqueue_inbox(
            &admitted.run.session_id,
            InboxKind::FollowUp,
            followup.clone(),
        )
        .await
        .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        let run = state.runs.get_mut(&admitted.run.id).unwrap();
        // Pending is a persisted state supported by the domain; fresh admission
        // currently produces Running, so seed it without inventing a public API.
        run.status = RunStatus::Pending;
        run.usage = Usage {
            input_tokens: 123,
            output_tokens: 45,
        };
        state.abandon_fail_after_tools = true;
    }
    clock.set(now + time::Duration::seconds(30));
    let original_run = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    let original_events = store
        .list_events(&admitted.run.session_id, None, 100)
        .await
        .unwrap();
    let original_messages = store
        .list_all_messages(&admitted.run.session_id)
        .await
        .unwrap();
    let agent = crabber::Agent::builder()
        .store(Arc::new(PendingFacadeStore(store.clone())))
        .provider(Arc::new(crabber::FakeProvider::scripted(vec![])))
        .config(crabber::AgentConfig::new(crabber::Selection {
            provider_id: "unavailable".into(),
            model_id: "unavailable".into(),
        }))
        .build()
        .unwrap();
    let abandon = AbandonRequest {
        expected: admitted.fence,
        expected_owner: "owner".into(),
        authority: AbandonAuthority::ExpiredLease,
    };
    assert!(matches!(
        agent.abandon(abandon.clone()).await,
        Err(AbandonError::Store(_))
    ));
    assert_eq!(
        store.get_run(&admitted.run.id).await.unwrap(),
        Some(original_run.clone())
    );
    assert_eq!(store.state.lock().unwrap().calls, original_calls);
    assert_eq!(
        store
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap(),
        original_events
    );
    assert_eq!(
        store
            .list_all_messages(&admitted.run.session_id)
            .await
            .unwrap(),
        original_messages
    );
    store.state.lock().unwrap().abandon_fail_after_tools = false;
    let outcome = agent.abandon(abandon.clone()).await.unwrap();
    assert_eq!(outcome.run.usage, original_run.usage);
    assert_eq!(outcome.run.status, RunStatus::Interrupted);
    assert_eq!(outcome.interrupted_tools.len(), 2);
    assert_eq!(
        store.state.lock().unwrap().calls[&completed.id],
        original_calls[&completed.id]
    );
    for id in [&call.id, &running.id] {
        let state = store.state.lock().unwrap();
        let result = state.calls[id].result.as_ref().unwrap();
        assert_eq!(state.calls[id].status, ToolCallStatus::Interrupted);
        assert_eq!(result.status, ToolResultStatus::Interrupted);
        assert_eq!(
            state
                .messages
                .iter()
                .flat_map(|m| &m.parts)
                .filter(|p| matches!(&p.content,
            crabber_core::ContentBlock::ToolResult { call_id, content, is_error: true }
                if call_id == id && content == &result.content))
                .count(),
            1
        );
        assert_eq!(
            state
                .events
                .iter()
                .filter(|e| e.kind == EventKind::ToolCallSettled
                    && e.correlation.as_deref() == Some(id.0.as_str())
                    && e.payload["status"] == "interrupted")
                .count(),
            1
        );
    }
    assert_eq!(
        store.state.lock().unwrap().calls[&call.id].status,
        ToolCallStatus::Interrupted
    );
    assert_eq!(
        store.state.lock().unwrap().calls[&call.id]
            .result
            .as_ref()
            .unwrap()
            .status,
        ToolResultStatus::Interrupted
    );
    assert!(
        store.state.lock().unwrap().inbox[0]
            .consumed_by_run
            .is_none()
    );
    let settled_events = store
        .list_events(&admitted.run.session_id, None, 100)
        .await
        .unwrap();
    let settled_messages = store
        .list_all_messages(&admitted.run.session_id)
        .await
        .unwrap();
    assert_eq!(agent.abandon(abandon).await.unwrap(), outcome);
    assert_eq!(
        store
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap(),
        settled_events
    );
    assert_eq!(
        store
            .list_all_messages(&admitted.run.session_id)
            .await
            .unwrap(),
        settled_messages
    );
    let next = store
        .admit_run(AdmitRequest {
            session_id: Some(admitted.run.session_id.clone()),
            user_message: inbox.clone(),
            ..request()
        })
        .await
        .unwrap();
    let next_execution = store.execution(next.fence).await.unwrap();
    assert_eq!(
        next_execution.claim_inbox(InboxKind::Steer).await.unwrap(),
        vec![inbox]
    );
    assert_eq!(
        next_execution
            .claim_inbox(InboxKind::FollowUp)
            .await
            .unwrap(),
        vec![followup]
    );
}

#[tokio::test]
async fn replacement_owner_and_unrelated_terminal_are_not_abandonment_replay() {
    let clock = Arc::new(crabber_core::ManualClock::new(SystemClock.now()));
    let store = MemoryStore::with_clock(clock.clone());
    let admitted = store.admit_run(request()).await.unwrap();
    let abandon = AbandonRequest {
        expected: admitted.fence.clone(),
        expected_owner: "owner".into(),
        authority: AbandonAuthority::HostStoppedOwner,
    };
    clock.set(admitted.run.lease_until);
    let new_fence = store
        .claim_expired_run(&admitted.run.id, "replacement")
        .await
        .unwrap();
    assert_eq!(
        store.abandon_run(abandon.clone()).await.unwrap_err(),
        AbandonError::StaleOwner
    );
    let execution = store.execution(new_fence).await.unwrap();
    execution
        .settle_run(
            RunStatus::Interrupted,
            None,
            Usage::default(),
            event(&admitted.run),
        )
        .await
        .unwrap();
    assert_eq!(
        store.abandon_run(abandon).await.unwrap_err(),
        AbandonError::AlreadyTerminal
    );
    let missing = AbandonRequest {
        expected: RunFence {
            run_id: RunId::new(),
            claim_token: "none".into(),
        },
        expected_owner: "none".into(),
        authority: AbandonAuthority::ExpiredLease,
    };
    assert_eq!(
        store.abandon_run(missing).await.unwrap_err(),
        AbandonError::NotFound
    );
}

// A dev dependency compiles the session library twice (normal and unit-test).
// Bridge only the abandonment boundary to the normal facade trait, retaining
// the privately seeded Pending state and fault injection in this test instance.
struct PendingFacadeStore(MemoryStore);
#[allow(unused_variables)]
#[async_trait]
impl crabber::session::Store for PendingFacadeStore {
    async fn abandon_run(
        &self,
        request: AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, AbandonError> {
        Store::abandon_run(&self.0, request).await
    }
    async fn admit_run(
        &self,
        request: crabber::session::AdmitRequest,
    ) -> Result<crabber::session::AdmitOutcome, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn execution(
        &self,
        fence: RunFence,
    ) -> Result<Box<dyn crabber::session::ExecutionStore>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: crabber::session::InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
}

#[tokio::test]
async fn paused_live_lease_is_not_permission_to_steal() {
    let store = MemoryStore::new();
    let admitted = store.admit_run(request()).await.unwrap();
    store
        .state
        .lock()
        .unwrap()
        .runs
        .get_mut(&admitted.run.id)
        .unwrap()
        .status = RunStatus::Paused;
    let original = store.get_run(&admitted.run.id).await.unwrap();
    let abandon = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::ExpiredLease,
    };
    assert_eq!(
        store.abandon_run(abandon).await.unwrap_err(),
        AbandonError::LiveLease
    );
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), original);
}
