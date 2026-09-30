// Compile private Memory fixtures into this test target; no product fault API.
pub use crabber::session::{AdmitOutcome, StoreError};
#[path = "../../crabber-session/src/abandonment.rs"]
mod abandonment;
#[path = "../../crabber-session/src/abandonment_contract.rs"]
mod abandonment_contract;
#[path = "../../crabber-session/src/memory.rs"]
#[allow(dead_code)]
mod memory;
use async_trait::async_trait;
use crabber::core::*;
use crabber::extension::{Extension, ExtensionError, Registrar, Scope};
use crabber::providers::{ProviderError, Resolver, Selection, Streamer};
use crabber::session::{
    AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest, Store,
};
use crabber::{AbandonAuthority, AbandonError, AbandonRequest, Agent, AgentConfig};
use memory::MemoryStore;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Unavailable(Arc<AtomicUsize>);
#[async_trait]
impl Resolver for Unavailable {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandon must not resolve a provider")
    }
}
struct Unmountable(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Unmountable {
    fn id(&self) -> &'static str {
        "unavailable"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "missing".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandon must not mount extensions or hooks")
    }
}

struct NeverTool(Arc<AtomicUsize>);
#[async_trait]
impl crabber::ToolExecutor for NeverTool {
    async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandon must not execute tools")
    }
}

fn message(session: &SessionId) -> Message {
    Message {
        id: MessageId::new(),
        session_id: session.clone(),
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: vec![],
        created_at: SystemClock.now(),
    }
}
fn event(run: &Run, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: run.created_at,
    }
}
fn admit_request(session: &SessionId) -> AdmitRequest {
    AdmitRequest {
        session_id: Some(session.clone()),
        workspace_id: "workspace".into(),
        directory: ".".into(),
        title: "abandon".into(),
        user_message: message(session),
        config_hash: "unavailable-config".into(),
        plan_fingerprint: "unavailable-plan".into(),
        owner: "dead-worker".into(),
        lease: Duration::from_secs(30),
    }
}
fn agent(store: Arc<dyn Store>, counters: Arc<AtomicUsize>) -> Agent {
    Agent::builder()
        .store(store)
        .provider(Arc::new(Unavailable(counters.clone())))
        .tool(Arc::new(crabber::ToolDefinition {
            info: ToolInfo {
                name: "registered-effect".into(),
                description: "never run".into(),
                parameters: serde_json::json!({}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(NeverTool(counters.clone())),
        }))
        .extension(Arc::new(Unmountable(counters)), Scope::Global)
        .config(AgentConfig::new(Selection {
            provider_id: "missing".into(),
            model_id: "missing".into(),
        }))
        .build()
        .unwrap()
}

#[tokio::test]
async fn public_memory_abandon_running_and_paused_without_execution() {
    let clock = Arc::new(ManualClock::new(SystemClock.now()));
    facade_matrix(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}

#[allow(clippy::too_many_lines)]
async fn facade_matrix<S: abandonment_contract::FixtureStore + 'static>(
    store: Arc<S>,
    clock: Arc<ManualClock>,
) {
    for authority in [
        AbandonAuthority::ExpiredLease,
        AbandonAuthority::HostStoppedOwner,
    ] {
        for status in [RunStatus::Pending, RunStatus::Running, RunStatus::Paused] {
            let paused = status == RunStatus::Paused;
            let now = SystemClock.now();
            clock.set(now);
            let session = SessionId::new();
            let key = AdmissionKey::new("original").unwrap();
            let mut worker = OwnerProcess::spawn();
            let mut request = admit_request(&session);
            request.owner = format!("process:{}", worker.0.id());
            let options = AdmissionOptions {
                key: key.clone(),
                fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
                behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
            };
            let KeyedAdmitOutcome::Started { receipt, admitted } = store
                .admit_keyed_run(KeyedAdmitRequest {
                    request: request.clone(),
                    options,
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(admitted.run.status, RunStatus::Running);
            let execution = store.execution(admitted.fence.clone()).await.unwrap();
            let mut calls = vec![];
            for index in 0..4 {
                let call = ToolCallRecord {
                    id: ToolCallId::new(),
                    run_id: admitted.run.id.clone(),
                    name: "unregistered-effect".into(),
                    arguments: serde_json::json!({"index": index}),
                    status: ToolCallStatus::Pending,
                    retry_safe: false,
                    result: None,
                };
                execution
                    .create_tool_call(
                        call.clone(),
                        event(&admitted.run, EventKind::ToolCallPending),
                    )
                    .await
                    .unwrap();
                if index > 0 {
                    execution
                        .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                        .await
                        .unwrap();
                }
                calls.push(call);
            }
            for done in &calls[2..] {
                let mut result_message = message(&session);
                result_message.role = Role::Tool;
                result_message.run_id = Some(admitted.run.id.clone());
                result_message.parts.push(Part {
                    id: PartId::new(),
                    message_id: result_message.id.clone(),
                    ordinal: 0,
                    kind: PartKind::FunctionToolResult,
                    content: ContentBlock::ToolResult {
                        call_id: done.id.clone(),
                        content: vec![],
                        is_error: false,
                    },
                });
                execution
                    .settle_tool_call(
                        &done.id,
                        ToolResult {
                            status: if done.id == calls[2].id {
                                ToolResultStatus::Completed
                            } else {
                                ToolResultStatus::Failed
                            },
                            content: vec![],
                        },
                        result_message.clone(),
                        event(&admitted.run, EventKind::ToolCallSettled),
                    )
                    .await
                    .unwrap();
            }
            if paused {
                execution
                    .pause_run(
                        serde_json::json!({"unavailable_continuation": true}),
                        event(&admitted.run, EventKind::RunPaused),
                    )
                    .await
                    .unwrap();
            }
            let mut seeded = store.get_run(&admitted.run.id).await.unwrap().unwrap();
            seeded.status = status;
            seeded.lease_until = admitted.run.lease_until;
            seeded.usage = Usage {
                input_tokens: 123,
                output_tokens: 45,
            };
            abandonment_contract::FixtureStore::seed_run(store.as_ref(), seeded.clone()).await;
            let original_calls =
                abandonment_contract::FixtureStore::calls(store.as_ref(), &admitted.run.id).await;
            if paused {
                assert_eq!(
                    store
                        .get_run(&admitted.run.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    RunStatus::Paused
                );
            }
            let counters = Arc::new(AtomicUsize::new(0));
            let agent = agent(store.clone(), counters.clone());
            let abandon = AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner.clone(),
                authority,
            };
            {
                assert_eq!(
                    agent
                        .abandon(AbandonRequest {
                            authority: AbandonAuthority::ExpiredLease,
                            ..abandon.clone()
                        })
                        .await
                        .unwrap_err(),
                    AbandonError::LiveLease
                );
            }
            worker.stop();
            if authority == AbandonAuthority::ExpiredLease {
                clock.set(now + time::Duration::seconds(30));
            }
            for kind in [InboxKind::Steer, InboxKind::FollowUp] {
                store
                    .enqueue_inbox(&session, kind, message(&session))
                    .await
                    .unwrap();
            }
            let before = store.list_all_messages(&session).await.unwrap();
            let outcome = agent.abandon(abandon.clone()).await.unwrap();
            assert_eq!(outcome.run.status, RunStatus::Interrupted);
            assert_eq!(outcome.run.usage, seeded.usage);
            assert_eq!(outcome.run.checkpoint, seeded.checkpoint);
            let settled_calls =
                abandonment_contract::FixtureStore::calls(store.as_ref(), &admitted.run.id).await;
            for prior in &original_calls {
                if matches!(
                    prior.status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                ) {
                    assert!(settled_calls.contains(prior));
                }
            }
            assert_ne!(outcome.run.claim_token, admitted.fence.claim_token);
            assert_eq!(outcome.interrupted_tools.len(), 2);
            assert!(
                store
                    .list_unfinished_tool_calls(&admitted.run.id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                store.lookup_admission(&session, &key).await.unwrap(),
                Some(receipt)
            );
            let messages = store.list_all_messages(&session).await.unwrap();
            assert_eq!(&messages[..before.len()], &before);
            for preserved in &before {
                assert!(messages.contains(preserved));
            }
            for call in &calls[..2] {
                assert_eq!(
                    messages
                        .iter()
                        .flat_map(|m| &m.parts)
                        .filter(|p| matches!(&p.content,
                ContentBlock::ToolResult { call_id, is_error: true, .. } if call_id == &call.id))
                        .count(),
                    1
                );
            }
            let events = store.list_events(&session, None, 100).await.unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.kind == EventKind::RunSettled)
                    .count(),
                1
            );
            assert!(outcome.terminal_event.cursor.is_some());
            assert_eq!(agent.abandon(abandon).await.unwrap(), outcome);
            assert_eq!(
                store.list_events(&session, None, 100).await.unwrap(),
                events
            );
            assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
            assert_eq!(
                execution
                    .renew_lease(now + time::Duration::hours(1))
                    .await
                    .unwrap_err(),
                CoreError::Conflict
            );
            assert_eq!(
                execution
                    .append_event(event(
                        &admitted.run,
                        EventKind::Custom {
                            name: "stale".into()
                        }
                    ))
                    .await
                    .unwrap_err(),
                CoreError::Conflict
            );
            assert_eq!(
                execution
                    .settle_run(
                        RunStatus::Completed,
                        None,
                        Usage::default(),
                        event(&admitted.run, EventKind::RunSettled)
                    )
                    .await
                    .unwrap_err(),
                CoreError::Conflict
            );
            assert_all_owner_writes_revoked(execution.as_ref(), &admitted.run).await;
            for alteration in 0..3 {
                let mut changed = AbandonRequest {
                    expected: admitted.fence.clone(),
                    expected_owner: admitted.run.owner.clone(),
                    authority,
                };
                match alteration {
                    0 => changed.expected_owner = "wrong".into(),
                    1 => changed.expected.claim_token = "wrong".into(),
                    _ => {
                        changed.authority = if authority == AbandonAuthority::ExpiredLease {
                            AbandonAuthority::HostStoppedOwner
                        } else {
                            AbandonAuthority::ExpiredLease
                        }
                    }
                }
                assert_eq!(
                    agent.abandon(changed).await.unwrap_err(),
                    AbandonError::StaleOwner
                );
            }
            assert_eq!(
                abandonment_contract::FixtureStore::unconsumed_inbox(store.as_ref(), &session)
                    .await,
                2
            );
            let next = store
                .admit_run(AdmitRequest {
                    user_message: message(&session),
                    ..request
                })
                .await
                .unwrap();
            let next_execution = store.execution(next.fence).await.unwrap();
            assert_eq!(
                next_execution
                    .claim_inbox(InboxKind::Steer)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                next_execution
                    .claim_inbox_into_history(InboxKind::FollowUp)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(counters.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn public_host_stopped_requires_exact_owner_and_fence() {
    stopped_owner(Arc::new(MemoryStore::new())).await;
}
async fn stopped_owner(store: Arc<dyn Store>) {
    let mut worker = OwnerProcess::spawn();
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            owner: format!("process:{}", worker.0.id()),
            ..admit_request(&SessionId::new())
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let agent = agent(store.clone(), Arc::new(AtomicUsize::new(0)));
    assert_eq!(
        agent
            .abandon(AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner.clone(),
                authority: AbandonAuthority::ExpiredLease
            })
            .await
            .unwrap_err(),
        AbandonError::LiveLease
    );
    let mut request = AbandonRequest {
        expected: admitted.fence.clone(),
        expected_owner: "wrong-owner".into(),
        authority: AbandonAuthority::HostStoppedOwner,
    };
    assert_eq!(
        agent.abandon(request.clone()).await.unwrap_err(),
        AbandonError::StaleOwner
    );
    request.expected_owner.clone_from(&admitted.run.owner);
    request.expected.claim_token = "wrong-token".into();
    assert_eq!(
        agent.abandon(request.clone()).await.unwrap_err(),
        AbandonError::StaleOwner
    );
    request.expected = admitted.fence.clone();
    // This child represents the authoritative worker process. A host must stop
    // actual execution and renewal, and wait for confirmed exit.
    worker.stop();
    assert_eq!(
        agent.abandon(request).await.unwrap().run.status,
        RunStatus::Interrupted
    );
    assert_all_owner_writes_revoked(execution.as_ref(), &admitted.run).await;
}

#[allow(clippy::too_many_lines)]
async fn assert_all_owner_writes_revoked(execution: &dyn ExecutionStore, run: &Run) {
    let mut msg = message(&run.session_id);
    msg.run_id = Some(run.id.clone());
    let part = Part {
        id: PartId::new(),
        message_id: msg.id.clone(),
        ordinal: 0,
        kind: PartKind::AssistantText,
        content: ContentBlock::Text {
            text: "stale".into(),
        },
    };
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: run.id.clone(),
        name: "stale".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    let epoch = ContextEpoch {
        id: EpochId::new(),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        parent: Some(run.epoch_id.clone()),
        summarized_range: None,
        summary_message_id: None,
        tail_start_message_id: None,
        provider_id: "missing".into(),
        model_id: "missing".into(),
        reason: "stale".into(),
        next_policy: None,
    };
    let writes = [
        execution
            .renew_lease(SystemClock.now() + time::Duration::hours(1))
            .await,
        execution.append_message(msg.clone()).await,
        execution.append_part(part).await,
        execution
            .append_event(event(run, EventKind::MessageCommitted))
            .await,
        execution
            .create_tool_call(call.clone(), event(run, EventKind::ToolCallPending))
            .await,
        execution
            .claim_tool_call(&call.id, event(run, EventKind::ToolCallRunning))
            .await,
        execution
            .settle_tool_call(
                &call.id,
                ToolResult {
                    status: ToolResultStatus::Interrupted,
                    content: vec![],
                },
                msg.clone(),
                event(run, EventKind::ToolCallSettled),
            )
            .await,
        execution.start_epoch(epoch).await,
        execution.finish_epoch(&run.epoch_id, msg).await,
        execution
            .pause_run(serde_json::json!({}), event(run, EventKind::RunPaused))
            .await,
        execution
            .settle_run(
                RunStatus::Completed,
                None,
                Usage::default(),
                event(run, EventKind::RunSettled),
            )
            .await,
        execution
            .put_extension_state("stale", vec![("key".into(), Some("value".into()))])
            .await,
        execution.claim_inbox(InboxKind::Steer).await.map(|_| ()),
        execution
            .claim_inbox_into_history(InboxKind::FollowUp)
            .await
            .map(|_| ()),
    ];
    for write in writes {
        assert_eq!(write.unwrap_err(), CoreError::Conflict);
    }
}

#[cfg(feature = "postgres")]
#[path = "fenced_support/postgres.rs"]
mod postgres;

#[tokio::test]
async fn facade_memory_missing_and_unrelated_terminal_are_honest() {
    error_semantics(Arc::new(MemoryStore::new())).await;
}
async fn error_semantics(store: Arc<dyn Store>) {
    let counters = Arc::new(AtomicUsize::new(0));
    let host = agent(store.clone(), counters.clone());
    let missing = AbandonRequest {
        expected: RunFence {
            run_id: RunId::new(),
            claim_token: "missing".into(),
        },
        expected_owner: "missing".into(),
        authority: AbandonAuthority::ExpiredLease,
    };
    assert_eq!(
        host.abandon(missing).await.unwrap_err(),
        AbandonError::NotFound
    );
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            ..admit_request(&SessionId::new())
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted.run, EventKind::RunSettled),
        )
        .await
        .unwrap();
    let before = store
        .list_events(&admitted.run.session_id, None, 100)
        .await
        .unwrap();
    let request = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::HostStoppedOwner,
    };
    assert_eq!(
        host.abandon(request).await.unwrap_err(),
        AbandonError::AlreadyTerminal
    );
    assert_eq!(
        store
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap(),
        before
    );
    assert_eq!(counters.load(Ordering::SeqCst), 0);
}
#[path = "fenced_support/response_loss.rs"]
mod response_loss;

struct OwnerProcess(std::process::Child);
impl OwnerProcess {
    fn spawn() -> Self {
        Self(
            std::process::Command::new("sleep")
                .arg("60")
                .spawn()
                .unwrap(),
        )
    }
    fn stop(&mut self) {
        assert!(self.0.try_wait().unwrap().is_none());
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
        assert!(self.0.try_wait().unwrap().is_some());
    }
}
impl Drop for OwnerProcess {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
