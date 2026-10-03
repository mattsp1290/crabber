use super::*;
use async_trait::async_trait;
use crabber::{core::*, session::*};
use std::{collections::BTreeMap, time::Duration};
use time::OffsetDateTime;
use tokio::sync::Notify;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    AdmissionLost,
    AdmissionDelayed,
    ClaimLost,
    BeginLost,
    BeginRejected,
    BeforeBegin,
}
struct FaultStore {
    inner: Arc<MemoryStore>,
    fault: Fault,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
struct FaultExecution {
    inner: Box<dyn ExecutionStore>,
    fault: Fault,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl Store for FaultStore {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let outcome = self.inner.admit_keyed_run(request).await?;
        if self.fault == Fault::AdmissionLost {
            return Err(StoreError::Validation("unknown admission response".into()));
        }
        if self.fault == Fault::AdmissionDelayed {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(outcome)
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &crabber_core::AdmissionKey,
    ) -> Result<
        Option<crabber_session::AdmissionExecutionRecord>,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: ClaimUnstartedAdmissionRequest,
    ) -> Result<ClaimedAdmission, AdmissionExecutionError> {
        let outcome = self.inner.claim_unstarted_admission(request).await?;
        if self.fault == Fault::ClaimLost {
            return Err(AdmissionExecutionError::UnknownStoreFailure);
        }
        Ok(outcome)
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        Ok(Box::new(FaultExecution {
            inner: self.inner.execution(fence).await?,
            fault: self.fault,
            entered: self.entered.clone(),
            release: self.release.clone(),
        }))
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::AdmissionExecutionState>, StoreError> {
        self.inner.admission_execution_state(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
}

#[async_trait]
impl ExecutionStore for FaultExecution {
    async fn renew_lease(&self, until: OffsetDateTime) -> Result<(), StoreError> {
        self.inner.renew_lease(until).await
    }
    async fn begin_admission_execution(&self) -> Result<(), AdmissionExecutionError> {
        if self.fault == Fault::BeforeBegin {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.fault == Fault::BeginRejected {
            return Err(AdmissionExecutionError::UnknownStoreFailure);
        }
        self.inner.begin_admission_execution().await?;
        if self.fault == Fault::BeginLost {
            return Err(AdmissionExecutionError::UnknownStoreFailure);
        }
        Ok(())
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        self.inner.append_message(message).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.inner.append_part(part).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        self.inner.append_event(event).await
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.create_tool_call(call, pending_event).await
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.claim_tool_call(id, running_event).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner
            .settle_tool_call(id, result, result_message, terminal_event)
            .await
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        self.inner.start_epoch(epoch).await
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        self.inner.finish_epoch(id, summary).await
    }
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.pause_run(checkpoint, event).await
    }
    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.settle_run(status, error, usage, event).await
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        self.inner.put_extension_state(extension_id, entries).await
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox(kind).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox_into_history(kind).await
    }
}

fn host(store: Arc<dyn Store>, provider: Arc<FakeProvider>) -> Agent {
    Agent::builder()
        .store(store)
        .provider(provider)
        .config(config())
        .build()
        .unwrap()
}
fn fault(store: Arc<MemoryStore>, kind: Fault) -> Arc<FaultStore> {
    Arc::new(FaultStore {
        inner: store,
        fault: kind,
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    })
}
fn fixture() -> (
    Arc<ManualClock>,
    Arc<MemoryStore>,
    Arc<FakeProvider>,
    SessionId,
) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    (clock, store, provider(), SessionId::new())
}
async fn lost(
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    session: &SessionId,
) -> AdmissionReceipt {
    assert!(
        host(fault(store.clone(), Fault::AdmissionLost), provider)
            .prompt_keyed(session.clone(), "hello", options())
            .await
            .is_err()
    );
    store
        .lookup_admission(session, &options().key)
        .await
        .unwrap()
        .unwrap()
}
async fn complete(agent: &Agent, session: &SessionId) -> AdmissionReceipt {
    let Admission::Started { handle, receipt } = agent
        .recover_admission(session.clone(), "hello", options())
        .await
        .unwrap()
    else {
        panic!("expected execution authority")
    };
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(result.run_id, receipt.run_id);
    receipt
}
async fn proof(store: &MemoryStore, provider: &FakeProvider, receipt: &AdmissionReceipt) {
    let messages = store.list_all_messages(&receipt.session_id).await.unwrap();
    assert_eq!(messages.iter().filter(|m| m.role == Role::User).count(), 1);
    assert_eq!(
        messages.iter().find(|m| m.role == Role::User).unwrap().id,
        receipt.user_message_id
    );
    assert!(
        messages
            .iter()
            .all(|m| m.run_id.as_ref() == Some(&receipt.run_id))
    );
    assert!(
        messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .flat_map(|m| &m.parts)
            .any(|p| p.content
                == ContentBlock::Text {
                    text: "done".into()
                })
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn lost_admission_reply_completes_original_turn_and_replays_only_metadata() {
    let (clock, store, provider, session) = fixture();
    let receipt = lost(store.clone(), provider.clone(), &session).await;
    let agent = host(store.clone(), provider.clone());
    let before = store.get_run(&receipt.run_id).await.unwrap();
    assert!(matches!(
        agent
            .recover_admission(session.clone(), "hello", options())
            .await,
        Err(RuntimeError::AdmissionExecution(
            AdmissionExecutionError::LiveLease
        ))
    ));
    assert_eq!(store.get_run(&receipt.run_id).await.unwrap(), before);
    assert_eq!(provider.requests().len(), 0);
    assert!(matches!(
        agent
            .prompt_keyed(session.clone(), "hello", options())
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert!(matches!(
        agent.resume(&receipt.run_id).await,
        Err(RuntimeError::Store(CoreError::AdmissionRecoveryRequired))
    ));
    assert_eq!(agent.recover().await.unwrap().recovered, Vec::new());
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    let replacement = host(store.clone(), provider.clone());
    let (a, b) = tokio::join!(
        agent.recover_admission(session.clone(), "hello", options()),
        replacement.recover_admission(session.clone(), "hello", options())
    );
    let mut handles = Vec::new();
    for result in [a, b] {
        match result {
            Ok(Admission::Started { handle, receipt: r }) => {
                assert_eq!(r, receipt);
                handles.push(handle);
            }
            Ok(Admission::Replayed(r)) => assert_eq!(r, receipt),
            Err(RuntimeError::AdmissionExecution(
                AdmissionExecutionError::StaleOwner
                | AdmissionExecutionError::LiveLease
                | AdmissionExecutionError::AlreadyStarted
                | AdmissionExecutionError::AlreadyTerminal,
            )) => (),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(handles.len(), 1);
    assert_eq!(
        handles.pop().unwrap().done().await.unwrap().status,
        RunStatus::Completed
    );
    proof(&store, &provider, &receipt).await;
    let events = store.list_events(&session, None, 1000).await.unwrap();
    for _ in 0..3 {
        assert_eq!(
            agent
                .lookup_admission(&session, &options().key)
                .await
                .unwrap(),
            Some(receipt.clone())
        );
        assert!(matches!(
            agent
                .prompt_keyed(session.clone(), "hello", options())
                .await
                .unwrap(),
            Admission::Replayed(_)
        ));
        assert!(matches!(
            agent
                .recover_admission(session.clone(), "hello", options())
                .await
                .unwrap(),
            Admission::Replayed(_)
        ));
    }
    assert_eq!(
        events,
        store.list_events(&session, None, 1000).await.unwrap()
    );
    proof(&store, &provider, &receipt).await;
}

#[tokio::test]
async fn delayed_original_spawn_is_fenced_before_execution() {
    let (clock, store, provider, session) = fixture();
    let wrapped = fault(store.clone(), Fault::AdmissionDelayed);
    let original = host(wrapped.clone(), provider.clone());
    let s = session.clone();
    let task =
        tokio::spawn(async move { original.prompt_keyed(s, "hello", options()).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), wrapped.entered.notified())
        .await
        .unwrap();
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    let receipt = complete(&host(store.clone(), provider.clone()), &session).await;
    wrapped.release.notify_one();
    let Admission::Started {
        handle,
        receipt: old,
    } = task.await.unwrap()
    else {
        panic!()
    };
    assert_eq!(old, receipt);
    assert!(handle.done().await.is_err());
    proof(&store, &provider, &receipt).await;
}

#[tokio::test]
async fn claim_response_loss_and_begin_rollback_preserve_unstarted_liveness() {
    for kind in [Fault::ClaimLost, Fault::BeginRejected] {
        let (clock, store, provider, session) = fixture();
        let receipt = lost(store.clone(), provider.clone(), &session).await;
        clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
        let agent = host(fault(store.clone(), kind), provider.clone());
        let result = agent
            .recover_admission(session.clone(), "hello", options())
            .await;
        if kind == Fault::ClaimLost {
            assert!(matches!(
                result,
                Err(RuntimeError::AdmissionExecution(
                    AdmissionExecutionError::UnknownStoreFailure
                ))
            ));
        } else {
            let Admission::Started { handle, .. } = result.unwrap() else {
                panic!()
            };
            assert!(matches!(
                handle.done().await,
                Err(RuntimeError::AdmissionExecution(
                    AdmissionExecutionError::UnknownStoreFailure
                ))
            ));
        }
        assert_eq!(
            store.list_events(&session, None, 100).await.unwrap(),
            Vec::new()
        );
        assert_eq!(provider.requests().len(), 0);
        assert_eq!(
            store
                .load_admission_execution(&session, &options().key)
                .await
                .unwrap()
                .unwrap()
                .state,
            AdmissionExecutionState::Unstarted
        );
        clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60));
        assert_eq!(
            complete(&host(store.clone(), provider.clone()), &session).await,
            receipt
        );
        proof(&store, &provider, &receipt).await;
    }
}

#[tokio::test]
async fn lost_begin_acknowledgement_never_executes_and_generic_recovery_interrupts() {
    let (clock, store, provider, session) = fixture();
    let Admission::Started { handle, receipt } =
        host(fault(store.clone(), Fault::BeginLost), provider.clone())
            .prompt_keyed(session.clone(), "hello", options())
            .await
            .unwrap()
    else {
        panic!()
    };
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::AdmissionExecution(
            AdmissionExecutionError::UnknownStoreFailure
        ))
    ));
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(
        store.list_events(&session, None, 100).await.unwrap(),
        Vec::new()
    );
    let agent = host(store.clone(), provider.clone());
    assert!(matches!(
        agent
            .recover_admission(session, "hello", options())
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    assert_eq!(
        agent.resume(&receipt.run_id).await.unwrap().status,
        RunStatus::Interrupted
    );
    assert_eq!(provider.requests().len(), 0);
}

#[tokio::test]
async fn semantic_drift_conflicts_before_claim_and_after_terminal_replay() {
    let (clock, store, provider, session) = fixture();
    let receipt = lost(store.clone(), provider.clone(), &session).await;
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    for terminal in [false, true] {
        if terminal {
            complete(&host(store.clone(), provider.clone()), &session).await;
        }
        for change in 0..10 {
            let mut cfg = config();
            let mut text = "hello";
            let mut opts = options();
            match change {
                0 => text = "changed",
                1 => cfg.system_prompt = Some("changed".into()),
                2 => cfg.selection.model_id = "changed".into(),
                3 => cfg.selection.provider_id = "changed".into(),
                4 => cfg.title = "changed".into(),
                5 => opts.behavior_fingerprint = InputFingerprint::new("c".repeat(64)).unwrap(),
                9 => opts.fingerprint = InputFingerprint::new("d".repeat(64)).unwrap(),
                _ => (),
            }
            let mut builder = Agent::builder()
                .store(store.clone())
                .provider(provider.clone())
                .config(cfg);
            match change {
                6 => builder = builder.execution_mode(ExecutionMode::Parallel { max: 2 }),
                7 => {
                    builder = builder.compaction(CompactionPolicy {
                        trigger_ratio: 0.7,
                        keep_tail_messages: 4,
                    });
                }
                8 => {
                    builder = builder.prompt_section(Arc::new(PromptSection {
                        name: "new".into(),
                        order: 0,
                        text: "changed".into(),
                    }));
                }
                _ => (),
            }
            let before = store.get_run(&receipt.run_id).await.unwrap();
            assert!(
                matches!(
                    builder
                        .build()
                        .unwrap()
                        .recover_admission(session.clone(), text, opts)
                        .await,
                    Err(RuntimeError::Store(CoreError::AdmissionConflict))
                ),
                "change {change}, terminal {terminal}"
            );
            assert_eq!(store.get_run(&receipt.run_id).await.unwrap(), before);
        }
    }
    proof(&store, &provider, &receipt).await;
}

#[tokio::test]
async fn expiry_between_claim_and_begin_grants_no_execution() {
    let (clock, store, provider, session) = fixture();
    lost(store.clone(), provider.clone(), &session).await;
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    let wrapped = fault(store.clone(), Fault::BeforeBegin);
    let agent = host(wrapped.clone(), provider.clone());
    let Admission::Started { handle, .. } = agent
        .recover_admission(session.clone(), "hello", options())
        .await
        .unwrap()
    else {
        panic!()
    };
    tokio::time::timeout(Duration::from_secs(5), wrapped.entered.notified())
        .await
        .unwrap();
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60));
    wrapped.release.notify_one();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::AdmissionExecution(
            AdmissionExecutionError::StaleOwner
        ))
    ));
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(
        store.list_events(&session, None, 100).await.unwrap(),
        Vec::new()
    );
    complete(&host(store.clone(), provider.clone()), &session).await;
}

struct HookCounter {
    installs: Arc<std::sync::atomic::AtomicUsize>,
    effects: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait]
impl crabber::extension::Extension for HookCounter {
    fn config_hash(&self) -> String {
        "hook-counter-v1".into()
    }
    fn id(&self) -> &'static str {
        "hook-counter"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    async fn install(
        &self,
        registrar: &mut crabber::extension::Registrar,
    ) -> Result<(), crabber::ExtensionError> {
        use crabber::extension::{Point, RunAdmitted, RunBeforeExecute, RunStarted};
        self.installs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let counter = self.effects.clone();
        let callback: crabber::extension::Callback = Arc::new(move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(serde_json::Value::Bool(true)) })
        });
        registrar.on_gate(RunBeforeExecute::ID, 0, "before", callback.clone());
        registrar.on_notify(RunAdmitted::ID, 0, "admitted", callback.clone());
        registrar.on_notify(RunStarted::ID, 0, "started", callback);
        Ok(())
    }
}
#[tokio::test]
async fn failed_begin_blocks_business_hooks_but_allows_pre_admission_setup() {
    for kind in [Fault::BeginRejected, Fault::BeginLost] {
        let (_, store, provider, session) = fixture();
        let installs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent::builder()
            .store(fault(store.clone(), kind))
            .provider(provider.clone())
            .config(config())
            .extension(
                Arc::new(HookCounter {
                    installs: installs.clone(),
                    effects: effects.clone(),
                }),
                crabber::extension::Scope::Global,
            )
            .build()
            .unwrap();
        let Admission::Started { handle, .. } = agent
            .prompt_keyed(session.clone(), "hello", options())
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(handle.done().await.is_err());
        assert!(installs.load(std::sync::atomic::Ordering::SeqCst) > 0);
        assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(provider.requests().len(), 0);
        assert_eq!(
            store.list_events(&session, None, 100).await.unwrap(),
            Vec::new()
        );
    }
}

#[tokio::test]
async fn missing_and_legacy_evidence_never_invent_an_initial_executor() {
    let (_, store, provider, session) = fixture();
    let agent = host(store.clone(), provider.clone());
    assert!(matches!(
        agent
            .recover_admission(session.clone(), "hello", options())
            .await,
        Err(RuntimeError::Store(CoreError::NotFound))
    ));
    assert!(store.get_session(&session).await.unwrap().is_none());
    let id = MessageId::new();
    let cfg = config();
    store
        .admit_keyed_run(KeyedAdmitRequest {
            execution: None,
            options: options(),
            request: AdmitRequest {
                session_id: Some(session.clone()),
                workspace_id: cfg.workspace_id,
                directory: cfg.directory,
                title: cfg.title,
                user_message: Message {
                    id: id.clone(),
                    session_id: session.clone(),
                    run_id: None,
                    role: Role::User,
                    parent_id: None,
                    created_at: OffsetDateTime::now_utc(),
                    parts: vec![Part {
                        id: PartId::new(),
                        message_id: id,
                        ordinal: 0,
                        kind: PartKind::UserInputText,
                        content: ContentBlock::Text {
                            text: "hello".into(),
                        },
                    }],
                },
                config_hash: "legacy".into(),
                plan_fingerprint: "legacy".into(),
                owner: "legacy".into(),
                lease: Duration::from_secs(30),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        agent.recover_admission(session, "hello", options()).await,
        Err(RuntimeError::AdmissionExecution(
            AdmissionExecutionError::MissingEvidence
        ))
    ));
    assert_eq!(provider.requests().len(), 0);
}

#[tokio::test]
async fn generic_recovery_skips_drifted_unstarted_plan_and_recovers_ordinary_work() {
    use crabber::extension::{RunPlanProvider, StaticPlanProvider};
    let (clock, store, provider, session) = fixture();
    let receipt = lost(store.clone(), provider.clone(), &session).await;
    let before = store.get_run(&receipt.run_id).await.unwrap();
    let evidence = serde_json::to_value(
        store
            .load_admission_execution(&session, &options().key)
            .await
            .unwrap(),
    )
    .unwrap();
    let messages = store.list_all_messages(&session).await.unwrap();
    let section = Arc::new(PromptSection {
        name: "plan-b".into(),
        order: 0,
        text: "changed plan".into(),
    });
    let ordinary_session = SessionId::new();
    let plan = StaticPlanProvider::new(Vec::new(), vec![section.clone()])
        .acquire_plan(&ordinary_session)
        .await
        .unwrap();
    let mut user = messages[0].clone();
    user.id = MessageId::new();
    user.session_id = ordinary_session;
    user.run_id = None;
    for part in &mut user.parts {
        part.id = PartId::new();
        part.message_id = user.id.clone();
    }
    let ordinary = store
        .admit_run(AdmitRequest {
            session_id: None,
            workspace_id: "test".into(),
            directory: "/tmp".into(),
            title: "ordinary".into(),
            user_message: user,
            config_hash: "ordinary".into(),
            plan_fingerprint: plan.fingerprint().to_string(),
            owner: "ordinary-host".into(),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap();
    plan.release();
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    // The public facade wrapper must forward classification as well as claim.
    let agent = Agent::builder()
        .store(fault(store.clone(), Fault::BeginRejected))
        .provider(provider.clone())
        .config(config())
        .prompt_section(section)
        .build()
        .unwrap();
    assert!(matches!(
        agent.resume(&receipt.run_id).await,
        Err(RuntimeError::Store(CoreError::AdmissionRecoveryRequired))
    ));
    let recovered = agent.recover().await.unwrap().recovered;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].run_id, ordinary.run.id);
    assert_eq!(recovered[0].status, RunStatus::Interrupted);
    assert_eq!(store.get_run(&receipt.run_id).await.unwrap(), before);
    assert_eq!(
        store
            .lookup_admission(&session, &options().key)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        serde_json::to_value(
            store
                .load_admission_execution(&session, &options().key)
                .await
                .unwrap(),
        )
        .unwrap(),
        evidence
    );
    assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
    assert_eq!(
        store.list_events(&session, None, 100).await.unwrap(),
        Vec::new()
    );
    assert_eq!(provider.requests().len(), 0);
}

struct UnavailablePlan(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl crabber::extension::RunPlanProvider for UnavailablePlan {
    async fn acquire_plan(
        &self,
        _: &SessionId,
    ) -> Result<crabber::extension::RunPlan, crabber::extension::ExtensionError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(crabber::extension::ExtensionError::Plan(
            "unavailable".into(),
        ))
    }
}

#[tokio::test]
async fn generic_unstarted_classification_does_not_acquire_an_unavailable_plan() {
    let (clock, store, provider, session) = fixture();
    let receipt = lost(store.clone(), provider.clone(), &session).await;
    let before = store.get_run(&receipt.run_id).await.unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30));
    let runtime = crabber::runtime::Orchestrator::builder()
        .store(store.clone())
        .resolver(provider.clone())
        .clock(clock)
        .plan_provider(Arc::new(UnavailablePlan(calls.clone())))
        .build()
        .unwrap();
    assert!(matches!(
        runtime.resume(&receipt.run_id).await,
        Err(RuntimeError::Store(CoreError::AdmissionRecoveryRequired))
    ));
    assert_eq!(runtime.recover().await.unwrap().recovered, Vec::new());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(store.get_run(&receipt.run_id).await.unwrap(), before);
    assert_eq!(
        store.list_events(&session, None, 100).await.unwrap(),
        Vec::new()
    );
    assert_eq!(provider.requests().len(), 0);
}
