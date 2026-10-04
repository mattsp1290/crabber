//! D3/D9/D10 replay through the public entrypoints, using expired durable fixtures.

use super::*;
use crate::{INTERRUPTED_RESULT_TEXT, RuntimeError};
use crabber_core::{ManualClock, MessageId, Part, PartId, PartKind, RunStatus, ToolResultStatus};
use crabber_extension::{
    InputUnavailable, ToolInput, ToolOutcomeClass, TransformOutput, TransformPhase,
};
use crabber_session::AdmitRequest;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy)]
enum Entry {
    Resume,
    Recover,
}
impl Entry {
    async fn invoke(
        self,
        runtime: &Orchestrator,
        run: &RunId,
    ) -> Result<crate::RunResult, RuntimeError> {
        match self {
            Self::Resume => runtime.resume(run).await,
            Self::Recover => {
                let report = runtime.recover().await?;
                assert_eq!(report.skipped, [] as [crate::SkippedRun; 0]);
                assert_eq!(report.recovered.len(), 1);
                let result = report.recovered.into_iter().next().unwrap();
                assert_eq!(&result.run_id, run);
                Ok(result)
            }
        }
    }
}

type Observations = Arc<Mutex<Vec<(&'static str, ToolResultContext, Value)>>>;

#[derive(Default)]
struct ReplayPipeline {
    prepares: AtomicUsize,
    seen: Observations,
}
#[async_trait]
impl ToolPipeline for ReplayPipeline {
    async fn prepare(&self, _: &ToolInfo, input: Value) -> Result<Value, String> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    }
    async fn transform_result(
        &self,
        context: &ToolResultContext,
        _: &ToolInfo,
        value: Value,
    ) -> Result<TransformOutput, String> {
        self.seen
            .lock()
            .unwrap()
            .push(("pre-stage", context.clone(), value));
        Ok(TransformOutput::new(json!("SECRET-PRE-STAGE")))
    }
}

fn recording_chain(seen: Observations) -> Arc<ClosureExtension> {
    ClosureExtension::new("recovery-chain", move |registrar| {
        for (order, label, output) in [
            (1, "ordinary-one", "SECRET-ONE"),
            (2, "ordinary-two", "SECRET-TWO"),
        ] {
            let seen = Arc::clone(&seen);
            registrar.on_result_transform(
                order,
                label,
                Arc::new(move |context, value| {
                    let seen = Arc::clone(&seen);
                    Box::pin(async move {
                        seen.lock().unwrap().push((label, context, value));
                        Ok(TransformOutput::new(json!(output)))
                    })
                }),
            );
        }
        let seen = Arc::clone(&seen);
        // A lower order still follows every ordinary handler.
        registrar.on_final_redaction(
            -100,
            "final",
            Arc::new(move |context, value| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    seen.lock().unwrap().push(("final", context, value));
                    Ok(TransformOutput::new(json!({"safe": true})))
                })
            }),
        );
    })
}

fn user_message(session: SessionId, now: time::OffsetDateTime) -> Message {
    let message_id = MessageId::new();
    Message {
        id: message_id.clone(),
        session_id: session,
        run_id: None,
        role: Role::User,
        parent_id: None,
        created_at: now,
        parts: vec![Part {
            id: PartId::new(),
            message_id,
            ordinal: 0,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text {
                text: "hello".into(),
            },
        }],
    }
}

struct Fixture {
    harness: Harness,
    pipeline: Arc<ReplayPipeline>,
    clock: Arc<ManualClock>,
    session: SessionId,
    run: RunId,
    call: ToolCallRecord,
}
impl Fixture {
    // Store APIs create the crash boundary; no private orchestrator settlement helper is used.
    async fn new(
        paused: bool,
        running: bool,
        retry_safe: bool,
        name: &str,
        arguments: Value,
        admitted_fingerprint: Option<fn(&crabber_extension::RunPlan) -> String>,
    ) -> Self {
        let pipeline = Arc::new(ReplayPipeline::default());
        let mut harness = Harness::builder(vec![final_turn()])
            .tool_pipeline(pipeline.clone())
            .mount(recording_chain(Arc::clone(&pipeline.seen)), Scope::Global)
            .build()
            .await;
        let now = time::OffsetDateTime::UNIX_EPOCH;
        let clock = Arc::new(ManualClock::new(now));
        harness.store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let session = SessionId::new();
        let plan = harness.registry.acquire_plan(&session).await.unwrap();
        let fingerprint = admitted_fingerprint
            .map_or_else(|| plan.fingerprint().to_string(), |stale| stale(&plan));
        plan.release();
        let admitted = harness
            .store
            .admit_run(AdmitRequest {
                session_id: None,
                workspace_id: "test".into(),
                directory: ".".into(),
                title: "test".into(),
                user_message: user_message(session, now),
                config_hash: "test".into(),
                plan_fingerprint: fingerprint,
                owner: "crashed".into(),
                lease: std::time::Duration::from_secs(30),
            })
            .await
            .unwrap();
        let session = admitted.session.id;
        let run = admitted.run.id;
        let execution = harness.store.execution(admitted.fence).await.unwrap();
        let event = |kind| EventRecord {
            cursor: None,
            session_id: session.clone(),
            run_id: run.clone(),
            turn_id: None,
            kind,
            payload: Value::Null,
            correlation: None,
            live_only: false,
            created_at: now,
        };
        let mut call = ToolCallRecord {
            id: ToolCallId::new(),
            run_id: run.clone(),
            name: name.into(),
            arguments,
            status: ToolCallStatus::Pending,
            retry_safe,
            result: None,
        };
        execution
            .create_tool_call(call.clone(), event(EventKind::ToolCallPending))
            .await
            .unwrap();
        if running {
            execution
                .claim_tool_call(&call.id, event(EventKind::ToolCallRunning))
                .await
                .unwrap();
            call.status = ToolCallStatus::Running;
        }
        if paused {
            execution
                .pause_run(
                    json!({"request": {
                        "workspace_id": "test", "directory": ".", "title": "test", "text": "hello",
                        "provider_id": "fake", "model_id": "scripted", "system_prompt": null
                    }}),
                    event(EventKind::RunPaused),
                )
                .await
                .unwrap();
        }
        clock.set(now + time::Duration::seconds(31));
        Self {
            harness,
            pipeline,
            clock,
            session,
            run,
            call,
        }
    }

    fn fresh_runtime(&self) -> Orchestrator {
        Orchestrator::builder()
            .store(self.harness.store.clone() as Arc<dyn Store>)
            .resolver(Arc::new(self.harness.fake.clone()))
            .plan_provider(self.harness.registry.clone() as Arc<dyn RunPlanProvider>)
            .policy(Arc::clone(&self.harness.policy))
            .tool_pipeline(self.pipeline.clone())
            .clock(self.clock.clone())
            .build()
            .unwrap()
    }

    fn finished(&self) -> Finished<'_> {
        Finished {
            harness: &self.harness,
            session_id: self.session.clone(),
            run_id: self.run.clone(),
        }
    }

    async fn assert_persisted(
        &self,
        status: ToolCallStatus,
        text: &str,
        error: bool,
        paused: bool,
    ) {
        let done = self.finished();
        let record = done.record(&self.call.id).await;
        assert_eq!(record.id, self.call.id);
        assert_eq!(record.run_id, self.run);
        assert_eq!(record.name, self.call.name);
        assert_eq!(record.arguments, self.call.arguments);
        assert_eq!(record.retry_safe, self.call.retry_safe);
        assert_eq!(record.status, status);
        let result_status = match status {
            ToolCallStatus::Completed => ToolResultStatus::Completed,
            ToolCallStatus::Failed => ToolResultStatus::Failed,
            ToolCallStatus::Interrupted => ToolResultStatus::Interrupted,
            other => panic!("unfinished status {other:?}"),
        };
        assert_eq!(record.result.as_ref().unwrap().status, result_status);
        assert_eq!(record_text(&record), text);
        let (message, message_text, message_error) = done.tool_message(&self.call.id).await;
        assert_eq!(message.session_id, self.session);
        assert_eq!(message.run_id, Some(self.run.clone()));
        assert_eq!((message_text.as_str(), message_error), (text, error));
        let event = done.settled_event(&self.call.id).await;
        assert_eq!(event.session_id, self.session);
        assert_eq!(event.run_id, self.run);
        assert_eq!(event.payload["is_error"], error);
        assert_eq!(event.payload["content"][0]["text"], text);
        let settled = self
            .harness
            .store
            .list_events(&self.session, None, 1000)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled)
            .count();
        assert_eq!(settled, 1);
        assert_eq!(
            self.harness
                .store
                .list_unfinished_tool_calls(&self.run)
                .await
                .unwrap(),
            [] as [ToolCallRecord; 0]
        );
        if paused {
            assert_eq!(self.harness.fake.requests().len(), 1);
            assert_eq!(
                done.next_request_result(&self.call.id),
                (text.into(), error)
            );
        } else {
            // An expired Running run has no saved request and stops after call reconciliation.
            assert_eq!(self.harness.fake.requests(), [] as [ModelRequest; 0]);
        }
    }

    fn assert_chain(&self, class: ToolOutcomeClass, input: &ToolInput, expected_output: &Value) {
        let seen = self.pipeline.seen.lock().unwrap();
        let success = class == ToolOutcomeClass::Succeeded;
        let labels: Vec<_> = seen.iter().map(|(label, _, _)| *label).collect();
        assert_eq!(
            labels,
            if success {
                vec!["pre-stage", "ordinary-one", "ordinary-two", "final"]
            } else {
                vec!["ordinary-one", "ordinary-two", "final"]
            }
        );
        for (label, context, _) in seen.iter() {
            assert_eq!(context.call_id(), &self.call.id);
            assert_eq!(context.session_id(), &self.session);
            assert_eq!(context.run_id(), &self.run);
            assert_eq!(context.tool_name(), self.call.name);
            assert_eq!(context.resolved(), class != ToolOutcomeClass::UnknownTool);
            assert_eq!(context.class(), class);
            assert_eq!(context.input(), input);
            assert_eq!(context.is_error(), class.is_error());
            assert_eq!(
                context.phase(),
                if *label == "final" {
                    TransformPhase::FinalRedaction
                } else {
                    TransformPhase::Ordinary
                }
            );
            assert!(!context.cancellation().is_cancelled());
        }
        assert_eq!(&seen[0].2, expected_output);
        let ordinary = usize::from(success);
        if success {
            assert_eq!(seen[ordinary].2, json!("SECRET-PRE-STAGE"));
        }
        assert_eq!(seen[ordinary + 1].2, json!("SECRET-ONE"));
        assert_eq!(seen[ordinary + 2].2, json!("SECRET-TWO"));
        assert_eq!(self.harness.probe.results().len(), 1);
        assert_eq!(self.pipeline.prepares.load(Ordering::SeqCst), 0);
        assert_eq!(self.harness.probe.prepares(), [] as [Value; 0]);
    }
}

fn pre_contract_fingerprint(plan: &crabber_extension::RunPlan) -> String {
    use sha2::{Digest, Sha256};
    let mut components = plan.components.clone();
    components.sort_by(|a, b| (&a.id, &a.version).cmp(&(&b.id, &b.version)));
    crabber_extension::PlanFingerprint(
        Sha256::digest(serde_json::to_vec(&components).unwrap()).into(),
    )
    .to_string()
}

#[tokio::test]
async fn public_entrypoints_apply_the_full_d10_state_matrix() {
    for entry in [Entry::Resume, Entry::Recover] {
        for paused in [false, true] {
            for running in [false, true] {
                for safe in [false, true] {
                    // This stored normalized value would fail ToolPrepare if replay reran it.
                    let arguments = json!({"text": PREPARE_REJECTED});
                    let fixture =
                        Fixture::new(paused, running, safe, ECHO, arguments.clone(), None).await;
                    let result = entry
                        .invoke(&fixture.fresh_runtime(), &fixture.run)
                        .await
                        .unwrap();
                    assert_eq!(result.session_id, fixture.session);
                    assert_eq!(result.run_id, fixture.run);
                    if !paused {
                        assert_eq!(result.status, RunStatus::Interrupted);
                    }
                    let fixed = running || (!paused && !safe);
                    if fixed {
                        assert!(fixture.pipeline.seen.lock().unwrap().is_empty());
                        assert_eq!(fixture.harness.probe.results(), [] as [Value; 0]);
                        assert_eq!(fixture.harness.probe.executed(), [] as [(String, Value); 0]);
                        assert_eq!(fixture.pipeline.prepares.load(Ordering::SeqCst), 0);
                        assert_eq!(fixture.harness.probe.prepares(), [] as [Value; 0]);
                        fixture
                            .assert_persisted(
                                ToolCallStatus::Interrupted,
                                INTERRUPTED_RESULT_TEXT,
                                true,
                                paused,
                            )
                            .await;
                    } else {
                        assert_eq!(
                            fixture.harness.probe.executed(),
                            [(ECHO.into(), arguments.clone())]
                        );
                        fixture.assert_chain(
                            ToolOutcomeClass::Succeeded,
                            &ToolInput::Normalized(arguments.clone()),
                            &arguments,
                        );
                        fixture
                            .assert_persisted(
                                ToolCallStatus::Completed,
                                r#"{"safe":true}"#,
                                false,
                                paused,
                            )
                            .await;
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn replay_class_and_input_come_only_from_the_record() {
    for entry in [Entry::Resume, Entry::Recover] {
        for (name, arguments, class, input, seed) in [
            (
                FAIL,
                json!({"text": "stored"}),
                ToolOutcomeClass::ExecutionFailed,
                ToolInput::Normalized(json!({"text": "stored"})),
                json!("tool execution failed: executor exploded"),
            ),
            (
                FORBIDDEN,
                json!({"text": "stored"}),
                ToolOutcomeClass::PermissionDenied,
                ToolInput::Normalized(json!({"text": "stored"})),
                json!("permission denied"),
            ),
            // Even a name present in the plan cannot override the unknown sentinel.
            (
                ECHO,
                json!({"$crabber_unknown_tool": {"raw": "invalid JSON provider text"}}),
                ToolOutcomeClass::UnknownTool,
                ToolInput::Raw(json!("invalid JSON provider text")),
                json!("unknown tool: echo"),
            ),
            (
                MISSING,
                json!({"$crabber_unknown_tool": {"raw": {"text": "provider raw"}}}),
                ToolOutcomeClass::UnknownTool,
                ToolInput::Raw(json!({"text": "provider raw"})),
                json!("unknown tool: missing"),
            ),
            (
                ECHO,
                json!({"$crabber_unknown_tool": {"missing_raw": true}}),
                ToolOutcomeClass::UnknownTool,
                ToolInput::Unavailable {
                    reason: InputUnavailable::Unresolved,
                },
                json!("unknown tool: echo"),
            ),
            (
                ECHO,
                json!({"$crabber_prepare_error": 42}),
                ToolOutcomeClass::PrepareFailed,
                ToolInput::Unavailable {
                    reason: InputUnavailable::PrepareFailed,
                },
                json!("reserved argument key"),
            ),
            (
                ECHO,
                json!({"$crabber_future_reserved": "stored"}),
                ToolOutcomeClass::PrepareFailed,
                ToolInput::Unavailable {
                    reason: InputUnavailable::PrepareFailed,
                },
                json!("reserved argument key"),
            ),
            (
                MISSING,
                json!({"text": "previously normalized"}),
                ToolOutcomeClass::UnknownTool,
                ToolInput::Unavailable {
                    reason: InputUnavailable::Unresolved,
                },
                json!("unknown tool: missing"),
            ),
            // Error-looking text cannot override the prepare sentinel's class.
            (
                ECHO,
                json!({"$crabber_prepare_error": "unknown tool: echo"}),
                ToolOutcomeClass::PrepareFailed,
                ToolInput::Unavailable {
                    reason: InputUnavailable::PrepareFailed,
                },
                json!("unknown tool: echo"),
            ),
        ] {
            let fixture = Fixture::new(true, false, false, name, arguments.clone(), None).await;
            entry
                .invoke(&fixture.fresh_runtime(), &fixture.run)
                .await
                .unwrap();
            fixture.assert_chain(class, &input, &seed);
            assert_eq!(
                fixture.harness.probe.executed(),
                if name == FAIL {
                    vec![(name.into(), arguments)]
                } else {
                    vec![]
                }
            );
            // Both handlers return mark_error=false, which must never upgrade an error to success.
            fixture
                .assert_persisted(ToolCallStatus::Failed, r#"{"safe":true}"#, true, true)
                .await;
        }
    }
}

#[tokio::test]
async fn old_contract_fingerprint_is_refused_without_mutation() {
    for entry in [Entry::Resume, Entry::Recover] {
        for paused in [false, true] {
            let fixture = Fixture::new(
                paused,
                false,
                true,
                ECHO,
                json!({"text": "stored"}),
                Some(pre_contract_fingerprint),
            )
            .await;
            let run_before = fixture.harness.store.get_run(&fixture.run).await.unwrap();
            let calls_before = fixture
                .harness
                .store
                .list_unfinished_tool_calls(&fixture.run)
                .await
                .unwrap();
            let events_before = fixture
                .harness
                .store
                .list_events(&fixture.session, None, 1000)
                .await
                .unwrap();
            let messages_before = fixture
                .harness
                .store
                .list_messages(&fixture.session, None)
                .await
                .unwrap();
            let error = entry
                .invoke(&fixture.fresh_runtime(), &fixture.run)
                .await
                .unwrap_err();
            assert!(
                matches!(error, RuntimeError::PlanChanged),
                "{entry:?}: {error:?}"
            );
            assert_eq!(
                fixture.harness.store.get_run(&fixture.run).await.unwrap(),
                run_before
            );
            assert_eq!(
                fixture
                    .harness
                    .store
                    .list_unfinished_tool_calls(&fixture.run)
                    .await
                    .unwrap(),
                calls_before
            );
            assert_eq!(
                fixture
                    .harness
                    .store
                    .list_events(&fixture.session, None, 1000)
                    .await
                    .unwrap(),
                events_before
            );
            assert_eq!(
                fixture
                    .harness
                    .store
                    .list_messages(&fixture.session, None)
                    .await
                    .unwrap(),
                messages_before
            );
            assert_eq!(fixture.harness.probe.executed(), [] as [(String, Value); 0]);
            assert_eq!(fixture.harness.probe.prepares(), [] as [Value; 0]);
            assert_eq!(fixture.harness.probe.results(), [] as [Value; 0]);
            assert!(fixture.pipeline.seen.lock().unwrap().is_empty());
            assert_eq!(fixture.pipeline.prepares.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.harness.fake.requests(), [] as [ModelRequest; 0]);
        }
    }
}
