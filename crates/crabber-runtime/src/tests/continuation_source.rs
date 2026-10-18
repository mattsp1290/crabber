//! Two actual exchanges: safe native facts, not inferred null lifecycle events.
use super::*;
use crabber_core::continuation_source::{ResumeMode, ResumeSource, WaitingSource};
use crabber_core::{
    ControlId, FrontendResultStatus, FrontendToolResult, InputEnvelope, SessionRevision, ToolOutput,
};

#[derive(Default)]
struct Capture(Mutex<Vec<EventRecord>>);
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.0.lock().unwrap().push(event.clone());
    }
}
fn waiting(events: &[EventRecord]) -> WaitingSource {
    let event = events
        .iter()
        .find(|event| event.kind == EventKind::ContinuationWaiting)
        .unwrap();
    let source: WaitingSource = serde_json::from_value(event.payload.clone()).unwrap();
    source.validate().unwrap();
    assert_eq!(source.session_id, event.session_id);
    assert_eq!(source.run_id, event.run_id);
    assert_eq!(
        event.correlation.as_deref(),
        Some(source.operation_id.0.as_str())
    );
    source
}
fn resumed(events: &[EventRecord]) -> ResumeSource {
    let event = events
        .iter()
        .find(|event| event.kind == EventKind::RunResumed)
        .unwrap();
    let source: ResumeSource = serde_json::from_value(event.payload.clone()).unwrap();
    source.validate().unwrap();
    assert_eq!(source.mode, ResumeMode::Continued);
    assert_eq!(source.current_turn, event.turn_id);
    assert!(source.contents.state().is_null());
    assert_eq!(source.attempt.generation.get(), 2);
    source
}

struct Ask;
impl PermissionPolicy for Ask {
    fn decide(&self, _: &crabber_core::ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Ask
    }
}

#[tokio::test]
async fn human_two_exchange_sources_bind_actual_question_turn_attempt_and_parent() {
    let store = Arc::new(MemoryStore::new());
    let capture = Arc::new(Capture::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let call = ToolCallId::new();
    // The actual Echo tool requires text; malformed arguments settle before
    // permission policy and therefore cannot establish a human waiting source.
    let provider = FakeProvider::scripted(vec![
        call_script(call.clone(), r#"{"text":"approved input"}"#),
        text_script("done"),
    ]);
    let runtime = Orchestrator::builder()
        .behavior_version("continuation-source-human.v1")
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .observer(capture.clone())
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(effects.clone())))],
            vec![],
        )))
        .policy(Arc::new(Ask))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let (session, run) = (handle.session_id().clone(), handle.run_id().clone());
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let snapshot = store
        .approval_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let first = waiting(&capture.0.lock().unwrap());
    let durable = waiting(&store.list_events(&session, None, 1000).await.unwrap());
    assert_eq!(first, durable);
    assert_waiting_question(&first, &snapshot, &call);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let crabber_session::CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("actual question");
    };
    let answer = store
        .answer_approval(crabber_session::ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: crabber_session::ApprovalResolverClaim {
                resolver_id: "host".into(),
                authority: description.authority().clone(),
                authority_claim: "PRIVATE_RESOLVER_SENTINEL".into(),
            },
            reply: crabber_session::ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let crate::Continuation::Started { handle, .. } = runtime
        .continue_approval(crate::ContinueApproval {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation,
            expected_revision: answer.revision,
        })
        .await
        .unwrap()
    else {
        panic!("fresh continuation");
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let history = store.list_all_messages(&session).await.unwrap();
    let events = capture.0.lock().unwrap();
    let source = resumed(&events);
    assert_eq!(source.current_turn, first.current_turn);
    assert_eq!(source.continuation, Some(first.continuation));
    assert_eq!(source.calls.len(), 1);
    assert_eq!(source.calls[0].call_id, call);
    let settled = events
        .iter()
        .find(|event| event.kind == EventKind::ToolCallSettled)
        .unwrap();
    assert_eq!(settled.payload["call_id"], serde_json::json!(call));
    assert_original_parent(&history, &source, &call);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::RunStarted)
            .count(),
        1
    );
    assert!(
        !serde_json::to_string(&source)
            .unwrap()
            .contains("PRIVATE_RESOLVER_SENTINEL")
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn frontend_two_exchange_and_unsupported_only_fallback_are_truthful() {
    for fault in 0..4 {
        frontend_case(fault).await;
    }
}
async fn frontend_case(fault: usize) {
    let store = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.continuation_source_fault
        .store(fault, Ordering::SeqCst);
    let selected = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let capture = Arc::new(Capture::default());
    let call = ToolCallId::new();
    let provider =
        FakeProvider::scripted(vec![call_script(call.clone(), "{}"), text_script("done")]);
    let runtime = Orchestrator::builder()
        .behavior_version("continuation-source-frontend.v1")
        .store(selected)
        .resolver(Arc::new(provider.clone()))
        .observer(capture.clone())
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let admission = frontend_admission();
    let handle = runtime.start(admission).await.unwrap();
    let (session, run) = (handle.session_id().clone(), handle.run_id().clone());
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let first = waiting(&capture.0.lock().unwrap());
    assert_eq!(first.frontend_pending, vec![call.clone()]);
    assert!(first.backend_pending.is_empty() && first.interrupts.is_empty());
    let pending = store
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let crate::ToolContinuation::Started { handle, .. } = runtime
        .continue_frontend(crate::ContinueFrontend {
            continuation: crate::ContinueTool {
                id: ControlId::new(),
                session_id: session.clone(),
                run_id: run.clone(),
                continuation: pending.continuation,
                expected_revision: pending.revision,
            },
            resolver: crabber_session::FrontendResolverClaim {
                resolver_id: "host".into(),
                authority_claim: "PRIVATE_RESOLVER_SENTINEL".into(),
            },
            results: vec![
                FrontendToolResult::new(
                    call.clone(),
                    FrontendResultStatus::Completed,
                    ToolOutput::from_json(&json!("external result")).unwrap(),
                )
                .unwrap(),
            ],
        })
        .await
        .unwrap()
    else {
        panic!("fresh frontend continuation");
    };
    if fault < 2 {
        // This fixture holds the actual terminal ACK independently of the
        // continuation source reader. These cases exercise the reader, so
        // allow that same real terminal call to return before observing done.
        gate.release.notify_one();
    }
    let result = handle.done().await;
    let events = capture.0.lock().unwrap();
    if fault >= 2 {
        assert!(result.is_err());
        assert!(!events.iter().any(|event| matches!(
            event.kind,
            EventKind::RunResumed | EventKind::ToolCallSettled
        )));
        assert_eq!(provider.requests().len(), 1);
    } else {
        assert_eq!(result.unwrap().status, RunStatus::Completed);
        assert_eq!(provider.requests().len(), 2);
        if fault == 1 {
            assert!(
                events
                    .iter()
                    .find(|event| event.kind == EventKind::RunResumed)
                    .unwrap()
                    .payload
                    .is_null()
            );
        } else {
            let source = resumed(&events);
            assert_eq!(source.calls[0].call_id, call);
            assert_eq!(source.continuation, Some(first.continuation));
        }
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == EventKind::RunStarted)
                .count(),
            1
        );
    }
}

fn assert_original_parent(
    history: &[crabber_core::Message],
    source: &ResumeSource,
    call: &ToolCallId,
) {
    let parent = history
        .iter()
        .find(|message| message.id == source.calls[0].message_id)
        .unwrap();
    assert_eq!(parent.role, crabber_core::Role::Assistant);
    assert!(parent.parts.iter().any(|part| matches!(&part.content,
        ContentBlock::ToolCall { call_id, name, arguments } if call_id == call
            && name == &source.calls[0].name && arguments == &source.calls[0].arguments)));
}

fn frontend_admission() -> Request {
    let mut admission = request();
    admission.text.clear();
    let mut input =
        serde_json::to_value(InputEnvelope::text("external", SessionRevision::INITIAL).unwrap())
            .unwrap();
    input["frontend_tools"] = json!([{"name":"echo","description":"external","input_parameters":{"type":"object"},"result_contract":null}]);
    admission.input_envelope = Some(serde_json::from_value(input).unwrap());
    admission
}

fn assert_waiting_question(
    first: &WaitingSource,
    snapshot: &crabber_session::ApprovalSnapshot,
    call: &ToolCallId,
) {
    assert_eq!(first.interrupts.len(), 1);
    assert_eq!(
        first.interrupts[0].interaction_id,
        snapshot.interactions[0].id
    );
    assert_eq!(&first.interrupts[0].call_id, call);
    assert_eq!(first.backend_pending, vec![call.clone()]);
    assert_eq!(first.frontend_pending, [] as [ToolCallId; 0]);
}
