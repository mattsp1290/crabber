use super::{
    AttemptIdentity, ContinuationIdentity, FrozenCall, ResumeMode, ResumeSource, WaitingSource,
};
use crate::{
    AttemptGeneration, AttemptId, ContinuationGeneration, ContinuationId, ControlId, EventCursor,
    InputFingerprint, MessageId, PublicStateContents, RunId, SessionId, SessionRevision,
    ToolCallId, TurnId,
};

fn resume() -> ResumeSource {
    ResumeSource {
        version: 1,
        session_id: SessionId::new(),
        run_id: RunId::new(),
        attempt: AttemptIdentity {
            id: AttemptId::new(),
            generation: AttemptGeneration::INITIAL,
        },
        current_turn: None,
        continuation: None,
        mode: ResumeMode::NativeUnstarted,
        revision: SessionRevision::INITIAL,
        contents: PublicStateContents::default(),
        calls: Vec::new(),
    }
}
#[test]
fn unstarted_has_no_continued_scope_and_null_state_round_trips() {
    let original = resume();
    original.validate().unwrap();
    let encoded = serde_json::to_value(&original).unwrap();
    assert!(encoded["contents"]["state"].is_null());
    let decoded: ResumeSource = serde_json::from_value(encoded).unwrap();
    assert_eq!(original, decoded);
    let mut changed = original.clone();
    changed.current_turn = Some(TurnId::new());
    assert!(changed.validate().is_err());
    changed = original.clone();
    changed.continuation = Some(ContinuationIdentity {
        id: ContinuationId::new(),
        generation: ContinuationGeneration::INITIAL,
    });
    assert!(changed.validate().is_err());
    changed = original;
    changed.calls.push(FrozenCall {
        call_id: ToolCallId::new(),
        message_id: MessageId::new(),
        name: "tool".into(),
        arguments: serde_json::Value::Null,
    });
    assert!(changed.validate().is_err());
}
#[test]
fn exact_disjoint_pending_sets_and_native_receipt_cursor_are_required() {
    let source = resume();
    let call = ToolCallId::new();
    let mut waiting = WaitingSource {
        version: 1,
        session_id: source.session_id,
        run_id: source.run_id,
        attempt: source.attempt,
        current_turn: None,
        continuation: ContinuationIdentity {
            id: ContinuationId::new(),
            generation: ContinuationGeneration::INITIAL,
        },
        operation_id: ControlId::new(),
        semantic_digest: InputFingerprint::new("a".repeat(64)).unwrap(),
        revision: SessionRevision::INITIAL.next().unwrap(),
        source_event_cursor: EventCursor(1),
        interrupts: Vec::new(),
        frontend_pending: vec![call.clone()],
        backend_pending: Vec::new(),
    };
    waiting.validate().unwrap();
    waiting.backend_pending.push(call);
    assert!(waiting.validate().is_err());
    waiting.backend_pending.clear();
    waiting.source_event_cursor = EventCursor(0);
    assert!(waiting.validate().is_err());
}

#[test]
fn interrupt_response_schema_requires_object_or_boolean() {
    let source = resume();
    let call = ToolCallId::new();
    let mut waiting = WaitingSource {
        version: 1,
        session_id: source.session_id,
        run_id: source.run_id,
        attempt: source.attempt,
        current_turn: None,
        continuation: ContinuationIdentity {
            id: ContinuationId::new(),
            generation: ContinuationGeneration::INITIAL,
        },
        operation_id: ControlId::new(),
        semantic_digest: InputFingerprint::new("a".repeat(64)).unwrap(),
        revision: SessionRevision::INITIAL.next().unwrap(),
        source_event_cursor: EventCursor(1),
        interrupts: vec![super::HumanInterrupt {
            interaction_id: crate::InteractionId::new(),
            call_id: call.clone(),
            kind: crate::InteractionKind::UserInput,
            name: "question".into(),
            prompt: "Choose".into(),
            response_schema: serde_json::json!({}),
        }],
        frontend_pending: Vec::new(),
        backend_pending: vec![call],
    };
    for valid in [
        serde_json::json!({"type":"string"}),
        serde_json::json!(true),
        serde_json::json!(false),
    ] {
        waiting.interrupts[0].response_schema = valid;
        waiting.validate().unwrap();
    }
    for invalid in [
        serde_json::Value::Null,
        serde_json::json!([]),
        serde_json::json!("string"),
        serde_json::json!(0),
    ] {
        waiting.interrupts[0].response_schema = invalid;
        assert!(waiting.validate().is_err());
    }
}
