use crabber_agui::{
    Completion, ProjectionConfig, ProjectionError, Projector, ag_ui_core::event::Event,
};
use crabber_core::{
    AttemptGeneration, AttemptId, EventCursor, EventKind, EventRecord, InvocationId, MessageId,
    PublicActivity, PublicAttemptIdentity, PublicInvocationEvent, PublicInvocationIdentity,
    PublicInvocationStatus, PublicPatchOperation, PublicPresentationChange, PublicSourceChange,
    PublicSourceEvent, PublicSourceIdentity, PublicSourceMessage, PublicStateContents, Role, RunId,
    SessionId, SessionOwnerGeneration, SessionRevision,
};
use serde_json::{Value, json};

mod support;

fn source(id: Option<&str>) -> PublicSourceIdentity {
    PublicSourceIdentity {
        workspace_id: "workspace".into(),
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        attempt: PublicAttemptIdentity {
            id: AttemptId::from("attempt"),
            generation: AttemptGeneration::INITIAL,
        },
        call_id: None,
        invocation: id.map(|id| PublicInvocationIdentity {
            id: InvocationId::from(id),
            generation: SessionOwnerGeneration::INITIAL,
            parent_invocation: None,
            parent_generation: None,
            parent_call_id: None,
        }),
    }
}
fn record(kind: EventKind, revision: u64, payload: Value) -> EventRecord {
    EventRecord {
        cursor: Some(EventCursor(revision)),
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        turn_id: None,
        kind,
        payload,
        correlation: None,
        live_only: false,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
fn start(config: ProjectionConfig) -> (Projector, Vec<Event>) {
    let mut p = Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        config,
    )
    .unwrap();
    let events = p
        .push(&record(EventKind::RunStarted, 0, Value::Null))
        .unwrap();
    (p, events)
}
fn invocation(id: &str, revision: u64, status: PublicInvocationStatus) -> EventRecord {
    let kind = match status {
        PublicInvocationStatus::Started => EventKind::InvocationStarted,
        PublicInvocationStatus::Finished => EventKind::InvocationFinished,
        PublicInvocationStatus::Error => EventKind::InvocationError,
    };
    record(
        kind,
        revision,
        serde_json::to_value(PublicInvocationEvent {
            version: 1,
            source: source(Some(id)),
            revision: SessionRevision::new(revision).unwrap(),
            status,
        })
        .unwrap(),
    )
}
fn presentation(
    id: Option<&str>,
    revision: u64,
    change: PublicPresentationChange,
    contents: PublicStateContents,
) -> EventRecord {
    record(
        EventKind::PublicPresentation,
        revision,
        serde_json::to_value(PublicSourceEvent {
            version: 2,
            source: source(id),
            revision: SessionRevision::new(revision).unwrap(),
            change: PublicSourceChange::Presentation {
                presentation: change,
            },
            contents,
            before: None,
            before_event_cursor: None,
        })
        .unwrap(),
    )
}
fn message(id: &str) -> PublicPresentationChange {
    PublicPresentationChange::MessageStarted {
        message_id: MessageId::from(id),
        role: Role::Assistant,
    }
}
fn delta(id: &str, text: &str) -> PublicPresentationChange {
    PublicPresentationChange::TextDelta {
        message_id: MessageId::from(id),
        text: text.into(),
    }
}
fn ended(id: &str) -> PublicPresentationChange {
    PublicPresentationChange::MessageEnded {
        message_id: MessageId::from(id),
    }
}

#[tokio::test]
async fn two_interleaved_source_invocations_have_independent_real_sdk_message_slots() {
    let (mut p, mut output) = start(ProjectionConfig::default());
    let empty = PublicStateContents::default();
    for event in [
        invocation("a", 1, PublicInvocationStatus::Started),
        presentation(Some("a"), 2, message("a-message"), empty.clone()),
        invocation("b", 3, PublicInvocationStatus::Started),
        presentation(Some("b"), 4, message("b-message"), empty.clone()),
        presentation(Some("a"), 5, delta("a-message", "A1"), empty.clone()),
        presentation(Some("b"), 6, delta("b-message", "B"), empty.clone()),
        presentation(Some("a"), 7, delta("a-message", "A2"), empty.clone()),
        presentation(Some("b"), 8, ended("b-message"), empty.clone()),
        invocation("b", 9, PublicInvocationStatus::Finished),
        presentation(Some("a"), 10, ended("a-message"), empty),
        invocation("a", 11, PublicInvocationStatus::Finished),
    ] {
        output.extend(p.push(&event).unwrap());
    }
    p.push(&record(
        EventKind::RunSettled,
        12,
        json!({"status":"completed"}),
    ))
    .unwrap();
    output.extend(p.finish(Completion::Completed).unwrap());
    let result = support::consume(&output).await;
    let history = serde_json::to_value(result.messages).unwrap();
    let messages = history.as_array().unwrap();
    let a = messages
        .iter()
        .find(|message| message["id"] == "a-message")
        .unwrap();
    let b = messages
        .iter()
        .find(|message| message["id"] == "b-message")
        .unwrap();
    assert_eq!(a["content"], "A1A2");
    assert_eq!(b["content"], "B");
    assert_ne!(a["subagentRunId"], b["subagentRunId"]);
}

#[test]
fn child_cannot_replace_parent_history_or_write_after_its_actual_scope_closes() {
    let (mut p, _) = start(ProjectionConfig::default());
    p.push(&invocation("child", 1, PublicInvocationStatus::Started))
        .unwrap();
    let partial = PublicPresentationChange::MessagesSnapshot {
        messages: vec![PublicSourceMessage {
            message_id: MessageId::from("child-only"),
            role: Role::Assistant,
            text: "partial".into(),
        }],
    };
    assert_eq!(
        p.push(&presentation(
            Some("child"),
            2,
            partial,
            PublicStateContents::default()
        )),
        Err(ProjectionError::Identity)
    );
    let (mut p, _) = start(ProjectionConfig::default());
    p.push(&invocation("child", 1, PublicInvocationStatus::Started))
        .unwrap();
    p.push(&invocation("child", 2, PublicInvocationStatus::Finished))
        .unwrap();
    assert_eq!(
        p.push(&presentation(
            Some("child"),
            3,
            message("late"),
            PublicStateContents::default()
        )),
        Err(ProjectionError::Identity)
    );
}

#[tokio::test]
async fn global_text_snapshot_preserves_activity_delta_base_in_real_sdk_history() {
    let (mut p, mut output) = start(ProjectionConfig::default());
    let contents = PublicStateContents::new(
        Value::Null,
        vec![PublicActivity::new("progress", "loading", json!({"done":false})).unwrap()],
    )
    .unwrap();
    let snapshot = |revision| PublicSourceEvent {
        version: 2,
        source: source(None),
        revision: SessionRevision::new(revision).unwrap(),
        change: PublicSourceChange::StateSnapshot { state: Value::Null },
        contents: contents.clone(),
        before: None,
        before_event_cursor: None,
    };
    output.extend(
        p.push(&record(
            EventKind::PublicStateSnapshot,
            1,
            serde_json::to_value(snapshot(1)).unwrap(),
        ))
        .unwrap(),
    );
    let messages = PublicPresentationChange::MessagesSnapshot {
        messages: vec![PublicSourceMessage {
            message_id: MessageId::from("history"),
            role: Role::Assistant,
            text: "retained history".into(),
        }],
    };
    output.extend(
        p.push(&presentation(None, 2, messages, contents.clone()))
            .unwrap(),
    );
    let mut before = record(
        EventKind::PublicStateSnapshot,
        3,
        serde_json::to_value(snapshot(2)).unwrap(),
    );
    before.cursor = Some(EventCursor(3));
    output.extend(p.push(&before).unwrap());
    let change = PublicSourceChange::ActivityDelta {
        activity_id: "progress".into(),
        activity_type: "loading".into(),
        delta: vec![
            PublicPatchOperation::new(
                json!({"op":"replace","path":"/done","value":true,"extension":null}),
            )
            .unwrap(),
        ],
    };
    let payload = PublicSourceEvent {
        version: 2,
        source: source(None),
        revision: SessionRevision::new(3).unwrap(),
        contents: change.apply(&contents).unwrap(),
        change,
        before: Some(contents),
        before_event_cursor: Some(EventCursor(3)),
    };
    output.extend(
        p.push(&record(
            EventKind::ActivityDelta,
            4,
            serde_json::to_value(payload).unwrap(),
        ))
        .unwrap(),
    );
    p.push(&record(
        EventKind::RunSettled,
        5,
        json!({"status":"completed"}),
    ))
    .unwrap();
    output.extend(p.finish(Completion::Completed).unwrap());
    let result = support::consume(&output).await;
    let history = serde_json::to_value(result.messages).unwrap();
    assert!(
        history
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["id"] == "history" && message["content"] == "retained history")
    );
    assert!(
        history
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["id"] == "progress" && message["content"]["done"] == true)
    );
}

fn nested(mut event: EventRecord, parent: &str) -> EventRecord {
    event.payload["source"]["invocation"]["parent_invocation"] = json!(parent);
    event.payload["source"]["invocation"]["parent_generation"] = json!(1);
    event
}

#[tokio::test]
async fn parent_terminal_closes_presentation_not_independent_child_execution() {
    let (mut parent, mut events) = start(ProjectionConfig::default());
    let empty = PublicStateContents::default();
    let child_start = nested(
        invocation("child", 2, PublicInvocationStatus::Started),
        "parent",
    );
    for event in [
        invocation("parent", 1, PublicInvocationStatus::Started),
        child_start.clone(),
        nested(
            presentation(Some("child"), 3, message("partial-child"), empty.clone()),
            "parent",
        ),
        nested(
            presentation(
                Some("child"),
                4,
                delta("partial-child", "before parent joins"),
                empty.clone(),
            ),
            "parent",
        ),
        invocation("parent", 5, PublicInvocationStatus::Finished),
    ] {
        events.extend(parent.push(&event).unwrap());
    }
    assert!(events.iter().any(|event| matches!(event, Event::SubagentError(error) if error.code.as_deref() == Some("crabber_projection_scope_closed"))));
    parent
        .push(&record(
            EventKind::RunSettled,
            6,
            json!({"status":"completed"}),
        ))
        .unwrap();
    events.extend(parent.finish(Completion::Completed).unwrap());
    let result = support::consume(&events).await;
    assert!(
        serde_json::to_value(result.messages)
            .unwrap()
            .to_string()
            .contains("before parent joins")
    );
    let late = nested(
        presentation(Some("child"), 7, message("late-child"), empty.clone()),
        "parent",
    );
    assert_eq!(parent.push(&late), Err(ProjectionError::Terminal));

    assert_separate_child_exchange(&child_start, &late).await;
}

async fn assert_separate_child_exchange(child_start: &EventRecord, late: &EventRecord) {
    let empty = PublicStateContents::default();
    // The actual child's separately selected history opens only at its original
    // lifecycle Start; selecting the identity alone never fabricates startup.
    let selected: PublicInvocationEvent =
        serde_json::from_value(child_start.payload.clone()).unwrap();
    let mut child = Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        ProjectionConfig::default(),
    )
    .unwrap()
    .bind_source(selected.source)
    .unwrap();
    let mut events = child.push(child_start).unwrap();
    events.extend(child.push(late).unwrap());
    events.extend(
        child
            .push(&nested(
                presentation(
                    Some("child"),
                    8,
                    delta("late-child", "independent"),
                    empty.clone(),
                ),
                "parent",
            ))
            .unwrap(),
    );
    events.extend(
        child
            .push(&nested(
                presentation(Some("child"), 9, ended("late-child"), empty),
                "parent",
            ))
            .unwrap(),
    );
    events.extend(
        child
            .push(&nested(
                invocation("child", 10, PublicInvocationStatus::Finished),
                "parent",
            ))
            .unwrap(),
    );
    events.extend(child.finish(Completion::Completed).unwrap());
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::SubagentStarted(_)))
    );
    let result = support::consume(&events).await;
    let messages = serde_json::to_value(result.messages).unwrap();
    assert!(
        messages
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["id"] == "late-child"
                && message["content"] == "independent"
                && message.get("subagentRunId").is_none())
    );
}

#[test]
fn selecting_a_child_is_not_actual_start_or_completion() {
    let p = || {
        Projector::new(
            SessionId::from("session"),
            RunId::from("run"),
            "thread".into(),
            "wire".into(),
            ProjectionConfig::default(),
        )
        .unwrap()
        .bind_source(source(Some("child")))
        .unwrap()
    };
    let mut selected = p();
    assert!(
        selected
            .finish(Completion::Completed)
            .unwrap()
            .iter()
            .any(|event| matches!(event, Event::RunError(_)))
    );
    let mut selected = p();
    assert_eq!(
        selected.push(&presentation(
            Some("child"),
            1,
            message("early"),
            PublicStateContents::default()
        )),
        Err(ProjectionError::Order)
    );
    let mut selected = p();
    let mut wrong = invocation("child", 1, PublicInvocationStatus::Started);
    wrong.payload["source"]["invocation"]["generation"] = json!(2);
    assert_eq!(selected.push(&wrong), Err(ProjectionError::Identity));
}

#[tokio::test]
async fn different_child_actors_keep_one_session_global_activity_owner() {
    let (mut p, mut events) = start(ProjectionConfig::default());
    events.extend(
        p.push(&invocation("a", 1, PublicInvocationStatus::Started))
            .unwrap(),
    );
    events.extend(
        p.push(&invocation("b", 2, PublicInvocationStatus::Started))
            .unwrap(),
    );
    let before = PublicStateContents::new(
        Value::Null,
        vec![PublicActivity::new("global-progress", "progress", json!({"done":false})).unwrap()],
    )
    .unwrap();
    let mut snapshot = PublicSourceEvent {
        version: 2,
        source: source(Some("a")),
        revision: SessionRevision::new(3).unwrap(),
        change: PublicSourceChange::StateSnapshot { state: Value::Null },
        contents: before.clone(),
        before: None,
        before_event_cursor: None,
    };
    events.extend(
        p.push(&record(
            EventKind::PublicStateSnapshot,
            3,
            serde_json::to_value(&snapshot).unwrap(),
        ))
        .unwrap(),
    );
    snapshot.source = source(Some("b"));
    events.extend(
        p.push(&record(
            EventKind::PublicStateSnapshot,
            4,
            serde_json::to_value(snapshot).unwrap(),
        ))
        .unwrap(),
    );
    let change = PublicSourceChange::ActivityDelta {
        activity_id: "global-progress".into(),
        activity_type: "progress".into(),
        delta: vec![
            PublicPatchOperation::new(json!({"op":"replace","path":"/done","value":true})).unwrap(),
        ],
    };
    let delta = PublicSourceEvent {
        version: 2,
        source: source(Some("b")),
        revision: SessionRevision::new(4).unwrap(),
        contents: change.apply(&before).unwrap(),
        change,
        before: Some(before),
        before_event_cursor: Some(EventCursor(4)),
    };
    events.extend(
        p.push(&record(
            EventKind::ActivityDelta,
            5,
            serde_json::to_value(delta).unwrap(),
        ))
        .unwrap(),
    );
    for event in [
        invocation("a", 6, PublicInvocationStatus::Finished),
        invocation("b", 7, PublicInvocationStatus::Finished),
    ] {
        events.extend(p.push(&event).unwrap());
    }
    p.push(&record(
        EventKind::RunSettled,
        8,
        json!({"status":"completed"}),
    ))
    .unwrap();
    events.extend(p.finish(Completion::Completed).unwrap());
    for event in &events {
        match event {
            Event::ActivitySnapshot(activity) => assert_eq!(activity.base.subagent_run_id, None),
            Event::ActivityDelta(activity) => {
                assert_eq!(activity.base.subagent_run_id, None);
                let metadata = serde_json::to_value(&activity.base.metadata).unwrap();
                assert_eq!(metadata["crabber"]["source"]["invocation"]["id"], "b");
            }
            _ => {}
        }
    }
    let history = serde_json::to_value(support::consume(&events).await.messages).unwrap();
    assert!(history.as_array().unwrap().iter().any(|message| message["id"] == "global-progress" && message["content"]["done"] == true));
}

#[test]
fn typed_raw_custom_hooks_are_explicit_and_legacy_custom_is_not_a_bridge() {
    let raw = || PublicPresentationChange::Raw {
        source: "declared".into(),
        event: json!({"nested":[null,true]}),
    };
    let (mut p, _) = start(ProjectionConfig::default());
    assert_eq!(
        p.push(&presentation(
            None,
            1,
            raw(),
            PublicStateContents::default()
        )),
        Err(ProjectionError::Malformed)
    );
    let mut config = ProjectionConfig::default();
    config.raw_sources.insert("declared".into());
    config.custom_events.insert("declared".into());
    let (mut p, _) = start(config);
    let events = p
        .push(&presentation(
            None,
            1,
            raw(),
            PublicStateContents::default(),
        ))
        .unwrap();
    assert!(
        matches!(events.as_slice(), [Event::Raw(raw)] if raw.event == json!({"nested":[null,true]}))
    );
    assert_eq!(
        p.push(&record(
            EventKind::Custom {
                name: "declared".into()
            },
            2,
            json!({"private":"not forwarded"})
        ))
        .unwrap(),
        [] as [Event; 0]
    );
    let events = p
        .push(&presentation(
            None,
            2,
            PublicPresentationChange::Custom {
                name: "declared".into(),
                value: json!({"retained":null}),
            },
            PublicStateContents::default(),
        ))
        .unwrap();
    assert!(
        matches!(events.as_slice(), [Event::Custom(custom)] if custom.value == json!({"retained":null}))
    );
}

#[tokio::test]
async fn completed_parent_can_close_while_detached_child_remains_native_live() {
    let (mut parent, mut events) = start(ProjectionConfig::default());
    events.extend(
        parent
            .push(&invocation("detached", 1, PublicInvocationStatus::Started))
            .unwrap(),
    );
    events.extend(
        parent
            .push(&presentation(
                Some("detached"),
                2,
                message("in-flight"),
                PublicStateContents::default(),
            ))
            .unwrap(),
    );
    events.extend(
        parent
            .push(&presentation(
                Some("detached"),
                3,
                delta("in-flight", "independent progress"),
                PublicStateContents::default(),
            ))
            .unwrap(),
    );
    parent
        .push(&record(
            EventKind::RunSettled,
            4,
            json!({"status":"completed"}),
        ))
        .unwrap();
    events.extend(parent.finish(Completion::Completed).unwrap());
    assert!(matches!(events.last(), Some(Event::RunFinished(_))));
    assert!(events.iter().any(|event| matches!(event, Event::SubagentError(error) if error.code.as_deref() == Some("crabber_projection_scope_closed"))));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::SubagentFinished(_)))
    );
    let messages = serde_json::to_value(support::consume(&events).await.messages).unwrap();
    assert!(messages.as_array().unwrap().iter().any(
        |message| message["id"] == "in-flight" && message["content"] == "independent progress"
    ));
}
