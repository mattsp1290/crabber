use crabber_agui::{
    Completion, ProjectionConfig, ProjectionError, Projector, ag_ui_core::event::Event, encode_sse,
};
use crabber_core::{
    AttemptGeneration, AttemptId, EventKind, EventRecord, PublicActivity, PublicAttemptIdentity,
    PublicPatchOperation, PublicSourceChange, PublicSourceEvent, PublicSourceIdentity,
    PublicStateContents, RunId, SessionId, SessionRevision,
};
use serde_json::{Value, json};

fn scope() -> PublicSourceIdentity {
    PublicSourceIdentity {
        workspace_id: "workspace".into(),
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        attempt: PublicAttemptIdentity {
            id: AttemptId::from("attempt"),
            generation: AttemptGeneration::INITIAL,
        },
        call_id: None,
        invocation: None,
    }
}

fn projector() -> Projector {
    Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        ProjectionConfig::default(),
    )
    .unwrap()
    .bind_source(scope())
    .unwrap()
}

fn record(
    kind: EventKind,
    revision: u64,
    change: PublicSourceChange,
    contents: PublicStateContents,
) -> EventRecord {
    EventRecord {
        cursor: Some(crabber_core::EventCursor(revision)),
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        turn_id: None,
        kind,
        payload: serde_json::to_value(PublicSourceEvent {
            version: 2,
            source: scope(),
            revision: SessionRevision::new(revision).unwrap(),
            change,
            contents,
            before: None,
            before_event_cursor: None,
        })
        .unwrap(),
        correlation: None,
        live_only: false,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn wire(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .map(|event| {
            let frame = encode_sse(event, 1_048_576).unwrap();
            serde_json::from_slice(&frame[6..frame.len() - 2]).unwrap()
        })
        .collect()
}

fn snapshot(revision: u64, state: Value) -> EventRecord {
    record(
        EventKind::PublicStateSnapshot,
        revision,
        PublicSourceChange::StateSnapshot {
            state: state.clone(),
        },
        PublicStateContents::new(state, vec![]).unwrap(),
    )
}

#[test]
fn arbitrary_state_including_whole_null_is_not_normalized_to_an_object() {
    let mut p = projector();
    for (revision, state) in [
        (1, json!({"nested":[null,true]})),
        (2, Value::Null),
        (3, json!([null, "é😀", 17])),
    ] {
        let events = wire(&p.push(&snapshot(revision, state.clone())).unwrap());
        let state_event = events
            .iter()
            .find(|event| event["type"] == "STATE_SNAPSHOT")
            .unwrap();
        assert_eq!(state_event["snapshot"], state);
    }
}

#[test]
fn actual_patch_objects_preserve_legal_extension_members_through_the_encoder() {
    for operation in [
        json!({"op":"remove","path":"/old","value":{"legal":[null,"é😀"]}}),
        json!({"op":"add","path":"/new","value":null,"from":"/extension-only"}),
    ] {
        let mut p = projector();
        let initial = PublicStateContents::new(json!({"old":17}), vec![]).unwrap();
        p.push(&snapshot(1, initial.state().clone())).unwrap();
        let change = PublicSourceChange::StateDelta {
            delta: vec![PublicPatchOperation::new(operation.clone()).unwrap()],
        };
        let result = change.apply(&initial).unwrap();
        let mut delta = record(EventKind::PublicStateDelta, 2, change, result);
        delta.payload["before"] = serde_json::to_value(initial).unwrap();
        delta.payload["before_event_cursor"] = json!(1);
        let events = wire(&p.push(&delta).unwrap());
        assert_eq!(events[0]["delta"], json!([operation]));
    }
}

#[test]
fn activity_retyping_is_explicit_and_delta_keeps_exact_patch_members() {
    let mut p = projector();
    let mut contents = PublicStateContents::default();
    for (revision, activity_type) in [(1, "loading"), (2, "completed")] {
        let activity = PublicActivity::new("activity", activity_type, json!({"old":null})).unwrap();
        let change = PublicSourceChange::ActivitySnapshot { activity };
        contents = change.apply(&contents).unwrap();
        let projected = wire(
            &p.push(&record(
                EventKind::ActivitySnapshot,
                revision,
                change,
                contents.clone(),
            ))
            .unwrap(),
        );
        let event = projected.last().unwrap();
        assert_eq!(event["activityType"], activity_type);
        assert_eq!(event["replace"], true);
    }
    let before = contents.clone();
    let mut prestate = record(
        EventKind::PublicStateSnapshot,
        2,
        PublicSourceChange::StateSnapshot {
            state: before.state().clone(),
        },
        before.clone(),
    );
    prestate.cursor = Some(crabber_core::EventCursor(3));
    p.push(&prestate).unwrap();
    let operation = json!({"op":"remove","path":"/old","value":[null,"extension"]});
    let change = PublicSourceChange::ActivityDelta {
        activity_id: "activity".into(),
        activity_type: "completed".into(),
        delta: vec![PublicPatchOperation::new(operation.clone()).unwrap()],
    };
    contents = change.apply(&contents).unwrap();
    let mut delta = record(EventKind::ActivityDelta, 3, change, contents.clone());
    delta.cursor = Some(crabber_core::EventCursor(4));
    delta.payload["before"] = serde_json::to_value(before).unwrap();
    delta.payload["before_event_cursor"] = json!(3);
    let projected = wire(&p.push(&delta).unwrap());
    assert_eq!(projected[0]["patch"], json!([operation]));
    let mut prestate = record(
        EventKind::PublicStateSnapshot,
        3,
        PublicSourceChange::StateSnapshot {
            state: contents.state().clone(),
        },
        contents.clone(),
    );
    prestate.cursor = Some(crabber_core::EventCursor(5));
    p.push(&prestate).unwrap();
    let invalid = PublicSourceChange::ActivityDelta {
        activity_id: "activity".into(),
        activity_type: "loading".into(),
        delta: vec![],
    };
    let mut invalid = record(EventKind::ActivityDelta, 4, invalid, contents.clone());
    invalid.cursor = Some(crabber_core::EventCursor(6));
    invalid.payload["before"] = serde_json::to_value(contents).unwrap();
    invalid.payload["before_event_cursor"] = json!(5);
    assert_eq!(p.push(&invalid), Err(ProjectionError::Malformed));
}

#[test]
fn source_scope_revision_and_initial_snapshot_are_required() {
    let mut foreign = snapshot(1, json!({}));
    foreign.payload["source"]["attempt"]["generation"] = json!(2);
    assert_eq!(projector().push(&foreign), Err(ProjectionError::Identity));
    let mut p = projector();
    p.push(&snapshot(1, Value::Null)).unwrap();
    assert_eq!(
        p.push(&snapshot(1, Value::Null)),
        Err(ProjectionError::Order)
    );
    let mut delta = record(
        EventKind::PublicStateDelta,
        1,
        PublicSourceChange::StateDelta { delta: vec![] },
        PublicStateContents::default(),
    );
    delta.payload["before"] = serde_json::to_value(PublicStateContents::default()).unwrap();
    delta.payload["before_event_cursor"] = json!(1);
    assert_eq!(projector().push(&delta), Err(ProjectionError::Order));
    let mut p = projector();
    assert_eq!(
        p.push_with_delivery(&snapshot(1, Value::Null), |events| {
            assert_eq!(events.len(), 2);
            Err(ProjectionError::Transport)
        }),
        Err(ProjectionError::Transport)
    );
    let terminal = wire(&p.finish(Completion::Failed).unwrap());
    assert_eq!(
        terminal
            .iter()
            .map(|event| event["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["RUN_STARTED", "RUN_ERROR"]
    );
}
