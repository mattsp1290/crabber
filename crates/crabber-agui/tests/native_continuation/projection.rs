//! Preserve every committed record; regenerate only persisted message presentation.
use crabber_agui::{
    Completion, ProjectionConfig, Projector,
    ag_ui_core::{
        event::Event,
        types::{Message as WireMessage, RunFinishedOutcome},
    },
    encode_sse,
};
use crabber_core::{
    ContentBlock, EventCursor, EventKind, EventRecord, Message, Role, SessionId, ToolCallId,
    continuation_source::{ResumeSource, WaitingSource},
};
use crabber_session::{MemoryStore, Store};
use serde_json::json;

pub(super) struct Exchange {
    pub(super) output: Vec<Event>,
    pub(super) waiting: WaitingSource,
    pub(super) cursor: EventCursor,
}

pub(super) async fn records(
    store: &MemoryStore,
    session: &SessionId,
    after: Option<EventCursor>,
) -> Vec<EventRecord> {
    let records = store.list_events(session, after, 1000).await.unwrap();
    assert_ne!(records, Vec::<EventRecord>::new());
    assert!(
        records.len() < 1000,
        "fixture page must contain the complete committed cut"
    );
    let mut previous = after;
    for record in &records {
        let actual = record
            .cursor
            .expect("every selected record is genuinely committed");
        assert!(previous.is_none_or(|previous| actual > previous));
        previous = Some(actual);
    }
    records
}

pub(super) async fn exchange(
    store: &MemoryStore,
    session: &SessionId,
    after: Option<EventCursor>,
    completion: Completion,
) -> Exchange {
    let records = records(store, session, after).await;
    let messages = store.list_all_messages(session).await.unwrap();
    let waiting: WaitingSource = serde_json::from_value(
        records
            .iter()
            .find(|record| record.kind == EventKind::ContinuationWaiting)
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    waiting.validate().unwrap();
    let output = project(&records, &messages, completion);
    terminal(&output);
    Exchange {
        output,
        waiting,
        cursor: records.last().unwrap().cursor.unwrap(),
    }
}

pub(super) fn project(
    records: &[EventRecord],
    messages: &[Message],
    completion: Completion,
) -> Vec<Event> {
    let first = &records[0];
    let mut projector = Projector::new(
        first.session_id.clone(),
        first.run_id.clone(),
        "thread".into(),
        "wire".into(),
        ProjectionConfig::default(),
    )
    .unwrap();
    let mut output = Vec::new();
    for record in records {
        if record.kind == EventKind::MessageCommitted {
            present(&mut projector, record, messages, &mut output);
        }
        // Every original record is pushed unchanged, exactly once, in its real
        // cursor order. A bad native record must fail this round trip.
        output.extend(projector.push(record).unwrap_or_else(|error| {
            panic!(
                "committed {:?} at {:?}: {error}",
                record.kind, record.cursor
            )
        }));
    }
    output.extend(projector.finish(completion).unwrap());
    output
}

fn present(
    projector: &mut Projector,
    record: &EventRecord,
    messages: &[Message],
    output: &mut Vec<Event>,
) {
    let message = messages
        .iter()
        .find(|message| record.payload["message_id"] == message.id.0)
        .unwrap();
    assert_eq!(message.role, Role::Assistant);
    let mut live = record.clone();
    live.live_only = true;
    live.kind = EventKind::MessageStarted;
    live.payload = json!({"message_id":message.id});
    output.extend(projector.push(&live).unwrap());
    for part in &message.parts {
        match &part.content {
            ContentBlock::Text { text } => {
                live.kind = EventKind::TextDelta;
                live.payload = json!({"message_id":message.id,"text":text});
                output.extend(projector.push(&live).unwrap());
            }
            ContentBlock::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                for (kind, payload) in [
                    (
                        EventKind::ToolCallStarted,
                        json!({"message_id":message.id,"call_id":call_id,"name":name}),
                    ),
                    (
                        EventKind::ToolCallArgsDelta,
                        json!({"message_id":message.id,"call_id":call_id,"text":arguments.to_string()}),
                    ),
                    (
                        EventKind::ToolCallArgsCompleted,
                        json!({"message_id":message.id,"call_id":call_id}),
                    ),
                ] {
                    live.kind = kind;
                    live.payload = payload;
                    output.extend(projector.push(&live).unwrap());
                }
            }
            _ => panic!("the real fixture declares only text and tool-call content"),
        }
    }
    live.kind = EventKind::MessageStreamEnded;
    live.payload = json!({"message_id":message.id,"outcome":"completed"});
    output.extend(projector.push(&live).unwrap());
}

pub(super) fn terminal(output: &[Event]) {
    assert_eq!(
        output
            .iter()
            .filter(|event| matches!(event, Event::RunStarted(_)))
            .count(),
        1
    );
    assert_eq!(
        output
            .iter()
            .filter(|event| matches!(event, Event::RunFinished(_) | Event::RunError(_)))
            .count(),
        1
    );
    assert!(matches!(output.last(), Some(Event::RunFinished(_))));
}

pub(super) async fn continued(
    store: &MemoryStore,
    session: &SessionId,
    first: Exchange,
    messages: Vec<WireMessage>,
    call: &ToolCallId,
    private: &str,
) {
    let records = records(store, session, Some(first.cursor)).await;
    assert!(
        !records
            .iter()
            .any(|record| record.kind == EventKind::RunStarted)
    );
    let resume: ResumeSource = serde_json::from_value(
        records
            .iter()
            .find(|record| record.kind == EventKind::RunResumed)
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    resume.validate().unwrap();
    assert_eq!(resume.continuation, Some(first.waiting.continuation));
    assert_eq!(resume.current_turn, first.waiting.current_turn);
    assert_eq!(resume.calls.len(), 1);
    assert_eq!(&resume.calls[0].call_id, call);
    let history = store.list_all_messages(session).await.unwrap();
    let parent = history
        .iter()
        .find(|message| message.id == resume.calls[0].message_id)
        .unwrap();
    assert!(parent.parts.iter().any(|part| matches!(&part.content,
        ContentBlock::ToolCall { call_id, name, arguments } if call_id == call
            && name == &resume.calls[0].name && arguments == &resume.calls[0].arguments)));
    let settled = records
        .iter()
        .find(|record| record.kind == EventKind::ToolCallSettled)
        .unwrap();
    assert_eq!(settled.payload["call_id"], json!(call));
    let output = project(&records, &history, Completion::Completed);
    terminal(&output);
    for event in first.output.iter().chain(&output) {
        let frame = encode_sse(event, 1_048_576).unwrap();
        assert!(
            !frame
                .windows(private.len())
                .any(|window| window == private.as_bytes())
        );
    }
    let result = super::support::consume_with_messages(&output, messages).await;
    assert!(matches!(
        result.outcome,
        Some(RunFinishedOutcome::Success { .. })
    ));
    let public = serde_json::to_value(result.messages).unwrap();
    assert!(!public.to_string().contains(private));
    let items = public.as_array().unwrap();
    assert_eq!(
        items
            .iter()
            .filter(|message| message["id"] == json!(parent.id))
            .count(),
        1
    );
    let result = items
        .iter()
        .find(|message| message["id"] == settled.payload["message_id"])
        .unwrap();
    assert_eq!(result["toolCallId"], json!(call));
    let blocks: Vec<ContentBlock> =
        serde_json::from_value(settled.payload["content"].clone()).unwrap();
    let parts = blocks
        .into_iter()
        .map(|block| {
            let ContentBlock::Text { text } = block else {
                panic!("native fixture returned non-text content")
            };
            crabber_agui::ag_ui_core::types::ContentPart::Text {
                text,
                id: None,
                metadata: None,
            }
        })
        .collect();
    // The actual SDK result retains typed parts, not a stringified wrapper.
    assert_eq!(
        result["content"],
        serde_json::to_value(crabber_agui::ag_ui_core::types::MessageContent::Parts(
            parts
        ))
        .unwrap()
    );
}
