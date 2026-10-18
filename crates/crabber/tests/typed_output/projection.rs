use crabber::{
    RunHandle,
    core::{RunStatus, ToolCallId},
};
use crabber_agui::{
    Completion, ProjectionConfig, Projector,
    ag_ui_core::{
        event::Event,
        types::{ContentPart, MessageContent, PartSource},
    },
};
use serde_json::json;

/// Drain the actual public Agent stream, then replay the complete durable
/// stream with presentation reconstructed from committed rows. Neither path
/// may hide a projection failure or reorder stored native/control records.
pub(super) async fn run(
    mut handle: RunHandle,
    store: &dyn crabber::session::Store,
    call: &ToolCallId,
    is_error: bool,
) {
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    let mut projector = Projector::new(
        handle.session_id().clone(),
        handle.run_id().clone(),
        "typed-output-thread".into(),
        "typed-output-exchange".into(),
        ProjectionConfig::default(),
    )
    .unwrap();
    let mut receiver = handle.events();
    let mut wire = Vec::new();
    let mut source_starts = 0;
    while let Some(record) = receiver.recv().await.unwrap() {
        source_starts += usize::from(record.kind == crabber::core::EventKind::RunStarted);
        wire.extend(projector.push(&record).unwrap_or_else(|error| {
            panic!(
                "actual source {:?} failed projection: {error:?}",
                record.kind
            )
        }));
    }
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    wire.extend(projector.finish(Completion::Completed).unwrap());
    assert_eq!(source_starts, 1);
    assert_wire(&wire, call, is_error);
    let history = store.list_all_messages(&session).await.unwrap();
    let records = store.list_events(&session, None, 1000).await.unwrap();
    assert!(
        records.len() < 1000,
        "complete fixture event stream must fit"
    );
    assert!(
        records
            .iter()
            .any(|record| record.kind == crabber::core::EventKind::ExecutionAttemptChanged)
    );
    assert!(
        records
            .iter()
            .any(|record| record.kind == crabber::core::EventKind::ToolPhaseChanged)
    );
    let mut persisted = Projector::new(
        session,
        run,
        "persisted-thread".into(),
        "persisted-exchange".into(),
        ProjectionConfig::default(),
    )
    .unwrap();
    let mut output = Vec::new();
    for record in records {
        if record.kind == crabber::core::EventKind::MessageCommitted {
            let id: crabber::core::MessageId =
                serde_json::from_value(record.payload["message_id"].clone()).unwrap();
            let message = history.iter().find(|message| message.id == id).unwrap();
            assert_eq!(message.role, crabber::core::Role::Assistant);
            present(&mut persisted, &mut output, &record, message);
        }
        output.extend(persisted.push(&record).unwrap_or_else(|error| {
            panic!("stored {:?} failed projection: {error:?}", record.kind)
        }));
    }
    output.extend(persisted.finish(Completion::Completed).unwrap());
    assert_wire(&output, call, is_error);
}

fn present(
    projector: &mut Projector,
    output: &mut Vec<Event>,
    record: &crabber::core::EventRecord,
    message: &crabber::core::Message,
) {
    use crabber::core::{ContentBlock, EventKind};
    let mut presentation = record.clone();
    presentation.cursor = None;
    presentation.live_only = true;
    presentation.kind = EventKind::MessageStarted;
    presentation.payload = json!({"message_id": message.id});
    output.extend(projector.push(&presentation).unwrap());
    for part in &message.parts {
        match &part.content {
            ContentBlock::Text { text } => {
                presentation.kind = EventKind::TextDelta;
                presentation.payload = json!({"message_id": message.id, "text": text});
                output.extend(projector.push(&presentation).unwrap());
            }
            ContentBlock::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                for (kind, payload) in [
                    (
                        EventKind::ToolCallStarted,
                        json!({"message_id":message.id, "call_id":call_id, "name":name}),
                    ),
                    (
                        EventKind::ToolCallArgsDelta,
                        json!({"message_id":message.id, "call_id":call_id, "text":arguments.to_string()}),
                    ),
                    (
                        EventKind::ToolCallArgsCompleted,
                        json!({"message_id":message.id, "call_id":call_id}),
                    ),
                ] {
                    presentation.kind = kind;
                    presentation.payload = payload;
                    output.extend(projector.push(&presentation).unwrap());
                }
            }
            _ => panic!("mixed-output fixture assistant contains unexpected presentation content"),
        }
    }
    presentation.kind = EventKind::MessageStreamEnded;
    presentation.payload = json!({"message_id": message.id, "outcome":"completed"});
    output.extend(projector.push(&presentation).unwrap());
}

fn assert_wire(wire: &[Event], call: &ToolCallId, is_error: bool) {
    assert_eq!(
        wire.iter()
            .filter(|event| matches!(event, Event::RunStarted(_)))
            .count(),
        1
    );
    assert_eq!(
        wire.iter()
            .filter(|event| matches!(event, Event::RunFinished(_)))
            .count(),
        1
    );
    assert!(!wire.iter().any(|event| matches!(event, Event::RunError(_))));
    let results: Vec<_> = wire
        .iter()
        .filter_map(|event| match event {
            Event::ToolCallResult(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    let result = results[0];
    assert_eq!(result.tool_call_id.as_str(), call.0);
    let MessageContent::Parts(parts) = &result.content else {
        panic!("actual mixed tool output must use protocol content parts");
    };
    assert_eq!(parts.len(), 3);
    assert!(matches!(&parts[0], ContentPart::Text { text, .. } if text == "public caption"));
    assert!(matches!(&parts[1], ContentPart::Image {
        source: PartSource::File { value, mime_type: Some(mime_type), provider: None }, ..
    } if value == "opaque-image" && mime_type == "image/png"));
    assert!(matches!(&parts[2], ContentPart::Text { text, .. } if text == "trailing text"));
    assert_eq!(
        result.base.metadata.as_ref().unwrap()["crabber"]["isError"],
        json!(is_error)
    );
    for event in wire {
        let frame = crabber_agui::encode_sse(event, 1 << 20).unwrap();
        assert!(frame.starts_with(b"data: ") && frame.ends_with(b"\n\n"));
        assert!(!frame[6..frame.len() - 2].contains(&b'\n'));
        let json = &frame[6..frame.len() - 2];
        let decoded: Event = serde_json::from_slice(json).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            crabber_agui::ag_ui_core::processing::normalize_producer_event(event).unwrap()
        );
        assert!(
            !String::from_utf8(frame)
                .unwrap()
                .contains("PRIVATE_CAPTION")
        );
    }
}
