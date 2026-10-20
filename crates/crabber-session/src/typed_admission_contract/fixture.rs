//! Retained-admission inputs shared by private fault and transition fixtures.
use super::{
    AdmissionKind, BoundKeyedAdmitRequest, InputContent, InputContext, InputEnvelope, InputMessage,
    InputPart, InputState, MediaKind, MediaSource, MessageContent, Role, SessionId,
    SessionRevision,
};
use serde_json::json;
use time::OffsetDateTime;
pub(super) fn envelope(session: &SessionId) -> InputEnvelope {
    InputEnvelope::new(
        vec![
            InputMessage {
                id: Some(format!("typed-user-{}", session.0)),
                parent_id: None,
                role: Role::User,
                parts: vec![
                    InputPart {
                        id: Some(format!("typed-text-{}", session.0)),
                        content: InputContent::Content {
                            content: MessageContent::Text {
                                text: "PRIVATE_CAPTION".into(),
                            },
                        },
                    },
                    InputPart {
                        id: None,
                        content: InputContent::Content {
                            content: MessageContent::Media {
                                kind: MediaKind::Image,
                                mime_type: "image/png".into(),
                                source: MediaSource::File {
                                    file_id: "opaque-artifact".into(),
                                },
                            },
                        },
                    },
                ],
            },
            InputMessage {
                id: None,
                parent_id: None,
                role: Role::Assistant,
                parts: vec![InputPart {
                    id: None,
                    content: InputContent::Content {
                        content: MessageContent::Text {
                            text: "host-validated-history".into(),
                        },
                    },
                }],
            },
        ],
        vec![],
        vec![InputContext {
            description: "context".into(),
            value: json!({"lossless":"\u{0}", "nested":[null,1]}),
        }],
        InputState::Present {
            value: json!({"lossless":"\u{0}"}),
        },
        SessionRevision::INITIAL,
        AdmissionKind::Ordinary,
    )
    .unwrap()
}
pub(crate) fn prepare(
    mut fixture: BoundKeyedAdmitRequest,
    input: InputEnvelope,
    now: OffsetDateTime,
) -> BoundKeyedAdmitRequest {
    let session = fixture.binding.request.session_id.clone();
    let mut records = input.materialize_messages(&session, now).unwrap();
    fixture.keyed.request.user_message = records.remove(0);
    fixture.keyed.request.additional_messages = records;
    fixture.keyed.request.expected_revision = Some(input.expected_revision());
    fixture.keyed.request.input_envelope = Some(input.clone());
    fixture.binding.version = 3;
    fixture.binding.input_envelope = Some(input.clone());
    fixture.binding.request.text.clear();
    fixture.binding.input_fingerprint = fixture.binding.computed_input_fingerprint().unwrap();
    let original = fixture.keyed.execution.as_mut().unwrap();
    original.version = 2;
    original.input_envelope = Some(input);
    original.request.text.clear();
    let digest = fixture.keyed.semantic_digest().unwrap();
    fixture.keyed.execution.as_mut().unwrap().semantic_digest = digest;
    fixture
}

pub(crate) fn fixture(
    session: &SessionId,
    now: OffsetDateTime,
    backend: &str,
) -> BoundKeyedAdmitRequest {
    prepare(
        crate::execution_binding_contract::fixture(session, now, backend, 'b'),
        envelope(session),
        now,
    )
}
