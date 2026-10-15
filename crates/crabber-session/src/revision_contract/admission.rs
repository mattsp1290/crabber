use super::{
    AdmissionKey, AdmissionOptions, AdmitRequest, Clock, EventKind, InputFingerprint,
    KeyedAdmitOutcome, KeyedAdmitRequest, ManualClock, Role, RunStatus, SessionId, SessionRevision,
    Store, StoreError, Usage, event, message, request,
};
use crabber_core::{
    AdmissionKind, InputContent, InputEnvelope, InputMessage, InputPart, InputState, MediaKind,
    MediaSource, MessageContent,
};

/// A stale host comparison cannot admit, even while a previous run is busy.
/// Exact receipt reconciliation must run before this admission-only precondition.
pub(super) async fn conditional_admission(store: &impl Store, clock: &ManualClock) {
    let mut first = request(clock.now());
    first.expected_revision = Some(SessionRevision::INITIAL);
    let admitted = store.admit_run(first.clone()).await.unwrap();
    let session = &admitted.session.id;
    let original_revision = store.session_revision(session).await.unwrap();
    let mut stale = request(clock.now());
    stale.session_id = Some(session.clone());
    stale.user_message = message(session, None, clock.now());
    stale.expected_revision = Some(SessionRevision::INITIAL);
    let before_messages = store.list_all_messages(session).await.unwrap();
    let before_run = store.get_run(&admitted.run.id).await.unwrap();
    assert_eq!(
        store.admit_run(stale.clone()).await.unwrap_err(),
        StoreError::AdmissionRevisionConflict
    );
    assert_eq!(
        store.list_all_messages(session).await.unwrap(),
        before_messages
    );
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), before_run);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        original_revision
    );
    settle(store, clock, &admitted).await;
    let snapshot_revision = store.session_revision(session).await.unwrap();
    let keyed = KeyedAdmitRequest {
        request: AdmitRequest {
            expected_revision: Some(snapshot_revision),
            ..stale.clone()
        },
        options: AdmissionOptions {
            key: AdmissionKey::new("revision-guard").unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
        },
        execution: None,
    };
    let KeyedAdmitOutcome::Started {
        receipt,
        admitted: next,
    } = store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!("fresh guarded admission must start")
    };
    assert!(store.session_revision(session).await.unwrap() > snapshot_revision);
    let replay = store.admit_keyed_run(keyed.clone()).await.unwrap();
    assert!(matches!(replay, KeyedAdmitOutcome::Replayed(actual) if actual == receipt));
    let mut conflict = keyed.clone();
    conflict.request.title = "changed input".into();
    assert_eq!(
        store.admit_keyed_run(conflict).await.unwrap_err(),
        StoreError::AdmissionConflict
    );
    let mut fresh_key = keyed;
    fresh_key.options.key = AdmissionKey::new("stale-new-key").unwrap();
    assert_eq!(
        store.admit_keyed_run(fresh_key).await.unwrap_err(),
        StoreError::AdmissionRevisionConflict
    );
    let before_revision = store.session_revision(session).await.unwrap();
    settle(store, clock, &next).await;
    assert!(store.session_revision(session).await.unwrap() > before_revision);
    assert_eq!(
        store.admit_run(stale.clone()).await.unwrap_err(),
        StoreError::AdmissionRevisionConflict
    );
    stale.expected_revision = Some(store.session_revision(session).await.unwrap());
    stale.user_message = message(session, None, clock.now());
    let accepted = store.admit_run(stale).await.unwrap();
    settle(store, clock, &accepted).await;
    concurrent_admission(store, clock, session).await;
    // New-session revision is zero, and an invalid precondition creates no session.
    first.user_message = message(&SessionId::new(), None, clock.now());
    first.expected_revision = Some(SessionRevision::INITIAL.next().unwrap());
    let missing = first.user_message.session_id.clone();
    assert_eq!(
        store.admit_run(first).await.unwrap_err(),
        StoreError::AdmissionRevisionConflict
    );
    assert_eq!(store.get_session(&missing).await.unwrap(), None);
}

pub(super) async fn message_batch(store: &impl Store, clock: &ManualClock) {
    let envelope = batch_envelope();
    let session = SessionId::new();
    let mut messages = envelope
        .materialize_messages(&session, clock.now())
        .unwrap();
    let mut original = request(clock.now());
    original.expected_revision = Some(SessionRevision::INITIAL);
    original.user_message = messages.remove(0);
    original.additional_messages = messages;
    let admitted = store.admit_run(original.clone()).await.unwrap();
    let persisted = store.list_all_messages(&session).await.unwrap();
    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].id, original.user_message.id);
    assert_eq!(persisted[1].id, original.additional_messages[0].id);
    assert_eq!(persisted[0].parts, original.user_message.parts);
    assert_eq!(persisted[1].role, Role::Assistant);
    assert_eq!(persisted[1].parts, original.additional_messages[0].parts);
    assert!(
        persisted
            .iter()
            .all(|record| record.run_id.as_ref() == Some(&admitted.run.id))
    );
    store
        .execution(admitted.fence)
        .await
        .unwrap()
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted.run, EventKind::RunSettled, clock.now()),
        )
        .await
        .unwrap();
    let revision = store.session_revision(&session).await.unwrap();
    let pending_before = store.list_unfinished_runs().await.unwrap();
    for duplicate_part in [false, true] {
        let mut messages = envelope
            .materialize_messages(&session, clock.now())
            .unwrap();
        if duplicate_part {
            messages[1].parts[0].id = persisted[1].parts[0].id.clone();
        } else {
            messages[1].id = persisted[1].id.clone();
            messages[1].parts[0].message_id = persisted[1].id.clone();
        }
        let mut next = original.clone();
        next.session_id = Some(session.clone());
        next.expected_revision = Some(revision);
        next.user_message = messages.remove(0);
        next.additional_messages = messages;
        assert!(store.admit_run(next).await.is_err());
        assert_eq!(store.list_all_messages(&session).await.unwrap(), persisted);
        assert_eq!(store.session_revision(&session).await.unwrap(), revision);
        assert_eq!(store.list_unfinished_runs().await.unwrap(), pending_before);
    }
    let mut invalid = original;
    invalid.session_id = Some(session.clone());
    invalid.expected_revision = Some(revision);
    invalid.additional_messages[0].session_id = SessionId::new();
    assert!(store.admit_run(invalid).await.is_err());
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(store.list_all_messages(&session).await.unwrap(), persisted);
}

async fn concurrent_admission(store: &impl Store, clock: &ManualClock, session: &SessionId) {
    let snapshot_revision = store.session_revision(session).await.unwrap();
    let make_racer = || {
        let mut candidate = request(clock.now());
        candidate.session_id = Some(session.clone());
        candidate.user_message = message(session, None, clock.now());
        candidate.expected_revision = Some(snapshot_revision);
        candidate
    };
    let before = store.list_all_messages(session).await.unwrap().len();
    let (left, right) =
        futures::join!(store.admit_run(make_racer()), store.admit_run(make_racer()));
    assert_eq!(
        [&left, &right]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1
    );
    assert_eq!(
        [&left, &right]
            .iter()
            .filter(|result| matches!(result, Err(StoreError::AdmissionRevisionConflict)))
            .count(),
        1
    );
    assert_eq!(
        store.list_all_messages(session).await.unwrap().len(),
        before + 1
    );
}

fn batch_envelope() -> InputEnvelope {
    InputEnvelope::new(
        vec![
            InputMessage {
                id: None,
                parent_id: None,
                role: Role::User,
                parts: vec![
                    InputPart {
                        id: None,
                        content: InputContent::Content {
                            content: MessageContent::Text {
                                text: "caption".into(),
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
        vec![],
        InputState::Absent,
        SessionRevision::INITIAL,
        AdmissionKind::Ordinary,
    )
    .unwrap()
}

async fn settle(store: &impl Store, clock: &ManualClock, admitted: &crate::AdmitOutcome) {
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted.run, EventKind::RunSettled, clock.now()),
        )
        .await
        .unwrap();
}
