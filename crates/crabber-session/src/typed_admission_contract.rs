//! Qualification for typed original input, generated-ID replay and revision guards.
use crate::{BoundKeyedAdmitRequest, KeyedAdmitOutcome, Store, StoreError};
use crabber_core::{
    AdmissionKey, AdmissionKind, Clock, ContentBlock, EventKind, EventRecord, InputContent,
    InputContext, InputEnvelope, InputMessage, InputPart, InputState, ManualClock, MediaKind,
    MediaSource, MessageContent, Role, RunStatus, SessionId, SessionRevision, Usage,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

mod fixture;
pub(crate) use fixture::fixture;
use fixture::{envelope, prepare};

/// Validate full typed binding/capsule retention, exact replay after revision
/// changes, uncertainty-safe Unstarted claim, and generated-ID independence.
/// # Panics
/// Panics if the adapter violates the typed admission contract.
#[allow(clippy::too_many_lines)]
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    let session = SessionId::new();
    let input = envelope(&session);
    let original = fixture(&session, clock.now(), "typed-backend");
    let mut forged = original.keyed.request.clone();
    let foreign = SessionId::new();
    forged.session_id = None;
    forged.user_message.session_id.clone_from(&foreign);
    for message in &mut forged.additional_messages {
        message.session_id.clone_from(&foreign);
    }
    assert_eq!(
        original.binding.validate(&forged).unwrap_err(),
        StoreError::AdmissionConflict
    );
    let expected_digest = original.keyed.validated_digest().unwrap();
    let KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_bound_keyed_run(original.clone()).await.unwrap()
    else {
        panic!("fresh typed input must admit")
    };
    assert_eq!(receipt.semantic_digest_version, 2);
    assert_eq!(receipt.semantic_digest, expected_digest);
    let retained = store
        .execution_binding(&session, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.capsule, original.binding);
    assert_eq!(retained.capsule.input_envelope, Some(input.clone()));
    assert_eq!(
        store
            .execution_binding(&SessionId::new(), &receipt.run_id)
            .await
            .unwrap(),
        None
    );
    let messages = store.list_all_messages(&session).await.unwrap();
    input
        .validate_message_projection(&session, Some(&receipt.run_id), &messages)
        .unwrap();
    assert_eq!(store.list_messages(&session, None).await.unwrap(), messages);
    assert!(
        matches!(&messages[0].parts[1].content, ContentBlock::MediaContent { source: MediaSource::File { file_id }, .. } if file_id == "opaque-artifact")
    );
    // Private context is absent from message rows; its captured fingerprint
    // must detect corruption independently of message projection validation.
    let mut corrupt = retained.clone();
    let mut wire = serde_json::to_value(corrupt.capsule.input_envelope.as_ref().unwrap()).unwrap();
    wire["context"][0]["value"] = json!({"corrupt":true});
    corrupt.capsule.input_envelope = Some(serde_json::from_value(wire).unwrap());
    assert_eq!(
        corrupt
            .validate_messages(
                &admitted.run.id,
                &session,
                &admitted.run.config_hash,
                &admitted.run.plan_fingerprint,
                &messages
            )
            .unwrap_err(),
        StoreError::AdmissionConflict
    );
    let revision = store.session_revision(&session).await.unwrap();
    assert!(revision > SessionRevision::INITIAL);
    let retry = prepare(original.clone(), input.clone(), clock.now());
    assert_ne!(
        retry.keyed.request.additional_messages[0].id,
        original.keyed.request.additional_messages[0].id
    );
    assert_ne!(
        retry.keyed.request.user_message.parts[1].id,
        original.keyed.request.user_message.parts[1].id
    );
    assert_eq!(retry.keyed.validated_digest().unwrap(), expected_digest);
    assert!(
        matches!(store.admit_bound_keyed_run(retry.clone()).await.unwrap(), KeyedAdmitOutcome::Replayed(actual) if actual == receipt)
    );
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
    for field in 0..5 {
        let mut wire = serde_json::to_value(&input).unwrap();
        match field {
            0 => wire["messages"][0]["id"] = json!("different-supplied-id"),
            1 => wire["context"][0]["value"] = json!({"changed":true}),
            2 => wire["state"] = json!({"kind":"absent"}),
            3 => wire["expected_revision"] = json!(revision.get()),
            4 => wire["messages"][0]["parts"][0]["id"] = json!("different-supplied-part"),
            _ => unreachable!(),
        }
        let changed = prepare(
            original.clone(),
            serde_json::from_value(wire).unwrap(),
            clock.now(),
        );
        assert_ne!(changed.keyed.validated_digest().unwrap(), expected_digest);
        assert_eq!(
            store.admit_bound_keyed_run(changed).await.unwrap_err(),
            StoreError::AdmissionConflict
        );
    }
    let mut stale = retry.clone();
    stale.keyed.options.key = AdmissionKey::new("another-key").unwrap();
    assert_eq!(
        store.admit_bound_keyed_run(stale).await.unwrap_err(),
        StoreError::AdmissionRevisionConflict
    );
    let record = store
        .load_admission_execution(&session, &original.keyed.options.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.capsule.input_envelope, Some(input));
    record.validate_messages(&admitted.run, &messages).unwrap();
    clock.set(admitted.run.lease_until + time::Duration::seconds(1));
    let claimed = store
        .claim_unstarted_admission(crate::ClaimUnstartedAdmissionRequest {
            session_id: session.clone(),
            key: original.keyed.options.key.clone(),
            expected_fence: admitted.fence.clone(),
            expected_owner: admitted.run.owner.clone(),
            fingerprint: original.keyed.options.fingerprint.clone(),
            behavior_fingerprint: original.keyed.options.behavior_fingerprint.clone(),
            semantic_digest: expected_digest.clone(),
            capsule_digest: record.capsule.digest().unwrap(),
            owner: "fresh-typed-owner".into(),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap();
    let execution = store.execution(claimed.fence).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            EventRecord {
                cursor: None,
                session_id: session.clone(),
                run_id: receipt.run_id.clone(),
                turn_id: None,
                kind: EventKind::RunSettled,
                payload: json!({}),
                correlation: None,
                live_only: false,
                created_at: clock.now(),
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(store.admit_bound_keyed_run(retry).await.unwrap(), KeyedAdmitOutcome::Replayed(actual) if actual == receipt)
    );
    let terminal = store
        .load_admission_execution(&session, &original.keyed.options.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.state, crate::AdmissionExecutionState::Started);
    assert_eq!(terminal.capsule, record.capsule);
    assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
}
