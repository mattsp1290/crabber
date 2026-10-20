//! Reusable public-revision contract for custom Store implementers.
//! Unstable until durable-host qualification.
mod admission;

use crate::{AdmitOutcome, AdmitRequest, KeyedAdmitOutcome, KeyedAdmitRequest, Store, StoreError};
use crabber_core::{
    AdmissionKey, AdmissionOptions, Clock, ContentBlock, EventKind, EventRecord, InputFingerprint,
    ManualClock, Message, MessageId, Part, PartId, PartKind, Role, Run, RunStatus, SessionId,
    SessionRevision, Usage,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

/// Verify public changes, rollback, private ownership maintenance, isolation and
/// exact replay without depending on a backend's numeric revision increments.
/// # Panics
/// Panics when an adapter violates the contract or cannot perform fixture writes.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    assert_eq!(
        store.session_revision(&SessionId::new()).await.unwrap_err(),
        StoreError::NotFound
    );
    public_writes(&store, &clock).await;
    tool_records(&store, &clock).await;
    context_epochs_and_abandonment(&store, &clock).await;
    ownership_and_terminal(&store, &clock).await;
    receipt_replay(&store, &clock).await;
    admission::conditional_admission(&store, &clock).await;
    admission::message_batch(&store, &clock).await;
    admission::raw_message_part_ids(&store, &clock).await;
}
fn message(session: &SessionId, run: Option<&Run>, now: OffsetDateTime) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: run.map(|run| run.id.clone()),
        role: Role::User,
        parent_id: None,
        created_at: now,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text {
                text: "fixture".into(),
            },
        }],
    }
}
fn request(now: OffsetDateTime) -> AdmitRequest {
    let session = SessionId::new();
    AdmitRequest {
        input_envelope: None,
        additional_messages: Vec::new(),
        expected_revision: None,
        session_id: None,
        workspace_id: "workspace".into(),
        directory: "/workspace".into(),
        title: "revision fixture".into(),
        user_message: message(&session, None, now),
        config_hash: "config".into(),
        plan_fingerprint: "plan".into(),
        owner: "fixture-owner".into(),
        lease: Duration::from_secs(30),
    }
}
fn event(run: &Run, kind: EventKind, now: OffsetDateTime) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: json!({}),
        correlation: None,
        live_only: false,
        created_at: now,
    }
}
async fn advanced(
    store: &impl Store,
    admitted: &AdmitOutcome,
    before: SessionRevision,
) -> SessionRevision {
    let after = store.session_revision(&admitted.session.id).await.unwrap();
    assert!(
        after > before,
        "committed public mutation must advance the revision"
    );
    after
}
async fn public_writes(store: &impl Store, clock: &ManualClock) {
    let admitted = store.admit_run(request(clock.now())).await.unwrap();
    let isolated = store.admit_run(request(clock.now())).await.unwrap();
    let isolated_revision = store.session_revision(&isolated.session.id).await.unwrap();
    let mut revision = advanced(store, &admitted, SessionRevision::INITIAL).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let message = message(&admitted.session.id, Some(&admitted.run), clock.now());
    execution.append_message(message.clone()).await.unwrap();
    revision = advanced(store, &admitted, revision).await;
    let part = Part {
        id: PartId::new(),
        message_id: message.id,
        ordinal: 1,
        kind: PartKind::UserInputText,
        content: ContentBlock::Text {
            text: "appended".into(),
        },
    };
    execution.append_part(part.clone()).await.unwrap();
    revision = advanced(store, &admitted, revision).await;
    assert!(execution.append_part(part).await.is_err());
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
    execution
        .append_event(event(&admitted.run, EventKind::RunStarted, clock.now()))
        .await
        .unwrap();
    revision = advanced(store, &admitted, revision).await;
    store
        .enqueue_inbox(
            &admitted.session.id,
            crate::InboxKind::Steer,
            self::message(&admitted.session.id, None, clock.now()),
        )
        .await
        .unwrap();
    revision = advanced(store, &admitted, revision).await;
    assert_eq!(
        execution
            .claim_inbox_into_history(crate::InboxKind::Steer)
            .await
            .unwrap()
            .len(),
        1
    );
    revision = advanced(store, &admitted, revision).await;
    assert_eq!(
        execution
            .claim_inbox_into_history(crate::InboxKind::Steer)
            .await
            .unwrap(),
        Vec::<Message>::new()
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
    assert_eq!(
        store.session_revision(&isolated.session.id).await.unwrap(),
        isolated_revision
    );
}
async fn ownership_and_terminal(store: &impl Store, clock: &ManualClock) {
    let admitted = store.admit_run(request(clock.now())).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let revision = store.session_revision(&admitted.session.id).await.unwrap();
    execution
        .renew_lease(clock.now() + time::Duration::seconds(60))
        .await
        .unwrap();
    execution
        .put_extension_state("private", vec![("key".into(), Some("value".into()))])
        .await
        .unwrap();
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
    execution
        .pause_run(
            json!({"private":"checkpoint"}),
            event(&admitted.run, EventKind::RunPaused, clock.now()),
        )
        .await
        .unwrap();
    let revision = advanced(store, &admitted, revision).await;
    let replacement = store
        .claim_expired_run(&admitted.run.id, "replacement")
        .await
        .unwrap();
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
    assert!(
        execution
            .append_event(event(&admitted.run, EventKind::RunStarted, clock.now()))
            .await
            .is_err()
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
    let recovered = store.execution(replacement).await.unwrap();
    recovered
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted.run, EventKind::RunSettled, clock.now()),
        )
        .await
        .unwrap();
    advanced(store, &admitted, revision).await;
}
async fn receipt_replay(store: &impl Store, clock: &ManualClock) {
    let mut request = request(clock.now());
    request.session_id = Some(request.user_message.session_id.clone());
    let keyed = KeyedAdmitRequest {
        request,
        options: AdmissionOptions {
            key: AdmissionKey::new("revision-retry").unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
        },
        execution: None,
    };
    let KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!("fresh admission must start")
    };
    let revision = store.session_revision(&receipt.session_id).await.unwrap();
    let KeyedAdmitOutcome::Replayed(replayed) = store.admit_keyed_run(keyed).await.unwrap() else {
        panic!("exact retry must replay")
    };
    assert_eq!(replayed, receipt);
    assert_eq!(
        store
            .lookup_admission(
                &receipt.session_id,
                &AdmissionKey::new("revision-retry").unwrap()
            )
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        store.session_revision(&receipt.session_id).await.unwrap(),
        revision
    );
}

async fn tool_records(store: &impl Store, clock: &ManualClock) {
    use crabber_core::{ToolCallId, ToolCallRecord, ToolCallStatus, ToolResult, ToolResultStatus};
    let admitted = store.admit_run(request(clock.now())).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let mut revision = store.session_revision(&admitted.session.id).await.unwrap();
    let id = ToolCallId::new();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: id.clone(),
                run_id: admitted.run.id.clone(),
                name: "fixture".into(),
                arguments: json!({}),
                status: ToolCallStatus::Pending,
                retry_safe: false,
                result: None,
            },
            event(&admitted.run, EventKind::ToolCallPending, clock.now()),
        )
        .await
        .unwrap();
    revision = advanced(store, &admitted, revision).await;
    execution
        .claim_tool_call(
            &id,
            event(&admitted.run, EventKind::ToolCallRunning, clock.now()),
        )
        .await
        .unwrap();
    revision = advanced(store, &admitted, revision).await;
    let content = vec![ContentBlock::Text {
        text: "result".into(),
    }];
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: content.clone(),
    };
    let mut result_message = message(&admitted.session.id, Some(&admitted.run), clock.now());
    result_message.role = Role::Tool;
    result_message.parts[0].kind = PartKind::FunctionToolResult;
    result_message.parts[0].content = ContentBlock::ToolResult {
        call_id: id.clone(),
        content,
        is_error: false,
    };
    execution
        .settle_tool_call(
            &id,
            result,
            result_message,
            event(&admitted.run, EventKind::ToolCallSettled, clock.now()),
        )
        .await
        .unwrap();
    advanced(store, &admitted, revision).await;
}

async fn context_epochs_and_abandonment(store: &impl Store, clock: &ManualClock) {
    use crabber_core::{AbandonAuthority, AbandonRequest, ContextEpoch, EpochId};
    let admitted = store.admit_run(request(clock.now())).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let mut revision = store.session_revision(&admitted.session.id).await.unwrap();
    let id = EpochId::new();
    execution
        .start_epoch(ContextEpoch {
            id: id.clone(),
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            parent: Some(admitted.epoch.clone()),
            summarized_range: None,
            summary_message_id: None,
            tail_start_message_id: None,
            provider_id: "fixture".into(),
            model_id: "model".into(),
            reason: "revision".into(),
            next_policy: None,
        })
        .await
        .unwrap();
    revision = advanced(store, &admitted, revision).await;
    let mut summary = message(&admitted.session.id, Some(&admitted.run), clock.now());
    summary.role = Role::Assistant;
    summary.parts[0].kind = PartKind::CompactionSummary;
    execution.finish_epoch(&id, summary).await.unwrap();
    revision = advanced(store, &admitted, revision).await;
    // This fixture never starts a worker: the host can truthfully assert it is stopped.
    let request = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner.clone(),
        authority: AbandonAuthority::HostStoppedOwner,
    };
    let settled = store.abandon_run(request.clone()).await.unwrap();
    let after = store.session_revision(&admitted.session.id).await.unwrap();
    assert!(after > revision);
    assert_eq!(store.abandon_run(request).await.unwrap(), settled);
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        after
    );
}
