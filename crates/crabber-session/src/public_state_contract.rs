//! Public conformance for atomic state/activity replacement and reconciliation.
//! Unstable until durable-host qualification.
use crate::{AdmitOutcome, AdmitRequest, PublicStateError as Error, PublicStateMutation, Store};
use crabber_core::{
    Clock, ContentBlock, ControlId, EventKind, EventRecord, ManualClock, Message, MessageId, Part,
    PartId, PartKind, PublicActivity, PublicStateContents, Role, RunStatus, SessionId, Usage,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

/// Prove bounded typed values survive storage/events, exact replay is read-only,
/// stale owners/revisions cannot mutate, and competing replacements have one winner.
/// # Panics
/// Panics when a Store violates the contract or fixture setup fails.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    round_trip_and_replay(&store, &clock).await;
    competing_replacements(&store, &clock).await;
    protected_source_event(&store, &clock).await;
}
pub(crate) async fn admit(store: &impl Store, now: OffsetDateTime) -> AdmitOutcome {
    let session = SessionId::new();
    let id = MessageId::new();
    store
        .admit_run(AdmitRequest {
            input_envelope: None,
            additional_messages: Vec::new(),
            expected_revision: None,
            session_id: None,
            workspace_id: "workspace".into(),
            directory: "/workspace".into(),
            title: "state fixture".into(),
            user_message: Message {
                id: id.clone(),
                session_id: session,
                run_id: None,
                role: Role::User,
                parent_id: None,
                created_at: now,
                parts: vec![Part {
                    id: PartId::new(),
                    message_id: id,
                    ordinal: 0,
                    kind: PartKind::UserInputText,
                    content: ContentBlock::Text {
                        text: "request".into(),
                    },
                }],
            },
            config_hash: "config".into(),
            plan_fingerprint: "plan".into(),
            owner: "owner".into(),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap()
}
pub(crate) fn contents() -> PublicStateContents {
    PublicStateContents::new(
        json!([null,{"nul\u{0}":"value\u{0}","zero":-0.0},true]),
        vec![PublicActivity::new("progress", "fixture", json!({"text":"activity\u{0}"})).unwrap()],
    )
    .unwrap()
}
async fn round_trip_and_replay(store: &impl Store, clock: &ManualClock) {
    let admitted = admit(store, clock.now()).await;
    let other = admit(store, clock.now()).await;
    let initial = store.public_state(&admitted.session.id).await.unwrap();
    let isolated = store.public_state(&other.session.id).await.unwrap();
    assert_eq!(initial.contents, PublicStateContents::default());
    let history = store.list_all_messages(&admitted.session.id).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let mutation = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: initial.revision,
        contents: contents(),
    };
    let receipt = execution
        .mutate_public_state(mutation.clone())
        .await
        .unwrap();
    assert_eq!(
        receipt.semantic_digest,
        mutation
            .fingerprint(&admitted.session.id, &admitted.run.id)
            .unwrap()
    );
    let after = store.public_state(&admitted.session.id).await.unwrap();
    assert_eq!(after.contents, mutation.contents);
    assert_eq!(after.revision, receipt.revision);
    assert!(after.revision > initial.revision);
    assert_eq!(
        after.contents.state()[1]["zero"]
            .as_f64()
            .unwrap()
            .to_bits(),
        (-0.0f64).to_bits()
    );
    assert_eq!(
        store.list_all_messages(&admitted.session.id).await.unwrap(),
        history,
        "display activity/state must not become conversation text"
    );
    assert_eq!(
        store.public_state(&other.session.id).await.unwrap(),
        isolated
    );
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].cursor, Some(receipt.event_cursor));
    assert_eq!(events[0].kind, EventKind::PublicStateSnapshot);
    let restored: PublicStateContents =
        serde_json::from_value(events[0].payload["contents"].clone()).unwrap();
    assert_eq!(restored, mutation.contents);
    assert_eq!(
        restored.state()[1]["zero"].as_f64().unwrap().to_bits(),
        (-0.0f64).to_bits()
    );
    assert_eq!(
        execution
            .mutate_public_state(mutation.clone())
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .lookup_public_state_mutation(&admitted.session.id, &mutation.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    let mut conflict = mutation.clone();
    conflict.contents = PublicStateContents::default();
    assert_eq!(
        execution.mutate_public_state(conflict).await.unwrap_err(),
        Error::SemanticConflict
    );
    let mut stale = mutation.clone();
    stale.id = ControlId::new();
    assert_eq!(
        execution
            .mutate_public_state(stale.clone())
            .await
            .unwrap_err(),
        Error::RevisionConflict
    );
    replay_after_ownership_change(
        store,
        clock,
        &admitted,
        mutation,
        receipt,
        execution.as_ref(),
    )
    .await;
}
async fn protected_source_event(store: &impl Store, clock: &ManualClock) {
    let admitted = admit(store, clock.now()).await;
    let before = store.public_state(&admitted.session.id).await.unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    let event = EventRecord {
        cursor: None,
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id,
        turn_id: None,
        kind: EventKind::PublicStateSnapshot,
        payload: json!({"version":1,"revision":before.revision,"contents":before.contents}),
        correlation: Some(ControlId::new().0),
        live_only: false,
        created_at: clock.now(),
    };
    assert!(execution.append_event(event).await.is_err());
    assert_eq!(
        store.public_state(&admitted.session.id).await.unwrap(),
        before
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap(),
        [] as [EventRecord; 0]
    );
}
async fn replay_after_ownership_change(
    store: &impl Store,
    clock: &ManualClock,
    admitted: &AdmitOutcome,
    mutation: PublicStateMutation,
    receipt: crate::PublicStateReceipt,
    execution: &dyn crate::ExecutionStore,
) {
    let mut stale = mutation.clone();
    stale.id = ControlId::new();
    clock.set(admitted.run.lease_until);
    let replacement = store
        .claim_expired_run(&admitted.run.id, "replacement")
        .await
        .unwrap();
    stale.expected_revision = receipt.revision;
    assert_eq!(
        execution.mutate_public_state(stale).await.unwrap_err(),
        Error::StaleOwner
    );
    assert_eq!(
        execution
            .mutate_public_state(mutation.clone())
            .await
            .unwrap(),
        receipt
    );
    let replacement = store.execution(replacement).await.unwrap();
    replacement
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
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
    assert_eq!(
        replacement.mutate_public_state(mutation).await.unwrap(),
        receipt
    );
}

async fn competing_replacements(store: &impl Store, clock: &ManualClock) {
    let admitted = admit(store, clock.now()).await;
    let initial = store.public_state(&admitted.session.id).await.unwrap();
    let a = store.execution(admitted.fence.clone()).await.unwrap();
    let b = store.execution(admitted.fence).await.unwrap();
    let left = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: initial.revision,
        contents: contents(),
    };
    let right = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: initial.revision,
        contents: PublicStateContents::new(json!({"winner":"right"}), vec![]).unwrap(),
    };
    let (a, b) = futures::join!(
        a.mutate_public_state(left.clone()),
        b.mutate_public_state(right.clone())
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let winner = match a {
        Ok(_) => {
            assert_eq!(b.unwrap_err(), Error::RevisionConflict);
            left
        }
        Err(error) => {
            assert_eq!(error, Error::RevisionConflict);
            assert!(b.is_ok());
            right
        }
    };
    assert_eq!(
        store
            .public_state(&admitted.session.id)
            .await
            .unwrap()
            .contents,
        winner.contents
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[path = "public_state_contract/faults.rs"]
mod faults;
pub use faults::{FaultPoint, run_fault_contract};

#[cfg(test)]
pub(crate) async fn exhausted(store: &impl Store, admitted: AdmitOutcome) {
    let before = store.public_state(&admitted.session.id).await.unwrap();
    assert_eq!(before.revision.get(), i64::MAX as u64);
    let mutation = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: before.revision,
        contents: contents(),
    };
    assert_eq!(
        store
            .execution(admitted.fence)
            .await
            .unwrap()
            .mutate_public_state(mutation.clone())
            .await
            .unwrap_err(),
        Error::RevisionExhausted
    );
    assert_eq!(
        store.public_state(&admitted.session.id).await.unwrap(),
        before
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap(),
        [] as [EventRecord; 0]
    );
    assert_eq!(
        store
            .lookup_public_state_mutation(&admitted.session.id, &mutation.id)
            .await
            .unwrap(),
        None
    );
}

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "public_state_contract/process.rs"]
pub(crate) mod process;

native_pending_initial_items! {
    #[cfg(test)]
    #[path = "public_state_contract/source.rs"]
    pub(crate) mod source;
}
