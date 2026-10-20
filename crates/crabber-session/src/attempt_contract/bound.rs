//! Positive, transactional evidence for the first attempt after admission loss.
use super::{applied, reserve};
use crate::{
    AttemptCommand, AttemptError as Error, AttemptMutation, AttemptMutationOutcome as Outcome,
    BoundAdmitRequest, Store,
};
use crabber_core::{Clock, EventKind, EventRecord, ManualClock, SessionId};

pub(super) async fn admit(store: &impl Store, now: time::OffsetDateTime) -> crate::AdmitOutcome {
    let mut fixture =
        crate::typed_admission_contract::fixture(&SessionId::new(), now, "bound-start");
    fixture.keyed.request.session_id = None;
    store
        .admit_bound_run(BoundAdmitRequest {
            request: fixture.keyed.request,
            binding: fixture.binding,
        })
        .await
        .unwrap()
}
pub(super) fn request(revision: crabber_core::SessionRevision) -> AttemptMutation {
    let mut request = reserve(revision);
    request.command = AttemptCommand::ReserveBound {
        target: request.command.target().clone(),
    };
    request
}
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let admitted = admit(store, clock.now()).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let request = request(store.session_revision(&admitted.session.id).await.unwrap());
    let mut competing = request.clone();
    competing.id = crabber_core::ControlId::new();
    if let AttemptCommand::ReserveBound { target } = &mut competing.command {
        target.id = crabber_core::AttemptId::new();
    }
    let (left, right) = futures::join!(
        execution.mutate_attempt(request.clone(), None),
        execution.mutate_attempt(competing.clone(), None)
    );
    let (outcome, winning) = match (left, right) {
        (Ok(outcome), Err(Error::RevisionConflict)) => (outcome, request),
        (Err(Error::RevisionConflict), Ok(outcome)) => (outcome, competing),
        other => panic!("one initial reservation must win: {other:?}"),
    };
    let (receipt, owner) = applied(outcome);
    let begin = AttemptMutation {
        id: crabber_core::ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: receipt.attempt.target.clone(),
        },
    };
    execution.mutate_attempt(begin, owner).await.unwrap();
    assert!(
        matches!(execution.mutate_attempt(winning.clone(), None).await.unwrap(), Outcome::Replayed(saved) if saved == receipt)
    );
    let mut raw = winning;
    raw.command = AttemptCommand::Reserve {
        target: raw.command.target().clone(),
    };
    assert_eq!(
        execution.mutate_attempt(raw, None).await.unwrap_err(),
        Error::SemanticConflict
    );
    for dirty in [Dirty::Unbound, Dirty::Event, Dirty::Message, Dirty::Call] {
        rejects_dirty(store, clock, dirty).await;
    }
}
#[derive(Clone, Copy)]
enum Dirty {
    Unbound,
    Event,
    Message,
    Call,
}
async fn rejects_dirty(store: &impl Store, clock: &ManualClock, dirty: Dirty) {
    let admitted = if matches!(dirty, Dirty::Unbound) {
        crate::public_state_contract::admit(store, clock.now()).await
    } else {
        admit(store, clock.now()).await
    };
    let execution = store.execution(admitted.fence).await.unwrap();
    let event = EventRecord {
        cursor: None,
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        turn_id: None,
        kind: EventKind::Custom {
            name: "business-evidence".into(),
        },
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: clock.now(),
    };
    match dirty {
        Dirty::Event => execution.append_event(event).await.unwrap(),
        Dirty::Message => {
            let mut message = store
                .list_all_messages(&admitted.session.id)
                .await
                .unwrap()
                .remove(0);
            message.id = crabber_core::MessageId::new();
            for part in &mut message.parts {
                part.id = crabber_core::PartId::new();
                part.message_id = message.id.clone();
            }
            execution.append_message(message).await.unwrap();
        }
        Dirty::Call => {
            execution
                .create_tool_call(
                    crabber_core::ToolCallRecord {
                        id: crabber_core::ToolCallId::new(),
                        run_id: admitted.run.id.clone(),
                        name: "effect".into(),
                        arguments: serde_json::json!({}),
                        status: crabber_core::ToolCallStatus::Pending,
                        retry_safe: true,
                        result: None,
                    },
                    event,
                )
                .await
                .unwrap();
        }
        Dirty::Unbound => (),
    }
    let before = store.session_revision(&admitted.session.id).await.unwrap();
    let request = request(before);
    assert_eq!(
        execution
            .mutate_attempt(request.clone(), None)
            .await
            .unwrap_err(),
        Error::InvalidTransition
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        before
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&admitted.session.id, &request.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .latest_execution_attempt(&admitted.run.id)
            .await
            .unwrap(),
        None
    );
}
