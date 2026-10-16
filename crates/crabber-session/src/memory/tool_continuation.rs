//! Atomic explicit checkpoint controls under the `MemoryStore` mutation lock.
#[path = "tool_continuation/original_plan.rs"]
mod original_plan;
use super::{MemoryExecution, MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::public_session::v2::proof::{Proof, ToolReadError};
use crate::tool_continuation::{self as domain, ToolContinuationRecord};
use crate::{
    AttemptCleanup, AttemptCommand, AttemptError, AttemptMutation, AttemptMutationOutcome,
    AttemptState, PhaseQuiescence, PhaseState, ToolContinuationClaim, ToolContinuationClaimOutcome,
    ToolContinuationError, ToolContinuationOutcome, ToolContinuationReceipt,
    ToolContinuationSnapshot, ToolContinuationState, ToolContinuationSuspension,
    ToolInteractionAnswer, ToolPhaseResume, ToolSuspensionAuthority,
};
use crabber_core::{
    ControlId, EventCursor, InputFingerprint, Run, RunId, SessionId, SessionRevision,
};
use time::OffsetDateTime;
#[path = "tool_continuation/claim.rs"]
mod claim;
#[path = "tool_continuation/current.rs"]
mod current;
#[path = "tool_continuation/scan.rs"]
pub(super) mod scan;
pub(super) use scan::pending as pending_in_session;
fn active(state: &State, run: &Run) -> Result<Option<ToolContinuationRecord>, Error> {
    scan::run(state, run).map(|(_, active)| active)
}
pub(super) use claim::waiting_context;
#[path = "tool_continuation/cancel.rs"]
mod cancel;
pub(super) use cancel::cancel_waiting;
#[path = "tool_continuation/evidence.rs"]
mod evidence;
#[path = "tool_continuation/phases.rs"]
mod phases;
#[path = "tool_continuation/read.rs"]
mod read;
#[path = "tool_continuation/recover.rs"]
mod recover;
#[path = "tool_continuation/suspend.rs"]
mod suspend;
type Error = ToolContinuationError;
// These two adapters map directly to the cursor-keyed EventLog integration.
fn event_at(state: &State, cursor: EventCursor) -> Result<&crabber_core::EventRecord, Error> {
    state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)
}
fn append_audit(
    state: &mut State,
    mut event: crabber_core::EventRecord,
) -> Result<EventCursor, Error> {
    let cursor = state
        .events
        .next_cursor()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    event.cursor = Some(cursor);
    state
        .events
        .append(event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(cursor)
}
fn revision(state: &State, session: &SessionId) -> SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL)
}
pub(super) fn receipt(
    state: &State,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<ToolContinuationReceipt>, Error> {
    receipt_with_proof(state, session, id, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn receipt_with_proof(
    state: &State,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Option<ToolContinuationReceipt>, ToolReadError> {
    let Some(saved) = state.operation_receipts.get(&(session.clone(), id.clone())) else {
        return Ok(None);
    };
    let OperationReceipt::ToolContinuation(saved) = saved else {
        return Err(Error::SemanticConflict.into());
    };
    if saved.session_id != *session || saved.operation_id != *id {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let event = event_at(state, saved.event_cursor)?;
    proof.measure(saved).map_err(ToolReadError::Budget)?;
    proof.measure(event).map_err(ToolReadError::Budget)?;
    domain::verify_receipt(saved, event)?;
    Ok(Some(saved.clone()))
}
pub(super) fn replay(
    state: &State,
    session: &SessionId,
    run: &RunId,
    id: &ControlId,
    digest: &InputFingerprint,
) -> Result<Option<ToolContinuationReceipt>, Error> {
    let saved = receipt(state, session, id)?;
    if saved
        .as_ref()
        .is_some_and(|r| r.run_id != *run || r.semantic_digest != *digest)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(saved)
}
fn publish(
    state: &mut State,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: ToolContinuationOutcome,
    now: OffsetDateTime,
) -> Result<ToolContinuationReceipt, Error> {
    publish_handoff(state, run, id, digest, outcome, now, None)
}
fn publish_handoff(
    state: &mut State,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: ToolContinuationOutcome,
    now: OffsetDateTime,
    handoff: Option<&crate::continuation_handoff::Handoff>,
) -> Result<ToolContinuationReceipt, Error> {
    bump_public_revision(state, &run.session_id).map_err(|_| Error::UnknownStoreOutcome)?;
    let mut receipt = ToolContinuationReceipt {
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        operation_id: id.clone(),
        semantic_digest: digest,
        revision: revision(state, &run.session_id),
        event_cursor: EventCursor(0),
        outcome,
    };
    let mut event = domain::audit(&receipt, now);
    if let Some(handoff) = handoff {
        event.payload["handoff"] =
            serde_json::to_value(handoff).map_err(|_| Error::InvalidRequest)?;
    }
    receipt.event_cursor = append_audit(state, event)?;
    state.operation_receipts.insert(
        (run.session_id.clone(), id),
        OperationReceipt::ToolContinuation(receipt.clone()),
    );
    Ok(receipt)
}
pub(super) fn origin_record_with_proof(
    state: &State,
    id: &crabber_core::ContinuationId,
    proof: &mut Proof,
) -> Result<ToolContinuationRecord, ToolReadError> {
    let borrowed = state
        .tool_continuations
        .get(id)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof.measure(borrowed).map_err(ToolReadError::Budget)?;
    if crate::snapshot::encoded_bytes(borrowed, 4 * 1024 * 1024).is_none() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let record = borrowed.clone();
    record.validate()?;
    let binding =
        super::execution_binding::read_with_proof(state, &record.session_id, &record.run_id, proof)
            .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?
            .ok_or(Error::UnknownStoreOutcome)?;
    if binding.capsule.request.workspace_id != record.workspace_id {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let run = state
        .runs
        .get(&record.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    for frontend in &record.batch.frontend {
        super::frontend::verify_frozen_with_proof(state, run, frontend, proof)?;
    }

    let original = receipt_with_proof(state, &record.session_id, &record.batch.id, proof)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if original.run_id != record.run_id
        || original.semantic_digest
            != record
                .batch
                .fingerprint(&record.session_id, &record.run_id)?
        || original.outcome
            != (ToolContinuationOutcome::Suspended {
                continuation: record.batch.continuation.clone(),
            })
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let event = event_at(state, original.event_cursor)?;
    if event.created_at != record.created_at
        || record.handoff.as_deref()
            != crate::continuation_handoff::Handoff::from_event(event)
                .map_err(|()| Error::UnknownStoreOutcome)?
                .as_ref()
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            let accepted = receipt_with_proof(state, &record.session_id, &answer.id, proof)?
                .ok_or(Error::UnknownStoreOutcome)?;
            if accepted.run_id != record.run_id
                || accepted.semantic_digest != answer.fingerprint()?
                || accepted.outcome
                    != (ToolContinuationOutcome::Answered {
                        continuation: record.batch.continuation.clone(),
                        interaction_id: question.view.id.clone(),
                        state: question.view.state,
                    })
            {
                return Err(Error::UnknownStoreOutcome.into());
            }
        }
    }
    if let Some(claim) = &record.claim {
        let claimed = receipt_with_proof(state, &record.session_id, &claim.id, proof)?
            .ok_or(Error::UnknownStoreOutcome)?;
        record.verify_claim_receipt(&claimed)?;
    }
    if let Some(recovery) = &record.recovery {
        let receipt = receipt_with_proof(state, &record.session_id, &recovery.id, proof)?
            .ok_or(Error::UnknownStoreOutcome)?;
        recovery
            .verify_receipt(&receipt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(recovery) = &record.frontend_recovery {
        let receipt = receipt_with_proof(state, &record.session_id, &recovery.id, proof)?
            .ok_or(Error::UnknownStoreOutcome)?;
        recovery
            .verify_receipt(&receipt)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    verify_native_origin(state, &record, proof)?;
    Ok(record)
}
fn verify_native_origin(
    state: &State,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    if proof.active() {
        let key = domain::derived_id("tool-continuation-suspend-attempt", &record.batch.id);
        let Some(OperationReceipt::Attempt(native)) = state
            .operation_receipts
            .get(&(record.session_id.clone(), key))
        else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        proof.measure(native).map_err(ToolReadError::Budget)?;
        proof
            .measure(event_at(state, native.event_cursor)?)
            .map_err(ToolReadError::Budget)?;
        super::attempt::verify(state, native).map_err(|_| Error::UnknownStoreOutcome)?;
        let source = state
            .events
            .iter()
            .rev()
            .find(|e| {
                e.session_id == record.session_id
                    && e.cursor.is_some_and(|c| c < native.event_cursor)
            })
            .ok_or(Error::UnknownStoreOutcome)?;
        proof.measure(source).map_err(ToolReadError::Budget)?;
        crate::continuation_handoff::verify_native_origin(&record.batch, native, source)?;
    }
    Ok(())
}
pub(super) fn checked_record(
    state: &State,
    id: &crabber_core::ContinuationId,
) -> Result<ToolContinuationRecord, Error> {
    checked_record_with_proof(state, id, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn checked_record_with_proof(
    state: &State,
    id: &crabber_core::ContinuationId,
    proof: &mut Proof,
) -> Result<ToolContinuationRecord, ToolReadError> {
    let record = origin_record_with_proof(state, id, proof)?;
    current::verify_with_proof(state, &record, proof)?;
    Ok(record)
}
pub(super) fn fault(store: &MemoryStore) -> u8 {
    #[cfg(test)]
    {
        store
            .tool_continuation_fault
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    #[cfg(not(test))]
    {
        let _ = store;
        0
    }
}
pub(super) fn before(fault: u8) -> Result<(), Error> {
    if fault == 1 {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(())
    }
}
pub(super) fn after<T>(result: Result<(T, bool), Error>, fault: u8) -> Result<T, Error> {
    match result {
        Ok((_, true)) if fault == 2 => Err(Error::UnknownStoreOutcome),
        Ok((value, _)) => Ok(value),
        Err(error) => Err(error),
    }
}
pub(super) fn guard_attempt(
    state: &State,
    run: &Run,
    request: &AttemptMutation,
) -> Result<(), AttemptError> {
    let Some(record) = active(state, run).map_err(|_| AttemptError::UnknownStoreOutcome)? else {
        return Ok(());
    };
    if record.run_id != run.id
        || record.state != ToolContinuationState::Claimed
        || record.batch.next_attempt != *request.command.target()
    {
        return Err(AttemptError::Busy);
    }
    Ok(())
}
pub(super) fn release_terminal(state: &mut State, run: &Run) -> Result<(), AttemptError> {
    let Some(mut record) = active(state, run).map_err(|_| AttemptError::UnknownStoreOutcome)?
    else {
        return Ok(());
    };
    let id = record.batch.continuation.id.clone();
    if record.run_id != run.id
        || record.state != ToolContinuationState::Claimed
        || record.questions.iter().any(|q| !q.view.consumed)
    {
        return Err(AttemptError::Busy);
    }
    record.state = ToolContinuationState::Released;
    state.tool_continuations.insert(id, record);
    state.tool_barriers.remove(&run.session_id);
    Ok(())
}

pub(super) use claim::preparation_context;
pub(super) use claim::preparation_frontend_context;
pub(super) use recover::preparation_frontend_context as preparation_frontend_recovery_context;

pub(super) use recover::preparation_context as preparation_recovery_context;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn original_checkpoint_origin_and_answers_are_anchored() {
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::MemoryStore::with_clock(clock.clone());
        for kind in 0..10 {
            let fixture =
                crate::tool_continuation_contract::corruption::setup(&store, &clock, kind).await;
            {
                let mut state = store.state.lock().unwrap();
                let record = state
                    .tool_continuations
                    .get_mut(&fixture.suspension.continuation.id)
                    .unwrap();
                crate::tool_continuation_contract::corruption::replace(record, kind);
                if kind >= 8 {
                    state.tool_barriers.remove(&fixture.admitted.session.id);
                }
                if kind == 9 {
                    state
                        .tool_continuations
                        .remove(&fixture.suspension.continuation.id);
                }
            }
            crate::tool_continuation_contract::corruption::refused(&store, &fixture, kind).await;
        }
    }
    #[tokio::test]
    async fn tool_continuation_faults() {
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::MemoryStore::with_clock(clock.clone());
        crate::tool_continuation_contract::run_fault_contract(&store, &clock, |fault| {
            store
                .tool_continuation_fault
                .store(fault, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
    #[tokio::test]
    async fn tool_continuation_contract() {
        crate::tool_continuation_contract::run_contract(|clock| {
            super::MemoryStore::with_clock(clock)
        })
        .await;
    }
}
