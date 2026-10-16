//! Cross-domain barriers transfer only with an explicit immutable suspension attestation.
use super::{OperationReceipt, State};
use crate::ApprovalError as Error;
use crate::continuation_handoff::{Domain, Handoff, Source};
use crabber_core::{EventRecord, Run};
fn unknown<T>(_: T) -> Error {
    Error::UnknownStoreOutcome
}
pub(super) fn prepare(
    state: &State,
    run: &Run,
    domain: Domain,
    target: &crate::ContinuationTarget,
    operation: &crabber_core::ControlId,
    digest: &crabber_core::InputFingerprint,
    parent: &crate::AttemptTarget,
) -> Result<Option<Handoff>, Error> {
    let source = match domain {
        Domain::Approval => {
            let Some(id) = state.tool_barriers.get(&run.session_id) else {
                return Ok(None);
            };
            let record = super::tool_continuation::checked_record(state, id).map_err(unknown)?;
            if record.run_id != run.id
                || record.state != crate::ToolContinuationState::Claimed
                || record.batch.next_attempt != *parent
                || record.batch.expected_plan.as_str() != run.plan_fingerprint
                || record.questions.iter().any(|q| !q.view.consumed)
            {
                return Err(Error::InvalidTransition);
            }
            let id = record
                .recovery
                .as_ref()
                .map(|r| &r.id)
                .or(record.claim.as_ref().map(|c| &c.id))
                .ok_or(Error::UnknownStoreOutcome)?;
            let receipt = super::tool_continuation::receipt(state, &run.session_id, id)
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?;
            Source {
                domain: Domain::Tool,
                continuation: record.batch.continuation,
                operation_id: receipt.operation_id,
                digest: receipt.semantic_digest,
                cursor: receipt.event_cursor,
            }
        }
        Domain::Tool => {
            let Some(id) = state.approval_barriers.get(&run.session_id) else {
                return Ok(None);
            };
            let record = state.approvals.get(id).ok_or(Error::UnknownStoreOutcome)?;
            super::approval::verify_record(state, run, record)?;
            if record.batch.expected_plan.as_str() != run.plan_fingerprint
                || record.state != crate::ApprovalContinuationState::Claimed
                || record.batch.next_attempt != *parent
                || record.questions.iter().any(|q| !q.view.consumed)
            {
                return Err(Error::InvalidTransition);
            }
            let event = super::approval::current::selected(state, record, false)?
                .into_iter()
                .next()
                .ok_or(Error::UnknownStoreOutcome)?;
            let receipt = super::approval::current::source(state, event)?.receipt;
            Source {
                domain: Domain::Approval,
                continuation: record.batch.continuation.clone(),
                operation_id: receipt.operation_id,
                digest: receipt.semantic_digest,
                cursor: receipt.event_cursor,
            }
        }
    };
    Ok(Some(Handoff {
        predecessor: source,
        successor_domain: domain,
        successor: target.clone(),
        suspension_id: operation.clone(),
        suspension_digest: digest.clone(),
        plan: crabber_core::InputFingerprint::new(run.plan_fingerprint.clone()).map_err(unknown)?,
        parent: parent.clone(),
    }))
}
pub(super) fn release(state: &mut State, run: &Run, handoff: &Handoff) -> Result<(), Error> {
    match handoff.predecessor.domain {
        Domain::Approval => {
            state
                .approvals
                .get_mut(&handoff.predecessor.continuation.id)
                .ok_or(Error::UnknownStoreOutcome)?
                .state = crate::ApprovalContinuationState::Released;
            state.approval_barriers.remove(&run.session_id);
        }
        Domain::Tool => {
            state
                .tool_continuations
                .get_mut(&handoff.predecessor.continuation.id)
                .ok_or(Error::UnknownStoreOutcome)?
                .state = crate::ToolContinuationState::Released;
            state.tool_barriers.remove(&run.session_id);
        }
    }
    Ok(())
}
pub(super) fn candidate(
    event: &EventRecord,
    domain: Domain,
    id: &crabber_core::ContinuationId,
) -> bool {
    event.kind == crabber_core::EventKind::ContinuationChanged
        && event
            .payload
            .pointer("/handoff/predecessor/continuation/id")
            .and_then(serde_json::Value::as_str)
            == Some(id.0.as_str())
        && event
            .payload
            .pointer("/handoff/predecessor/domain")
            .and_then(serde_json::Value::as_str)
            == Some(match domain {
                Domain::Approval => "approval",
                Domain::Tool => "tool",
            })
}
pub(super) fn verify(
    state: &State,
    run: &Run,
    domain: Domain,
    target: &crate::ContinuationTarget,
    parent: &crate::AttemptTarget,
    plan: &crabber_core::InputFingerprint,
) -> Result<Option<Handoff>, Error> {
    let mut selected = state.events.iter().filter(|e| {
        e.session_id == run.session_id && e.run_id == run.id && candidate(e, domain, &target.id)
    });
    let Some(event) = selected.next() else {
        return Ok(None);
    };
    if selected.next().is_some() || crate::snapshot::encoded_bytes(event, 65536).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    let proof = Handoff::from_event(event)
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if proof.predecessor.continuation != *target
        || proof.parent != *parent
        || proof.plan != *plan
        || event.cursor.is_none_or(|c| c <= proof.predecessor.cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let (saved, native_key) = successor(state, run, &proof, parent, plan)?;
    let cursor = match saved {
        Some(OperationReceipt::Approval(r)) => r.event_cursor,
        Some(OperationReceipt::ToolContinuation(r)) => r.event_cursor,
        _ => return Err(Error::UnknownStoreOutcome),
    };
    let Some(OperationReceipt::Attempt(native)) = state
        .operation_receipts
        .get(&(run.session_id.clone(), native_key))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    super::attempt::verify(state, native).map_err(unknown)?;
    if proof.successor_domain == Domain::Tool {
        let next = state
            .tool_continuations
            .get(&proof.successor.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        let source = state
            .events
            .iter()
            .rev()
            .find(|e| {
                e.session_id == run.session_id && e.cursor.is_some_and(|c| c < native.event_cursor)
            })
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::continuation_handoff::verify_native_origin(&next.batch, native, source)
            .map_err(unknown)?;
    }

    if event.cursor != Some(cursor)
        || native.attempt.target != *parent
        || native.attempt.state != crate::AttemptState::Suspended
        || native.attempt.cleanup != crate::AttemptCleanup::Complete
        || native.event_cursor >= cursor
        || native.event_cursor <= proof.predecessor.cursor
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Some(proof))
}

pub(super) fn preflight(
    state: &State,
    run: &Run,
    domain: Domain,
    target: &crate::ContinuationTarget,
    proof: &mut crate::public_session::budget::Budget,
) -> Result<bool, crate::public_session::budget::Failure> {
    use crate::public_session::budget::unknown;
    let mut events = state.events.iter().filter(|e| {
        e.session_id == run.session_id && e.run_id == run.id && candidate(e, domain, &target.id)
    });
    let Some(event) = events.next() else {
        return Ok(false);
    };
    if events.next().is_some() {
        return Err(unknown("duplicate handoff"));
    }
    proof.measure(event)?;
    let h = Handoff::from_event(event)
        .map_err(unknown)?
        .ok_or_else(|| unknown("handoff"))?;
    match h.successor_domain {
        Domain::Approval => {
            let record = state
                .approvals
                .get(&h.successor.id)
                .ok_or_else(|| unknown("successor"))?;
            proof.measure(record)?;
            super::public_header::preflight(state, record, proof)?;
        }
        Domain::Tool => {
            let record = state
                .tool_continuations
                .get(&h.successor.id)
                .ok_or_else(|| unknown("successor"))?;
            proof.measure(record)?;
            let binding = state
                .execution_bindings
                .get(&run.id)
                .ok_or_else(|| unknown("binding"))?;
            proof.charge(binding.len())?;
            for message in state
                .messages
                .iter()
                .filter(|m| m.session_id == run.session_id && m.run_id.as_ref() == Some(&run.id))
            {
                proof.measure(message)?;
            }
            let ids = std::iter::once(&record.batch.id)
                .chain(
                    record
                        .questions
                        .iter()
                        .filter_map(|q| q.answer.as_ref().map(|a| &a.id)),
                )
                .chain(record.claim.iter().map(|c| &c.id))
                .chain(record.recovery.iter().map(|c| &c.id));
            for id in ids {
                crate::attempt::validate_identifier(&id.0).map_err(unknown)?;
                let saved = state
                    .operation_receipts
                    .get(&(run.session_id.clone(), id.clone()))
                    .ok_or_else(|| unknown("tool receipt"))?;
                let OperationReceipt::ToolContinuation(saved) = saved else {
                    return Err(unknown("tool receipt kind"));
                };
                proof.measure(saved)?;
                proof.measure(
                    state
                        .events
                        .get(saved.event_cursor)
                        .ok_or_else(|| unknown("tool event"))?,
                )?;
            }
            let key = crate::tool_continuation::derived_id(
                "tool-continuation-suspend-attempt",
                &record.batch.id,
            );
            let Some(OperationReceipt::Attempt(native)) =
                state.operation_receipts.get(&(run.session_id.clone(), key))
            else {
                return Err(unknown("native source"));
            };
            proof.measure(native)?;
            let source = state
                .events
                .iter()
                .rev()
                .find(|e| {
                    e.session_id == run.session_id
                        && e.cursor.is_some_and(|c| c < native.event_cursor)
                })
                .ok_or_else(|| unknown("native suspension source"))?;
            proof.measure(source)?;

            proof.measure(
                state
                    .events
                    .get(native.event_cursor)
                    .ok_or_else(|| unknown("native event"))?,
            )?;
        }
    }
    Ok(true)
}

fn successor<'a>(
    state: &'a State,
    run: &Run,
    proof: &Handoff,
    parent: &crate::AttemptTarget,
    plan: &crabber_core::InputFingerprint,
) -> Result<(Option<&'a OperationReceipt>, crabber_core::ControlId), Error> {
    Ok(match proof.successor_domain {
        Domain::Approval => {
            let next = state
                .approvals
                .get(&proof.successor.id)
                .ok_or(Error::UnknownStoreOutcome)?;
            super::approval::origin::verify(state, run, next)?;
            if next.handoff.as_ref() != Some(proof)
                || next.batch.continuation != proof.successor
                || next.batch.attempt != *parent
                || next.batch.id != proof.suspension_id
                || next.batch.expected_plan != *plan
                || next.batch.fingerprint(&run.session_id, &run.id)? != proof.suspension_digest
            {
                return Err(Error::UnknownStoreOutcome);
            }
            (
                state
                    .operation_receipts
                    .get(&(run.session_id.clone(), next.batch.id.clone())),
                crate::approval::derived_id("suspend-attempt", &next.batch.id),
            )
        }
        Domain::Tool => {
            let next = super::tool_continuation::origin_record(state, &proof.successor.id)
                .map_err(unknown)?;
            if next.handoff.as_ref() != Some(proof)
                || next.batch.continuation != proof.successor
                || next.batch.attempt != *parent
                || next.batch.id != proof.suspension_id
                || next.batch.expected_plan != *plan
                || next
                    .batch
                    .fingerprint(&run.session_id, &run.id)
                    .map_err(unknown)?
                    != proof.suspension_digest
            {
                return Err(Error::UnknownStoreOutcome);
            }
            (
                state
                    .operation_receipts
                    .get(&(run.session_id.clone(), next.batch.id.clone())),
                next.batch.conclusion(run).map_err(unknown)?.finish.id,
            )
        }
    })
}

#[cfg(test)]
#[path = "continuation_handoff/corruption.rs"]
mod corruption;

#[cfg(test)]
#[path = "continuation_handoff/plan_tests.rs"]
mod plan_tests;
