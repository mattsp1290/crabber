use super::{
    Error, OperationReceipt, State, ToolContinuationOutcome, ToolContinuationRecord, domain,
    event_at,
};
use crate::public_session::v2::proof::{Proof, ToolReadError};

pub(super) fn verify_with_proof(
    state: &State,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    proof
        .measure(&(
            &record.session_id,
            &record.run_id,
            &record.batch.continuation,
        ))
        .map_err(ToolReadError::Budget)?;
    proof
        .charge(
            record
                .questions
                .len()
                .checked_mul(std::mem::size_of::<crate::continuation_input::Input<'_>>())
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(ToolReadError::Budget)?;
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: record.session_id.clone(),
        run: record.run_id.clone(),
        continuation: record.batch.continuation.clone(),
    };
    let inputs: Vec<_> = record
        .questions
        .iter()
        .filter_map(|q| q.answer.as_ref())
        .map(crate::continuation_input::Input::ToolAnswer)
        .collect();
    super::super::continuation_input::answers(state, &scope, &inputs, proof)
        .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?;
    let mut audits = domain::CurrentAudits::default();
    let run = state
        .runs
        .get(&record.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let cross = super::super::continuation_handoff::verify_with_proof(
        state,
        run,
        crate::continuation_handoff::Domain::Tool,
        &record.batch.continuation,
        &record.batch.next_attempt,
        &record.batch.expected_plan,
        proof,
    )
    .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?;
    if let Some(handoff) = &cross {
        let (id, digest) = record.latest_claim_source()?;
        let source = super::receipt_with_proof(state, &record.session_id, &id, proof)?
            .ok_or(Error::UnknownStoreOutcome)?;
        if source.semantic_digest != digest
            || handoff.predecessor.operation_id != source.operation_id
            || handoff.predecessor.digest != source.semantic_digest
            || handoff.predecessor.cursor != source.event_cursor
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    let mut releases = u8::from(cross.is_some());
    for event in state
        .events
        .iter()
        .filter(|event| event.session_id == record.session_id && event.run_id == record.run_id)
    {
        if let Some(key) = domain::audit_key(event, record, proof)? {
            proof.measure(event).map_err(ToolReadError::Budget)?;
            let saved = super::receipt_with_proof(state, &record.session_id, &key, proof)?
                .ok_or(Error::UnknownStoreOutcome)?;
            if event.cursor != Some(saved.event_cursor) {
                return Err(Error::UnknownStoreOutcome.into());
            }
            audits.insert(saved, record)?;
        }
        if cross.is_none() && domain::terminal_candidate(event, record) {
            verify_native_with_proof(state, event, record, proof)?;
            releases += 1;
        }
        if cross.is_none() && is_successor(event, record) {
            verify_successor_with_proof(state, event, record, proof)?;
            releases += 1;
        }
        if releases > 1 {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    proof
        .measure(&state.tool_barriers.get(&record.session_id))
        .map_err(ToolReadError::Budget)?;
    if (state.tool_barriers.get(&record.session_id) == Some(&record.batch.continuation.id))
        != (record.state != crate::ToolContinuationState::Released)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    audits.verify(record, releases == 1)?;
    super::cancel::verify_with_proof(state, record, proof)
}
fn is_successor(event: &crabber_core::EventRecord, record: &ToolContinuationRecord) -> bool {
    event.kind == crabber_core::EventKind::ContinuationChanged
        && event
            .payload
            .get("kind")
            .and_then(serde_json::Value::as_str)
            == Some("tool_continuation")
        && event
            .payload
            .pointer("/outcome/operation")
            .and_then(serde_json::Value::as_str)
            == Some("suspended")
        && event
            .payload
            .pointer("/outcome/continuation/generation")
            .and_then(serde_json::Value::as_u64)
            == record.batch.continuation.generation.get().checked_add(1)
}

fn verify_native_with_proof(
    state: &State,
    event: &crabber_core::EventRecord,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    proof.measure(event).map_err(ToolReadError::Budget)?;
    let key = crabber_core::ControlId::from(
        event
            .correlation
            .as_deref()
            .ok_or(Error::UnknownStoreOutcome)?,
    );
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
    super::super::attempt::verify(state, native).map_err(|_| Error::UnknownStoreOutcome)?;
    let latest = state
        .attempts
        .values()
        .filter(|attempt| attempt.snapshot.run_id == record.run_id)
        .max_by_key(|attempt| attempt.snapshot.target.generation)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(&(
            &latest.snapshot,
            &latest.owner,
            &latest.token,
            &latest.run_token,
            latest.lease_until,
        ))
        .map_err(ToolReadError::Budget)?;
    latest.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    let run = state
        .runs
        .get(&record.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(&(&run.id, &run.session_id, run.status))
        .map_err(ToolReadError::Budget)?;
    let expected = match native.attempt.state {
        crate::AttemptState::Completed => crabber_core::RunStatus::Completed,
        crate::AttemptState::Failed => crabber_core::RunStatus::Failed,
        crate::AttemptState::Interrupted => crabber_core::RunStatus::Interrupted,
        _ => return Err(Error::UnknownStoreOutcome.into()),
    };
    if event.cursor != Some(native.event_cursor)
        || latest.snapshot != native.attempt
        || run.status != expected
        || run.id != record.run_id
        || run.session_id != record.session_id
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}

fn verify_successor_with_proof(
    state: &State,
    event: &crabber_core::EventRecord,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    proof.measure(event).map_err(ToolReadError::Budget)?;
    let target: crate::ContinuationTarget = serde_json::from_value(
        event
            .payload
            .pointer("/outcome/continuation")
            .cloned()
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let successor = state
        .tool_continuations
        .get(&target.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof.measure(event).map_err(ToolReadError::Budget)?;
    proof.measure(successor).map_err(ToolReadError::Budget)?;
    successor.validate()?;
    let saved = super::receipt_with_proof(state, &record.session_id, &successor.batch.id, proof)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if successor.session_id != record.session_id
        || successor.run_id != record.run_id
        || successor.batch.continuation != target
        || successor.batch.attempt != record.batch.next_attempt
        || saved.semantic_digest
            != successor
                .batch
                .fingerprint(&record.session_id, &record.run_id)?
        || saved.outcome
            != (ToolContinuationOutcome::Suspended {
                continuation: target,
            })
        || event.cursor != Some(saved.event_cursor)
        || event_at(state, saved.event_cursor)?.created_at != successor.created_at
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
