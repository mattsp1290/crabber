//! Borrowed original preparation/control sources on the caller's allowance.
use crate::deletion::history::{ClaimHistory, NoninitialClaims};
use crate::memory::{OperationReceipt, State};
use crate::native_parent::{CompletionOperation, RunIssuance, completion_history};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use crabber_core::{EventCursor, SessionId, SessionOwnerOperationId};
pub(super) fn read(
    state: &State,
    requested_session: &SessionId,
    requested_run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<(bool, ClaimHistory), Failure> {
    let mut claims = NoninitialClaims::default();
    let mut initial_present = false;
    let mut previous: Option<&crate::native_parent::RunIssuance> = None;
    for ((session, id, sequence), issuance) in &state.native_run_issuances {
        if session != requested_session || id != requested_run {
            continue;
        }
        proof.measure(&(session, id, sequence, issuance))?;
        if issuance.sequence != *sequence
            || issuance.state.session_id != *session
            || issuance.state.run_id != *id
            || crate::snapshot::encoded_bytes(issuance, 2048).is_none()
        {
            return Err(unknown("claim issuer routing/bound"));
        }
        let cursor = source(state, previous, issuance, proof)?;
        claims
            .observe(previous, issuance, cursor)
            .map_err(unknown)?;
        previous = Some(issuance);
        initial_present |= *sequence == 1;
    }
    Ok((initial_present, claims.finish(initial_present)))
}
pub(super) fn source(
    state: &State,
    previous: Option<&RunIssuance>,
    current: &RunIssuance,
    proof: &mut Proof,
) -> Result<Option<EventCursor>, Failure> {
    let Some(source) = &current.completion_source else {
        return Ok(None);
    };
    if matches!(
        source.native_operation,
        CompletionOperation::AdmissionRecovery | CompletionOperation::ExpiredRunResume
    ) {
        return Ok(None);
    }
    let previous = previous.ok_or_else(|| unknown("claim predecessor"))?;
    let session = &current.state.session_id;
    let join = audit(state, session, &source.root_join_id, proof)?;
    let reserve = audit(state, session, &join.record.origin, proof)?;
    let origin = reserve
        .record
        .preparation
        .as_ref()
        .ok_or_else(|| unknown("claim preparation origin"))?;
    let ids =
        crate::session_owner::preparation::identity::ids(&origin.key, session).map_err(unknown)?;
    let begin = audit(state, session, &ids.begin, proof)?;
    let (PreparationKey::Approval(id) | PreparationKey::ToolContinuation(id)) = &origin.key else {
        return Err(unknown("native claim preparation key"));
    };
    let receipt = state
        .operation_receipts
        .get(&(session.clone(), id.clone()))
        .ok_or_else(|| unknown("original native claim receipt"))?;
    proof.measure(receipt)?;
    let cursor = match (&origin.key, receipt) {
        (PreparationKey::Approval(_), OperationReceipt::Approval(receipt)) => receipt.event_cursor,
        (PreparationKey::ToolContinuation(_), OperationReceipt::ToolContinuation(receipt)) => {
            receipt.event_cursor
        }
        _ => return Err(unknown("native claim receipt kind")),
    };
    let event = state
        .events
        .get(cursor)
        .ok_or_else(|| unknown("original native claim event"))?;
    proof.measure(event)?;
    if event.cursor != Some(cursor) {
        return Err(unknown("native claim event cursor"));
    }
    let control = match receipt {
        OperationReceipt::Approval(receipt) => {
            completion_history::Control::Approval(receipt, event)
        }
        OperationReceipt::ToolContinuation(receipt) => {
            completion_history::Control::Tool(receipt, event)
        }
        _ => return Err(unknown("native claim receipt")),
    };
    if !completion_history::verify_control(previous, current, reserve, begin, join, &control)
        .map_err(unknown)?
    {
        return Err(unknown("unverified retained control source"));
    }
    Ok(Some(cursor))
}
fn audit<'a>(
    state: &'a State,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut Proof,
) -> Result<&'a Audit, Failure> {
    crate::attempt::validate_identifier(&id.0).map_err(unknown)?;
    let audit = state
        .session_owners
        .source_audit(session, id)
        .ok_or_else(|| unknown("original claim preparation audit"))?;
    proof.measure(audit)?;
    if crate::snapshot::encoded_bytes(audit, 32768).is_none() {
        return Err(unknown("claim audit bound"));
    }
    audit.validate().map_err(unknown)?;
    if audit.receipt.session_id != *session || audit.receipt.operation_id != *id {
        return Err(unknown("claim audit routing"));
    }
    Ok(audit)
}

#[cfg(test)]
#[path = "claims/tests.rs"]
mod tests;
