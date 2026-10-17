//! Borrowed selected admission before inherited State or selected body clones.
use super::super::{OperationReceipt, State};
use crate::native_enrollment::caller::{measure_authority, measure_record};
use crate::public_session::v2::proof::Proof;
use crate::{AttemptAuthority, AttemptError as Error, AttemptMutation};
use crabber_core::RunFence;

pub(super) fn selected(
    state: &State,
    fence: &RunFence,
    request: &AttemptMutation,
    authority: Option<&AttemptAuthority>,
    conclusion: Option<&crate::AttemptConclusion>,
    claiming: bool,
    proof: &mut Proof,
) -> Result<(), Error> {
    let run = state.runs.get(&fence.run_id).ok_or(Error::NotFound)?;
    proof
        .measure(&(fence, request, conclusion, run))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    measure_authority(authority, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    if let Some(saved) = state
        .operation_receipts
        .get(&(run.session_id.clone(), request.id.clone()))
    {
        if let OperationReceipt::Attempt(receipt) = saved {
            proof
                .measure(receipt)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            if let Some(event) = state.events.get(receipt.event_cursor) {
                proof
                    .measure(event)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
        }
        return Ok(());
    }
    if let Some(record) = state
        .attempts
        .values()
        .filter(|r| r.snapshot.run_id == run.id)
        .max_by_key(|r| r.snapshot.target.generation)
    {
        measure_record(record, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(record) = state.attempts.get(&request.command.target().id) {
        measure_record(record, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(record) = state.admission_executions.get(&run.id) {
        proof
            .measure(record)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    for ((id, _), source) in &state.admission_execution_sources {
        if *id == run.id {
            proof
                .measure(source)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    }
    if crate::native_enrollment::caller::relevant(&request.command) {
        super::super::approval::guard_attempt_with_proof(state, run, request, claiming, proof)?;
        super::super::tool_continuation::guard_attempt_with_proof(state, run, request, proof)?;
        if matches!(request.command, crate::AttemptCommand::ReserveBound { .. }) {
            super::super::execution_binding::read_with_proof(
                state,
                &run.session_id,
                &run.id,
                proof,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        if matches!(request.command, crate::AttemptCommand::Begin { .. }) {
            super::super::native_parent::source_protocol::current(
                state,
                &run.session_id,
                &run.id,
                proof,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        super::super::native_enrollment::original(state, &run.session_id, &run.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    Ok(())
}
