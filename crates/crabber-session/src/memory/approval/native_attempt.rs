//! Native Attempt readers retain their enclosing allowance through barriers.
use super::{State, current, origin};
use crate::public_session::v2::proof::Proof;
use crate::{ApprovalContinuationState, AttemptError as Error, AttemptMutation};
use crabber_core::Run;

pub(in crate::memory) fn guard_with_proof(
    state: &State,
    run: &Run,
    request: &AttemptMutation,
    claiming: bool,
    proof: &mut Proof,
) -> Result<(), Error> {
    if !claiming {
        current::session_with_proof(state, &run.session_id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let Some(id) = state.approval_barriers.get(&run.session_id) else {
        return if claiming {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(())
        };
    };
    proof.measure(id).map_err(|_| Error::UnknownStoreOutcome)?;
    let record = state.approvals.get(id).ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    origin::verify_with_proof(state, run, record, proof).map_err(|_| Error::UnknownStoreOutcome)?;
    current::verify_with_proof(state, run, record, false, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if record.run_id != run.id
        || record.batch.next_attempt != *request.command.target()
        || record.state
            != if claiming {
                ApprovalContinuationState::Waiting
            } else {
                ApprovalContinuationState::Claimed
            }
        || claiming && !matches!(request.command, crate::AttemptCommand::Reserve { .. })
    {
        return Err(Error::Busy);
    }
    Ok(())
}
