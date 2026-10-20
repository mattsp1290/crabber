//! Safe native continuation presentation, separate from private checkpoints.
#[cfg(test)]
pub(crate) mod tests;
mod waiting;
use crate::attempt::AttemptRecord;
use crate::public_session::v2::proof::Proof;
use crate::{AttemptAuthority, AttemptState, PublicStateError as Error};
use crabber_core::continuation_source::FrozenCall;
use crabber_core::{
    ContentBlock, MessageId, Part, PartKind, Run, RunFence, SessionId, ToolCallRecord,
};

pub(crate) fn verify_owner(
    run: &Run,
    fence: &RunFence,
    attempt: &AttemptRecord,
    authority: &AttemptAuthority,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    attempt.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if run.id != fence.run_id
        || run.claim_token != fence.claim_token
        || run.status != crabber_core::RunStatus::Running
        || run.lease_until <= now
        || attempt.snapshot.session_id != run.session_id
        || attempt.snapshot.run_id != run.id
        || attempt.snapshot.state != AttemptState::Started
        || attempt.lease_until <= now
        || attempt.run_token != run.claim_token
        || !authority.matches_store(&attempt.snapshot, &attempt.token, &attempt.run_token)
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
pub(crate) fn frozen_call(
    session: &SessionId,
    run: &crabber_core::RunId,
    call: &ToolCallRecord,
    message_id: &MessageId,
    part: &Part,
    proof: &mut Proof,
) -> Result<FrozenCall, Error> {
    let ContentBlock::ToolCall {
        call_id,
        name,
        arguments,
    } = &part.content
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    if call.run_id != *run
        || call.result.is_some()
        || part.message_id != *message_id
        || part.kind != PartKind::FunctionToolCall
        || call_id != &call.id
        || name != &call.name
    {
        return Err(Error::UnknownStoreOutcome);
    }
    // Presentation retains the actual assistant's original provider arguments.
    // The call record is the separate authoritative post-prepare business input;
    // normalization and private failure/unknown sentinels can legitimately differ.
    // Neither presentation arguments nor this snapshot grant execution authority.
    // Charge the new safe carrier before copying the selected actual fields.
    proof
        .measure(&(session, run, &call.id, message_id, name, arguments))
        .map_err(|_| Error::InvalidRequest)?;
    Ok(FrozenCall {
        call_id: call.id.clone(),
        message_id: message_id.clone(),
        name: name.clone(),
        arguments: arguments.clone(),
    })
}
