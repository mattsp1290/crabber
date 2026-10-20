//! Scope and representation checks enforce the public constructor's size bounds.
use super::{Error, ToolContinuationGrant, ToolContinuationReceipt};
pub(super) fn attempt(receipt: &ToolContinuationReceipt) -> Result<&crate::AttemptSnapshot, Error> {
    use crate::ToolContinuationOutcome as Outcome;
    match &receipt.outcome {
        Outcome::Claimed { attempt, .. }
        | Outcome::FrontendClaimed { attempt, .. }
        | Outcome::ReclaimedUnstarted { attempt, .. }
        | Outcome::FrontendReclaimedUnstarted { attempt, .. } => Ok(attempt),
        _ => Err(Error::InvalidRequest),
    }
}
pub(super) fn shape(
    grant: &ToolContinuationGrant,
    receipt: &ToolContinuationReceipt,
) -> Result<(), Error> {
    let parent = attempt(receipt)?;
    crate::attempt::validate_grant_ack_authority(&grant.authority)
        .map_err(|_| Error::InvalidRequest)?;
    if crate::snapshot::encoded_bytes(receipt, 32768).is_none()
        || crate::snapshot::encoded_bytes(&grant.fence, 2048).is_none()
        || grant.authority.snapshot != *parent
        || grant.authority.snapshot.session_id != receipt.session_id
        || grant.authority.snapshot.run_id != receipt.run_id
        || grant.fence.run_id != receipt.run_id
        || grant.fence.claim_token != grant.authority.run_token
        || parent.state != crate::AttemptState::Unstarted
        || parent.cleanup != crate::AttemptCleanup::Pending
        || grant.calls.is_empty()
        || grant.phases.is_empty() && grant.frontend.is_empty()
    {
        return Err(Error::AuthorityMismatch);
    }
    let public = crate::session_owner::preparation::tool_driver::payload::view(grant)
        .map_err(|_| Error::InvalidRequest)?;
    if crate::snapshot::encoded_bytes(
        &public,
        crate::session_owner::preparation::tool_driver::payload::CAP,
    )
    .is_none()
    {
        return Err(Error::InvalidRequest);
    }
    for (index, call) in grant.calls.iter().enumerate() {
        if call.target.attempt != parent.target
            || grant.calls[..index].iter().any(|other| {
                other.target.call_id == call.target.call_id || other.target.id == call.target.id
            })
        {
            return Err(Error::AuthorityMismatch);
        }
    }
    for phase in &grant.phases {
        member(grant, &phase.target, &phase.authority)?;
    }
    for phase in &grant.frontend {
        if phase.frozen.target != phase.target || phase.result.call_id() != &phase.target.call_id {
            return Err(Error::AuthorityMismatch);
        }
        let call = member(grant, &phase.target, &phase.authority)?;
        if phase.frozen.call_binding != call.call_binding {
            return Err(Error::AuthorityMismatch);
        }
    }
    Ok(())
}
fn member<'a>(
    grant: &'a ToolContinuationGrant,
    target: &crate::PhaseTarget,
    authority: &crate::PhaseAuthority,
) -> Result<&'a crate::GrantedApproval, Error> {
    let call = grant
        .calls
        .iter()
        .find(|call| call.target == *target)
        .ok_or(Error::AuthorityMismatch)?;
    crate::phase::validate_grant_ack_authority(
        authority,
        &grant.authority,
        target,
        &call.call_binding,
    )
    .map_err(|_| Error::AuthorityMismatch)?;
    Ok(call)
}
