//! Pure validation shared with external Store fixture implementations.
use super::super::{
    JoinedPreparation, PreparationCommitError, PreparationCompletion, PreparedOperation,
};
use super::PreparedDriverCompletion;
use crate::session_owner::SessionOwnerError as Error;

pub(crate) fn validate_original(joined: &JoinedPreparation) -> Result<(), PreparationCommitError> {
    joined.verify_original_intent()?;
    if !matches!(
        joined.observed_operation(),
        PreparedOperation::ApprovalClaim(_)
            | PreparedOperation::ApprovalRecovery(_)
            | PreparedOperation::ToolClaim(_)
            | PreparedOperation::ToolRecovery(_)
            | PreparedOperation::FrontendClaim(_)
            | PreparedOperation::FrontendRecovery(_)
    ) || joined.observed_registration().is_none()
        || joined
            .observed_grant()
            .original_reservation()
            .resource_contract
            .is_none()
    {
        return Err(Error::Unsupported.into());
    }
    Ok(())
}
pub(crate) fn protected_scope(
    joined: &JoinedPreparation,
) -> Result<crate::continuation_input::Scope, Error> {
    let (domain, session, run, continuation) = match joined.observed_operation() {
        PreparedOperation::ApprovalClaim(v) => (
            crate::continuation_input::Domain::Approval,
            &v.session_id,
            &v.run_id,
            &v.continuation,
        ),
        PreparedOperation::ApprovalRecovery(v) => (
            crate::continuation_input::Domain::Approval,
            &v.original.session_id,
            &v.original.run_id,
            &v.original.continuation,
        ),
        PreparedOperation::ToolClaim(v) => (
            crate::continuation_input::Domain::Tool,
            &v.session_id,
            &v.run_id,
            &v.continuation,
        ),
        PreparedOperation::ToolRecovery(v) => (
            crate::continuation_input::Domain::Tool,
            &v.original.session_id,
            &v.original.run_id,
            &v.original.continuation,
        ),
        PreparedOperation::FrontendClaim(v) => (
            crate::continuation_input::Domain::Tool,
            &v.claim.session_id,
            &v.claim.run_id,
            &v.claim.continuation,
        ),
        PreparedOperation::FrontendRecovery(v) => (
            crate::continuation_input::Domain::Tool,
            &v.original.claim.session_id,
            &v.original.claim.run_id,
            &v.original.claim.continuation,
        ),
        _ => return Err(Error::Unsupported),
    };
    Ok(crate::continuation_input::Scope {
        domain,
        session: session.clone(),
        run: run.clone(),
        continuation: continuation.clone(),
    })
}
pub(crate) fn replayed(completion: PreparationCompletion) -> PreparedDriverCompletion {
    PreparedDriverCompletion {
        completion,
        claim: None,
    }
}
pub(crate) fn native(
    completion: PreparationCompletion,
    proof: &super::super::proof::VerifiedPreparation<'_>,
) -> Result<PreparedDriverCompletion, PreparationCommitError> {
    let claim = proof.continuation_claim_locator()?;
    if matches!(
        completion,
        PreparationCompletion::Approval(_) | PreparationCompletion::ToolContinuation(_)
    ) && (completion.is_replayed() || claim.is_none())
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(PreparedDriverCompletion { completion, claim })
}
