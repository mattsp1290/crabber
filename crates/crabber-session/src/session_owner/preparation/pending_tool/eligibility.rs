//! Pure eligibility over already authenticated bounded carriers.
use super::{InspectPendingToolPreparation as Request, claim, frontend, recovery};
use crate::session_owner::SessionOwnerError as Error;
use crate::{
    AttemptCleanup, AttemptState, ToolContinuationOutcome as Outcome, ToolContinuationState,
};
use crabber_core::{Run, RunStatus};
pub(crate) fn eligible(
    record: &crate::tool_continuation::ToolContinuationRecord,
    run: &Run,
    request: &Request,
    original: Option<&crate::ToolContinuationReceipt>,
) -> Result<(), Error> {
    let c = claim(request);
    if record.batch.continuation != c.continuation
        || record.batch.expected_plan != c.expected_plan
        || run.plan_fingerprint != c.expected_plan.as_str()
        || run.usage != record.batch.usage
        || run.checkpoint.as_ref() != Some(&crate::tool_continuation::checkpoint(&record.batch))
    {
        return Err(Error::SemanticConflict);
    }
    match frontend(request) {
        Some(f) => f
            .validate_for_batch(&record.batch)
            .map_err(Error::ToolSource)?,
        None if !record.batch.frontend.is_empty() => return Err(Error::InvalidTransition),
        None => (),
    }
    if let Some(target) = recovery(request) {
        if record.state != ToolContinuationState::Claimed
            || run.status != RunStatus::Running
            || record.batch.next_attempt != *target
            || record.questions.iter().any(|q| !q.view.consumed)
            || record.claim.as_ref().is_none_or(|saved| {
                saved.id != c.id || saved.fingerprint().ok() != c.fingerprint().ok()
            })
        {
            return Err(Error::InvalidTransition);
        }
        let original = original.ok_or(Error::NotFound)?;
        let expected = match frontend(request) {
            Some(f) => f.fingerprint(),
            None => c.fingerprint(),
        }
        .map_err(Error::ToolSource)?;
        let exact = match (&original.outcome, frontend(request)) {
            (
                Outcome::Claimed {
                    continuation,
                    attempt,
                },
                None,
            )
            | (
                Outcome::FrontendClaimed {
                    continuation,
                    attempt,
                },
                Some(_),
            ) => continuation == &c.continuation && attempt.target == *target,
            _ => false,
        };
        if original.session_id != run.session_id
            || original.run_id != run.id
            || original.operation_id != c.id
            || original.semantic_digest != expected
            || !exact
        {
            return Err(Error::SemanticConflict);
        }
        if let Some(f) = frontend(request)
            && record.frontend_claim.as_ref().is_none_or(|saved| {
                saved.claim.id != f.claim.id || saved.fingerprint().ok() != f.fingerprint().ok()
            })
        {
            return Err(Error::SemanticConflict);
        }
    } else {
        if record.state != ToolContinuationState::Waiting || run.status != RunStatus::Paused {
            return Err(Error::InvalidTransition);
        }
        if record.questions.iter().any(|q| {
            q.answer.is_none()
                || q.view.state == crabber_core::InteractionState::Pending
                || q.view.consumed
        }) {
            return Err(Error::ToolSource(crate::ToolContinuationError::Pending));
        }
    }
    Ok(())
}
pub(crate) fn parent(
    record: &crate::tool_continuation::ToolContinuationRecord,
    run: &Run,
    request: &Request,
    current: &crate::attempt::AttemptRecord,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    current.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if current.snapshot.session_id != run.session_id || current.snapshot.run_id != run.id {
        return Err(Error::IdentityMismatch);
    }
    if let Some(target) = recovery(request) {
        if current.snapshot.target != *target
            || current.snapshot.state != AttemptState::Unstarted
            || current.snapshot.cleanup != AttemptCleanup::Pending
            || current.run_token != run.claim_token
        {
            return Err(Error::StaleOwner);
        }
        if current.lease_until > now {
            return Err(Error::LiveOwner);
        }
    } else if current.snapshot.target != record.batch.attempt
        || current.snapshot.state != AttemptState::Suspended
        || current.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::Busy);
    }
    Ok(())
}
pub(crate) fn call(
    record: &crate::tool_continuation::ToolContinuationRecord,
    call: &crabber_core::ToolCallRecord,
    recovered: bool,
) -> Result<(), Error> {
    let frozen = record
        .batch
        .calls
        .iter()
        .find(|v| v.target.call_id == call.id)
        .ok_or(Error::SemanticConflict)?;
    let waiting = !recovered
        && (record
            .batch
            .phases
            .iter()
            .any(|v| v.previous.call_id == call.id)
            || record
                .batch
                .frontend
                .iter()
                .any(|v| v.target.call_id == call.id));
    if call.run_id != record.run_id
        || call.result.is_some()
        || call.status
            != if waiting {
                crabber_core::ToolCallStatus::Waiting
            } else {
                crabber_core::ToolCallStatus::Pending
            }
        || crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
            != frozen.call_binding
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
