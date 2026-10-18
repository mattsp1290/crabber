//! A private historical source fact. It is neither a capability nor whole-Run
//! ownership or resource coverage.
use super::{CallKind, DriverRetirement, Failure, Original, Proof, Selection, unknown};
use crate::native_enrollment::pending_initial::{self as birth, CloseOutcome};
use crate::session_owner::owned_scope::OwnedMainDisposition;
use crabber_core::{Run, RunStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InitialConsumption {
    Unproven,
    AuthenticatedClosedPrefix,
    CompletedSource,
}
/// Native readers first authenticate selected original rows, their independent
/// source/cohort membership, historical preparation and actual native outputs.
/// This reducer then authenticates the complete canonical relation and decides
/// only how much of the fixed executed Initial package is present.
pub(crate) fn classify_closed(
    view: &Selection<'_>,
    birth: birth::Bundle<'_>,
    run: &Run,
    proof: &mut Proof,
) -> Result<InitialConsumption, Failure> {
    proof.measure(run)?;
    let epoch = birth
        .epoch
        .ok_or_else(|| unknown("Initial classification missing epoch"))?;
    let expected = &epoch.correlation;
    birth.verify(&expected.key, proof)?;
    view.verify(expected, proof)?;
    if expected.key.session_id != run.session_id
        || birth
            .assignment
            .is_none_or(|assignment| assignment.run_id != run.id)
    {
        return Err(unknown("Initial classification Run assignment"));
    }
    if view.empty() {
        return Ok(InitialConsumption::Unproven);
    }
    view.verify_birth(birth, proof)?;
    let Some(close) = view.close else {
        return Ok(InitialConsumption::Unproven);
    };
    let closed = InitialConsumption::AuthenticatedClosedPrefix;
    if close.version != 2
        || !matches!(close.retirement, DriverRetirement::Activated { .. })
        || close.disposition != OwnedMainDisposition::Returned
        || !matches!(
            birth.close.map(|close| &close.outcome),
            Some(CloseOutcome::Assigned { .. })
        )
    {
        return Ok(closed);
    }
    let keyed = birth
        .assignment
        .is_some_and(|assignment| assignment.keyed_receipt_digest.is_some());
    if !complete_inventory(view, keyed) {
        return Ok(closed);
    }
    let reserve = view
        .results
        .iter()
        .find_map(|r| match &r.original {
            Original::ReserveInitial(value) => Some(value.as_ref()),
            _ => None,
        })
        .ok_or_else(|| unknown("Initial Reserve inventory"))?;
    let begin = view
        .results
        .iter()
        .find_map(|r| match &r.original {
            Original::BeginInitial(value) => Some(value.as_ref()),
            _ => None,
        })
        .ok_or_else(|| unknown("Initial Begin inventory"))?;
    if !matches!(
        reserve.request.command,
        crate::AttemptCommand::ReserveBound { .. }
    ) || !matches!(begin.request.command, crate::AttemptCommand::Begin { .. })
        || reserve.after.snapshot.target != begin.after.snapshot.target
        || reserve.after.snapshot.target.generation != crabber_core::AttemptGeneration::INITIAL
    {
        return Err(unknown("Initial executed Reserve/Begin originals"));
    }
    let conclusion = view
        .results
        .iter()
        .find_map(|r| match &r.original {
            Original::Conclude(value) => Some(value.as_ref()),
            _ => None,
        })
        .ok_or_else(|| unknown("Initial Conclude inventory"))?;
    if conclusion.after_run != *run {
        return Err(unknown("Initial concluded current Run differs"));
    }
    if run.status != RunStatus::Completed
        || conclusion.request.validate().map_err(unknown)? != crate::AttemptFinish::Completed
        || conclusion.attempt.after.snapshot.state != crate::AttemptState::Completed
        || conclusion.attempt.after.snapshot.cleanup != crate::AttemptCleanup::Complete
    {
        return Ok(closed);
    }
    if conclusion.attempt.after.snapshot.target != begin.after.snapshot.target
        || conclusion.attempt.publication.is_none()
    {
        return Err(unknown("Initial terminal publication target"));
    }
    Ok(InitialConsumption::CompletedSource)
}
fn complete_inventory(view: &Selection<'_>, keyed: bool) -> bool {
    const REQUIRED: [CallKind; 11] = [
        CallKind::Register,
        CallKind::Activate,
        CallKind::ReserveInitial,
        CallKind::BeginInitial,
        CallKind::RunAdmitted,
        CallKind::RunStarted,
        CallKind::TurnStarted,
        CallKind::AssistantMessage,
        CallKind::MessageCommitted,
        CallKind::TurnCompleted,
        CallKind::Conclude,
    ];
    view.results.len() == REQUIRED.len() + usize::from(keyed)
        && REQUIRED.iter().all(|kind| {
            view.results
                .iter()
                .filter(|r| r.original.kind() == *kind)
                .count()
                == 1
        })
        && view
            .results
            .iter()
            .filter(|r| r.original.kind() == CallKind::KeyedBegin)
            .count()
            == usize::from(keyed)
}
